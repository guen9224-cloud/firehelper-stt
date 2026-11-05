# firehelper-stt — 플러그인(폰) ↔ Deepgram STT + conversate 소방 카드 + 세션 기록
#   플러그인이 /stt?sid=<세션id> 로 접속해 PCM(16kHz·16bit LE·mono)을 보내면:
#     ① Deepgram 한국어 실시간 전사
#     ② 한 문장 끝날 때마다 conversate(cue) 호출 → 소방 카드를 {card} 로 push
#     ③ 전사·카드를 conversate(glasses_save)로 주기 저장 → 앱 '지난 대화'에 그대로
#     ④ 플러그인이 "END"(두 번 탭 종료) 보내면 finalize=true 로 요약까지 생성
#   전사는 화면에 안 보냄(백그라운드 기록). 플러그인엔 카드만 보냄.
#   키는 전부 Render 환경변수로만. 플러그인엔 아무 키도 안 들어감.
#     DEEPGRAM_API_KEY / SUPABASE_URL / SUPABASE_ANON_KEY / GLASSES_TOKEN / OWNER_UID
import os, json, asyncio, time, uuid
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import websockets
import httpx

app = FastAPI()

DG_KEY = os.environ.get("DEEPGRAM_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
GLASSES_TOKEN = os.environ.get("GLASSES_TOKEN", "")
OWNER_UID = os.environ.get("OWNER_UID", "")
CARD_FREQ = os.environ.get("CARD_FREQ", "normal")  # many / normal / few
CONV_URL = (SUPABASE_URL + "/functions/v1/conversate") if SUPABASE_URL else ""
CUE_ON = bool(CONV_URL and GLASSES_TOKEN and OWNER_UID)
CONV_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer " + SUPABASE_ANON_KEY,
    "apikey": SUPABASE_ANON_KEY,
    "x-fh-glasses": GLASSES_TOKEN,
}
SAVE_EVERY = 12  # 주기 저장 최소 간격(초)

DG_URL = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-2&language=ko&encoding=linear16&sample_rate=16000&channels=1"
    "&interim_results=true&punctuate=true&smart_format=true&endpointing=300"
)

# 같은 세션(sid)으로 재접속해도 전사·카드가 이어지게 프로세스 메모리에 보관
SESSIONS: dict = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.get("/")
def health():
    return {"ok": True, "has_key": bool(DG_KEY), "cue": CUE_ON}


async def save_session(client, sid, st, finalize):
    if not CUE_ON or client is None or not st["lines"]:
        return
    payload = {
        "action": "glasses_save",
        "uid": OWNER_UID,
        "session_id": sid,
        "transcript": st["lines"],
        "cues": st["cues"],
        "finalize": bool(finalize),
    }
    try:
        await client.post(CONV_URL, headers=CONV_HEADERS, json=payload,
                           timeout=(130.0 if finalize else 20.0))
    except Exception:
        pass


@app.websocket("/stt")
async def stt(ws: WebSocket):
    await ws.accept()
    if not DG_KEY:
        await ws.send_json({"error": "no DEEPGRAM_API_KEY"})
        await ws.close()
        return

    sid = (ws.query_params.get("sid") or uuid.uuid4().hex).strip()[:40]
    st = SESSIONS.setdefault(sid, {"lines": [], "cues": []})
    utter = []
    end_requested = {"v": False}
    last_save = {"t": 0.0}
    client = httpx.AsyncClient(timeout=20.0) if CUE_ON else None

    async def fetch_cue(latest: str, at_iso: str):
        if not CUE_ON or client is None:
            return
        try:
            r = await client.post(CONV_URL, headers=CONV_HEADERS, json={
                "action": "cue",
                "uid": OWNER_UID,
                "latest": latest,
                "transcript": [l["t"] for l in st["lines"][-12:]],
                "recent": [c["title"] for c in st["cues"][-10:]],
                "cardFreq": CARD_FREQ,
            })
            d = r.json()
            cues = d.get("cues")
            if not cues:
                cues = [d["cue"]] if d.get("cue") else []
            for c in cues:
                if not c:
                    continue
                db_card = {
                    "type": c.get("type"), "title": c.get("title", ""),
                    "body": c.get("body", ""), "src": c.get("src", ""),
                    "at": at_iso, "anchor": at_iso,
                }
                if c.get("type") == "todo_add":
                    db_card["todo_title"] = c.get("todo_title", "")
                    db_card["todo_due"] = c.get("todo_due", "")
                st["cues"].append(db_card)
                await ws.send_json({"card": c})  # 전체 카드(ask 포함)를 플러그인에
        except Exception as e:
            try:
                await ws.send_json({"cue_err": str(e)[:120]})
            except Exception:
                pass

    try:
        async with websockets.connect(DG_URL, extra_headers={"Authorization": f"Token {DG_KEY}"}) as dg:
            async def pump_client_to_dg():
                try:
                    while True:
                        m = await ws.receive()
                        if m.get("type") == "websocket.disconnect":
                            break
                        b = m.get("bytes")
                        if b is not None:
                            await dg.send(b)
                            continue
                        t = m.get("text")
                        if t and t.strip().upper() == "END":
                            end_requested["v"] = True
                except WebSocketDisconnect:
                    pass
                finally:
                    try:
                        await dg.send(json.dumps({"type": "CloseStream"}))
                    except Exception:
                        pass

            async def pump_dg_to_client():
                async for msg in dg:
                    try:
                        d = json.loads(msg)
                        alts = (d.get("channel") or {}).get("alternatives") or [{}]
                        text = (alts[0] or {}).get("transcript", "")
                        if not text:
                            continue
                        is_final = bool(d.get("is_final"))
                        speech_final = bool(d.get("speech_final"))
                        if is_final:
                            utter.append(text)
                        if speech_final:
                            latest = " ".join(utter).strip()
                            utter.clear()
                            if not latest:
                                continue
                            at_iso = now_iso()
                            st["lines"].append({"t": latest, "at": at_iso})
                            try:
                                await ws.send_json({"hb": True})  # 마이크 살아있음 신호(선택)
                            except Exception:
                                pass
                            core = "".join(ch for ch in latest if ch.isalnum())
                            if CUE_ON and len(core) >= 4:
                                asyncio.create_task(fetch_cue(latest, at_iso))
                            if client is not None and (time.time() - last_save["t"]) >= SAVE_EVERY:
                                last_save["t"] = time.time()
                                asyncio.create_task(save_session(client, sid, st, False))
                    except Exception:
                        pass

            await asyncio.gather(pump_client_to_dg(), pump_dg_to_client())
    except Exception as e:
        try:
            await ws.send_json({"error": str(e)[:200]})
        except Exception:
            pass
    finally:
        # 마지막 카드 요청이 끝나도록 잠깐 여유
        if end_requested["v"]:
            await asyncio.sleep(1.2)
        if client is not None:
            try:
                await save_session(client, sid, st, end_requested["v"])
            except Exception:
                pass
            try:
                await client.aclose()
            except Exception:
                pass
        if end_requested["v"]:
            SESSIONS.pop(sid, None)
        try:
            await ws.close()
        except Exception:
            pass

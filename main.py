# firehelper-stt — 플러그인(폰) ↔ Deepgram STT + conversate 소방 카드 + 세션 기록 + 앱 실시간 뷰어
#   안경 플러그인: /stt?sid=<세션id> 로 PCM(16kHz·16bit LE·mono) 전송 (producer)
#   파이어헬퍼 앱:  /view?sid=<세션id> 로 접속해 전사(단어 단위)·카드를 실시간 수신 (viewer)
#   흐름:
#     ① Deepgram 한국어 실시간 전사 → 모든 viewer 에게 {text, final} 브로드캐스트(단어 단위)
#     ② 한 문장 끝날 때 conversate(cue) → 소방 카드 → 안경(producer)과 앱(viewer) 둘 다에 {card}
#     ③ 전사·카드를 conversate(glasses_save)로 주기 저장 → 앱 '지난 대화'
#     ④ 플러그인이 "END"(두 번 탭) 보내면 종료(요약은 앱에서 수동)
#   키는 전부 Render 환경변수로만. 플러그인/앱엔 아무 키도 안 들어감.
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
CARD_FREQ = os.environ.get("CARD_FREQ", "normal")
CONV_URL = (SUPABASE_URL + "/functions/v1/conversate") if SUPABASE_URL else ""
CUE_ON = bool(CONV_URL and GLASSES_TOKEN and OWNER_UID)
CONV_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer " + SUPABASE_ANON_KEY,
    "apikey": SUPABASE_ANON_KEY,
    "x-fh-glasses": GLASSES_TOKEN,
}
SAVE_EVERY = 12  # DB 주기 저장 최소 간격(초)

DG_URL = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-2&language=ko&encoding=linear16&sample_rate=16000&channels=1"
    "&interim_results=true&punctuate=true&smart_format=true&endpointing=300"
)

SESSIONS: dict = {}   # sid -> {"lines":[{t,at}], "cues":[...]}
VIEWERS: dict = {}    # sid -> set[WebSocket]  (앱 실시간 뷰어들)
PRODUCERS: dict = {}  # sid -> WebSocket (안경 producer; AI 답변을 안경으로 푸시)


def norm_sid(s) -> str:
    return (s or "").replace("-", "").lower().strip()[:40]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def broadcast(sid: str, obj: dict):
    dead = []
    for v in list(VIEWERS.get(sid, set())):
        try:
            await v.send_json(obj)
        except Exception:
            dead.append(v)
    if dead:
        s = VIEWERS.get(sid)
        if s:
            for v in dead:
                s.discard(v)


@app.get("/")
def health():
    return {"ok": True, "has_key": bool(DG_KEY), "cue": CUE_ON,
            "sessions": len(SESSIONS), "viewers": sum(len(v) for v in VIEWERS.values())}


async def save_session(client, sid, st, finalize, force=False):
    if not CUE_ON or client is None:
        return
    if not st["lines"] and not force:
        return
    payload = {
        "action": "glasses_save", "uid": OWNER_UID, "session_id": sid,
        "transcript": st["lines"], "cues": st["cues"], "finalize": bool(finalize),
    }
    try:
        await client.post(CONV_URL, headers=CONV_HEADERS, json=payload,
                          timeout=(60.0 if finalize else 20.0))
    except Exception:
        pass


# ───────── 앱 실시간 뷰어 ─────────
@app.websocket("/view")
async def view(ws: WebSocket):
    await ws.accept()
    sid = norm_sid(ws.query_params.get("sid"))
    if not sid:
        try:
            await ws.send_json({"error": "no sid"})
        except Exception:
            pass
        await ws.close()
        return
    VIEWERS.setdefault(sid, set()).add(ws)
    # 지금까지 쌓인 전사·카드 스냅샷 먼저(앱이 바로 이어 보게)
    st = SESSIONS.get(sid)
    try:
        await ws.send_json({
            "snapshot": {
                "lines": [l["t"] for l in (st["lines"] if st else [])],
                "cards": (st["cues"] if st else []),
                "live": st is not None,
            }
        })
    except Exception:
        pass
    try:
        while True:
            m = await ws.receive()
            if m.get("type") == "websocket.disconnect":
                break
            # 앱이 AI 답변을 보내면 → 안경(producer)에 카드로 푸시 + 세션에 저장
            t = m.get("text")
            if not t:
                continue
            try:
                cmd = json.loads(t)
            except Exception:
                cmd = None
            if cmd and cmd.get("type") == "ai_answer":
                answer = str(cmd.get("answer", "")).strip()
                title = (str(cmd.get("title", "")).strip() or "AI 답변")[:40]
                if not answer:
                    continue
                at_iso = now_iso()
                stx = SESSIONS.get(sid)
                if stx is not None:
                    stx["cues"].append({"type": "answer", "title": title, "body": answer, "src": "", "at": at_iso, "anchor": at_iso})
                prod = PRODUCERS.get(sid)
                if prod is not None:
                    try:
                        await prod.send_json({"card": {"type": "answer", "title": title, "body": answer[:300], "src": ""}})
                    except Exception:
                        pass
    except Exception:
        pass
    finally:
        s = VIEWERS.get(sid)
        if s:
            s.discard(ws)
            if not s:
                VIEWERS.pop(sid, None)
        try:
            await ws.close()
        except Exception:
            pass


# ───────── 안경 플러그인(오디오 producer) ─────────
@app.websocket("/stt")
async def stt(ws: WebSocket):
    await ws.accept()
    if not DG_KEY:
        await ws.send_json({"error": "no DEEPGRAM_API_KEY"})
        await ws.close()
        return

    sid = norm_sid(ws.query_params.get("sid")) or uuid.uuid4().hex
    st = SESSIONS.setdefault(sid, {"lines": [], "cues": []})
    PRODUCERS[sid] = ws   # AI 답변을 이 안경으로 푸시
    utter = []
    end_requested = {"v": False}
    last_save = {"t": 0.0}
    client = httpx.AsyncClient(timeout=20.0) if CUE_ON else None

    # 접속 즉시 세션 행 생성(앱이 sid 를 바로 찾을 수 있게)
    if client is not None:
        asyncio.create_task(save_session(client, sid, st, False, force=True))

    async def fetch_cue(latest: str, at_iso: str):
        if not CUE_ON or client is None:
            return
        try:
            r = await client.post(CONV_URL, headers=CONV_HEADERS, json={
                "action": "cue", "uid": OWNER_UID, "latest": latest,
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
                try:
                    await ws.send_json({"card": c})   # 안경(producer)
                except Exception:
                    pass
                await broadcast(sid, {"card": c})      # 앱(viewer)
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
                        if t:
                            tt = t.strip().upper()
                            if tt == "END":
                                end_requested["v"] = True
                            elif tt == "ASK":
                                # 안경 왼쪽 탭 → 방금 말한 문장을 앱에 AI 질문 요청으로 전달
                                q = st["lines"][-1]["t"] if st["lines"] else ""
                                await broadcast(sid, {"ask_request": q})
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
                        # 앱에 단어 단위 실시간 전사(중간결과 포함)
                        await broadcast(sid, {"text": text, "final": is_final})
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
                                await ws.send_json({"line": latest})   # 안경: 전사 한 줄(전사+카드 보기용)
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
        if end_requested["v"]:
            await asyncio.sleep(1.2)
        if client is not None:
            try:
                await save_session(client, sid, st, end_requested["v"])   # 먼저 저장
            except Exception:
                pass
        if end_requested["v"]:
            await broadcast(sid, {"ended": True})   # 저장 뒤 앱에 '저장됨' 신호
        if client is not None:
            try:
                await client.aclose()
            except Exception:
                pass
        if PRODUCERS.get(sid) is ws:
            PRODUCERS.pop(sid, None)
        if end_requested["v"]:
            SESSIONS.pop(sid, None)
        try:
            await ws.close()
        except Exception:
            pass

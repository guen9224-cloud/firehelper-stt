# firehelper-stt — 플러그인(폰) ↔ Deepgram 스트리밍 STT 프록시 + 소방 카드(conversate)
# 플러그인이 /stt 로 접속해 PCM(16kHz·16bit LE·mono) 바이트를 보내면,
#  ① Deepgram 한국어 실시간 전사 결과를 {text, final} JSON 으로 돌려주고
#  ② 한 문장(speech_final)이 끝날 때마다 conversate(cue)를 호출해 소방 카드를 {card} 로 밀어준다.
# 키는 전부 Render 환경변수로만 주입. 플러그인에는 아무 키도 안 들어감.
#   DEEPGRAM_API_KEY  : Deepgram 스트리밍 키
#   SUPABASE_URL      : https://<ref>.supabase.co
#   SUPABASE_ANON_KEY : conversate 게이트웨이 통과용 apikey (anon)
#   GLASSES_TOKEN     : conversate 안경 토큰(x-fh-glasses) — Supabase 비밀값과 동일해야 함
#   OWNER_UID         : 카드를 만들 사용자(사내자료·할 일 주인)의 user id
import os, json, asyncio
from collections import deque
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

DG_URL = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-2&language=ko&encoding=linear16&sample_rate=16000&channels=1"
    "&interim_results=true&punctuate=true&smart_format=true&endpointing=300"
)


@app.get("/")
def health():
    return {"ok": True, "has_key": bool(DG_KEY), "cue": CUE_ON}


@app.websocket("/stt")
async def stt(ws: WebSocket):
    await ws.accept()
    if not DG_KEY:
        await ws.send_json({"error": "no DEEPGRAM_API_KEY"})
        await ws.close()
        return

    history = deque(maxlen=12)      # 최근 '문장'(speech_final) 전사 — cue 문맥
    recent_cards = deque(maxlen=10)  # 최근 띄운 카드 제목 — cue 중복 방지
    utter = []                       # 현재 문장을 이루는 final 조각들
    client = httpx.AsyncClient(timeout=20.0) if CUE_ON else None

    async def fetch_cue(latest: str):
        if not CUE_ON or client is None:
            return
        payload = {
            "action": "cue",
            "uid": OWNER_UID,
            "latest": latest,
            "transcript": list(history),
            "recent": list(recent_cards),
            "cardFreq": CARD_FREQ,
        }
        try:
            r = await client.post(
                CONV_URL,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + SUPABASE_ANON_KEY,
                    "apikey": SUPABASE_ANON_KEY,
                    "x-fh-glasses": GLASSES_TOKEN,
                },
                json=payload,
            )
            d = r.json()
            cues = d.get("cues")
            if not cues:
                cues = [d["cue"]] if d.get("cue") else []
            for c in cues:
                if not c:
                    continue
                title = c.get("title", "")
                if title:
                    recent_cards.append(title)
                await ws.send_json({"card": c})
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
                        data = await ws.receive_bytes()
                        await dg.send(data)
                except WebSocketDisconnect:
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
                        # 전사는 조각마다 바로 화면에
                        await ws.send_json({"text": text, "final": is_final})
                        if is_final:
                            utter.append(text)
                        # 문장이 끝나면(말이 멈추면) 그 문장 전체로 카드 요청
                        if speech_final:
                            latest = " ".join(utter).strip()
                            utter.clear()
                            if latest:
                                history.append(latest)
                                core = "".join(ch for ch in latest if ch.isalnum())
                                if CUE_ON and len(core) >= 4:
                                    asyncio.create_task(fetch_cue(latest))
                    except Exception:
                        pass

            await asyncio.gather(pump_client_to_dg(), pump_dg_to_client())
    except Exception as e:
        try:
            await ws.send_json({"error": str(e)[:200]})
        except Exception:
            pass
    finally:
        if client is not None:
            try:
                await client.aclose()
            except Exception:
                pass
        try:
            await ws.close()
        except Exception:
            pass

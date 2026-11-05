# firehelper-stt — 플러그인(폰) ↔ Deepgram 스트리밍 STT 프록시 (FastAPI WebSocket)
# 플러그인이 /stt 로 접속해 PCM(16kHz·16bit LE·mono) 바이트를 보내면,
# Deepgram 한국어 실시간 전사 결과를 {text, final} JSON 으로 돌려준다.
# DEEPGRAM_API_KEY 는 Render 환경변수로만 주입. 키는 플러그인에 절대 안 들어감.
import os, json, asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import websockets

app = FastAPI()
DG_KEY = os.environ.get("DEEPGRAM_API_KEY", "")
DG_URL = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-2&language=ko&encoding=linear16&sample_rate=16000&channels=1"
    "&interim_results=true&punctuate=true&smart_format=true&endpointing=300"
)

@app.get("/")
def health():
    return {"ok": True, "has_key": bool(DG_KEY)}

@app.websocket("/stt")
async def stt(ws: WebSocket):
    await ws.accept()
    if not DG_KEY:
        await ws.send_json({"error": "no DEEPGRAM_API_KEY"})
        await ws.close(); return
    try:
        async with websockets.connect(DG_URL, extra_headers={"Authorization": f"Token {DG_KEY}"}) as dg:
            async def pump_client_to_dg():
                try:
                    while True:
                        data = await ws.receive_bytes()
                        await dg.send(data)
                except WebSocketDisconnect:
                    try: await dg.send(json.dumps({"type": "CloseStream"}))
                    except Exception: pass

            async def pump_dg_to_client():
                async for msg in dg:
                    try:
                        d = json.loads(msg)
                        alts = (d.get("channel") or {}).get("alternatives") or [{}]
                        text = (alts[0] or {}).get("transcript", "")
                        if text:
                            await ws.send_json({"text": text, "final": bool(d.get("is_final"))})
                    except Exception:
                        pass

            await asyncio.gather(pump_client_to_dg(), pump_dg_to_client())
    except Exception as e:
        try: await ws.send_json({"error": str(e)[:200]})
        except Exception: pass
        try: await ws.close()
        except Exception: pass

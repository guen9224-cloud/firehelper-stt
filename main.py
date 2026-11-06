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
import os, json, asyncio, time, uuid, re
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
import websockets
import httpx

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DG_KEY = os.environ.get("DEEPGRAM_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
GLASSES_TOKEN = os.environ.get("GLASSES_TOKEN", "")
OWNER_UID = os.environ.get("OWNER_UID", "")
CARD_FREQ = os.environ.get("CARD_FREQ", "normal")
CONV_URL = (SUPABASE_URL + "/functions/v1/conversate") if SUPABASE_URL else ""
AI_URL = (SUPABASE_URL + "/functions/v1/firehelper-ai") if SUPABASE_URL else ""
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

SESSIONS: dict = {}   # sid -> {"lines":[{t,at}], "cues":[...], "uid": <사용자>}
VIEWERS: dict = {}    # sid -> set[WebSocket]  (앱 실시간 뷰어들)
PRODUCERS: dict = {}  # sid -> WebSocket (안경 producer; AI 답변을 안경으로 푸시)
TOKEN_UID: dict = {}  # device_token -> uid (페어링 캐시)


def norm_sid(s) -> str:
    return (s or "").replace("-", "").lower().strip()[:40]


async def resolve_uid(device_token: str) -> str:
    # 안경 기기토큰 → 사용자 uid. 토큰 없으면 소유자(OWNER_UID) 기본값(미페어링 안경).
    t = (device_token or "").strip()
    if not t:
        return OWNER_UID
    if t in TOKEN_UID:
        return TOKEN_UID[t]
    if not CUE_ON:
        return OWNER_UID
    try:
        async with httpx.AsyncClient(timeout=15.0) as cli:
            r = await cli.post(CONV_URL, headers=CONV_HEADERS, json={"action": "glasses_whoami", "device_token": t})
            d = r.json()
            u = d.get("uid")
            if u:
                TOKEN_UID[t] = u
                return u
    except Exception:
        pass
    return OWNER_UID


def clean_md(t: str) -> str:
    # 마크다운 기호 제거(안경·앱 전부 평문으로)
    t = re.sub(r"[#*`_>]", "", t or "")
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


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


@app.post("/pair")
async def pair(req: Request):
    # 플러그인(폰)이 6자리 코드를 보내면 conversate로 교환해 device_token 반환
    if not CUE_ON:
        return {"error": "not_configured"}
    try:
        body = await req.json()
    except Exception:
        body = {}
    code = "".join(ch for ch in str(body.get("code", "")) if ch.isdigit())
    if len(code) != 6:
        return {"error": "bad_code"}
    try:
        async with httpx.AsyncClient(timeout=15.0) as cli:
            r = await cli.post(CONV_URL, headers=CONV_HEADERS, json={"action": "glasses_claim", "code": code})
            return r.json()
    except Exception as e:
        return {"error": str(e)[:120]}


async def save_session(client, sid, st, finalize, force=False):
    if not CUE_ON or client is None:
        return
    if not st["lines"] and not force:
        return
    payload = {
        "action": "glasses_save", "uid": st.get("uid") or OWNER_UID, "session_id": sid,
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
                q = str(cmd.get("q", "")).strip()
                if not answer:
                    continue
                at_iso = now_iso()
                stx = SESSIONS.get(sid)
                if stx is not None:
                    stx["cues"].append({"type": "answer", "title": title, "body": answer, "q": q, "src": "", "at": at_iso, "anchor": at_iso})
                prod = PRODUCERS.get(sid)
                if prod is not None:
                    try:
                        await prod.send_json({"card": {"type": "answer", "title": title, "body": clean_md(answer), "src": ""}})
                    except Exception:
                        pass
                await broadcast(sid, {"ai": {"q": q, "a": answer}})   # 앱 실시간에 전체 답변
                if stx is not None and CUE_ON:   # AI 답변 즉시 저장(세션에 남게)
                    try:
                        async with httpx.AsyncClient(timeout=20.0) as cli:
                            await save_session(cli, sid, stx, False)
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
    st["uid"] = await resolve_uid(ws.query_params.get("token"))   # 페어링 토큰 → 사용자
    PRODUCERS[sid] = ws   # AI 답변을 이 안경으로 푸시
    utter = []
    last_seg = {"t": 0.0}
    end_requested = {"v": False}
    ask_next = {"v": False}            # 다음 확정 문장을 AI 질문으로(카드 제외)
    last_save = {"t": 0.0}
    client = httpx.AsyncClient(timeout=20.0) if CUE_ON else None

    # 접속 즉시 세션 행 생성(앱이 sid 를 바로 찾을 수 있게)
    if client is not None:
        asyncio.create_task(save_session(client, sid, st, False, force=True))

    async def fetch_ai_answer(question: str):
        # 안경 질문 → 서버가 firehelper-ai 직접 호출(앱 없이도 동작). SSE data:{delta} 누적.
        if not CUE_ON or client is None or not AI_URL:
            return
        try:
            r = await client.post(AI_URL, headers=CONV_HEADERS, json={
                "message": question, "uid": st.get("uid") or OWNER_UID,
            }, timeout=90.0)
            txt = r.text
            tt = txt.strip()
            if tt.startswith("{"):
                try:
                    o = json.loads(tt)
                    if o.get("error"):
                        await ws.send_json({"cue_err": "AI: " + str(o.get("error"))[:80]})
                        return
                except Exception:
                    pass
            answer = ""
            for block in txt.split("\n\n"):
                for line in block.split("\n"):
                    if line.startswith("data:"):
                        try:
                            ev = json.loads(line[5:].strip())
                        except Exception:
                            continue
                        dd = ev.get("delta")
                        if isinstance(dd, str):
                            answer += dd
            answer = answer.strip()
            if not answer:
                try:
                    await ws.send_json({"cue_err": "AI 답변 비어있음"})
                except Exception:
                    pass
                return
            at_iso = now_iso()
            title = (question[:30] + "…") if len(question) > 30 else (question or "AI 답변")
            st["cues"].append({"type": "answer", "title": title, "body": answer, "q": question, "src": "", "at": at_iso, "anchor": at_iso})
            try:
                await ws.send_json({"card": {"type": "answer", "title": title, "body": clean_md(answer), "src": ""}})
            except Exception:
                pass
            await broadcast(sid, {"ai": {"q": question, "a": answer}})
            await save_session(client, sid, st, False)
        except Exception as e:
            try:
                await ws.send_json({"cue_err": str(e)[:120]})
            except Exception:
                pass

    async def fetch_cue(latest: str, at_iso: str):
        if not CUE_ON or client is None:
            return
        try:
            r = await client.post(CONV_URL, headers=CONV_HEADERS, json={
                "action": "cue", "uid": st.get("uid") or OWNER_UID, "latest": latest,
                "transcript": [l["t"] for l in st["lines"][-12:]],
                "recent": [c["title"] for c in st["cues"][-10:]],
                "cardFreq": CARD_FREQ,
            })
            d = r.json()
            cues = d.get("cues")
            if not cues:
                cues = [d["cue"]] if d.get("cue") else []
            added = False
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
                added = True
                try:
                    await ws.send_json({"card": c})   # 안경(producer)
                except Exception:
                    pass
                await broadcast(sid, {"card": c})      # 앱(viewer)
            if added:
                last_save["t"] = time.time()
                await save_session(client, sid, st, False)   # 카드 뜨면 즉시 저장
            # 할 일 완료 카드가 떴으면 안경 할일목록을 즉시 갱신(완료된 건 사라지게)
            if any(c and c.get("type") == "todo_done" for c in cues):
                asyncio.create_task(fetch_todos())
        except Exception as e:
            try:
                await ws.send_json({"cue_err": str(e)[:120]})
            except Exception:
                pass

    async def fetch_todos():
        # 안경 '할일목록' 뷰 요청(TODOS) → conversate로 미완료 할 일 조회 → 안경에 내려줌
        if not CUE_ON or client is None:
            try: await ws.send_json({"todos": []})
            except Exception: pass
            return
        try:
            r = await client.post(CONV_URL, headers=CONV_HEADERS, json={
                "action": "glasses_todos", "uid": st.get("uid") or OWNER_UID,
            })
            d = r.json()
            await ws.send_json({"todos": d.get("todos") or []})
        except Exception as e:
            try: await ws.send_json({"todos": [], "todo_err": str(e)[:120]})
            except Exception: pass

    async def fetch_schedule():
        # 안경 '일정' 뷰 요청(SCHED) → conversate로 오늘 + 다음 일정 있는 날 조회 → 안경에 내려줌
        if not CUE_ON or client is None:
            try: await ws.send_json({"sched": {"today": [], "next": []}})
            except Exception: pass
            return
        try:
            r = await client.post(CONV_URL, headers=CONV_HEADERS, json={
                "action": "glasses_schedule", "uid": st.get("uid") or OWNER_UID,
            })
            d = r.json()
            await ws.send_json({"sched": {
                "today": d.get("today") or [], "next": d.get("next") or [],
                "today_date": d.get("today_date") or "", "next_date": d.get("next_date") or "",
            }})
        except Exception as e:
            try: await ws.send_json({"sched": {"today": [], "next": []}, "sched_err": str(e)[:120]})
            except Exception: pass

    try:
        async with websockets.connect(
            DG_URL,
            extra_headers={"Authorization": f"Token {DG_KEY}"},
            ping_interval=10, ping_timeout=30, close_timeout=5, max_size=None,
        ) as dg:
            # 무음/프레임 공백에도 Deepgram 소켓이 유휴로 닫히지 않게 주기 KeepAlive
            async def keepalive_dg():
                try:
                    while True:
                        await asyncio.sleep(7)
                        try:
                            await dg.send(json.dumps({"type": "KeepAlive"}))
                        except Exception:
                            break
                except asyncio.CancelledError:
                    pass

            flushing = {"v": False}
            async def flush_utter():
                # 누적된 문장을 확정 → 세션 저장 + 소방 카드. speech_final 없이도 호출됨.
                if flushing["v"]:
                    return
                latest = " ".join(utter).strip()
                if not latest:
                    return
                flushing["v"] = True
                try:
                    utter.clear()
                    at_iso = now_iso()
                    st["lines"].append({"t": latest, "at": at_iso})
                    if ask_next["v"]:
                        ask_next["v"] = False
                        try:
                            await ws.send_json({"asked": latest})   # 안경에 질문 문장 표시
                        except Exception:
                            pass
                        asyncio.create_task(fetch_ai_answer(latest))   # 서버가 직접 AI 호출(카드 없이)
                    else:
                        core = "".join(ch for ch in latest if ch.isalnum())
                        if CUE_ON and len(core) >= 4:
                            asyncio.create_task(fetch_cue(latest, at_iso))
                    if client is not None:
                        last_save["t"] = time.time()
                        asyncio.create_task(save_session(client, sid, st, False))
                finally:
                    flushing["v"] = False

            async def auto_flush():
                # Deepgram speech_final이 안 와도, 말이 ~1.2s 멈추면 스스로 문장 확정
                try:
                    while True:
                        await asyncio.sleep(0.5)
                        thr = 2.0 if ask_next["v"] else 1.2
                        if utter and (time.time() - last_seg["t"]) >= thr:
                            await flush_utter()
                except asyncio.CancelledError:
                    pass

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
                                await flush_utter()
                            elif tt == "ASKNEXT":
                                # 탭 전 전사는 일반 라인으로 흘려보내고(질문에 안 섞임), 이후 문장을 질문으로
                                await flush_utter()
                                ask_next["v"] = True
                            elif tt == "ASKCANCEL":
                                ask_next["v"] = False
                            elif tt == "TODOS":
                                # 안경 할일목록 뷰 → 미완료 할 일 조회해 안경에 전달
                                asyncio.create_task(fetch_todos())
                            elif tt == "SCHED":
                                # 안경 일정 뷰 → 오늘·내일 일정 조회해 안경에 전달
                                asyncio.create_task(fetch_schedule())
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
                        # 앱·안경 둘 다에 단어 단위 실시간 전사(중간결과 포함)
                        await broadcast(sid, {"text": text, "final": is_final})
                        try:
                            await ws.send_json({"text": text, "final": is_final})
                        except Exception:
                            pass
                        if is_final:
                            utter.append(text)
                            last_seg["t"] = time.time()
                        if speech_final and not ask_next["v"]:
                            await flush_utter()
                    except Exception:
                        pass

            ka = asyncio.create_task(keepalive_dg())
            af = asyncio.create_task(auto_flush())
            try:
                await asyncio.gather(pump_client_to_dg(), pump_dg_to_client())
            finally:
                ka.cancel()
                af.cancel()
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

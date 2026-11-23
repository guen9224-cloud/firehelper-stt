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
import os, json, asyncio, time, uuid, re, hmac
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
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
AI_URL = os.environ.get("AI_URL") or ((SUPABASE_URL + "/functions/v1/firehelper-ai") if SUPABASE_URL else "")
AI_ALLOW_FALLBACK = os.environ.get("AI_ALLOW_FALLBACK", "0") == "1"   # 1이면 PC 미연결 시 API 폴백, 0이면 "PC 확인" 안내
CUE_ON = bool(CONV_URL and GLASSES_TOKEN and OWNER_UID)
CONV_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer " + SUPABASE_ANON_KEY,
    "apikey": SUPABASE_ANON_KEY,
    "x-fh-glasses": GLASSES_TOKEN,
}
SAVE_EVERY = 12  # DB 주기 저장 최소 간격(초)

import urllib.parse as _up
# Deepgram nova-3 키텀 부스팅: 소방 핵심 용어를 미리 알려 오인식(예: 소화기→수화기) 감소
DG_KEYTERMS = [
    "소화기","수신기","발신기","감지기","중계기","경계구역","스프링클러","프리액션밸브",
    "준비작동식","알람밸브","유수검지장치","옥내소화전","옥외소화전","유도등","비상조명등",
    "자동화재탐지설비","제연설비","방화문","방화셔터","방화댐퍼","방화구획","소화수조",
    "연결송수관설비","비상콘센트설비","무선통신보조설비","소방안전관리자","특정소방대상물",
    "작동점검","종합점검","자체점검","차동식","정온식","연기감지기","불꽃감지기",
    "완강기","피난기구","가스계소화설비","이산화탄소소화설비","할로겐화합물","제어반","템퍼스위치",
]
DG_URL = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-3&language=ko&encoding=linear16&sample_rate=16000&channels=1"
    "&interim_results=true&punctuate=true&smart_format=true&endpointing=300"
    + "".join("&keyterm=" + _up.quote(t) for t in DG_KEYTERMS)
)

TYPE_SEC = {"concept": "용어", "law": "기준", "todo": "할일", "todo_add": "할일", "site": "현장"}
FAST_TERMS = [
    "작동점검","종합점검","정밀점검","자체점검","외관점검","결과보고서","점검표",
    "감지기","차동식","정온식","연기감지기","불꽃감지기","아날로그감지기",
    "수신기","발신기","중계기","경계구역",
    "스프링클러","프리액션밸브","준비작동식","알람밸브","유수검지장치","습식","건식","일제살수식","드렌처",
    "소화기","분말소화기","옥내소화전","옥외소화전","호스릴",
    "유도등","유도표지","비상조명등","휴대용비상조명등",
    "피난기구","완강기","구조대","피난사다리","간이완강기",
    "자동화재탐지설비","비상방송설비","비상경보설비","시각경보기","가스누설경보기",
    "가스계소화설비","이산화탄소소화설비","할론","할로겐화합물","청정소화약제",
    "제연설비","배연창","방화문","방화셔터","방화댐퍼","내화구조","방화구획",
    "소화수조","저수조","연결송수관설비","연결살수설비","비상콘센트설비","무선통신보조설비",
    "소방안전관리자","소방안전관리대상물","특정소방대상물","방염",
    "위험물","제조소","옥내저장소","옥외탱크저장소","지정수량","과태료","조치명령",
]
_FAST_SHOWN: dict = {}   # sid -> set(term)  세션당 같은 용어 재발 방지

_DEFS: dict = {}          # term -> {"body","src"}  화재안전기준 '정의' 전체
_DEF_TERMS: list = []     # 매칭용(긴 용어 우선)
_DEF_ASK = re.compile(r"(뜻|정의|뭐|뭔|무슨|무엇|뭣|설명해|설명 좀|모르|헷갈|처음 들|들어봤|아세요|아시나)")   # '정의를 묻는' 신호가 있을 때만(단순 물음표 제외)
_DEFS_LOADED = False

async def load_defs(client):
    # 화재안전기준 '정의' 전체를 1회 로드(fast_defs RPC) → 메모리 매칭. 정의는 맥락 무관하게 항상 맞음.
    global _DEFS, _DEF_TERMS, _DEFS_LOADED
    if _DEFS_LOADED:
        return
    _DEFS_LOADED = True
    try:
        r = await client.post(
            SUPABASE_URL + "/rest/v1/rpc/fast_defs",
            json={},
            headers={"apikey": SUPABASE_ANON_KEY, "Authorization": "Bearer " + SUPABASE_ANON_KEY,
                     "Content-Type": "application/json"},
            timeout=15.0,
        )
        rows = r.json() if r.status_code == 200 else []
    except Exception:
        rows = []
    d = {}
    for row in rows or []:
        t = (row.get("term") or "").strip()
        c = clean_md(row.get("content") or "").strip()
        if t and c:
            d[t] = {"body": c[:180], "src": (row.get("law_name") or "").strip()[:36]}
    if d:
        _DEFS = d
        _DEF_TERMS = sorted(d.keys(), key=len, reverse=True)


async def fast_law_lookup(client, term: str):
    # fast_law_card RPC(SECURITY DEFINER) 호출 — RLS 우회, 랭킹은 SQL에서. 임베딩/LLM 없이 1건.
    try:
        r = await client.post(
            SUPABASE_URL + "/rest/v1/rpc/fast_law_card",
            json={"q": term},
            headers={"apikey": SUPABASE_ANON_KEY, "Authorization": "Bearer " + SUPABASE_ANON_KEY,
                     "Content-Type": "application/json"},
            timeout=6.0,
        )
        rows = r.json() if r.status_code == 200 else []
    except Exception:
        return None
    if isinstance(rows, list) and rows:
        return rows[0]
    if isinstance(rows, dict) and rows:
        return rows
    return None


SESSIONS: dict = {}   # sid -> {"lines":[{t,at}], "cues":[...], "uid": <사용자>}
HELPERS: dict = {}        # uid -> 개인 AI 헬퍼 WebSocket (PC에서 아웃바운드 접속)
HELPER_PENDING: dict = {} # req_id -> asyncio.Future(answer)
HELPER_PENDING_FULL: dict = {} # req_id -> Future(full/chat answer) — /ask 전용
HELPER_STREAM: dict = {}  # req_id -> asyncio.Queue — /ask 스트리밍(SSE) 전용
HELPER_STREAM_WS: dict = {}  # req_id -> 그 요청을 처리 중인 헬퍼 ws (끊기면 스트림 종료 통지)
HELPER_MAXV: dict = {}  # uid -> (헬퍼 코드버전 v, 마지막으로 본 시각) — 구버전(좀비) 헬퍼가 자리를 뺏지 못하게
REQ_SID: dict = {}        # req_id -> sid (헬퍼의 뒤이은 say[브리핑 음성]를 해당 세션 뷰어에게 전달)
VIEWERS: dict = {}    # sid -> set[WebSocket]  (앱 실시간 뷰어들)
PRODUCERS: dict = {}  # sid -> WebSocket (안경 producer; AI 답변을 안경으로 푸시)
TOKEN_UID: dict = {}  # device_token -> uid (페어링 캐시)
GLASSES_BY_UID: dict = {}  # uid -> sid (워치/폰 원격제어 ctrl 라우팅)
WATCHES: dict = {}  # uid -> set(워치 수신 WebSocket). 답변·진행 단계·승인 요청·사진을 워치로 보냄 (2026-11-20)
WORK_PENDING: dict = {}  # uid -> ts (워치 '작업' 트리거: 다음 발화를 helper1 작업큐로 보냄)
# ── helper2 작업 승인(워치 버튼) ──
FH_RELAY_KEY = os.environ.get("FH_RELAY_KEY", "").strip()   # fh-work(엣지함수) ↔ 릴레이 공유키. 없으면 승인 기능 꺼짐 (앞뒤 공백·줄바꿈은 무시)
FH_WORK_URL = (SUPABASE_URL + "/functions/v1/fh-work") if SUPABASE_URL else ""
WORK_CARD_STRICT = os.environ.get("WORK_CARD_STRICT", "0") == "1"   # 1이면 /work_card 도 공유키 필수
APPROVAL_IDS: dict = {}      # uid -> {승인요청 id: 보낸 시각}  (릴레이가 그 사용자 안경에 보낸 요청만 결정 가능)
APPROVAL_KEEP = 35 * 60      # 초. 승인 요청 id 를 기억하는 시간(서버 만료 30분 + 여유)
APPROVAL_MODAL_TTL = 120     # 초. 안경 승인 창이 떠 있는 시간(헬퍼가 기다리는 시간과 같음)
# 승인은 안경의 '승인 창' 안에서만 한다(플러그인 cap=appr). 릴레이는 워치 제스처를 가로채지 않고 그대로 넘긴다.


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


async def watch_send(uid: str, msg: dict):
    # 그 사용자의 워치(들)에 한 줄 보낸다. 워치가 없거나 끊겼으면 조용히 넘어간다.
    for _w in list(WATCHES.get(uid) or ()):
        try:
            await _w.send_json(msg)
        except Exception:
            pass


def clean_md(t: str) -> str:
    # 마크다운 기호 제거(안경·앱 전부 평문으로)
    t = re.sub(r"[#*`_>]", "", t or "")
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


WATCH_SAY = {}        # 워치 질문 id -> 읽어줄 문장(say)을 기다리는 future
WATCH_SAY_FLAG = set()   # 헬퍼가 "읽어줄 문장이 곧 따라온다"고 알린 워치 질문 id
_WD = {"월": "월요일", "화": "화요일", "수": "수요일", "목": "목요일", "금": "금요일", "토": "토요일", "일": "일요일"}


def speech_clean(t: str) -> str:
    # 워치가 소리로 읽을 글 다듬기(2026-11-23): 이모티콘·기호를 빼고, 날짜 표기를 말로 바꾼다.
    # 워치는 화면 글자를 그대로 읽으므로 워치 질문의 답에만 쓴다(안경·앱·업무방 알림은 그대로).
    t = t or ""
    t = re.sub(r"(?<!\d)(\d{4})[-./](\d{1,2})[-./](\d{1,2})(?!\d)", lambda m: "%s년 %d월 %d일" % (m.group(1), int(m.group(2)), int(m.group(3))), t)
    # 11/24(화) 처럼 요일이 붙은 것만 날짜로 본다(1/30 같은 분수·비율은 건드리지 않는다)
    t = re.sub(r"(?<![\d/])(\d{1,2})/(\d{1,2})\s*\(([월화수목금토일])\)", lambda m: "%d월 %d일 %s" % (int(m.group(1)), int(m.group(2)), _WD[m.group(3)]), t)
    t = re.sub(r"(일)\s*\(([월화수목금토일])\)", lambda m: m.group(1) + " " + _WD[m.group(2)], t)
    t = re.sub(r"(?<=[\d일])\s*[~∼〜]\s*(?=\d)", "부터 ", t)
    t = re.sub(r"[\U0001F000-\U0001FAFF\U0001F1E6-\U0001F1FF\u2600-\u27BF\u2B00-\u2BFF\u2190-\u21FF\u25A0-\u25FF\uFE0F\u200D\u20E3]", " ", t)
    t = re.sub(r"[·•ㆍ∙※|\[\]{}<>=^\\]", " ", t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r" *\n *", "\n", t)
    t = re.sub(r"\n{2,}", "\n", t)
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
            "sessions": len(SESSIONS), "viewers": sum(len(v) for v in VIEWERS.values()),
            "approve": bool(FH_RELAY_KEY), "approve_len": len(FH_RELAY_KEY)}   # 승인 공유키가 들어왔는지(값은 내보내지 않음)


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


AI_PAIR_FN = (SUPABASE_URL + "/functions/v1/fh-ai-pair") if SUPABASE_URL else ""

_AI_CONNECT_PAGE = """<!doctype html>
<html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>FireHelper 개인 AI 연결</title>
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<style>
  :root{color-scheme:dark}*{box-sizing:border-box}
  body{margin:0;background:#0b0d10;color:#e8eaed;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Noto Sans KR',sans-serif}
  .wrap{max-width:560px;margin:0 auto;padding:28px 18px 60px}
  h1{font-size:20px;margin:0 0 4px}.sub{color:#8b909a;font-size:13px;margin:0 0 22px}
  .card{background:#14171c;border:1px solid #232830;border-radius:14px;padding:18px;margin:14px 0}
  label{display:block;font-size:12px;color:#9aa0aa;margin:10px 0 5px}
  input{width:100%;padding:11px 12px;border-radius:9px;border:1px solid #2a2f38;background:#0f1216;color:#e8eaed;font-size:14px}
  button{cursor:pointer;border:0;border-radius:10px;font-weight:700;font-size:14px;padding:12px 14px}
  .primary{width:100%;background:linear-gradient(135deg,#ff5a52,#d32f2f);color:#fff}
  .ghost{background:#20242b;color:#cfd3da;padding:7px 10px;font-size:12px;border:1px solid #2a2f38}
  .muted{color:#8b909a;font-size:12px}
  pre{background:#0f1216;border:1px solid #2a2f38;border-radius:10px;padding:12px;overflow-x:auto;font-size:12.5px;white-space:pre-wrap;word-break:break-all}
  .row{display:flex;gap:8px;align-items:center;justify-content:space-between}
  .tok{font-family:ui-monospace,Menlo,monospace;font-size:12px;color:#cfd3da}
  .hide{display:none}.ok{color:#7ee081}.err{color:#ff8a85;font-size:13px}
</style></head>
<body><div class="wrap">
  <h1>개인 AI 연결</h1>
  <p class="sub">이 PC의 구독 AI를 FireHelper 대화지원에 연결합니다. 로그인 후 "이 PC 연결"을 누르세요.</p>
  <div id="login" class="card">
    <p class="muted" style="margin:0 0 10px">기존 FireHelper 계정(이메일/비밀번호)으로 로그인하세요.</p>
    <label>이메일</label><input id="email" type="email" autocomplete="username">
    <label>비밀번호</label><input id="pw" type="password" autocomplete="current-password">
    <button class="primary" style="margin-top:12px" onclick="emailLogin()">이메일로 로그인</button>
    <p id="loginErr" class="err"></p>
  </div>
  <div id="app" class="card hide">
    <div class="row"><span class="muted">로그인: <b id="who"></b></span><button class="ghost" onclick="logout()">로그아웃</button></div>
    <label>이 PC 이름 (구분용)</label>
    <input id="label" placeholder="예: 회사 데스크탑">
    <button class="primary" style="margin-top:12px" onclick="issue()">이 PC 연결 (토큰 발급)</button>
    <div id="result" class="hide" style="margin-top:14px">
      <p class="ok">발급됨 — 이 PC 터미널(헬퍼 폴더)에서 아래를 실행하세요:</p>
      <pre id="cmd"></pre>
      <button class="ghost" onclick="copyCmd()">명령 복사</button>
    </div>
    <div style="margin-top:18px"><div class="muted" style="margin-bottom:8px">발급된 토큰</div><div id="tokens"></div></div>
  </div>
<script>
const SB_URL="__SB_URL__", ANON="__ANON__", PAIR_FN="__PAIR_FN__";
const $=(id)=>document.getElementById(id);
function showErr(m){ var e=$("loginErr"); if(e) e.textContent=m; }
let sb=null, session=null;
try {
  if(!window.supabase || !window.supabase.createClient) throw new Error("supabase-js 로드 실패 (window.supabase 없음)");
  sb = window.supabase.createClient(SB_URL, ANON);
} catch(e){ showErr("초기화 오류: "+e.message); }
async function refresh(){
  if(!sb) return;
  try {
    const {data}=await sb.auth.getSession(); session=data.session;
    if(session){ $("login").classList.add("hide"); $("app").classList.remove("hide"); $("who").textContent=session.user.email||session.user.id.slice(0,8); loadTokens(); }
    else { $("app").classList.add("hide"); $("login").classList.remove("hide"); }
  } catch(e){ showErr("세션 확인 오류: "+e.message); }
}
if(sb) sb.auth.onAuthStateChange(()=>refresh());
async function emailLogin(){ if(!sb){ showErr("로그인 모듈 미로드 — 새로고침/인터넷 확인"); return; } showErr(""); try { const {error}=await sb.auth.signInWithPassword({email:$("email").value.trim(),password:$("pw").value}); if(error) showErr(error.message); } catch(e){ showErr(e.message); } }
async function logout(){ await sb.auth.signOut(); }
async function api(action,extra){ const r=await fetch(PAIR_FN,{method:"POST",headers:{"Content-Type":"application/json","Authorization":"Bearer "+session.access_token},body:JSON.stringify(Object.assign({action},extra||{}))}); return r.json(); }
async function issue(){ const d=await api("issue",{label:$("label").value.trim()}); if(d.error){ alert(d.error); return; } $("cmd").textContent=d.command; $("result").classList.remove("hide"); loadTokens(); }
function copyCmd(){ navigator.clipboard.writeText($("cmd").textContent); }
async function loadTokens(){ const d=await api("list"); const el=$("tokens"); el.innerHTML=""; (d.tokens||[]).forEach(function(t){ const div=document.createElement("div"); div.className="row"; div.style.padding="7px 0"; div.style.borderTop="1px solid #232830"; const used=t.last_used?("최근 "+new Date(t.last_used).toLocaleString('ko-KR')):"미사용"; div.innerHTML='<span class="tok">'+(t.revoked?'[폐기] ':'[활성] ')+(t.label||'(이름없음)')+' · '+used+'</span>'; if(!t.revoked){ const b=document.createElement("button"); b.className="ghost"; b.textContent="폐기"; b.onclick=async function(){await api("revoke",{token:t.token}); loadTokens();}; div.appendChild(b); } el.appendChild(div); }); }
if(sb) refresh();
</script>
</div></body></html>"""


@app.get("/ai-connect")
def ai_connect():
    html = (_AI_CONNECT_PAGE
            .replace("__SB_URL__", SUPABASE_URL)
            .replace("__ANON__", SUPABASE_ANON_KEY)
            .replace("__PAIR_FN__", AI_PAIR_FN))
    return HTMLResponse(content=html)


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
@app.post("/ctrl")
async def ctrl(req: Request):
    # 워치(또는 폰)가 안경을 원격 조작. body 또는 query: token(페어링 토큰), action
    try:
        body = await req.json()
    except Exception:
        body = {}
    token = str(body.get("token") or req.query_params.get("token") or "")
    action = str(body.get("action") or req.query_params.get("action") or "").strip()
    if action not in ("ai", "select", "back", "up", "down", "exit", "work", "ping", "dismiss"):
        return JSONResponse({"ok": False, "error": "bad_action"}, status_code=400)
    uid = await resolve_uid(token) if token else ""
    if not uid:
        return JSONResponse({"ok": False, "error": "auth"}, status_code=401)
    # strict = 실제 페어링이 확인된 토큰(TOKEN_UID 는 glasses_whoami 성공 때만 채워짐). OWNER 폴백 토큰은 strict 아님.
    # 안경 플러그인은 승인 창에서 strict 제스처만 승인/거절로 받아들인다.
    strict = bool(TOKEN_UID.get(token.strip(), ""))
    try:
        n = int(body.get("n") or req.query_params.get("n") or 1)
    except Exception:
        n = 1
    n = max(1, min(n, 10))   # 베젤 스크롤: 여러 칸을 한 번에
    sid = GLASSES_BY_UID.get(uid)
    ws = PRODUCERS.get(sid) if sid else None
    if action == "ping":   # 워치 설정의 '연결 확인': 안경엔 아무 것도 보내지 않고 상태만 알려준다 (2026-11-20)
        return JSONResponse({"ok": True, "action": "ping", "glasses": ws is not None, "helper": HELPERS.get(uid) is not None})
    if ws is None:
        return JSONResponse({"ok": False, "error": "no_glasses"}, status_code=409)   # 안경 세션 없음
    # 'work'(옛 워치 앱의 작업 버튼)는 'ai'와 같게 처리한다. '작업' 접두어·작업 큐는 없앰 (2026-11-20)
    send_action = "ai" if action == "work" else action
    try:
        await ws.send_json({"ctrl": {"action": send_action, "strict": strict, "n": n}})
    except Exception:
        return JSONResponse({"ok": False, "error": "send_failed"}, status_code=502)
    return JSONResponse({"ok": True, "action": action})


async def _approval_decide(approval_id, uid, decision) -> dict:
    # 워치 승인/거절을 fh-work 에 기록(공유키). 이미 다른 곳(앱)에서 처리됐으면 already=True 로 돌아옴.
    if not (FH_RELAY_KEY and FH_WORK_URL and approval_id):
        return {"ok": False}
    try:
        async with httpx.AsyncClient(timeout=10.0) as cli:
            r = await cli.post(
                FH_WORK_URL,
                headers={"Content-Type": "application/json", "Authorization": "Bearer " + SUPABASE_ANON_KEY,
                         "apikey": SUPABASE_ANON_KEY, "x-fh-relay": FH_RELAY_KEY},
                json={"action": "approval_decide_relay", "approval_id": approval_id, "uid": uid, "decision": decision},
            )
            d = r.json()
            return d if isinstance(d, dict) else {"ok": False}
    except Exception:
        return {"ok": False}


def _approval_remember(uid: str, approval_id: str):
    now = time.time()
    m = APPROVAL_IDS.setdefault(uid, {})
    for k in [k for k, v in m.items() if now - v > APPROVAL_KEEP]:
        m.pop(k, None)
    m[approval_id] = now


async def handle_appr(st: dict, ws, approval_id: str, decision: str):
    # 안경 승인 창에서 온 결정(APPR <id> approve|deny). 실제 페어링된 안경 세션 + 릴레이가 그 안경에 보낸 요청만 처리.
    uid = str(st.get("uid") or "")
    aid = (approval_id or "").strip().lower()
    res = {"ok": False}
    known = APPROVAL_IDS.get(uid, {}).get(aid)
    if (st.get("strict") and uid and decision in ("approve", "deny")
            and re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", aid)
            and known and (time.time() - known) <= APPROVAL_KEEP):
        res = await _approval_decide(aid, uid, decision)
    _out = {"appr_result": {"id": aid, "ok": bool(res.get("ok")),
                            "status": str(res.get("status") or ""), "already": bool(res.get("already"))}}
    try:
        await ws.send_json(_out)
    except Exception:
        pass
    if uid:
        await watch_send(uid, _out)   # 워치에 떠 있는 승인 요청도 같이 닫는다


@app.post("/approve_card")
async def approve_card(req: Request):
    # fh-work 가 helper2 의 승인 요청을 안경 카드로 띄우고 워치 승인 대기를 건다. 공유키(x-fh-relay) 필수.
    _rk = req.headers.get("x-fh-relay", "")
    if not FH_RELAY_KEY or not _rk or not hmac.compare_digest(_rk, FH_RELAY_KEY):
        return JSONResponse({"ok": False, "error": "auth"}, status_code=401)
    try:
        body = await req.json()
    except Exception:
        body = {}
    uid = str(body.get("uid") or "").strip()
    approval_id = str(body.get("approval_id") or "").strip()
    title = (str(body.get("title") or "").strip() or "승인 필요")
    text = str(body.get("body") or "").strip()
    if not (uid and approval_id):
        return JSONResponse({"ok": False, "error": "bad_request"}, status_code=400)
    card_title = (title[:30] + "…") if len(title) > 30 else title
    # 워치(실제 페어링 토큰으로 붙은 것만 WATCHES 에 있음)에도 승인 요청을 띄운다. 워치만 있을 땐 푸시도 그대로 나간다.
    if WATCHES.get(uid):
        _approval_remember(uid, approval_id.lower())
        await watch_send(uid, {"appr": {"id": approval_id.lower(), "title": card_title,
                                        "body": clean_md(text), "ttl": APPROVAL_MODAL_TTL}})
    sid = GLASSES_BY_UID.get(uid)
    prod = PRODUCERS.get(sid) if sid else None
    if prod is None:
        return JSONResponse({"ok": True, "delivered": "none", "watch": False})   # 안경 세션 없음 → fh-work 가 푸시
    st = SESSIONS.get(sid) or {}
    # 승인 창을 띄울 수 있는 안경: 새 플러그인(cap 에 appr) + 실제 페어링 토큰으로 붙은 세션
    can_decide = bool(st.get("strict")) and ("appr" in str(st.get("cap") or ""))
    try:
        if can_decide:
            _approval_remember(uid, approval_id.lower())
            await prod.send_json({"card": {"type": "approval", "id": approval_id.lower(), "title": card_title,
                                           "body": clean_md(text), "ttl": APPROVAL_MODAL_TTL, "src": ""}})
        else:
            await prod.send_json({"card": {"type": "answer", "title": card_title,
                                           "body": clean_md(text + "\n\n앱 채팅에서 승인해 주세요"), "src": ""}})
    except Exception:
        return JSONResponse({"ok": True, "delivered": "none", "watch": False})
    # watch=True 는 "안경에서 바로 승인/거절할 수 있다"는 뜻(fh-work 가 이때만 푸시를 생략)
    return JSONResponse({"ok": True, "delivered": "glasses", "watch": can_decide})


def _wav_wrap(pcm: bytes, sr: int = 16000) -> bytes:
    # 16비트 모노 PCM 앞에 WAV 머리말을 붙인다.
    import struct
    n = len(pcm)
    return (b"RIFF" + struct.pack("<I", 36 + n) + b"WAVEfmt " + struct.pack("<IHHIIHH", 16, 1, 1, sr, sr * 2, 2, 16)
            + b"data" + struct.pack("<I", n) + pcm)


async def _watch_token_uid(token: str) -> str:
    # 워치 수신·단독 질문은 실제 페어링이 확인된 토큰만 허용(OWNER 폴백 불가).
    t = (token or "").strip()
    if not t:
        return ""
    uid = await resolve_uid(t)
    return uid if (uid and t in TOKEN_UID) else ""


async def _watch_todos(ws: WebSocket, uid: str):
    # 워치 첫 화면(시계 + 할 일)용: 미완료 할 일을 조회해 요청한 워치에만 보낸다(안경 할일 화면과 같은 조회).
    # 실패하면 todo_err 만 보낸다 → 워치는 직전 목록을 그대로 둔다.
    out = None
    if CUE_ON:
        try:
            async with httpx.AsyncClient(timeout=15.0) as cli:
                r = await cli.post(CONV_URL, headers=CONV_HEADERS, json={"action": "glasses_todos", "uid": uid})
            d = r.json()
            if r.status_code == 200 and isinstance(d.get("todos"), list):
                out = [{"title": str(x.get("title") or "")[:60], "due": str(x.get("due") or "")[:10]}
                       for x in d["todos"][:15] if isinstance(x, dict) and x.get("title")]
        except Exception:
            out = None
    try:
        await ws.send_json({"todos": out} if out is not None else {"todo_err": 1})
    except Exception:
        pass


@app.websocket("/watch")
async def watch_ws(ws: WebSocket):
    # 워치 수신 연결: 질문(asked)·진행 단계(status)·답변(answer)·승인 요청(appr)·사진(image)을 받는다.
    # 워치 → 서버: {"type":"ping"} / {"type":"appr","id":..,"decision":"approve|deny"} / {"type":"todos"}
    await ws.accept()
    uid = await _watch_token_uid(ws.headers.get("x-fh-token") or ws.query_params.get("token") or "")
    if not uid:
        try:
            await ws.send_json({"error": "auth"})
            await ws.close(code=4401)
        except Exception:
            pass
        return
    WATCHES.setdefault(uid, set()).add(ws)
    _todo_at = 0.0
    try:
        _gs = GLASSES_BY_UID.get(uid)
        await ws.send_json({"hello": {"glasses": bool(_gs and PRODUCERS.get(_gs) is not None),
                                      "helper": HELPERS.get(uid) is not None}})
        _gb = (SESSIONS.get(_gs) or {}).get("bat") if (_gs and PRODUCERS.get(_gs) is not None) else None
        await ws.send_json({"bat": _gb or {"g": -1, "p": -1}})   # 안경이 연결돼 있으면 마지막 배터리 값, 아니면 숨김
        while True:
            raw = await ws.receive_text()
            try:
                m = json.loads(raw)
            except Exception:
                continue
            t = m.get("type")
            if t == "ping":
                await ws.send_json({"pong": 1})
            elif t == "todos":
                # 조회가 끝날 때까지 이 연결의 다른 메시지(승인 등)를 막지 않도록 따로 돌린다. 5초 안의 반복 요청은 무시.
                if time.time() - _todo_at >= 5.0:
                    _todo_at = time.time()
                    asyncio.create_task(_watch_todos(ws, uid))
            elif t == "appr":
                aid = str(m.get("id") or "").strip().lower()
                decision = str(m.get("decision") or "")
                res = {"ok": False}
                known = APPROVAL_IDS.get(uid, {}).get(aid)
                if (decision in ("approve", "deny")
                        and re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", aid)
                        and known and (time.time() - known) <= APPROVAL_KEEP):
                    res = await _approval_decide(aid, uid, decision)
                _out = {"appr_result": {"id": aid, "ok": bool(res.get("ok")),
                                        "status": str(res.get("status") or ""), "already": bool(res.get("already"))}}
                await watch_send(uid, _out)
                _gs = GLASSES_BY_UID.get(uid)
                _gp = PRODUCERS.get(_gs) if _gs else None
                if _gp is not None:   # 안경에 같은 승인 창이 떠 있으면 같이 닫힌다
                    try:
                        await _gp.send_json(_out)
                    except Exception:
                        pass
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _set = WATCHES.get(uid)
        if _set is not None:
            _set.discard(ws)
            if not _set:
                WATCHES.pop(uid, None)


async def _watch_ask_run(uid: str, q: str):
    # 워치 단독 질문: PC 헬퍼에 묻고, 답을 워치로 보낸다(앱 채팅 기록은 헬퍼가 남긴다).
    await watch_send(uid, {"asked": q})
    answer = ""
    hws = HELPERS.get(uid)
    if hws is not None:
        rid = uuid.uuid4().hex
        fut = asyncio.get_event_loop().create_future()
        HELPER_PENDING[rid] = fut
        sfut = asyncio.get_event_loop().create_future()
        WATCH_SAY[rid] = sfut
        try:
            await hws.send_text(json.dumps({"type": "ask", "id": rid, "q": q + "\n\n(반드시 한국어로, 핵심만 2~4문장 간결하게.)"}, ensure_ascii=False))
            answer = (await asyncio.wait_for(fut, timeout=90.0)) or ""
            # 브리핑처럼 화면용 카드가 먼저 오는 답: 헬퍼가 곧 보내는 "읽어줄 문장"을 잠깐 기다려 그걸 워치에 보낸다(못 받으면 카드를 다듬어 읽는다).
            if answer and rid in WATCH_SAY_FLAG:
                try:
                    _say = (await asyncio.wait_for(sfut, timeout=8.0)) or ""
                    if _say.strip():
                        answer = _say
                except Exception:
                    pass
        except Exception:
            answer = ""
        finally:
            HELPER_PENDING.pop(rid, None)
            WATCH_SAY.pop(rid, None)
            WATCH_SAY_FLAG.discard(rid)
    answer = speech_clean(clean_md(answer or ""))
    await watch_send(uid, {"answer": {"q": q, "text": answer, "ok": bool(answer)}})


@app.post("/watch_ask")
async def watch_ask(req: Request):
    # 워치 단독 질문: 본문 = 16kHz 16비트 모노 PCM(누르고 있는 동안 녹음). 글자로 바꿔 헬퍼에 묻는다.
    uid = await _watch_token_uid(req.headers.get("x-fh-token") or "")
    if not uid:
        return JSONResponse({"ok": False, "error": "auth"}, status_code=401)
    pcm = await req.body()
    if len(pcm) < 9600:
        return JSONResponse({"ok": False, "error": "too_short"})
    if len(pcm) > 1200000:
        return JSONResponse({"ok": False, "error": "too_long"}, status_code=413)
    if HELPERS.get(uid) is None:
        return JSONResponse({"ok": False, "error": "no_helper"})
    if not DG_KEY:
        return JSONResponse({"ok": False, "error": "no_stt"}, status_code=500)
    q = ""
    try:
        _u = ("https://api.deepgram.com/v1/listen?model=nova-3&language=ko&punctuate=true&smart_format=true"
              + "".join("&keyterm=" + _up.quote(t) for t in DG_KEYTERMS))
        async with httpx.AsyncClient(timeout=30.0) as cli:
            r = await cli.post(_u, headers={"Authorization": "Token " + DG_KEY, "Content-Type": "audio/wav"},
                               content=_wav_wrap(pcm))
            d = r.json()
            q = str(d["results"]["channels"][0]["alternatives"][0].get("transcript") or "").strip()
    except Exception:
        return JSONResponse({"ok": False, "error": "stt_failed"}, status_code=502)
    if not q:
        return JSONResponse({"ok": False, "error": "no_speech"})
    asyncio.create_task(_watch_ask_run(uid, q))
    return JSONResponse({"ok": True, "q": q})


@app.post("/work_card")
async def work_card(req: Request):
    # 작업 헬퍼(helper2)가 작업 결과를 해당 사용자의 '안경'에 답변 카드로 띄움.
    # 안경 세션이 있으면 카드 전송 → delivered:"glasses" (helper2 는 푸시 안 함).
    # 안경 세션이 없으면 delivered:"none" → helper2 가 그때만 푸시.
    try:
        body = await req.json()
    except Exception:
        body = {}
    _rk = req.headers.get("x-fh-relay", "")
    if FH_RELAY_KEY and ((_rk and not hmac.compare_digest(_rk, FH_RELAY_KEY)) or (WORK_CARD_STRICT and not _rk)):
        return JSONResponse({"ok": False, "error": "auth"}, status_code=401)   # 키가 틀리면 거절(키 없는 기존 helper2 는 WORK_CARD_STRICT=1 전까지 허용)
    token = str(body.get("token") or req.query_params.get("token") or "")
    title = (str(body.get("title") or "").strip() or "작업 결과")
    text = str(body.get("body") or "").strip()
    uid = str(body.get("uid") or req.query_params.get("uid") or "").strip()   # helper2 는 큐의 uid 를 직접 넘김
    if not uid:
        uid = await resolve_uid(token) if token else ""
    if not uid:
        return JSONResponse({"ok": False, "error": "auth"}, status_code=401)
    sid = GLASSES_BY_UID.get(uid)
    prod = PRODUCERS.get(sid) if sid else None
    if prod is None:
        return JSONResponse({"ok": True, "delivered": "none"})   # 안경 세션 없음 → helper2 가 푸시
    card_title = (title[:30] + "…") if len(title) > 30 else title
    try:
        await prod.send_json({"card": {"type": "answer", "title": card_title, "body": clean_md(text), "src": "", "hold": 12}})
    except Exception:
        return JSONResponse({"ok": True, "delivered": "none"})   # 전송 실패 → helper2 가 푸시
    st = SESSIONS.get(sid)
    if st is not None:
        at_iso = now_iso()
        st["cues"].append({"type": "answer", "title": card_title, "body": text, "q": "작업", "src": "", "at": at_iso, "anchor": at_iso})
        try:
            await broadcast(sid, {"ai": {"q": "작업", "a": text}})   # 앱 실시간 보기에도 반영
        except Exception:
            pass
    return JSONResponse({"ok": True, "delivered": "glasses"})


@app.post("/chat_notify")
async def chat_notify(req: Request):
    # 업무방 새 글 알림(2026-11-21): fh-chat(엣지 함수)이 푸시를 보낼 때 함께 부른다.
    # 받은 사용자들 가운데 안경이 연결된 사람에게는 카드를, 워치가 붙어 있는 사람에게는 진동·표시를 보낸다.
    # 대화 기록(cues)·앱 실시간 보기에는 남기지 않는다(알림일 뿐 AI 답변이 아니므로).
    try:
        body = await req.json()
    except Exception:
        body = {}
    _rk = req.headers.get("x-fh-relay", "")
    if not FH_RELAY_KEY or not _rk or not hmac.compare_digest(_rk, FH_RELAY_KEY):
        return JSONResponse({"ok": False, "error": "auth"}, status_code=401)
    uids = [str(u).strip() for u in (body.get("uids") or []) if str(u).strip()][:50]
    title = (str(body.get("title") or "").strip() or "업무방")[:30]
    text = clean_md(str(body.get("body") or ""))[:300]
    n_glasses = 0
    n_watch = 0
    for uid in uids:
        sid = GLASSES_BY_UID.get(uid)
        prod = PRODUCERS.get(sid) if sid else None
        if prod is not None:
            try:
                await prod.send_json({"card": {"type": "answer", "title": title, "body": text, "src": "", "hold": 8}})
                n_glasses += 1
            except Exception:
                pass
        if WATCHES.get(uid):
            await watch_send(uid, {"answer": {"q": title, "text": text, "ok": True}})
            n_watch += 1
    return JSONResponse({"ok": True, "glasses": n_glasses, "watch": n_watch})


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
async def resolve_helper_uid(token: str):
    # per-user 페어링 토큰 -> uid (fh_ai_token_uid RPC). 무효/폐기면 None.
    if not (token and SUPABASE_URL and SUPABASE_ANON_KEY):
        return None
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(
                SUPABASE_URL + "/rest/v1/rpc/fh_ai_token_uid",
                headers={"apikey": SUPABASE_ANON_KEY, "Authorization": "Bearer " + SUPABASE_ANON_KEY, "Content-Type": "application/json"},
                json={"p_token": token},
            )
        v = r.json()
        return v if isinstance(v, str) and len(v) >= 20 else None
    except Exception:
        return None


async def uid_from_jwt(jwt: str):
    if not (jwt and SUPABASE_URL and SUPABASE_ANON_KEY):
        return None
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(SUPABASE_URL + "/auth/v1/user",
                            headers={"apikey": SUPABASE_ANON_KEY, "Authorization": "Bearer " + jwt})
        if r.status_code != 200:
            return None
        u = r.json()
        return u.get("id") if isinstance(u, dict) else None
    except Exception:
        return None


@app.post("/ask")
async def ask(req: Request):
    # 앱 채팅 → PC 헬퍼 온라인이면 그쪽으로, 아니면 offline(앱이 API로 폴백).
    auth = req.headers.get("authorization", "")
    jwt = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    try:
        body = await req.json()
    except Exception:
        body = {}
    q = str(body.get("q") or body.get("message") or "").strip()
    conv_id = str(body.get("conversation_id") or "").strip()
    context = str(body.get("context") or "")[:4000]
    if not q:
        return {"ok": False, "error": "empty"}
    uid = await uid_from_jwt(jwt)
    if not uid:
        return {"ok": False, "error": "auth"}
    hws = HELPERS.get(uid)
    if hws is None:
        return {"ok": True, "offline": True}
    rid = uuid.uuid4().hex
    if body.get("stream"):
        sq = asyncio.Queue()
        HELPER_STREAM[rid] = sq
        HELPER_STREAM_WS[rid] = hws
        try:
            await hws.send_text(json.dumps({"type": "ask", "id": rid, "q": q, "conversation_id": conv_id, "context": context}, ensure_ascii=False))
        except Exception:
            HELPER_STREAM.pop(rid, None)
            HELPER_STREAM_WS.pop(rid, None)
            return {"ok": True, "offline": True}
        async def _gen():
            _dead = time.time() + 240.0
            try:
                yield ": ok\n\n"   # 헤더·첫 바이트 즉시 전송(프록시 버퍼링 방지)
                while True:
                    try:
                        item = await asyncio.wait_for(sq.get(), timeout=15.0)
                    except asyncio.TimeoutError:
                        if time.time() > _dead:
                            yield "data: " + json.dumps({"done": True, "text": "", "error": "timeout"}) + "\n\n"
                            break
                        yield ": ping\n\n"   # 하트비트(유휴 연결 끊김 방지)
                        continue
                    yield "data: " + json.dumps(item, ensure_ascii=False) + "\n\n"
                    if item.get("done"):
                        break
            finally:
                HELPER_STREAM.pop(rid, None)
                HELPER_STREAM_WS.pop(rid, None)
        return StreamingResponse(_gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
    loop = asyncio.get_event_loop()
    fut1 = loop.create_future(); HELPER_PENDING[rid] = fut1
    fut2 = loop.create_future(); HELPER_PENDING_FULL[rid] = fut2
    try:
        await hws.send_text(json.dumps({"type": "ask", "id": rid, "q": q, "conversation_id": conv_id, "context": context}, ensure_ascii=False))
        part1 = ""
        try:
            part1 = (await asyncio.wait_for(fut1, timeout=90.0)) or ""
        except asyncio.TimeoutError:
            part1 = ""
        part2 = ""
        if fut2.done():
            part2 = fut2.result() or ""
        elif part1:
            try:
                part2 = (await asyncio.wait_for(fut2, timeout=8.0)) or ""
            except asyncio.TimeoutError:
                part2 = ""
        answer = (part2 or part1).strip()
    except Exception:
        answer = ""
    finally:
        HELPER_PENDING.pop(rid, None)
        HELPER_PENDING_FULL.pop(rid, None)
    if not answer:
        return {"ok": True, "offline": False, "answer": "", "error": "no_answer"}
    return {"ok": True, "answer": answer}


@app.websocket("/helper")
async def helper_ws(ws: WebSocket):
    # 직원 PC의 개인 AI 헬퍼가 아웃바운드로 접속. per-user 토큰 -> uid 인증(GLASSES_TOKEN 불필요).
    token = ws.query_params.get("token", "")
    uid = await resolve_helper_uid(token)
    if not uid:
        await ws.close(code=4401)
        return
    await ws.accept()
    # 버전 게이트: 같은 사용자의 더 새 버전 헬퍼가 붙어 있거나 최근(15분)까지 있었으면 구버전은 거절.
    try:
        hv = int(ws.query_params.get("v") or 0)
    except Exception:
        hv = 0
    _mv, _mts = HELPER_MAXV.get(uid, (0, 0.0))
    _cur = HELPERS.get(uid)
    if hv < _mv and (_cur is not None or (time.time() - _mts) < 900):
        try:
            await ws.close(code=4409)
        except Exception:
            pass
        return
    HELPERS[uid] = ws
    HELPER_MAXV[uid] = (hv, time.time())
    if _cur is not None and _cur is not ws and hv > _mv:
        try:
            await _cur.close(code=4409)   # 더 새 버전이 왔으니 구버전 연결 정리
        except Exception:
            pass
    try:
        while True:
            raw = await ws.receive_text()
            try:
                m = json.loads(raw)
            except Exception:
                continue
            t = m.get("type")
            _sq = HELPER_STREAM.get(m.get("id"))
            if _sq is not None:
                if t == "app_status":
                    try: _sq.put_nowait({"status": m.get("text") or ""})
                    except Exception: pass
                elif t == "app_delta":
                    try: _sq.put_nowait({"delta": m.get("text") or ""})
                    except Exception: pass
                elif t == "app_done":
                    _dn = {"done": True, "text": m.get("text") or ""}
                    if m.get("spoken"):   # 브리핑처럼 헬퍼가 읽어줄 문장을 따로 만들어 준 경우(2026-11-23)
                        _dn["spoken"] = str(m.get("spoken"))[:2000]; _dn["spoken_made"] = True
                    try: _sq.put_nowait(_dn)
                    except Exception: pass
                elif t == "answer":
                    # 읽어줄 문장(답의 앞부분) → 앱에도 전달. 앱이 소리로 읽을 때 이 문장을 그대로 쓴다(따로 요약하지 않음). 2026-11-23
                    try: _sq.put_nowait({"spoken": str(m.get("text") or "")[:600]})
                    except Exception: pass
                elif t == "error":
                    try: _sq.put_nowait({"done": True, "text": "", "error": (m.get("msg") or "error")})
                    except Exception: pass
            elif t == "app_status":
                # 안경에서 한 질문(REQ_SID 에 세션이 있음): 헬퍼의 진행 단계(법령 조회·답변 작성 등)를 안경에 전달 (2026-11-20)
                _gsid = REQ_SID.get(m.get("id"))
                _gws = PRODUCERS.get(_gsid) if _gsid else None
                if _gws is not None:
                    try:
                        await _gws.send_json({"ai_status": str(m.get("text") or "")[:40]})
                    except Exception:
                        pass
                await watch_send(uid, {"status": str(m.get("text") or "")[:40]})   # 워치에도 진행 단계
            elif t == "glasses_image":
                # 안경·워치에서 한 질문의 CCTV 사진. data = 안경용 288x144·4비트 회색조(두 픽셀=1바이트 base64),
                # jpeg = 워치용 작은 컬러 사진(base64). 안경엔 회색조, 워치엔 둘 다 보낸다(워치가 컬러를 우선 사용).
                _gsid = REQ_SID.get(m.get("id"))
                _gws = PRODUCERS.get(_gsid) if _gsid else None
                _gd = str(m.get("data") or "")
                if len(_gd) > 40000:
                    _gd = ""
                _gj = str(m.get("jpeg") or "")
                if len(_gj) > 200000:
                    _gj = ""
                _glabel = str(m.get("label") or "")[:30]
                _gw, _gh = int(m.get("w") or 288), int(m.get("h") or 144)
                if _gws is not None and _gd:
                    try:
                        await _gws.send_json({"image": {"w": _gw, "h": _gh, "label": _glabel, "data": _gd}})
                    except Exception:
                        pass
                if _gd or _gj:
                    await watch_send(uid, {"image": {"w": _gw, "h": _gh, "label": _glabel,
                                                     "jpeg": _gj, "data": ("" if _gj else _gd)}})
            if t in ("answer", "error"):
                if t == "answer" and m.get("say_follows") and m.get("id") in WATCH_SAY:
                    WATCH_SAY_FLAG.add(m.get("id"))
                fut = HELPER_PENDING.get(m.get("id"))
                if fut is not None and not fut.done():
                    fut.set_result(m.get("text") if t == "answer" else "")
            elif t == "answer_full":
                _ff = HELPER_PENDING_FULL.get(m.get("id"))
                if _ff is not None and not _ff.done():
                    _ff.set_result(m.get("text") or "")
                # 전체 마크다운 답(part2) → 앱 실시간보기(/view)로만 중계. 안경 하드웨어엔 보내지 않음(짧은 답 유지).
                _sid = REQ_SID.get(m.get("id"))
                if _sid:
                    asyncio.create_task(broadcast(_sid, {"ai_full": {"id": m.get("id"), "text": m.get("text") or ""}}))
            elif t == "say":
                _wsf = WATCH_SAY.get(m.get("id"))
                if _wsf is not None and not _wsf.done():
                    _wsf.set_result(m.get("text") or "")
                _sid = REQ_SID.get(m.get("id"))
                if _sid:
                    asyncio.create_task(broadcast(_sid, {"say": {"id": m.get("id"), "text": m.get("text") or ""}}))
            elif t == "ping":
                try:
                    await ws.send_text(json.dumps({"type": "pong"}))
                except Exception:
                    pass
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if HELPERS.get(uid) is ws:
            HELPERS.pop(uid, None)
            HELPER_MAXV[uid] = (HELPER_MAXV.get(uid, (hv, 0.0))[0], time.time())
        # 이 헬퍼가 처리 중이던 스트림 요청은 끊김을 통지(앱이 무한 대기하지 않게)
        for _rid, _w in list(HELPER_STREAM_WS.items()):
            if _w is ws:
                _q2 = HELPER_STREAM.get(_rid)
                if _q2 is not None:
                    try: _q2.put_nowait({"done": True, "text": "", "error": "helper_disconnected"})
                    except Exception: pass


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
    st["strict"] = (ws.query_params.get("token") or "").strip() in TOKEN_UID   # 실제 페어링이 확인된 안경인가(OWNER 폴백 아님)
    st["cap"] = (ws.query_params.get("cap") or "")[:60]                          # 플러그인 기능 표시(예: appr1 = 승인 창)
    ai_on = ws.query_params.get("ai", "1") != "0"                    # AI 힌트 on/off
    _secraw = ws.query_params.get("sec")
    secs = set(x for x in (_secraw or "").split(",") if x) if _secraw is not None else None   # 섹션 필터(None=전체)
    PRODUCERS[sid] = ws   # AI 답변을 이 안경으로 푸시
    if st.get("uid"):
        GLASSES_BY_UID[st["uid"]] = sid   # 워치 원격제어(ctrl) 라우팅
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
        # 안경 질문 → 개인 AI 헬퍼(PC)가 연결돼 있으면 그쪽. 없으면 (기본) "PC 연결 확인" 안내.
        if not CUE_ON or client is None:
            return
        uid = st.get("uid") or OWNER_UID
        answer = ""
        hws = HELPERS.get(uid)
        # 개인 AI(PC) 미연결 → 기본은 API 폴백 대신 안내 카드. (AI_ALLOW_FALLBACK=1 이면 폴백)
        if hws is None and not (AI_ALLOW_FALLBACK and AI_URL):
            try:
                await ws.send_json({"card": {"type": "answer", "title": "개인 AI 미연결",
                    "body": "이 PC(개인 AI)가 연결되지 않았어요. PC가 켜져 있고 FireHelper AI가 실행 중인지 확인하세요.", "src": ""}})
            except Exception:
                pass
            return
        # ① 개인 AI 헬퍼(아웃바운드 WS)
        if hws is not None:
            try:
                rid = uuid.uuid4().hex
                REQ_SID[rid] = sid
                async def _drop_reqsid(_r=rid):
                    await asyncio.sleep(200); REQ_SID.pop(_r, None)   # 전체답변(answer_full) 중계까지 rid→sid 유지
                asyncio.create_task(_drop_reqsid())
                fut = asyncio.get_event_loop().create_future()
                HELPER_PENDING[rid] = fut
                try:
                    await hws.send_text(json.dumps({"type": "ask", "id": rid, "q": question + "\n\n(반드시 한국어로, 핵심만 2~4문장 간결하게.)"}, ensure_ascii=False))
                    answer = (await asyncio.wait_for(fut, timeout=90.0)) or ""
                finally:
                    HELPER_PENDING.pop(rid, None)
            except Exception:
                answer = ""
        # ② (옵션) API 폴백 — AI_ALLOW_FALLBACK=1 일 때만
        if not answer and AI_ALLOW_FALLBACK and AI_URL:
            try:
                r = await client.post(AI_URL, headers=CONV_HEADERS, json={
                    "message": question, "uid": uid,
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
            except Exception as e:
                try:
                    await ws.send_json({"cue_err": str(e)[:120]})
                except Exception:
                    pass
                return
        answer = (answer or "").strip()
        if not answer:
            try:
                await ws.send_json({"cue_err": "AI 답변 비어있음"})
            except Exception:
                pass
            await watch_send(uid, {"answer": {"q": question, "text": "", "ok": False}})
            return
        await watch_send(uid, {"answer": {"q": question, "text": clean_md(answer), "ok": True}})   # 워치에 답(글자·음성)
        at_iso = now_iso()
        title = (question[:30] + "…") if len(question) > 30 else (question or "AI 답변")
        st["cues"].append({"type": "answer", "title": title, "body": answer, "q": question, "src": "", "at": at_iso, "anchor": at_iso})
        # '작업 …' 접수 안내는 결과 카드가 따로 오므로 카드 목록에 쌓지 않는다(transient = 잠깐 보였다 사라짐)
        _is_ack = question.lstrip().startswith("작업") and ("접수" in answer[:60])
        _card = {"type": "answer", "title": title, "body": clean_md(answer), "src": ""}
        if _is_ack and "appr" in str(st.get("cap") or ""):
            _card["transient"] = True
        try:
            await ws.send_json({"card": _card})
        except Exception:
            pass
        await broadcast(sid, {"ai": {"q": question, "a": answer}})
        await save_session(client, sid, st, False)

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
                if secs is not None and c.get("type") != "answer" and TYPE_SEC.get(c.get("type"), "사내자료") not in secs:
                    continue   # 섹션 필터
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

    async def fetch_fast_cards(latest: str, at_iso: str):
        # 빠른 레인: 화재안전기준 '정의' 매칭 → 즉시 용어 카드. 정의는 맥락 무관하게 항상 맞음. LLM 없음.
        if client is None:
            return
        await load_defs(client)
        if not _DEF_TERMS:
            return
        if not _DEF_ASK.search(latest):   # 용어가 나와도 '물어볼 때/헷갈릴 때'만 정의 표시
            return
        shown = _FAST_SHOWN.setdefault(sid, set())
        hits = [t for t in _DEF_TERMS if t in latest and t not in shown][:2]
        if not hits:
            return
        for term in hits:
            d = _DEFS.get(term)
            if not d:
                continue
            shown.add(term)
            card = {"type": "concept", "title": term[:22], "body": d["body"], "src": d["src"]}
            st["cues"].append({**card, "at": at_iso, "anchor": at_iso})
            try:
                await ws.send_json({"card": card})
            except Exception:
                pass
            await broadcast(sid, {"card": card})

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

    async def fetch_notifs():
        # 안경 '알림목록' 뷰 요청(NOTIFS) → conversate로 내 알림 조회 → 안경에 내려줌
        if not CUE_ON or client is None:
            try: await ws.send_json({"notifs": []})
            except Exception: pass
            return
        try:
            r = await client.post(CONV_URL, headers=CONV_HEADERS, json={
                "action": "glasses_notifications", "uid": st.get("uid") or OWNER_UID,
            })
            d = r.json()
            await ws.send_json({"notifs": d.get("notifs") or []})
        except Exception as e:
            try: await ws.send_json({"notifs": [], "notif_err": str(e)[:120]})
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
                        await watch_send(st.get("uid") or OWNER_UID, {"asked": latest})   # 워치에도
                        asyncio.create_task(fetch_ai_answer(latest))   # 서버가 직접 AI 호출(카드 없이)
                    else:
                        core = "".join(ch for ch in latest if ch.isalnum())
                        if CUE_ON and ai_on and len(core) >= 4:
                            asyncio.create_task(fetch_cue(latest, at_iso))   # 전부 맥락 AI(빠른 레인 비활성)
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
                            elif tt == "NOTIFS":
                                # 안경 알림목록 뷰 → 내 알림 조회해 안경에 전달
                                asyncio.create_task(fetch_notifs())
                            elif tt.startswith("BAT "):
                                # 안경 플러그인이 알려 주는 배터리: "BAT <안경 %> <폰 %>" (모르면 -1) → 워치 어두운 화면에 표시
                                _bp = tt.split()
                                try:
                                    _bg = int(_bp[1]); _bph = int(_bp[2])
                                    st["bat"] = {"g": _bg if 0 <= _bg <= 100 else -1, "p": _bph if 0 <= _bph <= 100 else -1}
                                    await watch_send(st.get("uid") or OWNER_UID, {"bat": st["bat"]})
                                except Exception:
                                    pass
                            elif tt.startswith("APPR "):
                                # 안경 승인 창: "APPR <승인요청 id> approve|deny"
                                _pp = t.strip().split()
                                if len(_pp) == 3:
                                    asyncio.create_task(handle_appr(st, ws, _pp[1], _pp[2].lower()))
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
                        if text:
                            last_seg["t"] = time.time()   # 발화끝: interim 포함 리셋(말하는 중 안 끊김)
                        if is_final:
                            utter.append(text)
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
            _u = st.get("uid")
            if _u and GLASSES_BY_UID.get(_u) == sid:
                GLASSES_BY_UID.pop(_u, None)
                try:   # 안경 연결이 끊기면 워치의 안경·폰 배터리 표시를 숨긴다
                    await watch_send(_u, {"bat": {"g": -1, "p": -1}})
                except Exception:
                    pass
        if end_requested["v"]:
            SESSIONS.pop(sid, None)
        try:
            await ws.close()
        except Exception:
            pass

# firehelper-stt (Render 배포)

플러그인 ↔ Deepgram 스트리밍 STT 프록시. (FastAPI WebSocket)

## 배포 (Render)
1. 이 폴더를 깃 레포로 올리거나, 기존 레포 하위폴더로 커밋.
2. Render → New → Web Service → 레포 선택 (root = 이 폴더).
   - Runtime: Python 3
   - Build: `pip install -r requirements.txt`
   - Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - Region: Singapore, Plan: Free (테스트) — 15분 미사용 시 잠듦(첫 접속 ~30s)
3. Environment → **DEEPGRAM_API_KEY** = (Deepgram 콘솔에서 발급한 키)
4. 배포 후 URL 예: `https://firehelper-stt.onrender.com`
   - 헬스체크: 브라우저로 그 URL 열면 `{"ok":true,"has_key":true}`
   - 플러그인 접속 주소: `wss://firehelper-stt.onrender.com/stt`

## Deepgram
- deepgram.com 가입 → 무료 $200 크레딧 → API Keys → 키 생성 → 위 환경변수에.
- 설정: nova-2, language=ko, linear16 16kHz mono (플러그인 PCM 포맷과 일치).

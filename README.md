# KBO 데이터 크롤러

이 저장소에는 한국야구위원회(KBO) 웹사이트에서 2021-2025 시즌의 타자, 투수, 수비 기록을 크롤링하여
CSV로 저장하는 간단한 파이썬 스크립트가 포함되어 있습니다.

파일 구조

- scripts/crawl_kbo.py  - 크롤러 메인 스크립트
- requirements.txt      - 필요한 패키지
- data/                 - 스크립트 실행 시 생성되는 데이터 디렉터리
  - 2021/
    - Doosan/
      - hitter.csv
      - pitcher.csv
      - defense.csv
    - LG/
    - ...

사용 방법

1. 가상환경 생성(권장)

   python -m venv .venv
   .venv\Scripts\activate

2. 패키지 설치

   pip install -r requirements.txt

3. 크롤러 실행

   python scripts\crawl_kbo.py

설정과 가정

- 스크립트는 구단 폴더명을 영문으로 `TEAMS` 리스트에 정의합니다. 필요시 수정하세요.
- 웹사이트 구조(특히 select 요소의 name/value)가 변경되면 스크립트가 실패할 수 있습니다.
- 네트워크 요청 제한을 고려해 실행 속도를 조절하세요.

다음 단계 제안

- 병렬화 및 재시도 로직 추가
- 로깅을 파일로 남기기
- 특정 시즌/구단만 선택해 크롤링하도록 CLI 옵션 추가

#!/usr/bin/env python3
"""
Add `GAME_ID` column to hitter/pitcher daily files by matching:

- daily.GAME_DATE  ↔  Games_all.GAME_DATE
- daily.OPPONENT   ↔  Games_all.TEAM_AWAY or TEAM_HOME

매칭되면 해당 Games_all 행의 GAME_ID를 daily 파일의 첫 번째 컬럼(GAME_ID)으로 추가한다.

전제:
- 통합된 Games_all 파일: data_etl/all_years_Games_all.csv
  - 주요 컬럼: GAME_ID, GAME_DATE, TEAM_AWAY, TEAM_HOME

- daily 파일들:
  - data_etl/all_years_hitter_daily.csv
  - data_etl/all_years_pitcher_daily.csv
  - data/{year}/player_stats/{year}_hitter_daily.csv
  - data/{year}/player_stats/{year}_pitcher_daily.csv

Usage:
    python scripts/add_game_id_to_daily.py
"""

import os
import csv
from datetime import datetime

# 프로젝트 루트 경로 (현재 스크립트 기준 상위 폴더)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# 통합 Games_all 파일
PTH_ALL_GAMES = os.path.join(ROOT, 'data_etl', 'all_years_Games_all.csv')

# 통합 daily 파일
PTH_ALL_HITTER = os.path.join(ROOT, 'data_etl', 'all_years_hitter_daily.csv')
PTH_ALL_PITCHER = os.path.join(ROOT, 'data_etl', 'all_years_pitcher_daily.csv')

# 연도별 처리용
YEARS = [2021, 2022, 2023, 2024, 2025]


def normalize_date(s: str) -> str:
    """
    GAME_DATE 문자열을 키로 쓰기 위해 정규화하는 함수.

    - 기본 전략:
      1) 양 끝 공백 제거
      2) ISO 형식(YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS 등)이면
         date 부분만 뽑아서 YYYY-MM-DD 로 통일
      3) 실패하면 원문 그대로 사용 (daily와 games_all이 같은 형식이면 그대로 매칭 가능)

    이 함수는 games_all 쪽과 daily 쪽 모두에 동일하게 적용되어야 한다.
    """
    if s is None:
        return ''
    s = str(s).strip()
    if not s:
        return ''
    try:
        # fromisoformat은 "YYYY-MM-DD" 또는 "YYYY-MM-DDTHH:MM:SS" 등 지원
        dt = datetime.fromisoformat(s)
        return dt.date().isoformat()
    except Exception:
        # 파싱 실패 시 원본 문자열로 매칭 시도
        return s


def build_games_index_by_date_and_team(games_path):
    """
    all_years_Games_all.csv 파일에서 다음과 같은 인덱스를 구축한다.

    key:   (norm_game_date, team_name)
           - norm_game_date = normalize_date(GAME_DATE)
           - team_name = TEAM_AWAY 또는 TEAM_HOME (좌우 공백 제거)

    value: GAME_ID

    같은 (날짜, 팀) 조합이 여러 번 등장할 경우,
    가장 먼저 등장한 GAME_ID를 사용한다. (더블헤더 등은 단순 매칭)
    """
    index = {}

    if not os.path.exists(games_path):
        print(f"[WARN] Games_all file not found: {games_path}")
        return index

    with open(games_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for r in reader:
            # GAME_ID 컬럼 이름은 대문자 기준으로 우선, 소문자도 백업으로 지원
            game_id = r.get('GAME_ID') or r.get('game_id')
            if not game_id:
                continue

            # GAME_DATE 컬럼에서 날짜를 읽고 정규화
            game_date_raw = r.get('GAME_DATE') or r.get('game_date') or r.get('date')
            if not game_date_raw:
                continue
            date_key = normalize_date(game_date_raw)
            if not date_key:
                continue

            # TEAM_AWAY / TEAM_HOME 을 모두 인덱스로 등록
            for col in ('TEAM_AWAY', 'team_away', 'TEAM_HOME', 'team_home'):
                team = r.get(col)
                if not team:
                    continue
                team_key = str(team).strip()
                if not team_key:
                    continue

                key = (date_key, team_key)
                # 이미 동일 key가 있으면 첫 값 유지 (overwrite 방지)
                if key not in index:
                    index[key] = game_id

    print(f"[INFO] Built games index with {len(index)} (date, team) entries.")
    return index


def process_daily_file(path, games_index):
    """
    단일 daily CSV 파일에 대해 GAME_ID 컬럼을 추가하는 함수.

    매칭 규칙:
    - daily의 날짜:   GAME_DATE / game_date / date / 일자 중 하나
    - daily의 상대팀: OPPONENT / opponent / 상대 중 하나
    - 키: (normalize_date(daily_date), opponent)

    games_index[(날짜, 상대팀)] 에서 GAME_ID를 찾아
    daily 파일의 첫 번째 컬럼인 GAME_ID 로 기록한다.

    이미 GAME_ID 또는 game_id 컬럼이 존재하면 해당 파일은 스킵한다.
    """
    if not os.path.exists(path):
        return 0

    tmp_path = path + '.tmp'
    rows_processed = 0

    with open(path, newline='', encoding='utf-8-sig') as fr, \
            open(tmp_path, 'w', newline='', encoding='utf-8-sig') as fw:

        reader = csv.DictReader(fr)
        fieldnames = reader.fieldnames.copy() if reader.fieldnames else []

        # 이미 GAME_ID / game_id 컬럼이 있으면 재처리하지 않음
        if 'GAME_ID' in fieldnames or 'game_id' in fieldnames:
            print(f"[SKIP] {path} already has GAME_ID/game_id column.")
            return 0

        # GAME_ID를 맨 앞 컬럼으로 추가
        out_fields = ['GAME_ID'] + fieldnames
        writer = csv.DictWriter(fw, fieldnames=out_fields)
        writer.writeheader()

        for r in reader:
            # 날짜 컬럼 추출 (우선순위: GAME_DATE)
            date_s = (
                r.get('GAME_DATE')
                or r.get('game_date')
                or r.get('date')
                or r.get('일자')
            )

            # 상대팀 컬럼 추출 (우선순위: OPPONENT)
            opp = (
                r.get('OPPONENT')
                or r.get('opponent')
                or r.get('상대')
                or ''
            )

            game_id = ''

            # 날짜와 상대팀이 둘 다 있어야 매칭 시도
            if date_s and opp:
                date_key = normalize_date(date_s)
                opp_key = str(opp).strip()
                if date_key and opp_key:
                    game_id = games_index.get((date_key, opp_key), '')

            # 기존 row에 GAME_ID 추가
            newrow = {k: v for k, v in r.items()}
            newrow['GAME_ID'] = game_id

            # 필드 순서를 ['GAME_ID'] + 기존 필드 순서로 맞춰서 기록
            ordered = {fn: newrow.get(fn, '') for fn in out_fields}
            writer.writerow(ordered)
            rows_processed += 1

    # 임시 파일로 원본 파일 교체
    os.replace(tmp_path, path)
    return rows_processed


def main():
    # 1) 통합 Games_all 파일에서 (GAME_DATE, 팀) → GAME_ID 인덱스 생성
    games_index = build_games_index_by_date_and_team(PTH_ALL_GAMES)

    total = 0

    # 2) 통합 daily 파일(타자/투수)을 먼저 처리
    for fn in (PTH_ALL_HITTER, PTH_ALL_PITCHER):
        n = process_daily_file(fn, games_index)
        print(f'Processed {n} rows for {fn}')
        total += n

    # 3) 연도별 daily 파일도 동일한 인덱스로 처리
    for y in YEARS:
        for kind in ('hitter', 'pitcher'):
            fn = os.path.join(
                ROOT, 'data', str(y), 'player_stats', f'{y}_{kind}_daily.csv'
            )
            n = process_daily_file(fn, games_index)
            print(f'Processed {n} rows for {fn}')
            total += n

    print('Done. Total rows processed:', total)


if __name__ == '__main__':
    main()

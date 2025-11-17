#!/usr/bin/env python3
"""
KBO 선수 일자별 기록 크롤러

설명:
 - `data/player_info/player_attributes.csv`의 `선수_ID`와 `포지션`을 사용해
   각 선수의 일자별 기록 페이지를 크롤링합니다.
 - 포지션에 '투수'가 포함된 선수는 투수 페이지로, 아닌 선수는 타자 페이지로 처리합니다.
 - 타자: 페이지 내 `class="player_records"`의 모든 `table.tbl-type02.tbl-type02-pd0`의 tbody를 읽습니다.
 - 투수: 같은 컨테이너의 모든 `table.tbl-type02`의 tbody를 읽습니다.
 - 결과는 연도별로 합쳐 `data/<year>/player_stats/<year>_hitter_daily.csv` 및
   `data/<year>/player_stats/<year>_pitcher_daily.csv`로 저장합니다.

주의: 사이트 구조 또는 ASP.NET 폼 동작이 변경되면 동작하지 않을 수 있습니다.
"""
import os
import time
import logging
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DEBUG_DIR = os.path.join(BASE_DIR, 'debug')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

YEARS = list(range(2021, 2026))

# canonical headers
TARGET_HITTER_HEADERS = [
    '일자','상대','AVG1','PA','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','HBP','SO','GDP','AVG2'
]
TARGET_PITCHER_HEADERS = [
    '일자','상대','구분','결과','ERA1','TBF','IP','H','HR','BB','HBP','SO','R','ER','ERA2'
]


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def collect_hidden_inputs(soup: BeautifulSoup) -> Dict[str, str]:
    data = {}
    for inp in soup.find_all('input'):
        name = inp.get('name')
        if not name:
            continue
        # include hidden and viewstate-like inputs
        val = inp.get('value', '')
        data[name] = val
    return data


def find_select_option_value(select: BeautifulSoup, match_text: str) -> Optional[str]:
    for opt in select.find_all('option'):
        txt = opt.get_text(strip=True)
        val = opt.get('value') or ''
        if match_text in txt or val == match_text:
            return val
    return None


def parse_tables_from_player_records(soup: BeautifulSoup, for_hitter: bool) -> List[Dict]:
    container = soup.select_one('.player_records')
    if not container:
        return []

    rows = []
    # target (canonical) headers for hitters and pitchers
    target_hitter_headers = [
        '일자','상대','AVG1','PA','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','HBP','SO','GDP','AVG2'
    ]
    target_pitcher_headers = [
        '일자','상대','구분','결과','ERA1','TBF','IP','H','HR','BB','HBP','SO','R','ER','ERA2'
    ]
    # Be flexible: prefer tables with known classes but fall back to any table
    all_tables = container.find_all('table')
    tables = []
    if for_hitter:
        for t in all_tables:
            cls = ' '.join(t.get('class') or [])
            if 'tbl-type02' in cls and 'pd0' in cls:
                tables.append(t)
        if not tables:
            # fallback: any table in container
            tables = all_tables
    else:
        for t in all_tables:
            cls = ' '.join(t.get('class') or [])
            if 'tbl-type02' in cls or 'tbl' in cls:
                tables.append(t)
        if not tables:
            tables = all_tables

    # For each table, ignore thead and map tbody rows to canonical headers
    for table in tables:
        tbody = table.find('tbody')
        if not tbody:
            continue
        trs = tbody.find_all('tr')
        for tr in trs:
            cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            # skip empty rows
            if all((not c) for c in cells):
                continue
            # skip "no data" placeholder rows like '검색된 데이터가 없습니다.'
            if len(cells) == 1 and any(k in cells[0] for k in ['검색', '데이터', '없']):
                continue
            # choose canonical headers
            if for_hitter:
                headers = TARGET_HITTER_HEADERS.copy()
            else:
                headers = TARGET_PITCHER_HEADERS.copy()
            # normalize length by padding or trimming
            if len(cells) < len(headers):
                cells += [''] * (len(headers) - len(cells))
            if len(cells) > len(headers):
                cells = cells[:len(headers)]
            row = {h: c for h, c in zip(headers, cells)}
            rows.append(row)

    return rows


def crawl_player_for_year(session: requests.Session, player_id: int, year: int, for_hitter: bool) -> List[Dict]:
    base_url = 'https://www.koreabaseball.com/Record/Player/'
    if for_hitter:
        url = f"{base_url}HitterDetail/Daily.aspx?playerId={player_id}"
    else:
        url = f"{base_url}PitcherDetail/Daily.aspx?playerId={player_id}"

    logging.info('Fetching %s (year=%s)', url, year)
    r = session.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        logging.warning('Failed to GET %s: %s', url, r.status_code)
        return []

    soup = BeautifulSoup(r.text, 'lxml')

    # collect hidden inputs
    form_values = collect_hidden_inputs(soup)

    # find select controls inside .player_records
    container = soup.select_one('.player_records') or soup
    year_select = None
    series_select = None
    for s in container.find_all('select'):
        name = s.get('name') or ''
        lname = name.lower()
        if 'ddlyear' in lname or 'ddlyear' in name.lower() or 'ddlYear'.lower() in name.lower():
            year_select = s
        if 'ddlseries' in lname or 'ddlSeries'.lower() in name.lower():
            series_select = s

    # try to choose year option
    if year_select:
        chosen_year = None
        for opt in year_select.find_all('option'):
            val = opt.get('value') or ''
            txt = opt.get_text(strip=True)
            if val == str(year) or str(year) in txt:
                chosen_year = val
                break
        if chosen_year is None:
            # fallback: take first option that contains year
            chosen_year = find_select_option_value(year_select, str(year))
        if chosen_year:
            form_values[year_select.get('name')] = chosen_year

    # choose series -> 'KBO 정규시즌'
    if series_select:
        # prefer explicit value '0' (KBO 정규시즌) if available
        chosen_series = None
        for opt in series_select.find_all('option'):
            val = opt.get('value') or ''
            txt = opt.get_text(strip=True)
            if val == '0' or 'KBO 정규시즌' in txt:
                chosen_series = val
                break
        # fallback: pick first option that contains 'KBO'
        if chosen_series is None:
            for opt in series_select.find_all('option'):
                if 'KBO' in opt.get_text(strip=True):
                    chosen_series = opt.get('value') or ''
                    break
        if chosen_series is not None:
            form_values[series_select.get('name')] = chosen_series

    # POST back to apply selects; many pages accept a plain POST of hidden inputs + selects
    try:
        p = session.post(url, data=form_values, headers=HEADERS, timeout=30)
        p.raise_for_status()
    except Exception:
        logging.exception('POST failed for %s', url)
        return []

    soup2 = BeautifulSoup(p.text, 'lxml')

    rows = parse_tables_from_player_records(soup2, for_hitter=for_hitter)
    return rows


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Crawl player daily records (hitter/pitcher)')
    parser.add_argument('--year', type=int, help='Season year to crawl (e.g. 2025)')
    parser.add_argument('--sleep', type=float, default=0.6, help='Delay between requests (seconds)')
    args = parser.parse_args()

    years = YEARS if args.year is None else [args.year]

    # read player attributes
    pa_path = os.path.join(DATA_DIR, 'player_info', 'player_attributes.csv')
    if not os.path.exists(pa_path):
        logging.error('player_attributes.csv not found at %s', pa_path)
        return

    df_pa = pd.read_csv(pa_path, dtype=str)
    # normalize column names
    df_pa.columns = [c.strip() for c in df_pa.columns]
    if '선수_ID' not in df_pa.columns or '포지션' not in df_pa.columns:
        logging.error('player_attributes.csv missing required columns: 선수_ID or 포지션')
        return

    session = requests.Session()

    for year in years:
        out_dir = os.path.join(DATA_DIR, str(year), 'player_stats')
        ensure_dir(out_dir)

        # prepare log file for this year
        log_dir = os.path.join(BASE_DIR, 'log')
        ensure_dir(log_dir)
        log_path = os.path.join(log_dir, f"{year}_player_crawl_log.csv")
        log_exists = os.path.exists(log_path)
        log_file = open(log_path, 'a', newline='', encoding='utf-8-sig')
        log_writer = csv.writer(log_file)
        if not log_exists:
            log_writer.writerow(['연도', '선수_ID', '선수명', '포지션', '데이터유무'])

        # prepare output file paths
        hitter_path = os.path.join(out_dir, f"{year}_hitter_daily.csv")
        pitcher_path = os.path.join(out_dir, f"{year}_pitcher_daily.csv")

        # build list of players to crawl to report progress
        players = []
        for idx, row in df_pa.iterrows():
            pid = row.get('선수_ID')
            name = row.get('선수명', '')
            pos = row.get('포지션', '') or ''
            if pd.isna(pid) or str(pid).strip() == '':
                continue
            try:
                player_id = int(str(pid).strip())
            except Exception:
                continue
            players.append((player_id, name, pos))

        total_players = len(players)
        processed = 0
        saved_hitter = 0
        saved_pitcher = 0

        for i, (player_id, name_raw, pos_raw) in enumerate(players, start=1):
            # normalize name and pos to strings (handle NaN/float)
            name = '' if pd.isna(name_raw) else str(name_raw)
            pos = '' if pd.isna(pos_raw) else str(pos_raw)

            is_pitcher = '투수' in pos
            role = 'Pitcher' if is_pitcher else 'Hitter'
            print(f"[{i}/{total_players}] Crawling {player_id} {name} ({role})")

            data_flag = 'X'
            try:
                rows = crawl_player_for_year(session, player_id, year, for_hitter=(not is_pitcher))
                if not rows:
                    print(f"[{i}/{total_players}] No rows for {player_id}")
                    data_flag = 'X'
                else:
                    # attach player_id only
                    for r in rows:
                        r['player_id'] = player_id

                    # write rows to respective CSV immediately (append)
                    if is_pitcher:
                        df_p = pd.DataFrame(rows)
                        cols = ['player_id'] + TARGET_PITCHER_HEADERS
                        for c in cols:
                            if c not in df_p.columns:
                                df_p[c] = ''
                        df_p = df_p[cols]
                        write_header = not os.path.exists(pitcher_path)
                        df_p.to_csv(pitcher_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
                        saved_pitcher += len(df_p)
                        print(f"[{i}/{total_players}] Appended {len(df_p)} pitcher rows to {pitcher_path}")
                    else:
                        df_h = pd.DataFrame(rows)
                        cols = ['player_id'] + TARGET_HITTER_HEADERS
                        for c in cols:
                            if c not in df_h.columns:
                                df_h[c] = ''
                        df_h = df_h[cols]
                        write_header = not os.path.exists(hitter_path)
                        df_h.to_csv(hitter_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
                        saved_hitter += len(df_h)
                        print(f"[{i}/{total_players}] Appended {len(df_h)} hitter rows to {hitter_path}")

                    data_flag = 'O'
            except Exception:
                logging.exception('Error crawling player %s (%s)', player_id, name)
                data_flag = 'X'

            # write per-player crawl log (연도,선수id,선수명,포지션,데이터유무)
            pos_label = '투수' if is_pitcher else '타자'
            try:
                log_writer.writerow([year, player_id, name, pos_label, data_flag])
                log_file.flush()
            except Exception:
                logging.exception('Failed writing log for player %s', player_id)

            processed += 1
            # progress summary every 10 players
            if i % 10 == 0 or i == total_players:
                print(f"Progress: {processed}/{total_players} players processed — hitters:{saved_hitter} rows, pitchers:{saved_pitcher} rows")

            time.sleep(args.sleep)

        # close log file for this year
        try:
            log_file.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()

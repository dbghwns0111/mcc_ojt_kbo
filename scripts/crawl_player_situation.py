#!/usr/bin/env python3
"""
Crawl KBO player situation tables (Hitter/Pitcher Situation.aspx)
Saves per-year CSVs: data/<year>/player_stats/<year>_hitter_situation.csv and <year>_pitcher_situation.csv
Writes per-player log: log/<year>_player_situation_log.csv with columns Year,PlayerID,PlayerName,HasData(O/X)
"""
import os
import time
import logging
import csv
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

YEARS = list(range(2021, 2026))


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def collect_hidden_inputs(soup: BeautifulSoup) -> Dict[str, str]:
    data = {}
    for inp in soup.find_all('input'):
        name = inp.get('name')
        if not name:
            continue
        data[name] = inp.get('value', '')
    return data


def find_select_by_name(container: BeautifulSoup, name_fragment: str):
    for s in container.find_all('select'):
        name = s.get('name') or ''
        if name_fragment.lower() in name.lower():
            return s
    return None


def choose_option_value(select_tag, match_text: str) -> Optional[str]:
    if select_tag is None:
        return None
    for opt in select_tag.find_all('option'):
        val = opt.get('value') or ''
        txt = opt.get_text(strip=True)
        if val == match_text or match_text in txt:
            return val
    return None


def parse_situation_tables(soup: BeautifulSoup) -> tuple[List[str], List[Dict]]:
    """Return headers and list of row dicts (without player_id)"""
    container = soup.select_one('.player_records')
    if not container:
        return [], []
    # target containers: divs with class 'tbl-type02 mb35' that contain a table
    tables = []
    for div in container.find_all('div'):
        cls = ' '.join(div.get('class') or [])
        if 'tbl-type02' in cls and 'mb35' in cls:
            tbl = div.find('table')
            if tbl:
                tables.append(tbl)
    # fallback: if not found as div wrappers, try tables directly with both classes
    if not tables:
        for t in container.find_all('table'):
            cls = ' '.join(t.get('class') or [])
            if 'tbl-type02' in cls and 'mb35' in cls:
                tables.append(t)
    if not tables:
        return [], []

    # assume all tables have same headers; take headers from first table
    first = tables[0]
    headers = []
    thead = first.find('thead')
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all('th')]
    else:
        # try first row ths
        tbody = first.find('tbody')
        if tbody:
            first_row = tbody.find('tr')
            if first_row:
                ths = first_row.find_all('th')
                if ths:
                    headers = [th.get_text(strip=True) for th in ths]
                    # if headers were in first row, skip it when collecting data

    rows = []
    for table in tables:
        tbody = table.find('tbody')
        if not tbody:
            continue
        trs = tbody.find_all('tr')
        for tr in trs:
            # if headers were taken from first row as ths, and this row contains ths, skip
            if tr.find_all('th'):
                continue
            cells = [td.get_text(strip=True) for td in tr.find_all('td')]
            if not cells:
                continue
            # skip no-data
            if len(cells) == 1 and any(k in cells[0] for k in ['검색', '데이터', '없']):
                continue
            # if headers length differs, pad/truncate
            if headers and len(cells) < len(headers):
                cells += [''] * (len(headers) - len(cells))
            if headers and len(cells) > len(headers):
                cells = cells[:len(headers)]
            if headers:
                row = {h: c for h, c in zip(headers, cells)}
            else:
                # generic headers
                row = {f'C{i}': c for i, c in enumerate(cells)}
            rows.append(row)
    return headers, rows


def crawl_player_situation(session: requests.Session, player_id: int, year: int, for_hitter: bool) -> tuple[List[str], List[Dict]]:
    base = 'https://www.koreabaseball.com/Record/Player/'
    if for_hitter:
        url = f"{base}HitterDetail/Situation.aspx?playerId={player_id}"
    else:
        url = f"{base}PitcherDetail/Situation.aspx?playerId={player_id}"

    logging.info('Fetching situation %s (year=%s)', url, year)
    r = session.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        logging.warning('Failed to GET %s: %s', url, r.status_code)
        return [], []

    soup = BeautifulSoup(r.text, 'lxml')
    form_values = collect_hidden_inputs(soup)

    container = soup.select_one('.player_records') or soup
    year_select = find_select_by_name(container, 'ddlYear')
    series_select = find_select_by_name(container, 'ddlSeries')

    # choose year
    if year_select:
        chosen_year = choose_option_value(year_select, str(year))
        if chosen_year:
            form_values[year_select.get('name')] = chosen_year

    # choose series KBO 정규시즌 prefer value '0'
    if series_select:
        chosen_series = None
        for opt in series_select.find_all('option'):
            val = opt.get('value') or ''
            txt = opt.get_text(strip=True)
            if val == '0' or 'KBO 정규시즌' in txt:
                chosen_series = val
                break
        if chosen_series is None:
            chosen_series = choose_option_value(series_select, 'KBO')
        if chosen_series is not None:
            form_values[series_select.get('name')] = chosen_series

    try:
        p = session.post(url, data=form_values, headers=HEADERS, timeout=30)
        p.raise_for_status()
    except Exception:
        logging.exception('POST failed for %s', url)
        return [], []

    soup2 = BeautifulSoup(p.text, 'lxml')
    headers, rows = parse_situation_tables(soup2)
    return headers, rows


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, help='Season year to crawl')
    parser.add_argument('--sleep', type=float, default=0.6)
    args = parser.parse_args()

    years = YEARS if args.year is None else [args.year]

    pa_path = os.path.join(DATA_DIR, 'player_info', 'player_attributes.csv')
    if not os.path.exists(pa_path):
        logging.error('player_attributes.csv not found')
        return
    df_pa = pd.read_csv(pa_path, dtype=str)
    df_pa.columns = [c.strip() for c in df_pa.columns]
    if '선수_ID' not in df_pa.columns:
        logging.error('player_attributes missing 선수_ID')
        return

    session = requests.Session()

    for year in years:
        out_dir = os.path.join(DATA_DIR, str(year), 'player_stats')
        ensure_dir(out_dir)
        hitter_path = os.path.join(out_dir, f"{year}_hitter_situation.csv")
        pitcher_path = os.path.join(out_dir, f"{year}_pitcher_situation.csv")

        # log file
        log_dir = os.path.join(BASE_DIR, 'log')
        ensure_dir(log_dir)
        log_path = os.path.join(log_dir, f"{year}_player_situation_log.csv")
        log_exists = os.path.exists(log_path)
        log_file = open(log_path, 'a', newline='', encoding='utf-8-sig')
        log_writer = csv.writer(log_file)
        if not log_exists:
            log_writer.writerow(['연도', '선수_ID', '선수명', '포지션', '데이터유무'])

        players = []
        for _, r in df_pa.iterrows():
            pid = r.get('선수_ID')
            if pd.isna(pid):
                continue
            s = str(pid).strip()
            if not s.isdigit():
                continue
            players.append((int(s), r.get('선수명',''), r.get('포지션','')))

        total = len(players)
        processed = 0
        saved_h = 0
        saved_p = 0

        for i, (player_id, name_raw, pos_raw) in enumerate(players, start=1):
            name = '' if pd.isna(name_raw) else str(name_raw)
            pos = '' if pd.isna(pos_raw) else str(pos_raw)
            is_pitcher = '투수' in pos
            print(f"[{i}/{total}] Crawling situation {player_id} {name} ( {'P' if is_pitcher else 'H'} )")

            try:
                headers, rows = crawl_player_situation(session, player_id, year, for_hitter=(not is_pitcher))
                if not rows:
                    pos_label = '투수' if is_pitcher else '타자'
                    log_writer.writerow([year, player_id, name, pos_label, 'X'])
                    log_file.flush()
                    print(f"[{i}/{total}] No situation rows for {player_id}")
                else:
                    # prepare df
                    df_rows = pd.DataFrame(rows)
                    cols = headers if headers else list(df_rows.columns)
                    # ensure order and add player_id
                    out_cols = ['player_id'] + cols
                    df_rows = df_rows.reindex(columns=cols)
                    df_rows.insert(0, 'player_id', player_id)

                    # fill missing cols if file exists? assume consistent
                    write_header = not os.path.exists(hitter_path if not is_pitcher else pitcher_path)
                    if is_pitcher:
                        df_rows.to_csv(pitcher_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
                        saved_p += len(df_rows)
                    else:
                        df_rows.to_csv(hitter_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
                        saved_h += len(df_rows)

                    pos_label = '투수' if is_pitcher else '타자'
                    log_writer.writerow([year, player_id, name, pos_label, 'O'])
                    log_file.flush()
                    print(f"[{i}/{total}] Saved {len(df_rows)} rows for {player_id}")
            except Exception:
                logging.exception('Error crawling situation for player %s', player_id)
                pos_label = '투수' if is_pitcher else '타자'
                log_writer.writerow([year, player_id, name, pos_label, 'X'])
                log_file.flush()

            processed += 1
            if i % 10 == 0 or i == total:
                print(f"Progress: {processed}/{total} — hitters:{saved_h} rows, pitchers:{saved_p} rows")

            time.sleep(args.sleep)

        try:
            log_file.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()

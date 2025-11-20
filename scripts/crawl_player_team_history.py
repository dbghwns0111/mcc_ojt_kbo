#!/usr/bin/env python3
"""Crawl player team history for players listed in data/player_info/player_attributes.csv

For each player_id the script visits:
- Fetch the HitterDetail/Total.aspx page
- Select the option whose text contains 'KBO 정규시즌' in the series dropdown
- Submit the form (GET or POST fallback) to retrieve the table with class 'tbl tt mb5'
- From the table's tbody rows, collect rows where the first td is a year between 2021 and 2025
  and take the second td as the team for that row.

Output: `data/player_info/player_team_history.csv` with columns:
  player_id, name, team, start_date, end_date

Rules:
- Consecutive identical teams across full years are merged into a single interval
- If a given year has two rows (trade mid-season), the first gets end_date year-06-30,
  the second gets start_date year-07-01
"""
import os
import time
import csv
import logging
import glob
from datetime import datetime, date, timedelta

import requests
from bs4 import BeautifulSoup
import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PLAYER_ATTR = os.path.join(ROOT, 'data', 'player_info', 'player_attributes.csv')
OUT_PATH = os.path.join(ROOT, 'data', 'player_info', 'player_team_history.csv')
BASE_URL = 'https://www.koreabaseball.com/Record/Player/HitterDetail/Total.aspx'


def read_players(path):
    df = pd.read_csv(path, dtype=str, encoding='utf-8-sig')
    # detect id and name columns
    id_col = None
    name_col = None
    for c in df.columns:
        cc = c.strip().lower()
        if 'id' in cc:
            id_col = c
        if '명' in c or 'name' in cc:
            name_col = c
    if not id_col:
        raise SystemExit('Cannot find id column in player attributes')
    if not name_col:
        name_col = df.columns[1] if len(df.columns) > 1 else None

    players = []
    for _, row in df.iterrows():
        pid = str(row.get(id_col, '')).strip()
        name = str(row.get(name_col, '')).strip() if name_col else ''
        if pid == '' or pid.lower() == 'nan':
            continue
        # normalize numeric ids like '55460.0' -> '55460'
        try:
            if '.' in pid:
                pid = str(int(float(pid)))
            else:
                pid = str(int(pid))
        except Exception:
            # keep as-is
            pid = pid
        players.append((pid, name))
    return players


def get_series_value(soup):
    # find select element name containing ddlSeries
    sel = soup.find('select', attrs={'name': lambda x: x and 'ddlSeries' in x})
    if not sel:
        # try common name
        sel = soup.find('select', id=lambda x: x and 'ddlSeries' in x)
    if not sel:
        return None, None
    name = sel.get('name')
    # find option whose text contains 'KBO 정규시즌'
    for opt in sel.find_all('option'):
        if opt.text and 'KBO' in opt.text and '정규' in opt.text:
            return name, opt.get('value')
    # fallback: try option text contains '정규시즌'
    for opt in sel.find_all('option'):
        if opt.text and '정규시즌' in opt.text:
            return name, opt.get('value')
    return name, None


def find_table_rows(soup):
    # find table with class 'tbl tt mb5'
    tables = soup.find_all('table', class_=lambda x: x and 'tbl' in x and 'tt' in x)
    for tbl in tables:
        # ensure there's tbody
        tbody = tbl.find('tbody')
        if not tbody:
            continue
        rows = tbody.find_all('tr')
        if rows:
            return rows
    return []


def parse_rows_for_years(rows, years=(2021, 2022, 2023, 2024, 2025)):
    # returns list of (year:int, team:str) in order of appearance
    result = []
    target = set(int(y) for y in years)
    for tr in rows:
        tds = tr.find_all(['td', 'th'])
        if not tds or len(tds) < 2:
            continue
        year_text = tds[0].get_text(strip=True)
        # extract leading 4-digit year
        import re
        m = re.search(r'(\d{4})', year_text)
        if not m:
            continue
        y = int(m.group(1))
        if y in target:
            team = tds[1].get_text(strip=True)
            result.append((y, team))
    return result


def split_year_entries(year, teams_list):
    """Given a year and list of teams in order for that year, produce atomic periods with start/end dates."""
    periods = []
    n = len(teams_list)
    if n == 1:
        start = date(year, 1, 1)
        end = date(year, 12, 31)
        periods.append({'year': year, 'team': teams_list[0], 'start': start, 'end': end})
        return periods

    # n >= 2: split year into n chunks. Special-case n==2 as user requested Jun30/Jul1 split.
    if n == 2:
        periods.append({'year': year, 'team': teams_list[0], 'start': date(year, 1, 1), 'end': date(year, 6, 30)})
        periods.append({'year': year, 'team': teams_list[1], 'start': date(year, 7, 1), 'end': date(year, 12, 31)})
        return periods

    # general split: divide days in year into n pieces
    is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
    days = 366 if is_leap else 365
    chunk = days // n
    starts = []
    cur = date(year, 1, 1)
    for i in range(n):
        starts.append(cur)
        # advance cur by chunk days
        cur = cur + timedelta(days=chunk)
    # ensure last ends at Dec31
    for i in range(n):
        s = starts[i]
        if i < n - 1:
            e = starts[i + 1] - timedelta(days=1)
        else:
            e = date(year, 12, 31)
        periods.append({'year': year, 'team': teams_list[i], 'start': s, 'end': e})
    return periods


def coalesce_periods(periods):
    # periods: list of dicts with keys team,start,end sorted by start
    if not periods:
        return []
    periods = sorted(periods, key=lambda x: x['start'])
    merged = [periods[0].copy()]
    for p in periods[1:]:
        last = merged[-1]
        # if same team and contiguous
        if p['team'] == last['team'] and (p['start'] - last['end']).days in (0, 1):
            # extend
            last['end'] = max(last['end'], p['end'])
        else:
            merged.append(p.copy())
    return merged


def fetch_player_history(session, pid, name, pause=0.5):
    url = f"{BASE_URL}?playerId={pid}"
    try:
        r = session.get(url, timeout=15)
    except Exception as e:
        logging.warning('Request failed for %s: %s', pid, e)
        return []
    soup = BeautifulSoup(r.text, 'lxml')

    rows = find_table_rows(soup)
    # if rows empty or do not contain years 2021-2025, try to set series
    parsed = parse_rows_for_years(rows)
    if not parsed:
        sel_name, sel_val = get_series_value(soup)
        if sel_name and sel_val:
            # try GET with param
            try:
                r2 = session.get(url + f"&{sel_name}={sel_val}", timeout=15)
                soup2 = BeautifulSoup(r2.text, 'lxml')
                rows2 = find_table_rows(soup2)
                parsed = parse_rows_for_years(rows2)
            except Exception:
                parsed = []
        # if still empty, attempt POST with hidden fields
        if not parsed and sel_name and sel_val:
            # collect hidden inputs
            form = {}
            for inp in soup.find_all('input', {'type': 'hidden'}):
                n = inp.get('name')
                v = inp.get('value', '')
                if n:
                    form[n] = v
            form[sel_name] = sel_val
            # also include eventtarget to trigger if needed
            try:
                r3 = session.post(url, data=form, timeout=20, headers={'Referer': url})
                soup3 = BeautifulSoup(r3.text, 'lxml')
                rows3 = find_table_rows(soup3)
                parsed = parse_rows_for_years(rows3)
            except Exception:
                parsed = []

    # parsed is list of (year, team)
    # organize per-year lists
    year_map = {}
    for y, team in parsed:
        year_map.setdefault(y, []).append(team)

    # build atomic periods
    atomic = []
    for y in sorted(year_map.keys()):
        parts = split_year_entries(y, year_map[y])
        atomic.extend(parts)

    # coalesce consecutive same-team periods across years
    merged = coalesce_periods(atomic)

    # format output rows
    out = []
    for m in merged:
        out.append({'player_id': pid, 'name': name, 'team': m['team'],
                    'start_date': m['start'].isoformat(), 'end_date': m['end'].isoformat()})

    time.sleep(pause)
    return out


def main():
    players = read_players(PLAYER_ATTR)
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (compatible; crawler/1.0)'})

    all_rows = []
    total = len(players)
    logging.info('Starting crawl for %d players', total)
    for i, (pid, name) in enumerate(players, 1):
        logging.info('(%d/%d) Fetching %s %s', i, total, pid, name)
        try:
            rows = fetch_player_history(session, pid, name)
            if rows:
                all_rows.extend(rows)
            else:
                logging.info('  No team rows found for %s', pid)
        except Exception as e:
            logging.warning('  Error processing %s: %s', pid, e)

    # write CSV
    if all_rows:
        keys = ['player_id', 'name', 'team', 'start_date', 'end_date']
        os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
        with open(OUT_PATH, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, keys)
            writer.writeheader()
            for r in all_rows:
                writer.writerow(r)
        logging.info('Wrote %d team-history rows to %s', len(all_rows), OUT_PATH)
    else:
        logging.info('No team history rows collected')


if __name__ == '__main__':
    main()

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
import argparse
from bs4 import BeautifulSoup
import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PLAYER_ATTR = os.path.join(ROOT, 'data', 'player_info', 'player_attributes.csv')
OUT_PATH = os.path.join(ROOT, 'data', 'player_info', 'player_team_history.csv')
# Use different base pages for pitchers vs hitters/position players
PITCHER_BASE = 'https://www.koreabaseball.com/Record/Player/PitcherDetail/Total.aspx'
HITTER_BASE = 'https://www.koreabaseball.com/Record/Player/HitterDetail/Total.aspx'


def read_players(path):
    df = pd.read_csv(path, dtype=str, encoding='utf-8-sig')
    # detect id, name and position columns
    id_col = None
    name_col = None
    pos_col = None
    for c in df.columns:
        cc = c.strip().lower()
        if 'id' in cc:
            id_col = c
        if '명' in c or 'name' in cc:
            name_col = c
        if 'position' in cc or '포지션' in cc or 'pos' in cc:
            pos_col = c
    if not id_col:
        raise SystemExit('Cannot find id column in player attributes')
    if not name_col:
        name_col = df.columns[1] if len(df.columns) > 1 else None
    if not pos_col:
        # best-effort: try common substrings
        for c in df.columns:
            if 'pos' in c.lower() or '포지' in c:
                pos_col = c

    players = []
    for _, row in df.iterrows():
        pid = str(row.get(id_col, '')).strip()
        name = str(row.get(name_col, '')).strip() if name_col else ''
        pos = str(row.get(pos_col, '')).strip() if pos_col else ''
        if pid == '' or pid.lower() == 'nan':
            continue
        # normalize numeric ids like '55460.0' -> '55460'
        try:
            if '.' in pid:
                pid = str(int(float(pid)))
            else:
                pid = str(int(pid))
        except Exception:
            pid = pid
        players.append((pid, name, pos))
    return players


def get_series_value(soup):
    # prefer select inside div.player_records
    container = soup.find('div', class_=lambda x: x and 'player_records' in x)
    sel = None
    if container:
        sel = container.find('select', attrs={'name': lambda x: x and 'ddlSeries' in x})
        if not sel:
            sel = container.find('select', id=lambda x: x and 'ddlSeries' in x)
    # fallback to global search
    if not sel:
        sel = soup.find('select', attrs={'name': lambda x: x and 'ddlSeries' in x})
        if not sel:
            sel = soup.find('select', id=lambda x: x and 'ddlSeries' in x)
    if not sel:
        return None, None, None, None
    name = sel.get('name')
    # determine currently selected option
    current_opt = sel.find('option', selected=True)
    if current_opt is None:
        # fallback to first option
        current_opt = sel.find('option')
    current_val = current_opt.get('value') if current_opt else None
    current_txt = (current_opt.text or '').strip() if current_opt else ''
    # find option whose text contains exact 'KBO 정규시즌' or both KBO and 정규
    desired_val = None
    for opt in sel.find_all('option'):
        txt = (opt.text or '').strip()
        if 'KBO 정규시즌' in txt or ('KBO' in txt and '정규' in txt):
            desired_val = opt.get('value')
            break
    # fallback: any option with '정규시즌'
    if desired_val is None:
        for opt in sel.find_all('option'):
            if opt.text and '정규시즌' in opt.text:
                desired_val = opt.get('value')
                break
    return name, desired_val, current_val, current_txt


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


def fetch_player_history(session, pid, name, base_url, pause=0.5):
    url = f"{base_url}?playerId={pid}"
    try:
        r = session.get(url, timeout=15)
    except Exception as e:
        logging.warning('Request failed for %s: %s', pid, e)
        return []
    soup = BeautifulSoup(r.text, 'lxml')

    rows = find_table_rows(soup)
    # if rows empty or do not contain years 2021-2025, try to set series
    parsed = parse_rows_for_years(rows)
    # always check series select; if current selection is not 'KBO 정규시즌' or parsed is empty,
    # explicitly postback to set the series to regular season and re-parse
    sel_name, sel_desired, sel_current, sel_text = get_series_value(soup)
    need_post = False
    if sel_name and sel_desired:
        # if current value differs from desired, we must postback
        if sel_current is None or str(sel_current) != str(sel_desired):
            need_post = True
        # also if parsed is empty, try postback anyway
        if not parsed:
            need_post = True
    if need_post and sel_name and sel_desired:
        form = {}
        for inp in soup.find_all('input', {'type': 'hidden'}):
            n = inp.get('name')
            v = inp.get('value', '')
            if n:
                form[n] = v
        form[sel_name] = sel_desired
        form['__EVENTTARGET'] = sel_name
        form['__EVENTARGUMENT'] = ''
        try:
            r2 = session.post(url, data=form, timeout=20, headers={'Referer': url})
            soup2 = BeautifulSoup(r2.text, 'lxml')
            rows2 = find_table_rows(soup2)
            parsed = parse_rows_for_years(rows2)
        except Exception:
            parsed = []

    # parsed is list of (year, team)
    # Build map year -> team, prefer the first occurrence for a year
    year_team = {}
    for y, team in parsed:
        if y not in year_team:
            year_team[y] = team

    # Consider only years 2021..2025 and group consecutive years with same team
    years = sorted([y for y in year_team.keys() if 2021 <= int(y) <= 2025])
    out = []
    if years:
        group_start = years[0]
        group_team = year_team[group_start]
        prev_year = group_start
        for y in years[1:]:
            if year_team[y] == group_team and int(y) == int(prev_year) + 1:
                # continue group
                prev_year = y
                continue
            else:
                # close previous group
                start_date = date(int(group_start), 1, 1).isoformat()
                end_date = date(int(prev_year), 12, 31).isoformat()
                out.append({'player_id': pid, 'name': name, 'team': group_team,
                            'start_date': start_date, 'end_date': end_date})
                # start new group
                group_start = y
                group_team = year_team[y]
                prev_year = y

        # close last group
        start_date = date(int(group_start), 1, 1).isoformat()
        end_date = date(int(prev_year), 12, 31).isoformat()
        out.append({'player_id': pid, 'name': name, 'team': group_team,
                    'start_date': start_date, 'end_date': end_date})

    time.sleep(pause)
    return out


def main():
    players = read_players(PLAYER_ATTR)
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (compatible; crawler/1.0)'})

    total = len(players)
    logging.info('Starting crawl for %d players', total)

    # prepare output file: if not exists, create with header
    keys = ['player_id', 'name', 'team', 'start_date', 'end_date']
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    file_exists = os.path.exists(OUT_PATH)
    if not file_exists:
        with open(OUT_PATH, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, keys)
            writer.writeheader()

    # iterate and append each player's rows immediately so file grows in real time
    # support optional test limits via environment args populated in __main__
    for i, (pid, name, pos) in enumerate(players, 1):
        logging.info('(%d/%d) Fetching %s %s (%s)', i, total, pid, name, pos)
        try:
            # decide base url by position (투수 -> pitcher page, else hitter page)
            base = PITCHER_BASE if ('투수' in str(pos)) else HITTER_BASE
            rows = fetch_player_history(session, pid, name, base)
            # if no rows found, write a placeholder so we can see progress in file
            if not rows:
                logging.info('  No team rows found for %s', pid)
                placeholder = {'player_id': pid, 'name': name, 'team': 'NO_DATA', 'start_date': '', 'end_date': ''}
                with open(OUT_PATH, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, keys)
                    writer.writerow(placeholder)
            else:
                with open(OUT_PATH, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, keys)
                    for r in rows:
                        writer.writerow(r)
        except Exception as e:
            logging.warning('  Error processing %s: %s', pid, e)


def run_from_cli():
    parser = argparse.ArgumentParser(description='Crawl player team history')
    parser.add_argument('--limit', type=int, default=0, help='Limit to first N players (0 = all)')
    parser.add_argument('--player', type=str, default=None, help='Crawl only single player_id')
    args = parser.parse_args()

    players = read_players(PLAYER_ATTR)
    if args.player:
        # find matching player (by id or name)
        filtered = [p for p in players if p[0] == args.player or p[1] == args.player]
        if not filtered:
            print(f'No player found matching {args.player}')
            return
        players = filtered
    elif args.limit and args.limit > 0:
        players = players[:args.limit]

    # call main loop but we need a local copy of session and write logic
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (compatible; crawler/1.0)'})
    total = len(players)
    logging.info('Starting crawl for %d players (cli)', total)

    keys = ['player_id', 'name', 'team', 'start_date', 'end_date']
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    if not os.path.exists(OUT_PATH):
        with open(OUT_PATH, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, keys)
            writer.writeheader()

    for i, (pid, name, pos) in enumerate(players, 1):
        logging.info('(%d/%d) Fetching %s %s (%s)', i, total, pid, name, pos)
        try:
            base = PITCHER_BASE if ('투수' in str(pos)) else HITTER_BASE
            rows = fetch_player_history(session, pid, name, base)
            if not rows:
                logging.info('  No team rows found for %s', pid)
                placeholder = {'player_id': pid, 'name': name, 'team': 'NO_DATA', 'start_date': '', 'end_date': ''}
                with open(OUT_PATH, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, keys)
                    writer.writerow(placeholder)
            else:
                with open(OUT_PATH, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, keys)
                    for r in rows:
                        writer.writerow(r)
        except Exception as e:
            logging.warning('  Error processing %s: %s', pid, e)


if __name__ == '__main__':
    run_from_cli()

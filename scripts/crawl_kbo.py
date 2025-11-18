#!/usr/bin/env python3
"""
KBO 데이터 크롤러

사용법:
    python scripts\crawl_kbo.py

설명: 2021-2025 시즌의 타자, 투수, 수비 데이터를 크롤링하여
`data/<year>/<Team>/` 폴더에 각각 hitter.csv, pitcher.csv, defense.csv 로 저장합니다.

주의: 한국야구위원회 웹사이트의 구조 변경에 의해 동작하지 않을 수 있습니다.
      네트워크 환경에서 직접 실행해야 하며, 요청 제한을 고려해 속도를 조절하세요.
"""
import os
import time
import logging
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUT_BASE = os.path.join(BASE_DIR, 'data')

YEARS = list(range(2021, 2026))
# 10개 구단 영문명(폴더명으로 사용)
TEAMS = [
    'Doosan', 'LG', 'KT', 'Samsung', 'Kiwoom',
    'SSG', 'Lotte', 'NC', 'Hanwha', 'KIA'
]

# English -> Korean mapping used for human-readable fallback
TEAM_NAME_KOR = {
    'Doosan': '두산',
    'LG': 'LG',
    'KT': 'KT',
    'Samsung': '삼성',
    'Kiwoom': '키움',
    'SSG': 'SSG',
    'Lotte': '롯데',
    'NC': 'NC',
    'Hanwha': '한화',
    'KIA': 'KIA',
}

# English folder name -> site team code (option value) used in the HTML
TEAM_CODE = {
    'Doosan': 'OB',
    'LG': 'LG',
    'KT': 'KT',
    'Samsung': 'SS',
    'Kiwoom': 'WO',
    'SSG': 'SK',
    'Lotte': 'LT',
    'NC': 'NC',
    'Hanwha': 'HH',
    'KIA': 'HT',
}

# site code -> Korean team name
CODE_TO_NAME = {code: TEAM_NAME_KOR.get(eng) for eng, code in TEAM_CODE.items()}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def find_option_values(container, target_texts):
    """container: BeautifulSoup element containing select controls
    target_texts: list of strings to find inside option.text

    Returns dict: {select_name: value}
    """
    params = {}
    selects = container.find_all('select')
    for s in selects:
        name = s.get('name')
        if not name:
            continue
        for opt in s.find_all('option'):
            txt = opt.get_text(strip=True)
            val = opt.get('value') or ''
            for target in target_texts:
                if not target:
                    continue
                # match either visible text or the option value (site uses short codes for teams)
                if target in txt or target == val:
                    params[name] = val
                    # once matched, stop checking this select
                    break
            if name in params:
                break
    return params


def build_select_params(container, year: int, team_code: str, for_hitter: bool = False):
    """Build a dict of select name -> option value by matching selects for season/team/series/situation.

    - season: set to year
    - team: set to team_code (two-letter code like 'OB')
    - if for_hitter: set series to 'KBO 정규시즌' and situation/situationDetail to desired defaults
    """
    params = {}
    selects = container.find_all('select')
    for s in selects:
        name = s.get('name')
        if not name:
            continue
        lname = name.lower()
        chosen = None

        # season select
        if 'ddlseason' in lname:
            # find option with value == year or text contains year
            for opt in s.find_all('option'):
                val = opt.get('value') or ''
                txt = opt.get_text(strip=True)
                if val == str(year) or str(year) in txt:
                    chosen = val
                    break
        # team select
        elif 'ddlteam' in lname:
            for opt in s.find_all('option'):
                val = opt.get('value') or ''
                txt = opt.get_text(strip=True)
                # match by option value (preferred) or visible text
                if val == team_code or team_code == txt or txt == TEAM_NAME_KOR.get(team_code, ''):
                    chosen = val
                    break
            # if not found by code, try matching Korean name for the team
            if not chosen:
                kor = TEAM_NAME_KOR.get(team_code)
                for opt in s.find_all('option'):
                    if kor and kor in opt.get_text(strip=True):
                        chosen = opt.get('value') or ''
                        break
        # series select (for hitters)
        elif for_hitter and 'ddlseries' in lname:
            for opt in s.find_all('option'):
                txt = opt.get_text(strip=True)
                if 'KBO 정규시즌' in txt:
                    chosen = opt.get('value') or ''
                    break
        # situation selects (for hitters)
        elif for_hitter and 'ddlsituation' in lname:
            for opt in s.find_all('option'):
                txt = opt.get_text(strip=True)
                if '경기상황별1' in txt or '경기상황별2' in txt:
                    chosen = opt.get('value') or ''
                    break

        if chosen is not None:
            params[name] = chosen

    return params


def extract_table_from_html(html):
    # pandas.read_html이 더 안정적일 때가 많음
    try:
        dfs = pd.read_html(html)
        if not dfs:
            return None
        # pick the largest table by columns
        dfs.sort(key=lambda d: d.shape[1], reverse=True)
        return dfs[0]
    except Exception:
        return None


def filter_df_by_team(df: pd.DataFrame, team_code: str) -> pd.DataFrame:
    """Return rows from df that match the given team_code (site code) or its Korean name.

    If a team column exists (contains '팀' or 'Team'), prefer exact match on Korean name then on team_code.
    If no team-like column is found, return df unchanged.
    """
    if df is None or df.empty:
        return df

    kor_name = CODE_TO_NAME.get(team_code) or TEAM_NAME_KOR.get(team_code) or ''

    # candidate column names
    candidates = [c for c in df.columns if ('팀' in c) or ('Team' in c) or ('team' in c) or c.lower().strip() == 'team']
    if not candidates:
        return df

    for col in candidates:
        series = df[col].fillna('').astype(str)
        # exact match on Korean name
        if kor_name and (series == kor_name).any():
            return df[series == kor_name].reset_index(drop=True)
        # exact match on team code (some tables may contain codes)
        if team_code and (series == team_code).any():
            return df[series == team_code].reset_index(drop=True)
        # contains fallback
        mask = series.str.contains(team_code, na=False) | (kor_name and series.str.contains(kor_name, na=False))
        if mask.any():
            return df[mask].reset_index(drop=True)

    return df


def crawl_paginated_table(session, start_url, params=None, container_selector='.compare.schItem'):
    logging.info('Crawling %s with params=%s', start_url, params)
    # initial GET to obtain form fields
    resp = session.get(start_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'lxml')

    # helper: collect hidden inputs (__VIEWSTATE, __EVENTVALIDATION, etc.)
    def collect_hidden_inputs(soup_obj):
        data = {}
        for inp in soup_obj.find_all('input', {'type': ['hidden', None]}):
            name = inp.get('name')
            if not name:
                continue
            value = inp.get('value', '')
            data[name] = value
        return data

    def extract_player_ids_from_soup(soup_obj, container_selector):
        """Return list of playerId strings ('' if not found) in the same order as tbody tr rows."""
        ids = []
        container = None
        try:
            if container_selector:
                container = soup_obj.select_one(container_selector) or soup_obj
            else:
                container = soup_obj
            # try to find the first table within the container
            table = container.find('table') if container else soup_obj.find('table')
            if not table:
                return ids
            tbody = table.find('tbody') or table
            for tr in tbody.find_all('tr'):
                tds = tr.find_all(['td', 'th'])
                if len(tds) >= 2:
                    second = tds[1]
                    a = second.find('a')
                    href = a.get('href') if a else ''
                    if href:
                        m = None
                        # try to find numeric playerId in query string
                        import re

                        m = re.search(r'playerId=(\d+)', href)
                        if m:
                            ids.append(m.group(1))
                            continue
                    # fallback: empty string
                    ids.append('')
                else:
                    ids.append('')
        except Exception:
            logging.exception('Error extracting player ids from soup')
        return ids

    base_form = collect_hidden_inputs(soup)

    # If select params were provided (e.g., season/team), POST them to get the page in the selected state
    collected_pages = []  # list of tuples (DataFrame, [player_id,...])
    if params:
        # merge hidden inputs and params into form and POST
        form = base_form.copy()
        for k, v in params.items():
            form[k] = v
        # submit form to apply selects
        r = session.post(start_url, data=form, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'lxml')
        # update base_form from the POST response
        base_form.update(collect_hidden_inputs(soup))
        df = extract_table_from_html(r.text)
        ids = extract_player_ids_from_soup(soup, container_selector)
        if df is not None:
            collected_pages.append((df, ids))
        resp = r
    else:
        # find tables on first page (no select POST needed)
        df = extract_table_from_html(resp.text)
        ids = extract_player_ids_from_soup(soup, container_selector)
        if df is not None:
            collected_pages.append((df, ids))

    # find pager
    paging = soup.select_one('.paging')
    page_events = []
    page_urls = []
    if paging:
        for a in paging.find_all('a'):
            href = a.get('href')
            if not href:
                continue
            href = href.strip()
            if href.startswith("javascript:__doPostBack"):
                # format: javascript:__doPostBack('control','arg')
                try:
                    inner = href[href.find("(") + 1: href.rfind(")")]
                    parts = [p.strip().strip("'\"") for p in inner.split(',')]
                    eventtarget = parts[0] if parts else ''
                    eventarg = parts[1] if len(parts) > 1 else ''
                    # Only collect numbered page buttons (btnNoX) to avoid duplicate first/prev/next/last posts
                    if 'btnNo' in eventtarget:
                        page_events.append(('postback', eventtarget, eventarg))
                except Exception:
                    continue
            else:
                full = urljoin(start_url, href)
                if full not in page_urls and full != resp.url:
                    page_urls.append(full)

    # fetch simple URL pages first
    for pu in page_urls:
        logging.info('Fetching page %s', pu)
        r = session.get(pu, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup_p = BeautifulSoup(r.text, 'lxml')
        dfp = extract_table_from_html(r.text)
        ids_p = extract_player_ids_from_soup(soup_p, container_selector)
        if dfp is not None:
            collected_pages.append((dfp, ids_p))
        time.sleep(1)

    # Now handle ASP.NET postback pager links by submitting form data
    seen_postbacks = set()
    for kind, target, arg in page_events:
        if kind != 'postback':
            continue
        # ensure we only post each numbered page once
        key = f"{target}|{arg}"
        if key in seen_postbacks:
            continue
        seen_postbacks.add(key)
        logging.info('Posting postback event %s %s', target, arg)
        form = base_form.copy()
        # include select params (if any) as form values
        if params:
            for k, v in params.items():
                form[k] = v
        # ASP.NET event fields
        form['__EVENTTARGET'] = target
        form['__EVENTARGUMENT'] = arg
        r = session.post(start_url, data=form, headers=HEADERS, timeout=30)
        r.raise_for_status()
        # update base_form for subsequent postbacks
        soup2 = BeautifulSoup(r.text, 'lxml')
        base_form.update(collect_hidden_inputs(soup2))
        dfp = extract_table_from_html(r.text)
        ids_p = extract_player_ids_from_soup(soup2, container_selector)
        if dfp is not None:
            collected_pages.append((dfp, ids_p))
        time.sleep(1)

    if not collected_pages:
        return None

    # For each collected page, attempt to attach player_id column if ids length matches
    processed_dfs = []
    for df_page, ids in collected_pages:
        try:
            if ids and len(ids) == len(df_page):
                df_page.insert(0, 'player_id', ids)
            else:
                # try to align by name if ids length differs
                if ids and len(ids) != len(df_page):
                    logging.warning('Mismatch between extracted player ids (%d) and table rows (%d) for %s', len(ids), len(df_page), start_url)
                # insert empty id column to preserve schema
                df_page.insert(0, 'player_id', [''] * len(df_page))
        except Exception:
            logging.exception('Failed to attach player_id for a page, inserting empty column')
            df_page.insert(0, 'player_id', [''] * len(df_page))
        processed_dfs.append(df_page)

    combined = pd.concat(processed_dfs, ignore_index=True)
    return combined


def crawl_hitter(session, year, team_code, out_path):
    url = 'https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx'
    # Desired option texts
    # initial request to pick up form/select names
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'lxml')
    container = soup.select_one('.compare.schItem') or soup
    # build params explicitly (season, team code, series, situations)
    params = build_select_params(container, year, team_code, for_hitter=True)
    df = crawl_paginated_table(session, url, params=params, container_selector='.compare.schItem')
    if df is None:
        logging.warning('No hitter data for %s %s', year, team_code)
        return
    # filter to selected team only
    df = filter_df_by_team(df, team_code)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
    logging.info('Saved hitters to %s', out_path)


def crawl_pitcher(session, year, team_code, out_path):
    url = 'https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx'
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'lxml')
    container = soup.select_one('.compare') or soup
    params = build_select_params(container, year, team_code, for_hitter=False)
    df = crawl_paginated_table(session, url, params=params, container_selector='.compare')
    if df is None:
        logging.warning('No pitcher data for %s %s', year, team_code)
        return
    df = filter_df_by_team(df, team_code)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
    logging.info('Saved pitchers to %s', out_path)


def crawl_defense(session, year, team_code, out_path):
    url = 'https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx'
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'lxml')
    container = soup.select_one('.compare') or soup
    params = build_select_params(container, year, team_code, for_hitter=False)
    # defense page may not be paginated the same way
    df = crawl_paginated_table(session, url, params=params, container_selector='.compare')
    if df is None:
        logging.warning('No defense data for %s %s', year, team_code)
        return
    df = filter_df_by_team(df, team_code)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
    logging.info('Saved defense to %s', out_path)


def crawl_runner(session, year, team_code, out_path):
    """Crawl runner (주루) statistics from Runner Basic page.

    The page uses a container with class 'compare mgt25' for selects and
    .paging for pagination. We will build select params (season/team) and
    use crawl_paginated_table to collect all pages then filter by team.
    """
    url = 'https://www.koreabaseball.com/Record/Player/Runner/Basic.aspx'
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'lxml')
    # container for selects is .compare.mgt25 per site structure
    container = soup.select_one('.compare.mgt25') or soup.select_one('.compare') or soup
    params = build_select_params(container, year, team_code, for_hitter=False)
    df = crawl_paginated_table(session, url, params=params, container_selector='.compare.mgt25')
    if df is None:
        logging.warning('No runner data for %s %s', year, team_code)
        return
    df = filter_df_by_team(df, team_code)
    # ensure parent directory exists (user requested player_stats/<Team> path)
    out_dir = os.path.join(OUT_BASE, str(year), 'player_stats', CODE_TO_NAME.get(team_code, team_code) if False else '')
    # Note: we will not use out_dir here because caller controls exact out_path; just save
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
    logging.info('Saved runner to %s', out_path)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='KBO crawler')
    parser.add_argument('--year', type=int, help='Season year to crawl (e.g. 2025)')
    parser.add_argument('--team', type=str, help='Team folder name in English (e.g. LG)')
    parser.add_argument('--only-runner', action='store_true', help='Only crawl runner (주루) stats and skip hitter/pitcher/defense')
    args = parser.parse_args()

    years = YEARS if args.year is None else [args.year]
    teams = TEAMS if args.team is None else [args.team]

    session = requests.Session()

    for year in years:
        for team in teams:
            # determine site team code and Korean name fallback
            team_code = TEAM_CODE.get(team)
            team_kor = TEAM_NAME_KOR.get(team)
            if not team_code:
                logging.warning('No site team code mapping for %s, skipping', team)
                continue

            # Save all outputs under data/<year>/player_stats/<Team>/
            out_dir = os.path.join(OUT_BASE, str(year), 'player_stats', team)
            ensure_dir(out_dir)

            hitter_path = os.path.join(out_dir, f"{year}_{team}_hitter.csv")
            pitcher_path = os.path.join(out_dir, f"{year}_{team}_pitcher.csv")
            defense_path = os.path.join(out_dir, f"{year}_{team}_defense.csv")
            runner_path = os.path.join(out_dir, f"{year}_{team}_runner.csv")

            # If only-runner flag is set, skip other crawls
            if args.only_runner:
                try:
                    crawl_runner(session, year, team_code, runner_path)
                except Exception as e:
                    logging.exception('Failed to crawl runner %s %s: %s', year, team, e)
                time.sleep(1)
                continue

            try:
                # always pass site team code to the crawl functions
                crawl_hitter(session, year, team_code, hitter_path)
            except Exception as e:
                logging.exception('Failed to crawl hitter %s %s: %s', year, team, e)
            time.sleep(1)

            try:
                crawl_pitcher(session, year, team_code, pitcher_path)
            except Exception as e:
                logging.exception('Failed to crawl pitcher %s %s: %s', year, team, e)
            time.sleep(1)

            try:
                crawl_defense(session, year, team_code, defense_path)
            except Exception as e:
                logging.exception('Failed to crawl defense %s %s: %s', year, team, e)
            time.sleep(1)

            # also crawl runner and save into same player_stats folder
            try:
                crawl_runner(session, year, team_code, runner_path)
            except Exception as e:
                logging.exception('Failed to crawl runner %s %s: %s', year, team, e)
            time.sleep(1)


if __name__ == '__main__':
    main()

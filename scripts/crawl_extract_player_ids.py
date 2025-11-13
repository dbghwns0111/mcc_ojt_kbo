#!/usr/bin/env python3
"""Extract player IDs by crawling the same pages used by `crawl_kbo.py`.

This script iterates YEARS x TEAM_CODE x roles (hitter/pitcher/defense/runner),
applies the same select POST, follows ASP.NET pager postbacks, collects page HTML,
parses anchor tags for player links and extracts numeric playerId values.

Output: data/player_ids_from_crawler.csv with columns: player_id,player_name,first_year,first_team,roles

Usage:
    python scripts\crawl_extract_player_ids.py
"""
import os
import re
import time
import logging
from collections import defaultdict

import requests
from bs4 import BeautifulSoup
import pandas as pd

# reuse constants and helper from crawl_kbo
from crawl_kbo import YEARS, TEAM_CODE, HEADERS, build_select_params

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUT_PATH = os.path.join(BASE_DIR, 'data', 'player_ids_from_crawler.csv')

ROLE_URLS = {
    'hitter': 'https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx',
    'pitcher': 'https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx',
    'defense': 'https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx',
    'runner': 'https://www.koreabaseball.com/Record/Player/Runner/Basic.aspx',
}


def collect_hidden_inputs(soup):
    data = {}
    for inp in soup.find_all('input'):
        name = inp.get('name')
        if not name:
            continue
        data[name] = inp.get('value', '')
    return data


def extract_id_from_href(href):
    if not href:
        return None
    m = re.search(r'playerid=(\d+)', href, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'pcode=(\d+)', href, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'(\d{4,7})', href)
    if m:
        return m.group(1)
    return None


def norm(s):
    return ''.join(str(s).split()).lower()


def crawl_role_year_team(session, url, year, team_code, role):
    """Return list of (player_name, player_id) found on all pages for the given selection."""
    out = []
    r = session.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'lxml')

    container = soup.select_one('.compare.schItem') or soup.select_one('.compare') or soup
    params = build_select_params(container, year, team_code, for_hitter=(role == 'hitter'))

    base_form = collect_hidden_inputs(soup)
    # submit selects
    form = base_form.copy()
    form.update(params)
    rp = session.post(url, data=form, headers=HEADERS, timeout=30)
    rp.raise_for_status()

    # collect page HTMLs (initial + pager postbacks)
    pages = [rp.text]

    # parse paging controls for ASP.NET postbacks
    sp = BeautifulSoup(rp.text, 'lxml')
    paging = sp.select_one('.paging')
    postbacks = []
    if paging:
        for a in paging.find_all('a'):
            href = a.get('href') or ''
            if href.startswith('javascript:__doPostBack') and 'btnNo' in href:
                try:
                    inner = href[href.find('(') + 1: href.rfind(')')]
                    parts = [p.strip().strip("'\"") for p in inner.split(',')]
                    eventtarget = parts[0] if parts else ''
                    eventarg = parts[1] if len(parts) > 1 else ''
                    postbacks.append((eventtarget, eventarg))
                except Exception:
                    continue

    seen = set()
    for target, arg in postbacks:
        key = f"{target}|{arg}"
        if key in seen:
            continue
        seen.add(key)
        pform = base_form.copy()
        pform.update(params)
        pform['__EVENTTARGET'] = target
        pform['__EVENTARGUMENT'] = arg
        rpp = session.post(url, data=pform, headers=HEADERS, timeout=30)
        rpp.raise_for_status()
        pages.append(rpp.text)
        # update base_form from response
        base_form.update(collect_hidden_inputs(BeautifulSoup(rpp.text, 'lxml')))
        time.sleep(0.4)

    # parse anchors
    for html in pages:
        soup_page = BeautifulSoup(html, 'lxml')
        for a in soup_page.find_all('a'):
            name = a.get_text(strip=True)
            if not name:
                continue
            href = a.get('href') or a.get('onclick') or ''
            pid = extract_id_from_href(href)
            if pid:
                out.append((name, pid))
    return out


def main():
    session = requests.Session()
    found = {}
    counts = 0
    total_tasks = len(YEARS) * len(TEAM_CODE) * len(ROLE_URLS)
    task_idx = 0
    logging.info('Starting crawl for player ids: %d years x %d teams x %d roles = %d tasks',
                 len(YEARS), len(TEAM_CODE), len(ROLE_URLS), total_tasks)

    for year in YEARS:
        for team_eng, team_code in TEAM_CODE.items():
            for role, url in ROLE_URLS.items():
                task_idx += 1
                try:
                    logging.info('[%d/%d] Crawling year=%s team=%s role=%s', task_idx, total_tasks, year, team_eng, role)
                    rows = crawl_role_year_team(session, url, year, team_code, role)
                    for name, pid in rows:
                        key = (pid, name)
                        if pid not in found:
                            found[pid] = {'player_name': name, 'first_year': year, 'first_team': team_eng, 'roles': set([role])}
                        else:
                            found[pid]['roles'].add(role)
                    counts += len(rows)
                    logging.info('  found %d anchors (unique ids so far=%d)', len(rows), len(found))
                except Exception as e:
                    logging.warning('  failed task year=%s team=%s role=%s: %s', year, team_eng, role, e)
                # polite pause
                time.sleep(0.6)

    # assemble DataFrame
    rows = []
    for pid, meta in found.items():
        rows.append({'player_id': pid, 'player_name': meta['player_name'], 'first_year': meta['first_year'],
                     'first_team': meta['first_team'], 'roles': ';'.join(sorted(meta['roles']))})

    df = pd.DataFrame(rows)
    df = df.sort_values(['player_name', 'player_id']).reset_index(drop=True)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, index=False, encoding='utf-8-sig')
    logging.info('Done. Extracted %d unique player ids (total anchors=%d). Wrote %s', len(df), counts, OUT_PATH)


if __name__ == '__main__':
    main()

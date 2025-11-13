#!/usr/bin/env python3
"""Extract player IDs from KBO player list pages using helpers in crawl_kbo.py.

This script visits hitter/pitcher/defense/runner list pages for YEARS and TEAMS,
applies the season/team selects, handles pager postbacks, and parses anchor
elements to extract playerId (from href or onclick). Results are written to:

 - data/player_ids_extracted.csv  (columns: player_id, player_name, year, team, role)
 - merged into data/player_id_map.csv (appends missing ids)

Run:
    python scripts\extract_player_ids.py
"""
import os
import time
import logging
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

from crawl_kbo import YEARS, TEAM_CODE, HEADERS, build_select_params

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUT_EXTRACT = os.path.join(BASE_DIR, 'data', 'player_ids_extracted.csv')
PLAYER_MAP = os.path.join(BASE_DIR, 'data', 'player_id_map.csv')


def extract_id_from_href(href):
    if not href:
        return None
    import re
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


def collect_from_list(session, url, params, year, team_eng, role):
    """Return list of (player_name, player_id) found for this selected page including paginated postbacks."""
    results = []

    # initial GET
    r = session.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'lxml')

    def collect_hidden_inputs(soup_obj):
        d = {}
        for inp in soup_obj.find_all('input'):
            name = inp.get('name')
            if not name:
                continue
            d[name] = inp.get('value', '')
        return d

    base_form = collect_hidden_inputs(soup)
    # POST with selects to get desired state
    form = base_form.copy()
    form.update(params)
    rp = session.post(url, data=form, headers=HEADERS, timeout=30)
    rp.raise_for_status()

    # collect pages: main plus postback pages
    pages = [rp.text]

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

    # submit postbacks
    for target, arg in postbacks:
        pform = base_form.copy()
        pform.update(params)
        pform['__EVENTTARGET'] = target
        pform['__EVENTARGUMENT'] = arg
        rpp = session.post(url, data=pform, headers=HEADERS, timeout=30)
        rpp.raise_for_status()
        pages.append(rpp.text)
        # update base_form
        base_form.update(collect_hidden_inputs(BeautifulSoup(rpp.text, 'lxml')))
        time.sleep(0.5)

    # parse anchors
    for html in pages:
        soup_p = BeautifulSoup(html, 'lxml')
        for a in soup_p.find_all('a'):
            name = a.get_text(strip=True)
            if not name:
                continue
            href = a.get('href') or a.get('onclick') or ''
            pid = extract_id_from_href(href)
            if pid:
                results.append({'player_id': pid, 'player_name': name, 'year': year, 'team': team_eng, 'role': role})

    return results


def main():
    session = requests.Session()
    all_found = []

    roles = {
        'hitter': 'https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx',
        'pitcher': 'https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx',
        'defense': 'https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx',
        'runner': 'https://www.koreabaseball.com/Record/Player/Runner/Basic.aspx',
    }

    for year in YEARS:
        for team_eng, team_code in TEAM_CODE.items():
            for role, url in roles.items():
                try:
                    logging.info('Processing year=%s team=%s role=%s', year, team_eng, role)
                    r0 = session.get(url, headers=HEADERS, timeout=30)
                    r0.raise_for_status()
                    soup0 = BeautifulSoup(r0.text, 'lxml')
                    container = soup0.select_one('.compare.schItem') or soup0.select_one('.compare') or soup0
                    params = build_select_params(container, year, team_code, for_hitter=(role=='hitter'))
                    found = collect_from_list(session, url, params, year, team_eng, role)
                    all_found.extend(found)
                    time.sleep(0.8)
                except Exception as e:
                    logging.exception('Failed %s %s %s: %s', year, team_eng, role, e)

    if not all_found:
        logging.info('No player ids found')
        return

    df = pd.DataFrame(all_found)
    # dedupe by player_id
    df = df.drop_duplicates(subset=['player_id'])
    df.to_csv(OUT_EXTRACT, index=False, encoding='utf-8-sig')
    logging.info('Wrote %d unique player ids to %s', len(df), OUT_EXTRACT)

    # merge into player_id_map.csv: append any ids not present
    if os.path.exists(PLAYER_MAP):
        df_map = pd.read_csv(PLAYER_MAP, dtype=str, encoding='utf-8-sig')
    else:
        df_map = pd.DataFrame(columns=['player_id','player_name'])

    existing_ids = set(df_map['player_id'].dropna().astype(str)) if 'player_id' in df_map.columns else set()
    to_append = df[~df['player_id'].astype(str).isin(existing_ids)].copy()
    if not to_append.empty:
        # make columns compatible
        to_append = to_append.rename(columns={'player_name': 'player_name'})
        # keep minimal columns for map
        map_rows = to_append[['player_id','player_name']]
        df_out = pd.concat([df_map, map_rows], ignore_index=True, sort=False)
        df_out.to_csv(PLAYER_MAP, index=False, encoding='utf-8-sig')
        logging.info('Appended %d new ids to %s', len(map_rows), PLAYER_MAP)
    else:
        logging.info('No new ids to append to %s', PLAYER_MAP)


if __name__ == '__main__':
    main()

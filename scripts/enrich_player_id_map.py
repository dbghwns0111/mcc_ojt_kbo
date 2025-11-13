#!/usr/bin/env python3
"""Enrich `data/player_id_map.csv` by scraping player detail pages.

For each player_id in the CSV, fetch:
  https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx?playerId=<id>

Parse the element with class `player_basic` and extract key/value pairs
from tables or dl lists (robust to table/dl variations). Merge the fields
into the existing CSV as additional columns and overwrite the CSV.

Usage:
    python scripts\enrich_player_id_map.py
"""
import os
import time
import logging
from collections import OrderedDict

import requests
from bs4 import BeautifulSoup
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
IN_PATH = os.path.join(BASE_DIR, 'data', 'player_id_map.csv')
OUT_PATH = IN_PATH

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

def parse_player_basic(elem):
    """Given a BeautifulSoup element for .player_basic, return dict of {key: value}.
    Handles table (th/td), dl (dt/dd), and fallback to rows with ':' separation.
    """
    out = OrderedDict()
    if elem is None:
        return out

    # Prefer table rows
    table = elem.find('table') if elem.find('table') else None
    if table:
        for tr in table.find_all('tr'):
            th = tr.find('th')
            td = tr.find('td')
            if th and td:
                key = th.get_text(' ', strip=True)
                val = td.get_text(' ', strip=True)
                out[key] = val
        if out:
            return out

    # Try dl lists
    dts = elem.find_all('dt')
    dds = elem.find_all('dd')
    if dts and dds and len(dts) == len(dds):
        for dt, dd in zip(dts, dds):
            key = dt.get_text(' ', strip=True)
            val = dd.get_text(' ', strip=True)
            out[key] = val
        if out:
            return out

    # Fallback: scan for label/value pairs in paragraphs or divs
    for row in elem.find_all(['p', 'li', 'div']):
        text = row.get_text(' ', strip=True)
        if ':' in text:
            parts = [p.strip() for p in text.split(':', 1)]
            if len(parts) == 2 and parts[0]:
                out[parts[0]] = parts[1]

    return out


def fetch_player_basic(session, player_id, max_retries=2, backoff=1.0):
    url = f'https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx?playerId={player_id}'
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'lxml')
            elem = soup.select_one('.player_basic')
            data = parse_player_basic(elem)
            return data
        except Exception as e:
            logging.warning('Failed fetch player %s attempt %d/%d: %s', player_id, attempt, max_retries, e)
            time.sleep(backoff * attempt)
    return {}


def main():
    if not os.path.exists(IN_PATH):
        logging.error('Input file not found: %s', IN_PATH)
        return

    df = pd.read_csv(IN_PATH, dtype=str, encoding='utf-8-sig')
    if 'player_id' not in df.columns:
        logging.error('player_id column not found in %s', IN_PATH)
        return

    session = requests.Session()
    all_keys = set()
    details_list = []

    total = len(df)
    logging.info('Starting enrichment for %d players', total)
    for idx, row in df.iterrows():
        pid = str(row['player_id']).strip()
        if not pid or pid.lower() == 'nan':
            details_list.append({})
            continue
        logging.info('(%d/%d) Fetching player_id=%s', idx + 1, total, pid)
        data = fetch_player_basic(session, pid)
        details_list.append(data)
        all_keys.update(data.keys())
        time.sleep(0.6)

    # Build details DataFrame
    details_df = pd.DataFrame(details_list)
    # Merge (left) on index alignment; keep original columns and then new keys
    out_df = pd.concat([df.reset_index(drop=True), details_df.reset_index(drop=True)], axis=1)

    # Save back
    out_df.to_csv(OUT_PATH, index=False, encoding='utf-8-sig')
    logging.info('Enrichment complete. Wrote %d rows to %s', len(out_df), OUT_PATH)


if __name__ == '__main__':
    main()

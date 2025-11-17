#!/usr/bin/env python3
"""
Crawl game-by-game box score data from KBO GameCenter Main.

Usage:
    python scripts\crawl_game_by_game.py --date 20210506

Output:
 - data/<YYYYMMDD>/game_stats/<YYYYMMDD>_games.csv
 - log/<YYYYMMDD>_gamecrawl_log.csv  (columns: date,game_id,success)

Notes:
 - Script tries several URL patterns to load a game's review/boxscore contents.
 - Fields extracted per game: game_id, game_info (record-etc text), run_T (joined cells), run_B (joined cells)
"""
import os
import re
import time
import logging
import csv
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

GAMECENTER_URL = 'https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx'


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def get_game_elements_from_main(session: requests.Session, date_str: str) -> List[Dict]:
    """Fetch main GameCenter page for date and return list of game element dicts with found game_id."""
    params = {}
    # many KBO pages accept date as query param name 'date' or 'gameDate'; try both
    # try common URL variants (main page with query, and ScoreBoard which is more static)
    try_urls = [
        GAMECENTER_URL,
        GAMECENTER_URL + f'?date={date_str}',
        GAMECENTER_URL + f'?gameDate={date_str}',
        f'https://www.koreabaseball.com/Schedule/ScoreBoard.aspx?date={date_str}',
        f'https://www.koreabaseball.com/Schedule/ScoreBoard.aspx?gameDate={date_str}',
    ]
    html = None
    for url in try_urls:
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                html = r.text
                break
        except Exception:
            logging.debug('GET failed for %s', url, exc_info=True)
    if not html:
        logging.error('Could not fetch GameCenter main page for date %s', date_str)
        return []

    soup = BeautifulSoup(html, 'lxml')
    # find today-game container
    container = soup.select_one('.today-game')
    if not container:
        logging.warning('No .today-game container found on GameCenter page')
        # save the HTML for debugging to help inspect why the container is missing
        try:
            debug_dir = os.path.join(BASE_DIR, 'debug')
            ensure_dir(debug_dir)
            debug_path = os.path.join(debug_dir, f'gamecenter_{date_str}_main.html')
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(html)
            logging.info('Saved GameCenter main HTML to %s for inspection', debug_path)
        except Exception:
            logging.exception('Failed to write debug HTML for GameCenter main')
        return []

    # inside, find bx-viewport with game items
    viewport = container.select_one('.bx-wrapper .bx-viewport')
    items = []
    if viewport:
        # possible game item elements with class game-cont and attribute g_id
        for el in viewport.find_all(class_=re.compile(r'\bgame-cont\b')):
            # try several attribute names that may contain game id
            gid = None
            for attr in ('g_id', 'data-gid', 'data-gameid', 'gid'):
                if el.has_attr(attr):
                    gid = el.get(attr)
                    break
            # sometimes g_id is inside class or onclick, look for 8+ digit number
            if not gid:
                onclick = el.get('onclick','') or el.get('data-onclick','') or ''
                m = re.search(r"(\d{6,})", onclick)
                if m:
                    gid = m.group(1)
            # also some markup uses attribute 'g_id' inside nested attrs
            if not gid:
                # try text attributes
                text_attrs = ' '.join([str(v) for v in el.attrs.values()])
                m = re.search(r"g_id\s*=\s*\"?(\d{6,})\"?", text_attrs)
                if m:
                    gid = m.group(1)
            if not gid:
                continue
            items.append({'game_element': el, 'game_id': str(gid)})
    else:
        logging.warning('No bx-viewport found under .today-game')

    return items


def fetch_game_content(session: requests.Session, game_id: str) -> Optional[str]:
    """Try several URL patterns to fetch a game's GameCenter content HTML."""
    candidates = [
        f'https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={game_id}',
        f'https://www.koreabaseball.com/Schedule/GameCenter/BoxScore.aspx?gameId={game_id}',
        f'https://www.koreabaseball.com/Schedule/GameCenter/GameCenter.aspx?gameId={game_id}',
    ]
    for url in candidates:
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and 'gameCenterContents' in r.text:
                return r.text
        except Exception:
            logging.debug('Failed fetching %s', url, exc_info=True)
    # as a last resort try POST to main with form data (ASP.NET style)
    return None


def parse_game_boxscore(html: str) -> Dict:
    """Parse provided game page HTML and extract game_info, run_T and run_B arrays."""
    soup = BeautifulSoup(html, 'lxml')
    result = {'game_info': '', 'run_T': [], 'run_B': []}
    gc = soup.select_one('#gameCenterContents')
    if not gc:
        # try alternate container
        gc = soup
    # record-etc
    rec = gc.select_one('.box-score-area .record-etc')
    if rec:
        # join lines
        txt = ' | '.join([l.strip() for l in rec.stripped_strings])
        result['game_info'] = txt
    # box-score table
    tbl = gc.select_one('.box-score-wrap .tbl-box-score.data3')
    if tbl:
        ttop = tbl.select_one('tbody.run_T')
        tbot = tbl.select_one('tbody.run_B')
        if ttop:
            # extract cell texts from first row or all rows joined by '|'
            rows = []
            for tr in ttop.find_all('tr'):
                cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                if cells:
                    rows.append('|'.join(cells))
            result['run_T'] = rows
        if tbot:
            rows = []
            for tr in tbot.find_all('tr'):
                cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                if cells:
                    rows.append('|'.join(cells))
            result['run_B'] = rows
    else:
        # fallback: locate any table with class data3
        tbl2 = gc.find('table', class_=re.compile(r'\bdata3\b'))
        if tbl2:
            ttop = tbl2.select_one('tbody.run_T')
            tbot = tbl2.select_one('tbody.run_B')
            if ttop:
                result['run_T'] = ['|'.join([td.get_text(strip=True) for td in tr.find_all('td')]) for tr in ttop.find_all('tr') if tr.find_all('td')]
            if tbot:
                result['run_B'] = ['|'.join([td.get_text(strip=True) for td in tr.find_all('td')]) for tr in tbot.find_all('tr') if tr.find_all('td')]
    return result


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, required=True, help='Date as YYYYMMDD (e.g. 20210506)')
    parser.add_argument('--sleep', type=float, default=0.6)
    args = parser.parse_args()

    date_str = args.date
    # prepare output dirs
    out_dir = os.path.join(DATA_DIR, date_str, 'game_stats')
    ensure_dir(out_dir)
    data_path = os.path.join(out_dir, f"{date_str}_games.csv")

    log_dir = os.path.join(BASE_DIR, 'log')
    ensure_dir(log_dir)
    log_path = os.path.join(log_dir, f"{date_str}_gamecrawl_log.csv")
    log_exists = os.path.exists(log_path)
    log_file = open(log_path, 'a', newline='', encoding='utf-8-sig')
    log_writer = csv.writer(log_file)
    if not log_exists:
        log_writer.writerow(['date', 'game_id', 'success'])

    session = requests.Session()

    items = get_game_elements_from_main(session, date_str)
    if not items:
        logging.error('No games found for date %s', date_str)
        log_writer.writerow([date_str, '', 'X'])
        log_file.close()
        return

    rows_out = []
    for item in items:
        gid = item.get('game_id')
        logging.info('Processing game %s', gid)
        html = fetch_game_content(session, gid)
        success = 'X'
        if html:
            parsed = parse_game_boxscore(html)
            # save one row per game with joined run_T/run_B
            rows_out.append({
                'date': date_str,
                'game_id': gid,
                'game_info': parsed.get('game_info',''),
                'run_T': ' || '.join(parsed.get('run_T',[])),
                'run_B': ' || '.join(parsed.get('run_B',[]))
            })
            success = 'O'
            log_writer.writerow([date_str, gid, success])
            log_file.flush()
        else:
            logging.warning('Could not fetch content for game %s', gid)
            log_writer.writerow([date_str, gid, success])
            log_file.flush()
        time.sleep(args.sleep)

    # write aggregated CSV
    if rows_out:
        df = pd.DataFrame(rows_out)
        df.to_csv(data_path, index=False, encoding='utf-8-sig')
        logging.info('Saved game CSV: %s (%d rows)', data_path, len(df))

    try:
        log_file.close()
    except Exception:
        pass


if __name__ == '__main__':
    main()

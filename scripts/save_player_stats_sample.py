#!/usr/bin/env python3
"""
Collect daily rows for top N players and save year CSVs.
"""
import os
import time
import logging
import importlib.util
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(ROOT, 'data')
PA_PATH = os.path.join(DATA_DIR, 'player_info', 'player_attributes.csv')

# load crawl_player_daily.py as module
spec = importlib.util.spec_from_file_location('crawl_player_daily', os.path.join(os.path.dirname(__file__), 'crawl_player_daily.py'))
cp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cp)

crawl_player_for_year = getattr(cp, 'crawl_player_for_year')
ensure_dir = getattr(cp, 'ensure_dir')


def main(year=2021, top_n=20, sleep=0.6):
    if not os.path.exists(PA_PATH):
        logging.error('player_attributes.csv not found at %s', PA_PATH)
        return
    df_pa = pd.read_csv(PA_PATH, dtype=str)
    df_pa.columns = [c.strip() for c in df_pa.columns]

    ids = []
    for _, r in df_pa.iterrows():
        pid = r.get('선수_ID')
        if pd.isna(pid):
            continue
        s = str(pid).strip()
        if not s:
            continue
        if s.isdigit():
            ids.append((int(s), r.get('선수명',''), r.get('포지션','')))
        if len(ids) >= top_n:
            break

    session = cp.requests.Session()

    out_dir = os.path.join(DATA_DIR, str(year), 'player_stats')
    ensure_dir(out_dir)

    hitter_rows = []
    pitcher_rows = []

    processed = 0
    for player_id, name, pos in ids:
        processed += 1
        is_hitter = '투수' not in (pos or '')
        logging.info('Crawling %s (%s) [%d/%d] hitter=%s', name, player_id, processed, len(ids), is_hitter)
        try:
            rows = crawl_player_for_year(session, player_id, year, for_hitter=is_hitter)
        except Exception:
            logging.exception('Error crawling player %s', player_id)
            rows = []
        if rows:
            for r in rows:
                r['player_id'] = player_id
                r['player_name'] = name
                r['season'] = year
                if is_hitter:
                    hitter_rows.append(r)
                else:
                    pitcher_rows.append(r)
        time.sleep(sleep)

    # write CSVs
    # write CSVs with fixed columns and player_id as first column
    hitter_cols = ['player_id','일자','상대','AVG1','PA','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','HBP','SO','GDP','AVG2']
    pitcher_cols = ['player_id','일자','상대','구분','결과','ERA1','TBF','IP','H','HR','BB','HBP','SO','R','ER','ERA2']

    if hitter_rows:
        df_h = pd.DataFrame(hitter_rows)
        # ensure player_id exists
        if 'player_id' not in df_h.columns:
            df_h['player_id'] = ''
        # reindex to fixed columns, add missing as empty
        for c in hitter_cols:
            if c not in df_h.columns:
                df_h[c] = ''
        df_h = df_h[hitter_cols]
        hitter_path = os.path.join(out_dir, f"{year}_hitter_daily.csv")
        df_h.to_csv(hitter_path, index=False, encoding='utf-8-sig')
        logging.info('Saved hitter CSV: %s (%d rows)', hitter_path, len(df_h))
    else:
        logging.info('No hitter rows to save for %s', year)

    if pitcher_rows:
        df_p = pd.DataFrame(pitcher_rows)
        if 'player_id' not in df_p.columns:
            df_p['player_id'] = ''
        for c in pitcher_cols:
            if c not in df_p.columns:
                df_p[c] = ''
        df_p = df_p[pitcher_cols]
        pitcher_path = os.path.join(out_dir, f"{year}_pitcher_daily.csv")
        df_p.to_csv(pitcher_path, index=False, encoding='utf-8-sig')
        logging.info('Saved pitcher CSV: %s (%d rows)', pitcher_path, len(df_p))
    else:
        logging.info('No pitcher rows to save for %s', year)


if __name__ == '__main__':
    main(year=2021, top_n=20)

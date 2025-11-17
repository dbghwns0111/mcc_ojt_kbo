#!/usr/bin/env python3
"""
Run situation crawler for top-N players for a given year (default N=20, year=2021).
This runner imports `crawl_player_situation` from `scripts/crawl_player_situation.py` using runpy and
executes it for top N players, appending results to the same CSV/log format.
"""
import os
import runpy
import argparse
import pandas as pd
import requests
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(ROOT, 'data')

# load functions from crawl_player_situation.py
mod = runpy.run_path(os.path.join(os.path.dirname(__file__), 'crawl_player_situation.py'))
crawl_fn = mod.get('crawl_player_situation')
ensure_dir = mod.get('ensure_dir')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, default=2021)
    parser.add_argument('--n', type=int, default=20)
    parser.add_argument('--sleep', type=float, default=0.6)
    args = parser.parse_args()

    pa_path = os.path.join(DATA_DIR, 'player_info', 'player_attributes.csv')
    df_pa = pd.read_csv(pa_path, dtype=str)
    df_pa.columns = [c.strip() for c in df_pa.columns]

    ids = []
    for _, r in df_pa.iterrows():
        pid = r.get('선수_ID')
        if pd.isna(pid):
            continue
        s = str(pid).strip()
        if not s.isdigit():
            continue
        ids.append((int(s), r.get('선수명',''), r.get('포지션','')))
        if len(ids) >= args.n:
            break

    session = requests.Session()

    out_dir = os.path.join(DATA_DIR, str(args.year), 'player_stats')
    ensure_dir(out_dir)
    hitter_path = os.path.join(out_dir, f"{args.year}_hitter_situation.csv")
    pitcher_path = os.path.join(out_dir, f"{args.year}_pitcher_situation.csv")

    log_dir = os.path.join(ROOT, 'log')
    ensure_dir(log_dir)
    log_path = os.path.join(log_dir, f"{args.year}_player_situation_log.csv")
    import csv
    log_exists = os.path.exists(log_path)
    log_file = open(log_path, 'a', newline='', encoding='utf-8-sig')
    log_writer = csv.writer(log_file)
    if not log_exists:
        log_writer.writerow(['연도', '선수_ID', '선수명', '포지션', '데이터유무'])

    for i, (player_id, name_raw, pos_raw) in enumerate(ids, start=1):
        name = '' if pd.isna(name_raw) else str(name_raw)
        pos = '' if pd.isna(pos_raw) else str(pos_raw)
        is_pitcher = '투수' in pos
        print(f"[{i}/{len(ids)}] Crawling situation {player_id} {name}")

        try:
            headers, rows = crawl_fn(session, player_id, args.year, for_hitter=(not is_pitcher))
            if not rows:
                pos_label = '투수' if is_pitcher else '타자'
                log_writer.writerow([args.year, player_id, name, pos_label, 'X'])
                log_file.flush()
                print(f"No rows for {player_id}")
            else:
                df_rows = pd.DataFrame(rows)
                cols = headers if headers else list(df_rows.columns)
                df_rows = df_rows.reindex(columns=cols)
                df_rows.insert(0, 'player_id', player_id)
                write_header = not os.path.exists(hitter_path if not is_pitcher else pitcher_path)
                if is_pitcher:
                    df_rows.to_csv(pitcher_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
                else:
                    df_rows.to_csv(hitter_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
                pos_label = '투수' if is_pitcher else '타자'
                log_writer.writerow([args.year, player_id, name, pos_label, 'O'])
                log_file.flush()
                print(f"Saved {len(df_rows)} rows for {player_id}")
        except Exception:
            import traceback
            traceback.print_exc()
            pos_label = '투수' if is_pitcher else '타자'
            log_writer.writerow([args.year, player_id, name, pos_label, 'X'])
            log_file.flush()

        time.sleep(args.sleep)

    try:
        log_file.close()
    except Exception:
        pass

    print('Done')

if __name__ == '__main__':
    main()

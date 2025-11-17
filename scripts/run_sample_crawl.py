#!/usr/bin/env python3
# quick runner to test crawl_player_daily functions for first N players
import os
import runpy
import requests
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(ROOT, 'data')

mod = runpy.run_path(os.path.join(os.path.dirname(__file__), 'crawl_player_daily.py'))
# mod now contains functions defined in that file
crawl_fn = mod.get('crawl_player_for_year')
ensure_dir = mod.get('ensure_dir')
TARGET_HITTER_HEADERS = mod.get('TARGET_HITTER_HEADERS')
TARGET_PITCHER_HEADERS = mod.get('TARGET_PITCHER_HEADERS')

pa_path = os.path.join(DATA_DIR, 'player_info', 'player_attributes.csv')
df = pd.read_csv(pa_path, dtype=str)
df.columns = [c.strip() for c in df.columns]

# select first 20 valid numeric player IDs
ids = []
for _, r in df.iterrows():
    pid = r.get('선수_ID')
    if pd.isna(pid):
        continue
    s = str(pid).strip()
    if not s:
        continue
    if s.isdigit():
        ids.append((int(s), r.get('선수명',''), r.get('포지션','')))
    if len(ids) >= 20:
        break

session = requests.Session()
hitter_rows = []
pitcher_rows = []

for player_id, name, pos in ids:
    is_hitter = '투수' not in (pos or '')
    print(f"Testing player {player_id} {name} hitter={is_hitter}")
    rows = crawl_fn(session, player_id, 2021, for_hitter=is_hitter)
    print(f"Found {len(rows)} rows for player {player_id}")
    # add metadata and accumulate
    for r in rows:
        # only include player_id as metadata
        r['player_id'] = player_id
        if is_hitter:
            hitter_rows.append(r)
        else:
            pitcher_rows.append(r)

# ensure output directory
out_dir = os.path.join(DATA_DIR, '2021', 'player_stats')
os.makedirs(out_dir, exist_ok=True)

if hitter_rows:
    df_h = pd.DataFrame(hitter_rows)
    # enforce canonical columns
    cols = ['player_id'] + (TARGET_HITTER_HEADERS or [])
    for c in cols:
        if c not in df_h.columns:
            df_h[c] = ''
    df_h = df_h[cols]
    hitter_path = os.path.join(out_dir, '2021_hitter_daily.csv')
    df_h.to_csv(hitter_path, index=False, encoding='utf-8-sig')
    print(f'Saved hitter CSV: {hitter_path} ({len(df_h)} rows)')
else:
    print('No hitter rows to save')

if pitcher_rows:
    df_p = pd.DataFrame(pitcher_rows)
    cols = ['player_id'] + (TARGET_PITCHER_HEADERS or [])
    for c in cols:
        if c not in df_p.columns:
            df_p[c] = ''
    df_p = df_p[cols]
    pitcher_path = os.path.join(out_dir, '2021_pitcher_daily.csv')
    df_p.to_csv(pitcher_path, index=False, encoding='utf-8-sig')
    print(f'Saved pitcher CSV: {pitcher_path} ({len(df_p)} rows)')
else:
    print('No pitcher rows to save')

print('Done')

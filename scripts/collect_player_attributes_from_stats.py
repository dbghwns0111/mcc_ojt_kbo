#!/usr/bin/env python3
"""
Collect player_id and player name from all year/team hitter,pitcher,defense,runner CSVs
and write a consolidated `data/player_info/player_attributes.csv` sorted by 선수명 (ascending).

Only rows with a non-empty player_id will be included. If multiple files provide different
names for the same id, the first non-empty name encountered is used.
"""
import os
import glob
import logging
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(ROOT, 'data')
OUT_DIR = os.path.join(ROOT, 'data', 'player_info')
OUT_PATH = os.path.join(OUT_DIR, 'player_attributes.csv')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


def read_csv_fallback(path):
    try:
        return pd.read_csv(path, dtype=str, encoding='utf-8-sig')
    except Exception:
        return pd.read_csv(path, dtype=str, encoding='cp949', errors='ignore')


def find_id_name_cols(df):
    id_col = None
    name_col = None
    for c in df.columns:
        lc = c.strip().lower()
        if lc in ('player_id', 'playerid', '선수_id', '선수_id'.lower()):
            id_col = c
        if 'id' in lc and 'player' in lc:
            id_col = id_col or c
        if lc in ('name', 'player_name', '선수명') or 'name' == lc or '선수' in c:
            # prefer exact matches
            if 'name' in lc or '선수명' in c:
                name_col = c
            else:
                name_col = name_col or c
    return id_col, name_col


def collect():

    # recursively find all player_stats CSVs under data/<year>/player_stats/**
    files = glob.glob(os.path.join(DATA_DIR, '*', 'player_stats', '**', '*.csv'), recursive=True)
    # filter only hitter/pitcher/defense/runner related files
    keep = []
    for f in files:
        fname = os.path.basename(f).lower()
        if any(k in fname for k in ('hitter', 'pitcher', 'defense', 'runner')):
            keep.append(f)
    files = sorted(set(keep))
    logging.info('Found %d files to scan for player ids (filtered by hitter/pitcher/defense/runner)', len(files))

    id_to_name = {}
    for f in files:
        logging.info('Scanning %s', f)
        try:
            df = read_csv_fallback(f)
        except Exception as e:
            logging.warning('Failed to read %s: %s', f, e)
            continue

        id_col, name_col = find_id_name_cols(df)
        if id_col is None or name_col is None:
            # try heuristics: many tables use first column as Name
            possible_names = [c for c in df.columns if 'name' in c.lower() or '선수' in c]
            if possible_names:
                name_col = name_col or possible_names[0]
            possible_ids = [c for c in df.columns if 'id' in c.lower() or 'player' in c.lower()]
            if possible_ids:
                id_col = id_col or possible_ids[0]

        if id_col is None or name_col is None:
            logging.warning('Could not detect id/name columns in %s; cols=%s', f, df.columns.tolist())
            continue

        series_id = df[id_col].fillna('').astype(str).str.strip()
        series_name = df[name_col].fillna('').astype(str).str.strip()

        for pid_raw, name in zip(series_id, series_name):
            if not pid_raw or str(pid_raw).lower() == 'nan' or pid_raw == '':
                continue
            # normalize numeric ids: remove .0 if present and ensure integer-like
            pid = str(pid_raw).strip()
            # try to extract integer portion
            try:
                if '.' in pid:
                    # convert float-like '55460.0' -> int
                    f = float(pid)
                    pid_int = int(f)
                    pid = str(pid_int)
                else:
                    # ensure it's numeric
                    pid_int = int(pid)
                    pid = str(pid_int)
            except Exception:
                # if not numeric, skip
                continue

            # keep first non-empty name observed for this id
            if pid not in id_to_name and name:
                id_to_name[pid] = name

    # Build DataFrame and sort by name (Korean order via default string sort)
    records = [(pid, nm) for pid, nm in id_to_name.items()]
    df_out = pd.DataFrame(records, columns=['선수_ID', '선수명'])
    df_out['선수명'] = df_out['선수명'].astype(str)
    df_out = df_out.sort_values(by='선수명', key=lambda s: s.str.normalize('NFKC'))

    os.makedirs(OUT_DIR, exist_ok=True)
    df_out.to_csv(OUT_PATH, index=False, encoding='utf-8-sig')
    logging.info('Wrote %d player attributes to %s', len(df_out), OUT_PATH)


if __name__ == '__main__':
    collect()

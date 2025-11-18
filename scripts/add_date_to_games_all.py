#!/usr/bin/env python3
"""Add '일자' column as first column to year Games_all CSVs.

For each file matching `data/*/game_info/*_Games_all.csv`, this script:
- reads the CSV (utf-8-sig or cp949 fallback)
- extracts the first 8 digits of `game_id` (YYYYMMDD)
- converts to `YYYY-MM-DD` and inserts as the first column named `일자`
- overwrites the CSV (utf-8-sig)

Usage:
    python scripts\add_date_to_games_all.py
"""
import os
import glob
import re
import pandas as pd


def extract_date_from_game_id(gid):
    try:
        s = str(gid)
        m = re.match(r"^(\d{8})", s)
        if not m:
            return ''
        ymd = m.group(1)
        # parse to YYYY-MM-DD
        return pd.to_datetime(ymd, format='%Y%m%d', errors='coerce').strftime('%Y-%m-%d')
    except Exception:
        return ''


def process_file(path: str) -> dict:
    info = {'path': path, 'rows': 0, 'updated': False, 'added': 0, 'errors': 0}
    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        try:
            df = pd.read_csv(path, encoding='cp949')
        except Exception as e:
            info['errors'] = 1
            info['error_msg'] = f'read_failed: {e}'
            return info

    info['rows'] = len(df)

    if 'game_id' not in df.columns:
        info['errors'] = 1
        info['error_msg'] = 'no_game_id_column'
        return info

    # create date column from game_id
    dates = df['game_id'].apply(extract_date_from_game_id)
    added = int((dates != '').sum())
    info['added'] = added

    # insert as first column
    df.insert(0, 'date', dates)

    try:
        df.to_csv(path, index=False, encoding='utf-8-sig')
        info['updated'] = True
    except Exception as e:
        info['errors'] = 1
        info['error_msg'] = f'write_failed: {e}'

    return info


def main():
    base = os.path.join(os.path.dirname(__file__), '..')
    pattern = os.path.join(base, 'data', '*', 'game_info', '*_Games_all.csv')
    files = sorted(glob.glob(pattern))
    results = []
    for f in files:
        print('Processing', f)
        res = process_file(f)
        results.append(res)
        print('  ->', res)

    print('\nSummary:')
    for r in results:
        print(r)


if __name__ == '__main__':
    main()

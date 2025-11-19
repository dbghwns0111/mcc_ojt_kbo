#!/usr/bin/env python3
"""Convert `time_run` column in all Games_all CSVs to integer minutes.

Searches `data/*/game_info/*_Games_all.csv`, finds a `time_run`-like column
(case-insensitive), parses values like `02:44` as 2 hours 44 minutes -> 164,
and writes the CSV back (UTF-8-sig) with the column converted to integer
where possible. Non-parseable or empty values are left blank.

Usage:
    python scripts\convert_games_time_run_to_minutes.py
"""
import os
import glob
import re
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


def parse_time_to_minutes(s: str):
    if s is None:
        return None
    st = str(s).strip()
    if st == '' or st.lower() == 'nan':
        return None

    # common pattern HH:MM or H:MM or HH:MM:SS
    if ':' in st:
        parts = st.split(':')
        try:
            if len(parts) == 2:
                hh = int(parts[0])
                mm = int(parts[1])
                return hh * 60 + mm
            elif len(parts) == 3:
                hh = int(parts[0]); mm = int(parts[1]); ss = int(parts[2])
                add = 1 if ss >= 30 else 0
                return hh * 60 + mm + add
        except Exception:
            # fall-through to other heuristics
            pass

    # Korean text like '2시간 44분' or '2시간44분'
    m = re.search(r"(\d{1,2})\s*시|(\d{1,2})\s*시간", st)
    if m:
        try:
            hh = int(m.group(1) or m.group(2))
        except Exception:
            hh = 0
        m2 = re.search(r"(\d{1,2})\s*분", st)
        mm = int(m2.group(1)) if m2 else 0
        return hh * 60 + mm

    # fallback: try to parse as pandas timedelta or datetime-like
    try:
        td = pd.to_timedelta(st)
        # total seconds -> minutes (floor)
        mins = int(td.total_seconds() // 60)
        return mins
    except Exception:
        pass

    # last resort: extract two numbers assume H:MM
    nums = re.findall(r"(\d{1,2})", st)
    if len(nums) >= 2:
        try:
            hh = int(nums[0]); mm = int(nums[1])
            return hh * 60 + mm
        except Exception:
            pass

    return None


def process_file(path: str):
    logging.info('Processing %s', path)
    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        try:
            df = pd.read_csv(path, encoding='cp949')
        except Exception as e:
            logging.error('Failed to read %s: %s', path, e)
            return {'path': path, 'error': 'read_failed'}

    # find time_run-like column
    time_col = None
    for c in df.columns:
        if c.strip().lower() == 'time_run' or 'time_run' in c.strip().lower() or '시간' in c.strip().lower():
            time_col = c
            break
    if not time_col:
        logging.warning('No time_run-like column found in %s, skipping', path)
        return {'path': path, 'updated': False, 'reason': 'no_time_col'}

    orig = df[time_col].fillna('').astype(str)
    converted = []
    cnt_conv = 0
    cnt_fail = 0
    for v in orig:
        mins = parse_time_to_minutes(v)
        if mins is None:
            converted.append('')
            if str(v).strip() != '' and str(v).lower() != 'nan':
                cnt_fail += 1
        else:
            converted.append(int(mins))
            cnt_conv += 1

    df[time_col] = converted

    # try to coerce to integer dtype where possible
    try:
        df[time_col] = pd.to_numeric(df[time_col], errors='coerce').astype('Int64')
    except Exception:
        pass

    try:
        df.to_csv(path, index=False, encoding='utf-8-sig')
    except Exception as e:
        logging.error('Failed to write %s: %s', path, e)
        return {'path': path, 'error': 'write_failed'}

    logging.info('Updated %s: converted=%d failed=%d rows=%d', path, cnt_conv, cnt_fail, len(df))
    return {'path': path, 'updated': True, 'converted': cnt_conv, 'failed': cnt_fail, 'rows': len(df)}


def main():
    base = os.path.join(os.path.dirname(__file__), '..')
    pattern = os.path.join(base, 'data', '*', 'game_info', '*_Games_all.csv')
    files = sorted(glob.glob(pattern))
    if not files:
        logging.warning('No Games_all CSV files found with pattern: %s', pattern)
        return

    results = []
    for f in files:
        res = process_file(f)
        results.append(res)

    print('\nSummary:')
    total_conv = sum(r.get('converted', 0) for r in results if r.get('updated'))
    total_fail = sum(r.get('failed', 0) for r in results if r.get('updated'))
    total_files = len([r for r in results if r.get('updated')])
    print(f'Files updated: {total_files}/{len(files)}')
    print(f'Total converted rows: {total_conv}, total failed rows: {total_fail}')


if __name__ == '__main__':
    main()

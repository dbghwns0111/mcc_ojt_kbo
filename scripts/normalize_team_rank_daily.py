#!/usr/bin/env python3
"""Normalize team_rank_daily files:

- Move `기준일` column to the first position.
- For `홈` and `방문` columns (format like '숫자-숫자-숫자'), if the first number has 4 digits,
  remove the first two digits (e.g. '2021-12-31' -> '21-12-31').

Writes files back with `utf-8-sig` encoding and prints a per-file summary.
"""
import os
import glob
import re
import logging
import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


def find_column(df, candidates):
    for c in df.columns:
        for cand in candidates:
            if cand == c or cand in c:
                return c
    return None


def normalize_triplet_field(s: str) -> str:
    if pd.isna(s):
        return s
    s_str = str(s).strip()
    if s_str == '' or s_str.lower() == 'nan':
        return s_str

    # allow spaces around hyphens
    m = re.match(r"^\s*(\d+)\s*[-]\s*(\d+)\s*[-]\s*(\d+)\s*$", s_str)
    if not m:
        return s_str
    a, b, c = m.group(1), m.group(2), m.group(3)
    if len(a) == 4:
        # remove first two characters
        a = a[2:]
    return f"{a}-{b}-{c}"


def process_file(path: str) -> dict:
    info = {'path': path, 'rows': 0, 'home_updated': 0, 'visit_updated': 0, 'moved_date': False, 'updated': False}
    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        try:
            df = pd.read_csv(path, encoding='cp949')
        except Exception as e:
            info['error'] = f'read_failed: {e}'
            return info

    info['rows'] = len(df)

    # find date (기준일) column
    date_col = find_column(df, ['기준일', '기준 날짜', '기준일자', 'date'])
    if date_col:
        # move to first position
        cols = [date_col] + [c for c in df.columns if c != date_col]
        df = df[cols]
        info['moved_date'] = True

    # find home and visit columns
    home_col = find_column(df, ['홈', '홈팀', '홈팀명', 'Home'])
    visit_col = find_column(df, ['방문', '방문팀', '원정', '방문팀명', 'Away'])

    if home_col:
        updated = 0
        new_vals = []
        for v in df[home_col]:
            newv = normalize_triplet_field(v)
            if str(newv) != str(v):
                updated += 1
            new_vals.append(newv)
        df[home_col] = new_vals
        info['home_updated'] = updated

    if visit_col:
        updated = 0
        new_vals = []
        for v in df[visit_col]:
            newv = normalize_triplet_field(v)
            if str(newv) != str(v):
                updated += 1
            new_vals.append(newv)
        df[visit_col] = new_vals
        info['visit_updated'] = updated

    try:
        df.to_csv(path, index=False, encoding='utf-8-sig')
        info['updated'] = True
    except Exception as e:
        info['error'] = f'write_failed: {e}'

    return info


def main():
    base = os.path.join(os.path.dirname(__file__), '..')
    pattern = os.path.join(base, 'data', '*', 'league_info', '*team_rank_daily.csv')
    files = sorted(glob.glob(pattern))
    if not files:
        logging.warning('No team_rank_daily files found with pattern: %s', pattern)
        return

    results = []
    for f in files:
        logging.info('Processing %s', f)
        res = process_file(f)
        results.append(res)
        logging.info('  -> rows=%d moved_date=%s home_updated=%d visit_updated=%d updated=%s',
                     res.get('rows', 0), res.get('moved_date', False), res.get('home_updated', 0),
                     res.get('visit_updated', 0), res.get('updated', False))

    # summary
    total_rows = sum(r.get('rows', 0) for r in results)
    total_files = len(results)
    logging.info('Processed %d files, total rows=%d', total_files, total_rows)


if __name__ == '__main__':
    main()

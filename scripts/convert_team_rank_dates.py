#!/usr/bin/env python3
"""Convert 기준일 column to ISO date (YYYY-MM-DD) for team_rank CSVs under data/*/league_info.

Usage:
    python scripts\convert_team_rank_dates.py

This script finds files matching '*team_rank*.csv' under each year's `data/<year>/league_info/`,
parses the `기준일` column using common separators (., /, -), converts to ISO date strings,
and overwrites the CSV in-place (keeping UTF-8-sig encoding).
"""
import os
import glob
import pandas as pd


def normalize_date_series(s: pd.Series) -> pd.Series:
    # convert to string, replace common separators, then parse
    s2 = s.astype(str).str.strip()
    # replace '.' and '/' with '-'
    s2 = s2.str.replace('.', '-', regex=False).str.replace('/', '-', regex=False)
    # pandas to_datetime with errors='coerce'
    dt = pd.to_datetime(s2, errors='coerce', format=None)
    # format back to ISO YYYY-MM-DD, keep NaT as empty string
    return dt.dt.strftime('%Y-%m-%d').fillna('')


def process_file(path: str) -> dict:
    info = {'path': path, 'updated': False, 'rows': 0, 'converted': 0, 'failed': 0}
    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        try:
            df = pd.read_csv(path, encoding='cp949')
        except Exception as e:
            info['error'] = f'read_failed: {e}'
            return info

    info['rows'] = len(df)

    # find column name containing '기준' (prefer exact '기준일')
    col = None
    if '기준일' in df.columns:
        col = '기준일'
    else:
        for c in df.columns:
            if '기준' in c:
                col = c
                break

    if not col:
        info['error'] = 'no_basis_date_column'
        return info

    orig = df[col].astype(str).copy()
    converted = normalize_date_series(df[col])

    # count conversions where original non-empty and converted non-empty
    mask_orig = orig.str.strip() != ''
    mask_conv = converted.str.strip() != ''
    info['converted'] = int((mask_orig & mask_conv).sum())
    info['failed'] = int((mask_orig & ~mask_conv).sum())

    # assign back and save
    df[col] = converted
    try:
        df.to_csv(path, index=False, encoding='utf-8-sig')
        info['updated'] = True
    except Exception as e:
        info['error'] = f'write_failed: {e}'

    return info


def main():
    base = os.path.join(os.path.dirname(__file__), '..')
    data_dir = os.path.join(base, 'data')
    patterns = [os.path.join(data_dir, '*', 'league_info', '*team_rank*.csv'),
                os.path.join(data_dir, '*', 'league_info', '*team_rank_daily*.csv')]

    files = []
    for p in patterns:
        files.extend(glob.glob(p))

    files = sorted(set(files))
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

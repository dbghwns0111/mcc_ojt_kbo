#!/usr/bin/env python3
"""Combine all years' hitter_daily and pitcher_daily CSVs into data_etl.

Creates:
  data_etl/all_years_hitter_daily.csv
  data_etl/all_years_pitcher_daily.csv

Each row will have a leading `year` column.
"""
import os
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(ROOT, 'data')
OUT_DIR = os.path.join(ROOT, 'data_etl')

HITTER_SUFFIX = '_hitter_daily.csv'
PITCHER_SUFFIX = '_pitcher_daily.csv'


def find_daily_files():
    # returns dict: 'hitter' -> list of (year, path), 'pitcher' -> list
    res = {'hitter': [], 'pitcher': []}
    if not os.path.isdir(DATA_DIR):
        return res
    for year_name in sorted(os.listdir(DATA_DIR)):
        year_path = os.path.join(DATA_DIR, year_name)
        if not os.path.isdir(year_path):
            continue
        # walk year tree to find daily files
        for dirpath, dirnames, filenames in os.walk(year_path):
            for fn in filenames:
                lower = fn.lower()
                if lower.endswith(HITTER_SUFFIX):
                    fpath = os.path.join(dirpath, fn)
                    try:
                        year = str(int(year_name))
                    except Exception:
                        year = str(year_name)
                    res['hitter'].append((year, fpath))
                elif lower.endswith(PITCHER_SUFFIX):
                    fpath = os.path.join(dirpath, fn)
                    try:
                        year = str(int(year_name))
                    except Exception:
                        year = str(year_name)
                    res['pitcher'].append((year, fpath))
    return res


def combine_and_write(mapping):
    os.makedirs(OUT_DIR, exist_ok=True)
    for cat in ('hitter', 'pitcher'):
        entries = mapping.get(cat, [])
        if not entries:
            print(f'No files found for {cat} daily')
            continue
        out_name = f'all_years_{cat}_daily.csv'
        out_path = os.path.join(OUT_DIR, out_name)
        print(f'Combining {len(entries)} files -> {out_path}')
        dfs = []
        for year, f in entries:
            try:
                df = pd.read_csv(f, dtype=str, encoding='utf-8-sig')
            except Exception:
                try:
                    df = pd.read_csv(f, dtype=str)
                except Exception as e:
                    print(f'  Failed to read {f}: {e}')
                    continue
            df.insert(0, 'year', year)
            dfs.append(df)
        if not dfs:
            print(f'  No readable files for {cat} daily')
            continue
        combined = pd.concat(dfs, ignore_index=True, sort=False)
        cols = combined.columns.tolist()
        if cols[0] != 'year':
            cols = ['year'] + [c for c in cols if c != 'year']
            combined = combined[cols]
        combined.to_csv(out_path, index=False, encoding='utf-8-sig')
    print('Done')


def main():
    mapping = find_daily_files()
    combine_and_write(mapping)


if __name__ == '__main__':
    main()

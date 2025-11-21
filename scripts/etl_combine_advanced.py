#!/usr/bin/env python3
"""Combine hitter/pitcher advanced CSVs across all years into two files.

Writes:
  data_etl/all_years_hitter_advanced.csv
  data_etl/all_years_pitcher_advanced.csv

Each row will have a leading `year` column.
"""
import os
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(ROOT, 'data')
OUT_DIR = os.path.join(ROOT, 'data_etl')

TARGET_SUFFIXES = ['_hitter_advanced.csv', '_pitcher_advanced.csv']


def find_advanced_files():
    # returns dict[suffix_without_dot] -> list of (year, filepath)
    mapping = {s: [] for s in TARGET_SUFFIXES}
    if not os.path.isdir(DATA_DIR):
        return mapping
    for year_name in sorted(os.listdir(DATA_DIR)):
        year_path = os.path.join(DATA_DIR, year_name)
        if not os.path.isdir(year_path):
            continue
        ps_dir = os.path.join(year_path, 'player_stats')
        if not os.path.isdir(ps_dir):
            continue
        for fn in os.listdir(ps_dir):
            fpath = os.path.join(ps_dir, fn)
            if not os.path.isfile(fpath) or not fn.lower().endswith('.csv'):
                continue
            lower = fn.lower()
            for suf in TARGET_SUFFIXES:
                if lower.endswith(suf):
                    try:
                        year = str(int(year_name))
                    except Exception:
                        year = str(year_name)
                    mapping[suf].append((year, fpath))
                    break
    return mapping


def combine_and_write(mapping):
    os.makedirs(OUT_DIR, exist_ok=True)
    for suf, entries in mapping.items():
        if not entries:
            print(f'No files found for pattern {suf}')
            continue
        cat = 'hitter' if 'hitter' in suf else 'pitcher'
        out_name = f'all_years_{cat}_advanced.csv'
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
            print(f'  No readable files for {suf}')
            continue
        combined = pd.concat(dfs, ignore_index=True, sort=False)
        # ensure year first
        cols = combined.columns.tolist()
        if cols[0] != 'year':
            cols = ['year'] + [c for c in cols if c != 'year']
            combined = combined[cols]
        combined.to_csv(out_path, index=False, encoding='utf-8-sig')
    print('Done')


def main():
    mapping = find_advanced_files()
    combine_and_write(mapping)


if __name__ == '__main__':
    main()

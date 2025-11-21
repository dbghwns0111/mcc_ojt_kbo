#!/usr/bin/env python3
"""Combine all years' Games_all CSVs into a single file in data_etl.

Writes `data_etl/all_years_Games_all.csv` with a leading `year` column.
"""
import os
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(ROOT, 'data')
OUT_DIR = os.path.join(ROOT, 'data_etl')


def find_games_all_files():
    entries = []  # list of (year_str, filepath)
    if not os.path.isdir(DATA_DIR):
        return entries
    for year_name in sorted(os.listdir(DATA_DIR)):
        year_path = os.path.join(DATA_DIR, year_name)
        if not os.path.isdir(year_path):
            continue
        gi_dir = os.path.join(year_path, 'game_info')
        if not os.path.isdir(gi_dir):
            continue
        for fn in os.listdir(gi_dir):
            lower = fn.lower()
            if lower.endswith('_games_all.csv') or lower.endswith('games_all.csv'):
                fpath = os.path.join(gi_dir, fn)
                if os.path.isfile(fpath):
                    try:
                        year = str(int(year_name))
                    except Exception:
                        year = str(year_name)
                    entries.append((year, fpath))
    return entries


def combine_and_write(entries):
    os.makedirs(OUT_DIR, exist_ok=True)
    if not entries:
        print('No Games_all files found')
        return
    out_path = os.path.join(OUT_DIR, 'all_years_Games_all.csv')
    print(f'Combining {len(entries)} Games_all files -> {out_path}')
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
        print('No readable Games_all files')
        return
    combined = pd.concat(dfs, ignore_index=True, sort=False)
    # make sure year is first
    cols = combined.columns.tolist()
    if cols[0] != 'year':
        cols = ['year'] + [c for c in cols if c != 'year']
        combined = combined[cols]
    combined.to_csv(out_path, index=False, encoding='utf-8-sig')
    print('Wrote', out_path)


def main():
    entries = find_games_all_files()
    combine_and_write(entries)


if __name__ == '__main__':
    main()

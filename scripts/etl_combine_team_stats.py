#!/usr/bin/env python3
"""Combine per-team player stats (hitter/pitcher/defense/runner) across all years.

This script scans `data/{year}/player_stats/{team}` for files and writes
four consolidated outputs to `data_etl/all_years_{category}.csv`.
Each row will have a `year` column inserted as the first column.
"""
import os
from collections import defaultdict
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(ROOT, 'data')
OUT_DIR = os.path.join(ROOT, 'data_etl')
CATEGORIES = ['hitter', 'pitcher', 'defense', 'runner']


def find_team_files_by_category():
    # returns dict[category] -> list of (year_str, filepath)
    mapping = defaultdict(list)
    if not os.path.isdir(DATA_DIR):
        return mapping
    for year_name in sorted(os.listdir(DATA_DIR)):
        year_path = os.path.join(DATA_DIR, year_name)
        if not os.path.isdir(year_path):
            continue
        # check player_stats directory
        ps_dir = os.path.join(year_path, 'player_stats')
        if not os.path.isdir(ps_dir):
            continue
        for team in os.listdir(ps_dir):
            team_dir = os.path.join(ps_dir, team)
            if not os.path.isdir(team_dir):
                continue
            for fn in os.listdir(team_dir):
                fpath = os.path.join(team_dir, fn)
                if not os.path.isfile(fpath) or not fn.lower().endswith('.csv'):
                    continue
                lower = fn.lower()
                for cat in CATEGORIES:
                    if f'_{cat}' in lower or lower.endswith(f'_{cat}.csv') or lower == f'{cat}.csv':
                        # normalize year as string
                        try:
                            year = str(int(year_name))
                        except Exception:
                            year = str(year_name)
                        mapping[cat].append((year, fpath))
                        break
    return mapping


def combine_all_years(mapping):
    os.makedirs(OUT_DIR, exist_ok=True)
    for cat in CATEGORIES:
        entries = mapping.get(cat, [])
        if not entries:
            print(f'No files found for category: {cat}')
            continue
        out_path = os.path.join(OUT_DIR, f'all_years_{cat}.csv')
        print(f'Combining {len(entries)} files for category {cat} -> {out_path}')
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
            # insert year as first column
            df.insert(0, 'year', year)
            dfs.append(df)
        if not dfs:
            print(f'  No readable files for category {cat}')
            continue
        combined = pd.concat(dfs, ignore_index=True, sort=False)
        # ensure 'year' is first column
        cols = combined.columns.tolist()
        if cols[0] != 'year':
            cols = ['year'] + [c for c in cols if c != 'year']
            combined = combined[cols]
        combined.to_csv(out_path, index=False, encoding='utf-8-sig')
    print('All categories combined')


def main():
    mapping = find_team_files_by_category()
    if not mapping:
        print('No team files found in', DATA_DIR)
        return
    combine_all_years(mapping)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Keep only Korean name (한글) in the `Name` column of advanced CSVs.

Targets:
- data/*/player_stats/*hitter_advanced.csv
- data/*/player_stats/*pitcher_advanced.csv

For each file, reads CSV (utf-8-sig, fallback cp949), finds `Name` column
and replaces its value with the substring starting at the first Hangul
character (U+AC00–U+D7A3). If no Hangul found, leaves original value.
Saves CSV back as utf-8-sig.
"""
import os
import glob
import re
import pandas as pd


HANGUL_RE = re.compile(r"([\uac00-\ud7a3].*)")


def extract_korean(s: str) -> str:
    if pd.isna(s):
        return s
    st = str(s).strip()
    m = HANGUL_RE.search(st)
    if m:
        return m.group(1).strip()
    return st


def process_file(path: str) -> dict:
    info = {'path': path, 'rows': 0, 'updated': False, 'converted': 0}
    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        try:
            df = pd.read_csv(path, encoding='cp949')
        except Exception as e:
            info['error'] = f'read_failed: {e}'
            return info

    if 'Name' not in df.columns:
        info['error'] = 'no_Name_column'
        return info

    info['rows'] = len(df)
    converted = 0
    new_names = []
    for v in df['Name']:
        newv = extract_korean(v)
        if str(newv).strip() != str(v).strip():
            converted += 1
        new_names.append(newv)

    df['Name'] = new_names

    try:
        df.to_csv(path, index=False, encoding='utf-8-sig')
        info['updated'] = True
        info['converted'] = converted
    except Exception as e:
        info['error'] = f'write_failed: {e}'

    return info


def main():
    base = os.path.join(os.path.dirname(__file__), '..')
    patterns = [os.path.join(base, 'data', '*', 'player_stats', '*hitter_advanced*.csv'),
                os.path.join(base, 'data', '*', 'player_stats', '*pitcher_advanced*.csv')]
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

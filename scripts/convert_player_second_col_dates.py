#!/usr/bin/env python3
"""Convert the 2nd column (일자) in player_stats daily CSVs to YYYY-MM-DD.

Behavior:
- Targets files: data/*/player_stats/*hitter_daily*.csv and *pitcher_daily*.csv
- Reads CSV with utf-8-sig, falls back to cp949
- Takes the 2nd column by position (index 1) and, for values like '04.04', '4.4',
  '04-04', '04/04', prepends the year inferred from the file path (data/<YEAR>/...) and
  converts to ISO date string 'YYYY-MM-DD'. If the value already contains a 4-digit
  year, it is parsed as-is. Empty values remain empty.
- Overwrites CSV with utf-8-sig encoding.
"""
import os
import glob
import re
import pandas as pd


def infer_year_from_path(path: str):
    m = re.search(r"data[\\/](\d{4})[\\/]", path)
    if m:
        return m.group(1)
    return None


def normalize_value(val: str, year: str):
    if pd.isna(val):
        return ''
    s = str(val).strip()
    if s == '' or s.lower() == 'nan':
        return ''

    # if contains 4-digit year, try parse directly
    if re.search(r"\d{4}", s):
        try:
            dt = pd.to_datetime(s, errors='coerce')
            return dt.strftime('%Y-%m-%d') if not pd.isna(dt) else ''
        except Exception:
            return ''

    # match MM.DD or MM-DD or M.D etc
    if re.match(r"^\d{1,2}[./-]\d{1,2}$", s):
        parts = re.split(r"[./-]", s)
        mm = parts[0].zfill(2)
        dd = parts[1].zfill(2)
        if year:
            candidate = f"{year}-{mm}-{dd}"
            try:
                dt = pd.to_datetime(candidate, format='%Y-%m-%d', errors='coerce')
                return dt.strftime('%Y-%m-%d') if not pd.isna(dt) else ''
            except Exception:
                return ''
        else:
            # no year info: return empty
            return ''

    # fallback: try pandas parser
    try:
        dt = pd.to_datetime(s, errors='coerce')
        return dt.strftime('%Y-%m-%d') if not pd.isna(dt) else ''
    except Exception:
        return ''


def process_file(path: str) -> dict:
    info = {'path': path, 'rows': 0, 'converted': 0, 'failed': 0, 'updated': False}
    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        try:
            df = pd.read_csv(path, encoding='cp949')
        except Exception as e:
            info['error'] = f'read_failed: {e}'
            return info

    info['rows'] = len(df)
    if df.shape[1] < 2:
        info['error'] = 'no_second_column'
        return info

    year = infer_year_from_path(path)
    colname = df.columns[1]
    orig = df[colname].copy()

    converted_count = 0
    failed_count = 0
    new_vals = []
    for v in orig:
        newv = normalize_value(v, year)
        if (str(v).strip() != '' and str(v).lower() != 'nan') and newv == '':
            failed_count += 1
        if newv != '':
            converted_count += 1
        new_vals.append(newv)

    df[colname] = new_vals
    info['converted'] = converted_count
    info['failed'] = failed_count

    try:
        df.to_csv(path, index=False, encoding='utf-8-sig')
        info['updated'] = True
    except Exception as e:
        info['error'] = f'write_failed: {e}'

    return info


def main():
    base = os.path.join(os.path.dirname(__file__), '..')
    pattern1 = os.path.join(base, 'data', '*', 'player_stats', '*hitter_daily*.csv')
    pattern2 = os.path.join(base, 'data', '*', 'player_stats', '*pitcher_daily*.csv')
    files = sorted(set(glob.glob(pattern1) + glob.glob(pattern2)))
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

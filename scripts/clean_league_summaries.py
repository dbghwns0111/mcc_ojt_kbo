#!/usr/bin/env python3
"""Clean league summary CSVs under data/<year>/league_info/.

Operations per CSV:
 - Drop column named '순위' if present; otherwise drop the first column if it looks like a rank column (Unnamed or numeric header)
 - Remove rows where any cell == '합계' or the first column == '합계'
 - Save back with UTF-8-SIG

Run:
    python scripts\clean_league_summaries.py
"""
import os
import pandas as pd
from glob import glob

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))


def read_csv(path):
    try:
        return pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        return pd.read_csv(path, encoding='utf-8')


def looks_like_rank_col(name, series):
    # heuristic: header contains '순위' or 'rank' or is unnamed, or values are integers like 1,2,3
    if name and ('순위' in str(name) or 'rank' in str(name).lower()):
        return True
    if str(name).lower().startswith('unnamed'):
        return True
    # if series looks numeric for many rows
    try:
        nums = pd.to_numeric(series.dropna().astype(str).str.strip(), errors='coerce')
        # if majority are integers, it's a rank column
        if len(nums) > 0 and nums.notna().sum() / max(1, len(series.dropna())) > 0.6:
            return True
    except Exception:
        pass
    return False


def process_file(path):
    df = read_csv(path)
    rows_before = len(df)

    # drop any completely empty trailing rows
    df = df.dropna(how='all')

    # drop '순위' column if present or guess first col
    dropped_rank = False
    if '순위' in df.columns:
        df = df.drop(columns=['순위'])
        dropped_rank = True
    else:
        first_col = df.columns[0]
        if looks_like_rank_col(first_col, df[first_col]):
            df = df.drop(columns=[first_col])
            dropped_rank = True

    # remove rows where any cell equals '합계' OR first column equals '합계'
    mask_sum = df.apply(lambda row: row.astype(str).str.strip().eq('합계').any(), axis=1)
    df = df[~mask_sum].reset_index(drop=True)

    rows_after = len(df)

    # save back
    df.to_csv(path, index=False, encoding='utf-8-sig')
    return {'path': path, 'rows_before': rows_before, 'rows_after': rows_after, 'dropped_rank': dropped_rank}


def main():
    files = glob(os.path.join(BASE, '*', 'league_info', '*.csv'))
    summary = []
    for f in sorted(files):
        try:
            res = process_file(f)
            summary.append(res)
            print('Processed:', os.path.relpath(f, BASE), 'rows_before=', res['rows_before'], 'rows_after=', res['rows_after'], 'dropped_rank=', res['dropped_rank'])
        except Exception as e:
            print('Failed:', f, e)

    total_before = sum(r['rows_before'] for r in summary)
    total_after = sum(r['rows_after'] for r in summary)
    print('\nSummary:')
    print('Files processed:', len(summary))
    print('Total rows before:', total_before)
    print('Total rows after :', total_after)
    print('Total rows removed:', total_before - total_after)


if __name__ == '__main__':
    main()

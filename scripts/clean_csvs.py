import os
import sys
import pandas as pd
from io import StringIO

DATA_ROOT = os.path.join(os.path.dirname(__file__), '..', 'data')
YEARS = [str(y) for y in range(2021, 2026)]

NAME_KEYWORDS = ['선수명', '선수', '이름', 'name']


def find_name_column(df):
    cols = list(df.columns)
    lower_cols = [c.lower() for c in cols]
    for kw in NAME_KEYWORDS:
        for c, lc in zip(cols, lower_cols):
            if kw in lc:
                return c
    return None


def process_file(path):
    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        # try default
        df = pd.read_csv(path, encoding='utf-8')

    before_rows = len(df)

    # If there are no columns, skip
    if df.shape[1] == 0:
        return (path, before_rows, 0, 0, 'no_columns')

    # Drop first column (rank) by position
    # 두 번째 열(인덱스 1)을 제거하고, 나머지 열(인덱스 0과 인덱스 2부터 끝까지)을 선택합니다.
    if df.shape[1] >= 2:
        # 첫 번째 열(인덱스 0)과 세 번째 열(인덱스 2)부터 끝까지를 선택합니다.
        df = pd.concat([df.iloc[:, [0]], df.iloc[:, 2:]], axis=1)

    # Drop duplicate rows
    df_before_dup = len(df)
    df = df.drop_duplicates()
    dup_dropped = df_before_dup - len(df)

    # Find name column to sort by
    name_col = find_name_column(df)
    sorted_flag = False
    if name_col is not None:
        try:
            df = df.sort_values(by=name_col, key=lambda s: s.astype(str))
            sorted_flag = True
        except Exception:
            # fallback: normal sort
            df = df.sort_values(by=name_col)
            sorted_flag = True

    # Reset index and write back
    df = df.reset_index(drop=True)
    df.to_csv(path, index=False, encoding='utf-8-sig')

    after_rows = len(df)
    return (path, before_rows, after_rows, dup_dropped, 'sorted' if sorted_flag else 'no_name_col')


def main(data_root=None):
    if data_root:
        root = data_root
    else:
        root = os.path.normpath(DATA_ROOT)

    results = []
    for year in YEARS:
        year_dir = os.path.join(root, year)
        if not os.path.isdir(year_dir):
            continue
        # Walk the year directory recursively to find CSVs in nested folders
        for dirpath, dirnames, filenames in os.walk(year_dir):
            for fname in filenames:
                if not fname.lower().endswith('.csv'):
                    continue
                fpath = os.path.join(dirpath, fname)
                res = process_file(fpath)
                results.append(res)
                print(f"Processed: {res[0]} | rows_before={res[1]} rows_after={res[2]} dup_removed={res[3]} status={res[4]}")

    # Summary
    total = len(results)
    total_before = sum(r[1] for r in results)
    total_after = sum(r[2] for r in results)
    total_dup = sum(r[3] for r in results)
    print('\nSummary:')
    print(f'Files processed: {total}')
    print(f'Total rows before: {total_before}')
    print(f'Total rows after : {total_after}')
    print(f'Total duplicates removed: {total_dup}')

    return results


if __name__ == '__main__':
    data_root = None
    if len(sys.argv) > 1:
        data_root = sys.argv[1]
    main(data_root)

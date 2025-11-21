#!/usr/bin/env python3
"""Fix GAME_DATE day for rows missing `game_id` in combined daily files.

Problem: 일부 크롤링에서 날짜의 일이 잘못 수집되어 'YYYY-MM-01'/'02'/'03'로 들어왔습니다.
해당 행들 중 `game_id`가 비어있는 행에 한해
 - '-01' -> '-10'
 - '-02' -> '-20'
 - '-03' -> '-30'
로 바꿔 저장합니다.

Usage:
    python scripts\fix_game_date_for_missing_game_id.py [--backup]

예: 백업을 원하면 --backup 옵션을 추가하세요 (원본 파일에 .bak 파일을 생성).
"""
import os
import csv
import argparse

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
FILES = [
    os.path.join(ROOT, 'data_etl', 'all_years_hitter_daily.csv'),
    os.path.join(ROOT, 'data_etl', 'all_years_pitcher_daily.csv'),
]

REMAP = {'-01': '-10', '-02': '-20', '-03': '-30'}


def fix_file(path, do_backup=False):
    if not os.path.exists(path):
        print('파일 없음:', path)
        return 0, 0
    if do_backup:
        bak = path + '.bak'
        if not os.path.exists(bak):
            os.replace(path, bak)
            # copy back tmp will be written to original path
            # if .bak exists we don't overwrite it
            print('백업 생성:', bak)
        else:
            print('백업 파일 이미 존재, 덮어쓰지 않음:', bak)

    tmp = path + '.tmp'
    rows = 0
    changed = 0
    with open((path + '.bak') if do_backup and os.path.exists(path + '.bak') else path, newline='', encoding='utf-8-sig') as fr, \
         open(tmp, 'w', newline='', encoding='utf-8-sig') as fw:
        reader = csv.DictReader(fr)
        if not reader.fieldnames:
            print('헤더 없음:', path)
            return 0, 0
        fieldnames = reader.fieldnames
        # possible game_id column names
        gid_col = 'game_id' if 'game_id' in fieldnames else None
        # find date column (GAME_DATE or GAME_DATE lower variants)
        date_col = None
        for c in ('GAME_DATE', 'game_date', 'GAMEdate', 'GAME_DATE'):
            if c in fieldnames:
                date_col = c
                break
        if date_col is None:
            # try common names
            for c in fieldnames:
                if 'date' in c.lower():
                    date_col = c
                    break
        if date_col is None:
            print('날짜 컬럼을 찾지 못함:', path)
            return 0, 0

        writer = csv.DictWriter(fw, fieldnames=fieldnames)
        writer.writeheader()
        for r in reader:
            rows += 1
            gid = r.get(gid_col) if gid_col else None
            date_val = r.get(date_col) or ''
            if (not gid or str(gid).strip() == '') and isinstance(date_val, str):
                date_val = date_val.strip()
                for old, new in REMAP.items():
                    if date_val.endswith(old):
                        newdate = date_val[:-3] + new
                        r[date_col] = newdate
                        changed += 1
                        break
            writer.writerow(r)

    os.replace(tmp, path)
    return rows, changed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backup', action='store_true', help='Create .bak backup before modifying')
    args = parser.parse_args()

    total_rows = 0
    total_changed = 0
    for p in FILES:
        rows, changed = fix_file(p, do_backup=args.backup)
        print(f'Processed {rows} rows in {p}, changed {changed} rows')
        total_rows += rows
        total_changed += changed

    print('전체 처리 완료. 총 행:', total_rows, '총 수정:', total_changed)


if __name__ == '__main__':
    main()

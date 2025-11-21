#!/usr/bin/env python3
"""Normalize `IP` columns in pitcher daily/season ETL files.

Replacements:
- '1/3' -> 0.33
- '2/3' -> 0.66

Also handles values like '5 2/3' -> 5.66 and existing decimal values.

Usage: python scripts\preprocess_ip.py
"""
import os
import csv
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
FILES = [
    os.path.join(ROOT, 'data_etl', 'all_years_pitcher_daily.csv'),
    os.path.join(ROOT, 'data_etl', 'all_years_pitcher_season.csv'),
]


def parse_ip_value(s):
    if s is None:
        return ''
    s = str(s).strip()
    if s == '':
        return ''
    # direct fraction
    if s == '1/3':
        return 0.33
    if s == '2/3':
        return 0.66
    # patterns like '5 2/3' or '98 2/3' (whole + fraction)
    if ' ' in s:
        parts = s.split()
        try:
            whole = float(parts[0])
        except Exception:
            whole = 0.0
        frac = parts[1]
        if frac == '1/3':
            return round(whole + 0.33, 2)
        if frac == '2/3':
            return round(whole + 0.66, 2)
        # fallback: try to parse as float
        try:
            return float(s)
        except Exception:
            return ''
    # fallback: try numeric parse
    try:
        return float(s)
    except Exception:
        # last attempt: mixed like '5/3' or other, try evaluate fraction
        if '/' in s:
            try:
                a, b = s.split('/')
                return round(float(a) / float(b), 2)
            except Exception:
                return ''
        return ''


def process_file(path):
    if not os.path.exists(path):
        print('missing:', path)
        return 0
    tmp = path + '.tmp'
    rows = 0
    with open(path, newline='', encoding='utf-8-sig') as fr, open(tmp, 'w', newline='', encoding='utf-8-sig') as fw:
        reader = csv.DictReader(fr)
        if not reader.fieldnames:
            print('no header in', path)
            return 0
        fieldnames = reader.fieldnames
        # find IP column name (case-sensitive exact 'IP' or 'Ip' fallback)
        ip_col = None
        for candidate in ('IP', 'Ip', 'ip'):
            if candidate in fieldnames:
                ip_col = candidate
                break
        if ip_col is None:
            print('IP column not found in', path)
            return 0
        writer = csv.DictWriter(fw, fieldnames=fieldnames)
        writer.writeheader()
        for r in reader:
            raw = r.get(ip_col)
            newval = parse_ip_value(raw)
            # write as float if not empty, else keep empty string
            r[ip_col] = ('{:.2f}'.format(newval) if newval != '' else '')
            writer.writerow(r)
            rows += 1
    os.replace(tmp, path)
    return rows


def main():
    total = 0
    for p in FILES:
        n = process_file(p)
        print('Processed', n, 'rows in', p)
        total += n
    print('Done. Total rows processed:', total)


if __name__ == '__main__':
    main()

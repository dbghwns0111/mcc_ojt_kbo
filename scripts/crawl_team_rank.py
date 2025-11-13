#!/usr/bin/env python3
"""Crawl yearly team rank (standings) from KBO and save per-year CSVs.

Saves files to data/<year>/league_info/<year>_team_rank.csv

Usage:
    python scripts\crawl_team_rank.py
"""
import os
import time
import logging
from io import StringIO

import requests
from bs4 import BeautifulSoup
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUT_BASE = os.path.join(BASE_DIR, 'data')
YEARS = list(range(2021, 2026))
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def collect_hidden_inputs(soup):
    data = {}
    for inp in soup.find_all('input'):
        name = inp.get('name')
        if not name:
            continue
        data[name] = inp.get('value', '')
    return data


def extract_table(html):
    try:
        buf = StringIO(html)
        dfs = pd.read_html(buf)
        if not dfs:
            return None
        dfs.sort(key=lambda d: d.shape[1], reverse=True)
        return dfs[0]
    except Exception:
        return None


def find_select_params_for_rank(soup, year):
    params = {}
    selects = soup.find_all('select')
    for s in selects:
        name = s.get('name')
        if not name:
            continue
        lname = name.lower()
        if 'ddlseason' in lname:
            for opt in s.find_all('option'):
                val = opt.get('value') or ''
                txt = opt.get_text(strip=True)
                if val == str(year) or str(year) in txt:
                    params[name] = val
                    break
        elif 'ddlseries' in lname:
            for opt in s.find_all('option'):
                txt = opt.get_text(strip=True)
                if '정규시즌' in txt:
                    params[name] = opt.get('value') or ''
                    break
    return params


def crawl_year(session, year):
    url = 'https://www.koreabaseball.com/Record/TeamRank/TeamRank.aspx'
    logging.info('Crawling %s year=%s', url, year)
    r = session.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'lxml')
    base_form = collect_hidden_inputs(soup)

    params = find_select_params_for_rank(soup, year)
    # if no season param found, try to set using heuristics
    if not any('ddlseason' in k.lower() for k in params.keys()):
        for s in soup.find_all('select'):
            nm = s.get('name') or ''
            if 'season' in nm.lower():
                for opt in s.find_all('option'):
                    if str(year) in (opt.get('value') or '') or str(year) in opt.get_text(strip=True):
                        params[nm] = opt.get('value') or ''
                        break

    # POST to apply selects
    form = base_form.copy()
    form.update(params)
    rp = session.post(url, data=form, headers=HEADERS, timeout=30)
    rp.raise_for_status()
    df = extract_table(rp.text)
    if df is None:
        df = extract_table(r.text)
    return df


def main():
    session = requests.Session()
    for year in YEARS:
        out_dir = os.path.join(OUT_BASE, str(year), 'league_info')
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, f"{year}_team_rank_final.csv")
        try:
            df = crawl_year(session, year)
            if df is None:
                logging.warning('No team rank table found for %s', year)
                continue
            # requested summary columns; select if present
            wanted = ['순위', '팀명', '승', '패', '무', '승률', '승차', '최근10경기', '연속', '홈', '방문']
            cols = df.columns.tolist()
            selected = [c for c in wanted if c in cols]
            out_df = df[selected] if selected else df
            out_df.to_csv(out_path, index=False, encoding='utf-8-sig')
            logging.info('Saved team rank to %s', out_path)
        except Exception as e:
            logging.exception('Failed to crawl team rank for %s: %s', year, e)
        time.sleep(1)


if __name__ == '__main__':
    main()

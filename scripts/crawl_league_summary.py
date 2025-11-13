#!/usr/bin/env python3
"""
Crawl league-level team summaries (team aggregates) from KBO site and save CSVs.

Generates files under `data/<year>/league_info/` named:
 - <year>_hitting_summary.csv
 - <year>_pitching_summary.csv
 - <year>_defense_summary.csv
 - <year>_running_summary.csv

Run:
    python scripts\crawl_league_summary.py
"""
import os
import time
import logging
from urllib.parse import urljoin
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
        value = inp.get('value', '')
        data[name] = value
    return data


def extract_table_from_html(html):
    """Return the largest table parsed from html using pandas.read_html (wrap string in StringIO)."""
    try:
        buf = StringIO(html)
        dfs = pd.read_html(buf)
        if not dfs:
            return None
        dfs.sort(key=lambda d: d.shape[1], reverse=True)
        return dfs[0]
    except Exception:
        return None


def build_select_params(soup, year: int, for_hitter: bool = False):
    params = {}
    selects = soup.find_all('select')
    for s in selects:
        name = s.get('name')
        if not name:
            continue
        lname = name.lower()
        # season
        if 'ddlseason' in lname:
            for opt in s.find_all('option'):
                val = opt.get('value') or ''
                txt = opt.get_text(strip=True)
                if val == str(year) or str(year) in txt:
                    params[name] = val
                    break
        elif for_hitter and 'ddlseries' in lname:
            for opt in s.find_all('option'):
                txt = opt.get_text(strip=True)
                if 'KBO 정규시즌' in txt:
                    params[name] = opt.get('value') or ''
                    break
        # leave other selects alone for league/team summary (we don't set team)
    return params


def crawl_paginated_table(session, start_url, params=None, container_selector='.compare'):
    logging.info('Crawling %s params=%s', start_url, params)
    r = session.get(start_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'lxml')
    base_form = collect_hidden_inputs(soup)

    collected = []
    # If we have params (season, series), POST to apply them
    if params:
        form = base_form.copy()
        form.update(params)
        rp = session.post(start_url, data=form, headers=HEADERS, timeout=30)
        rp.raise_for_status()
        soup = BeautifulSoup(rp.text, 'lxml')
        base_form.update(collect_hidden_inputs(soup))
        df = extract_table_from_html(rp.text)
        if df is not None:
            collected.append(df)
        resp_for_paging = rp
    else:
        df = extract_table_from_html(r.text)
        if df is not None:
            collected.append(df)
        resp_for_paging = r

    # find pager links
    paging = soup.select_one('.paging')
    page_urls = []
    page_events = []
    if paging:
        for a in paging.find_all('a'):
            href = a.get('href') or ''
            href = href.strip()
            if href.startswith("javascript:__doPostBack"):
                try:
                    inner = href[href.find('(')+1: href.rfind(')')]
                    parts = [p.strip().strip("'\"") for p in inner.split(',')]
                    eventtarget = parts[0] if parts else ''
                    eventarg = parts[1] if len(parts) > 1 else ''
                    if 'btnNo' in eventtarget:
                        page_events.append((eventtarget, eventarg))
                except Exception:
                    continue
            else:
                full = urljoin(start_url, href)
                if full and full != resp_for_paging.url and full not in page_urls:
                    page_urls.append(full)

    # fetch simple urls
    for pu in page_urls:
        logging.info('Fetching page %s', pu)
        r2 = session.get(pu, headers=HEADERS, timeout=30)
        r2.raise_for_status()
        dfp = extract_table_from_html(r2.text)
        if dfp is not None:
            collected.append(dfp)
        time.sleep(1)

    # handle postback numeric pager
    seen = set()
    for target, arg in page_events:
        key = f"{target}|{arg}"
        if key in seen:
            continue
        seen.add(key)
        logging.info('Posting postback %s %s', target, arg)
        form = base_form.copy()
        if params:
            form.update(params)
        form['__EVENTTARGET'] = target
        form['__EVENTARGUMENT'] = arg
        rp = session.post(start_url, data=form, headers=HEADERS, timeout=30)
        rp.raise_for_status()
        soup2 = BeautifulSoup(rp.text, 'lxml')
        base_form.update(collect_hidden_inputs(soup2))
        dfp = extract_table_from_html(rp.text)
        if dfp is not None:
            collected.append(dfp)
        time.sleep(1)

    if not collected:
        return None
    combined = pd.concat(collected, ignore_index=True)
    return combined


def crawl_and_save(session, url, year, out_path, for_hitter=False):
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'lxml')
        params = build_select_params(soup, year, for_hitter=for_hitter)
        df = crawl_paginated_table(session, url, params=params, container_selector='.compare')
        if df is None:
            logging.warning('No table found for %s %s', url, year)
            return False
        ensure_dir(os.path.dirname(out_path))
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
        logging.info('Saved %s', out_path)
        return True
    except Exception as e:
        logging.exception('Failed to crawl %s %s: %s', url, year, e)
        return False


def main():
    session = requests.Session()

    urls = {
        'hitting': 'https://www.koreabaseball.com/Record/Team/Hitter/Basic1.aspx',
        'pitching': 'https://www.koreabaseball.com/Record/Team/Pitcher/Basic1.aspx',
        'defense': 'https://www.koreabaseball.com/Record/Team/Defense/Basic.aspx',
        'running': 'https://www.koreabaseball.com/Record/Team/Runner/Basic.aspx',
    }

    for year in YEARS:
        out_dir = os.path.join(OUT_BASE, str(year), 'league_info')
        ensure_dir(out_dir)

        hitting_path = os.path.join(out_dir, f"{year}_hitting_summary.csv")
        pitching_path = os.path.join(out_dir, f"{year}_pitching_summary.csv")
        defense_path = os.path.join(out_dir, f"{year}_defense_summary.csv")
        running_path = os.path.join(out_dir, f"{year}_running_summary.csv")

        # hitting: set for_hitter True to pick series default
        crawl_and_save(session, urls['hitting'], year, hitting_path, for_hitter=True)
        time.sleep(1)
        crawl_and_save(session, urls['pitching'], year, pitching_path, for_hitter=False)
        time.sleep(1)
        crawl_and_save(session, urls['defense'], year, defense_path, for_hitter=False)
        time.sleep(1)
        crawl_and_save(session, urls['running'], year, running_path, for_hitter=False)
        time.sleep(1)


if __name__ == '__main__':
    main()

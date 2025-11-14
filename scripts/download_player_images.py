#!/usr/bin/env python3
"""
Download player photo images from KBO player detail pages.

Saves images to `data/player_info/image/<player_id>.jpg`.
Reads player ids from `data/player_attributes.csv` (column `player_id`) or
falls back to `data/player_ids_from_crawler.csv`.

Usage:
  python scripts\download_player_images.py
  python scripts\download_player_images.py --limit 10
"""
import os
import time
import argparse
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "https://www.koreabaseball.com"


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def read_player_ids():
    # Try multiple possible sources for player ids
    candidates = [
        Path('data/player_attributes.csv'),
        Path('data/player_info/player_attributes.csv'),
        Path('data/player_ids_from_crawler.csv'),
    ]
    for pa in candidates:
        if pa.exists():
            try:
                df = pd.read_csv(pa, dtype=str)
                # common column names
                for col in ['player_id', 'playerId', 'playerIdFromCrawl']:
                    if col in df.columns:
                        return df[col].dropna().astype(str).unique().tolist()
                # fallback to first column
                if df.shape[1] >= 1:
                    return df.iloc[:, 0].dropna().astype(str).unique().tolist()
            except Exception:
                logging.exception(f'Failed reading {pa}')
    raise FileNotFoundError('No source of player ids found. Create data/player_attributes.csv or data/player_ids_from_crawler.csv')


def find_photo_src(html: str):
    soup = BeautifulSoup(html, 'lxml')
    # Try common selector: .player_basic .photo img
    el = soup.select_one('.player_basic .photo img')
    if el and el.get('src'):
        return el.get('src')
    # fallback: find .player_basic .photo and any img inside
    photo = soup.select_one('.player_basic .photo')
    if photo:
        img = photo.find('img')
        if img and img.get('src'):
            return img.get('src')
    # last try: any img under .player_basic
    pb_img = soup.select_one('.player_basic img')
    if pb_img and pb_img.get('src'):
        return pb_img.get('src')
    return None


def normalize_src(src: str):
    if not src:
        return None
    src = src.strip()
    if src.startswith('http://') or src.startswith('https://'):
        return src
    if src.startswith('//'):
        return 'https:' + src
    # relative path
    return BASE_URL + src


def download_image(session: requests.Session, url: str, dest: Path):
    try:
        r = session.get(url, timeout=15)
        r.raise_for_status()
        with open(dest, 'wb') as f:
            f.write(r.content)
        return True
    except Exception:
        logging.exception(f'Failed download image from {url}')
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=0, help='Limit number of players (0 = all)')
    parser.add_argument('--delay', type=float, default=0.2, help='Delay between requests (seconds)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    out_dir = Path('data/player_info/image')
    ensure_dir(out_dir)

    ids = read_player_ids()
    if args.limit and args.limit > 0:
        ids = ids[: args.limit]

    session = requests.Session()
    session.headers.update({'User-Agent': 'mcc_ojt_kbo-bot/1.0 (+https://github.com)'})

    total = len(ids)
    success = 0
    failed = []

    for i, pid in enumerate(ids, start=1):
        pid_str = str(pid).strip()
        logging.info(f'[{i}/{total}] Fetching playerId={pid_str}')
        page_url = f'{BASE_URL}/Record/Player/HitterDetail/Basic.aspx?playerId={pid_str}'
        try:
            r = session.get(page_url, timeout=15)
            r.raise_for_status()
            src = find_photo_src(r.text)
            if not src:
                # try PitcherDetail as fallback
                page_url_p = f'{BASE_URL}/Record/Player/PitcherDetail/Basic.aspx?playerId={pid_str}'
                r2 = session.get(page_url_p, timeout=15)
                r2.raise_for_status()
                src = find_photo_src(r2.text)
                if src:
                    src = normalize_src(src)
                else:
                    logging.warning(f'No photo src found for playerId={pid_str}')
            else:
                src = normalize_src(src)

            if src:
                dest = out_dir / f'{pid_str}.jpg'
                ok = download_image(session, src, dest)
                if ok:
                    success += 1
                else:
                    failed.append(pid_str)
            else:
                failed.append(pid_str)

        except Exception:
            logging.exception(f'Error fetching page for playerId={pid_str}')
            failed.append(pid_str)

        # immediate write is the saved file itself; add polite delay
        time.sleep(args.delay)

    logging.info(f'Download completed. success={success}, failed={len(failed)}')
    if failed:
        logging.info('Failed ids sample: %s', ','.join(failed[:20]))


if __name__ == '__main__':
    main()

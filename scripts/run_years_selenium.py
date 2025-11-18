#!/usr/bin/env python3
"""Run Selenium game-by-game crawler across year ranges.

Usage:
    python scripts\run_years_selenium.py 2021 2025 --headless True --start 20210101 --end 20251231 --delay 0.6

By default this will iterate from Jan 1 of start_year to Dec 31 of end_year.
You can override exact start/end dates with --start/--end in YYYYMMDD format.
"""
import sys
import subprocess
import argparse
from datetime import datetime, timedelta, date

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def run_dates(start_date, end_date, headless=True, delay=0.6):
    py = sys.executable
    for dt in daterange(start_date, end_date):
        date_s = dt.strftime('%Y%m%d')
        print('===', date_s, '===')
        cmd = [py, 'scripts\\crawl_game_by_game_selenium.py', '--date', date_s, '--headless', 'True' if headless else 'False']
        try:
            p = subprocess.run(cmd, check=False)
            print('returncode', p.returncode)
        except Exception as e:
            print('failed', e)
        # polite pause
        from time import sleep
        sleep(delay)


def make_driver(headless=True, window_size='1200,900'):
    opts = ChromeOptions()
    if headless:
        opts.add_argument('--headless=new')
        opts.add_argument('--disable-gpu')
    opts.add_argument(f'--window-size={window_size}')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    service = ChromeService(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)


GAMECENTER_URL = 'https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx'


def run_seasons(start_year, end_year, headless=True, delay=0.6):
    # season ranges (inclusive) provided by user
    season_ranges = {
        2021: ('20210403', '20211031'),
        2022: ('20220402', '20221011'),
        2023: ('20230401', '20231017'),
        2024: ('20240323', '20241001'),
        2025: ('20250322', '20251004')
    }

    py = sys.executable

    for yr in range(start_year, end_year + 1):
        if yr not in season_ranges:
            print(f'Skipping year {yr}: no season range defined')
            continue
        s_start, s_end = season_ranges[yr]
        start_date = datetime.strptime(s_start, '%Y%m%d').date()
        end_date = datetime.strptime(s_end, '%Y%m%d').date()

        print(f'=== Year {yr}: crawling season {start_date} to {end_date} ===')

        driver = None
        try:
            driver = make_driver(headless=headless)
            # open at the season start date
            driver.get(GAMECENTER_URL + f'?gameDate={start_date.strftime("%Y%m%d")}')
            import time
            time.sleep(1.0)

            # loop clicking the next button until we pass end_date
            while True:
                # read current page date via possible elements
                cur_date = None
                try:
                    cur_date = driver.execute_script("return (document.getElementById('hidGameDate') && document.getElementById('hidGameDate').value) || (document.getElementById('lblGameDate') && document.getElementById('lblGameDate').innerText) || null;")
                except Exception:
                    cur_date = None

                if cur_date:
                    # normalize date string like '2021.03.21' or '20210321'
                    cur = ''.join([c for c in str(cur_date) if c.isdigit()])
                    if len(cur) >= 8:
                        cur = cur[:8]
                    try:
                        cur_dt = datetime.strptime(cur, '%Y%m%d').date()
                    except Exception:
                        cur_dt = None
                else:
                    cur_dt = None

                # if we couldn't determine date, break
                if not cur_dt:
                    print('Could not determine current date on page, aborting year', yr)
                    break

                # stop when past end_date
                if cur_dt > end_date:
                    print('Reached past season end:', cur_dt)
                    break

                # check if this date is before season start (some pages may start earlier)
                if cur_dt < start_date:
                    # click next to advance
                    try:
                        nxt = driver.find_element(By.ID, 'lnkNext')
                        nxt.click()
                        time.sleep(delay)
                        continue
                    except Exception:
                        print('Next button not found; aborting year', yr)
                        break

                # check if games present on page
                has_games = False
                try:
                    els = driver.find_elements(By.CSS_SELECTOR, '.game-list-n > li')
                    if els and len(els) > 0:
                        has_games = True
                except Exception:
                    has_games = False

                if has_games:
                    date_s = cur_dt.strftime('%Y%m%d')
                    print('Found games on', date_s, '- invoking crawler')
                    cmd = [py, 'scripts\\crawl_game_by_game_selenium.py', '--date', date_s, '--headless', 'True' if headless else 'False']
                    try:
                        p = subprocess.run(cmd, check=False)
                        print('crawler returncode', p.returncode)
                    except Exception as e:
                        print('crawler execution failed', e)

                # click next to go to next date
                try:
                    nxt = driver.find_element(By.ID, 'lnkNext')
                    nxt.click()
                    time.sleep(delay)
                except Exception:
                    print('Next button not found or not clickable; finishing year', yr)
                    break

        except WebDriverException as e:
            print('WebDriver failed for year', yr, e)
        finally:
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass


def parse_ymd(s):
    return datetime.strptime(s, '%Y%m%d').date()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('start_year', type=int, nargs='?', help='start year (e.g. 2021)')
    parser.add_argument('end_year', type=int, nargs='?', help='end year (e.g. 2025)')
    parser.add_argument('--start', type=str, help='explicit start date YYYYMMDD')
    parser.add_argument('--end', type=str, help='explicit end date YYYYMMDD')
    parser.add_argument('--headless', type=lambda s: s.lower() in ('true','1','yes'), default=True)
    parser.add_argument('--delay', type=float, default=0.6)
    args = parser.parse_args()

    if args.start and args.end:
        start_date = parse_ymd(args.start)
        end_date = parse_ymd(args.end)
        print('Running crawler from', start_date, 'to', end_date, 'headless=', args.headless)
        run_dates(start_date, end_date, headless=args.headless, delay=args.delay)
    else:
        if not args.start_year or not args.end_year:
            parser.error('Either provide start/end years or --start and --end dates')
        # use season-aware run which only crawls regular-season dates per year
        print(f'Running season-aware crawler for years {args.start_year} to {args.end_year} headless={args.headless}')
        run_seasons(args.start_year, args.end_year, headless=args.headless, delay=args.delay)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Selenium-based game-by-game crawler for KBO GameCenter.

This script opens the GameCenter page in a real browser (headless by default),
loads the specified date (via query param or JS), waits for the dynamic game
list to populate, then iterates games and extracts review/boxscore sections.

Usage:
    python scripts/crawl_game_by_game_selenium.py --date 20210321 [--headless True|False]

Outputs:
 - `data/<YYYYMMDD>/game_stats/<YYYYMMDD>_games.csv`
 - `log/games/<YYYYMMDD>_gamecrawl_log.csv`

Notes:
 - Uses `webdriver-manager` to download a matching ChromeDriver automatically.
 - If Chrome is not available, set `--browser firefox` and install geckodriver separately.
"""
import os
import time
import csv
import logging
from typing import List, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data')
GAMECENTER_URL = 'https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx'


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def make_driver(headless: bool = True, window_size: str = '1200,900'):
    opts = ChromeOptions()
    if headless:
        opts.add_argument('--headless=new')
        opts.add_argument('--disable-gpu')
    opts.add_argument(f'--window-size={window_size}')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    # instantiate with webdriver-manager
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver


def extract_game_items(driver) -> List[Dict]:
    """Return a list of dicts with game_id and element WebElement"""
    items = []
    try:
        # Wait up to 8s for the game list items to appear
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.game-list-n > li')))
    except TimeoutException:
        logging.warning('Timed out waiting for game list items')
        return items

    els = driver.find_elements(By.CSS_SELECTOR, '.game-list-n > li')
    for el in els:
        gid = el.get_attribute('g_id') or el.get_attribute('data-gid') or el.get_attribute('gId')
        if not gid:
            # try other attributes
            gid = el.get_attribute('g_id') or el.get_attribute('g_id')
        if gid:
            items.append({'game_id': gid, 'element': el})
    return items


def click_and_extract_review(driver, li_el) -> Dict:
    """Click a game list element, select REVIEW tab and extract page contents."""
    result = {'game_info': '', 'run_T': [], 'run_B': []}
    try:
        driver.execute_script("arguments[0].scrollIntoView(true);", li_el)
        li_el.click()
        # Wait a moment for tabs to be populated
        WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#tabDepth2 > li')))
        # find REVIEW tab
        try:
            review_tab = driver.find_element(By.CSS_SELECTOR, '#tabDepth2 > li[section=REVIEW] > a')
            review_tab.click()
        except Exception:
            # if no explicit REVIEW tab, try clicking tab containing '리뷰'
            tabs = driver.find_elements(By.CSS_SELECTOR, '#tabDepth2 > li > a')
            for t in tabs:
                if '리뷰' in t.text:
                    t.click()
                    break

        # wait for gameCenterContents record-etc or box-score
        try:
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#gameCenterContents .record-etc, #gameCenterContents .tbl-box-score.data3')))
        except TimeoutException:
            logging.debug('Review content did not appear quickly')

        # extract record-etc spans into separate keys by span id
        try:
            rec = driver.find_element(By.CSS_SELECTOR, '#gameCenterContents .box-score-area .record-etc')
            spans = rec.find_elements(By.CSS_SELECTOR, 'span[id]')
            for sp in spans:
                sid = sp.get_attribute('id')
                text = sp.text.strip()
                if sid:
                    # normalize key name
                    key = sid
                    result[key] = text
            # also keep a joined fallback
            lines = [s.strip() for s in rec.text.splitlines() if s.strip()]
            result['game_info'] = ' | '.join(lines)
        except Exception:
            pass

        # extract box-score td.run_T and td.run_B under box-score-wrap
        try:
            tds_top = driver.find_elements(By.CSS_SELECTOR, '#gameCenterContents .box-score-area .box-score-wrap .tbl-box-score.data3 td.run_T')
            if tds_top:
                result['run_T'] = [td.text.strip() for td in tds_top if td.text.strip()]
        except Exception:
            pass
        try:
            tds_bot = driver.find_elements(By.CSS_SELECTOR, '#gameCenterContents .box-score-area .box-score-wrap .tbl-box-score.data3 td.run_B')
            if tds_bot:
                result['run_B'] = [td.text.strip() for td in tds_bot if td.text.strip()]
        except Exception:
            pass

    except Exception as e:
        logging.exception('Failed extracting review: %s', e)

    return result


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help='YYYYMMDD')
    parser.add_argument('--headless', type=lambda s: s.lower() in ('true','1','yes'), default=True)
    parser.add_argument('--wait', type=float, default=0.6)
    args = parser.parse_args()

    date_str = args.date
    year = date_str[:4]
    out_dir = os.path.join(DATA_DIR, year, 'game_info')
    ensure_dir(out_dir)
    data_path = os.path.join(out_dir, f"{year}_Games_all.csv")

    log_dir = os.path.join(BASE_DIR, 'log', 'games')
    ensure_dir(log_dir)
    log_path = os.path.join(log_dir, f"{year}_games_log.csv")
    log_exists = os.path.exists(log_path)
    lf = open(log_path, 'a', newline='', encoding='utf-8-sig')
    lw = csv.writer(lf)
    if not log_exists:
        lw.writerow(['game_id','success'])

    driver = None
    try:
        driver = make_driver(headless=args.headless)
        # open with query param to set server-side param
        url = GAMECENTER_URL + f'?gameDate={date_str}'
        logging.info('Opening %s', url)
        driver.get(url)

        # wait short for scripts to run and content to load
        time.sleep(1.0)

        items = extract_game_items(driver)
        if not items:
            logging.info('No items found, attempting to call getGameDate via JS')
            try:
                driver.execute_script(f"getGameDate('{date_str}');")
            except Exception:
                logging.debug('getGameDate() call failed')
            time.sleep(1.0)
            items = extract_game_items(driver)

        if not items:
            logging.error('No games found for date %s', date_str)
            lw.writerow([date_str,'','X'])
            return

        rows = []
        for it in items:
            gid = it['game_id']
            li = it['element']
            parsed = click_and_extract_review(driver, li)
            # try to extract home/away team names or ids from the li element attributes
            try:
                away_team = li.get_attribute('away_id') or li.get_attribute('away') or li.get_attribute('away_nm') or ''
            except Exception:
                away_team = ''
            try:
                home_team = li.get_attribute('home_id') or li.get_attribute('home') or li.get_attribute('home_nm') or ''
            except Exception:
                home_team = ''
            # determine game status from li class (cancel/end)
            try:
                class_attr = (li.get_attribute('class') or '').lower()
            except Exception:
                class_attr = ''
            if 'cancel' in class_attr:
                status = 'canceled'
            else:
                # treat 'end' or default as finished
                status = 'finished'
            
            def normalize_team_alias(name: str) -> str:
                if not name:
                    return ''
                s = str(name).strip()
                key = s.lower().replace(' ', '').replace('-', '').replace('\u00a0','')
                mapping = {
                    # two-letter codes (user-provided canonical aliases)
                    'hh': '한화', 'ob': '두산', 'lt': '롯데', 'wo': '키움', 'ss': '삼성', 'ht': 'KIA', 'sk': 'SSG',
                    # common short codes
                    'lg': 'LG', 'kt': 'KT', 'nc': 'NC',
                    # Samsung
                    'samsung': '삼성', '삼성': '삼성', '삼성라이온즈': '삼성', 'samsunglions': '삼성',
                    # Lotte
                    'lotte': '롯데', '롯데': '롯데', 'lottegiants': '롯데', '롯데자이언츠': '롯데',
                    # KIA
                    'kia': 'KIA', '기아': 'KIA', 'kiatigers': 'KIA', '기아타이거즈': 'KIA',
                    # LG
                    'lg트윈스': 'LG', 'lgtwins': 'LG',
                    # Doosan
                    'doosan': '두산', '두산': '두산', 'doosanbears': '두산', '두산베어스': '두산',
                    # Kiwoom
                    'kiwoom': '키움', '키움': '키움', 'kiwoomheroes': '키움', '키움히어로즈': '키움', '넥센': '키움',
                    # KT
                    'ktwiz': 'KT', 'kt위즈': 'KT',
                    # NC
                    'ncdinos': 'NC', '엔씨': 'NC', 'ncdinos': 'NC', 'ncd': 'NC',
                    # SSG
                    'ssg': 'SSG', 'ssglanders': 'SSG', 'ssg랜더스': 'SSG', 'ssgland ers': 'SSG',
                    # Hanwha
                    'hanwha': '한화', '한화': '한화', 'hanwhaeagles': '한화', '한화이글스': '한화'
                }
                return mapping.get(key, s)

            away_team = normalize_team_alias(away_team)
            home_team = normalize_team_alias(home_team)
            # prepare row with canonical column names
            row = {
                'game_id': gid,
                'stadium': '',
                'team_away': away_team,
                'team_home': home_team,
                'status': status,
                'crowd': '',
                'time_start': '',
                'time_end': '',
                'time_run': ''
            }

            # helper to strip prefix before ':'
            def strip_after_colon(s):
                if not s or not isinstance(s, str):
                    return ''
                if ':' in s:
                    return s.split(':', 1)[1].strip()
                parts = s.split()
                if len(parts) >= 2:
                    return ' '.join(parts[1:]).strip()
                return s.strip()

            # copy parsed keys into canonical names when present
            if 'txtStadium' in parsed:
                row['stadium'] = strip_after_colon(parsed.get('txtStadium'))
            if 'txtCrowd' in parsed and status != 'canceled':
                import re
                crowd_val = strip_after_colon(parsed.get('txtCrowd'))
                digits = re.sub(r'[^0-9]', '', str(crowd_val))
                if digits:
                    try:
                        row['crowd'] = int(digits)
                    except Exception:
                        row['crowd'] = 0
                else:
                    row['crowd'] = 0
            if 'txtStartTime' in parsed:
                row['time_start'] = strip_after_colon(parsed.get('txtStartTime'))
            if 'txtEndTime' in parsed:
                row['time_end'] = strip_after_colon(parsed.get('txtEndTime'))
            if 'txtRunTime' in parsed and status != 'canceled':
                row['time_run'] = strip_after_colon(parsed.get('txtRunTime'))

            # try to read stadium from li attribute (s_nm) which appears in .game-list-n li
            if not row['stadium']:
                try:
                    s_nm = li.get_attribute('s_nm') or li.get_attribute('sname') or li.get_attribute('sNm')
                    if s_nm and str(s_nm).strip():
                        row['stadium'] = str(s_nm).strip()
                except Exception:
                    pass

            # fallback extraction from merged game_info if needed
            if not row['stadium'] and parsed.get('game_info'):
                import re
                m = re.search(r'구장\s*[:：]\s*([^,\n]+)', parsed.get('game_info'))
                if m:
                    row['stadium'] = m.group(1).strip()
                m2 = re.search(r'관중\s*[:：]\s*([0-9,]+)', parsed.get('game_info'))
                if m2 and (not row['crowd'] or row['crowd'] == '') and status != 'canceled':
                    try:
                        row['crowd'] = int(m2.group(1).replace(',', ''))
                    except Exception:
                        row['crowd'] = 0

            if status == 'canceled':
                # for canceled games set n/a for crowd/time/score fields
                row['crowd'] = 'n/a'
                row['time_start'] = 'n/a'
                row['time_end'] = 'n/a'
                row['time_run'] = 'n/a'
                row['score_away'] = 'n/a'
                row['score_home'] = 'n/a'
            else:
                # ensure crowd is an integer
                try:
                    if row['crowd'] is None or str(row['crowd']).strip() == '':
                        row['crowd'] = 0
                    else:
                        row['crowd'] = int(row['crowd'])
                except Exception:
                    try:
                        row['crowd'] = int(str(row['crowd']).replace(',', '').strip())
                    except Exception:
                        row['crowd'] = 0

                # compute total scores from per-inning run lists
                import re
                def sum_runs(lst):
                    total = 0
                    for v in lst or []:
                        s = str(v).strip()
                        if not s:
                            continue
                        m = re.search(r'(-?\d+)', s)
                        if m:
                            try:
                                total += int(m.group(1))
                            except Exception:
                                continue
                    return total

                row['score_away'] = sum_runs(parsed.get('run_T', []))
                row['score_home'] = sum_runs(parsed.get('run_B', []))

            has_data = any([row.get('score_away'), row.get('score_home'), row.get('stadium'), row.get('team_home'), row.get('team_away')])
            if has_data:
                rows.append(row)
                lw.writerow([gid, 'O'])
            else:
                lw.writerow([gid, 'X'])
            lf.flush()
            time.sleep(args.wait)

        if rows:
            # canonical columns we want to keep in final CSV
            cols_order = ['game_id', 'stadium', 'team_away', 'team_home', 'status', 'crowd',
                          'time_start', 'time_end', 'time_run', 'score_away', 'score_home']

            # helper to normalize legacy column names to canonical ones
            legacy_map = {
                'txtStadium': 'stadium', '홈': 'team_home', '원정팀': 'team_away',
                'txtCrowd': 'crowd', 'txtStartTime': 'time_start', 'txtEndTime': 'time_end',
                'txtRunTime': 'time_run', 'run_T': 'score_away', 'run_B': 'score_home'
            }

            newdf = pd.DataFrame(rows)
            # drop internal fallback column if present
            if 'game_info' in newdf.columns:
                newdf.drop(columns=['game_info'], inplace=True, errors='ignore')

            # rename legacy columns in newdf
            rename_map_new = {c: legacy_map[c] for c in newdf.columns if c in legacy_map}
            if rename_map_new:
                newdf.rename(columns=rename_map_new, inplace=True)

            if os.path.exists(data_path):
                try:
                    existing = pd.read_csv(data_path, encoding='utf-8-sig')
                except Exception:
                    existing = pd.DataFrame()
                # rename legacy columns in existing as well
                rename_map_exist = {c: legacy_map[c] for c in existing.columns if c in legacy_map}
                if rename_map_exist:
                    existing.rename(columns=rename_map_exist, inplace=True)

                # ensure no duplicate internal cols
                for df_ in (existing, newdf):
                    if 'game_info' in df_.columns:
                        df_.drop(columns=['game_info'], inplace=True, errors='ignore')

                combined = pd.concat([existing, newdf], ignore_index=True, sort=False)
                combined.drop_duplicates(subset=['game_id'], inplace=True)
            else:
                combined = newdf

            # coerce legacy string score/crowd formats into integers where possible
            import re
            def coerce_int_or_na(v):
                if v is None:
                    return ''
                s = str(v).strip()
                if s.lower() == 'n/a':
                    return 'n/a'
                if '||' in s:
                    parts = [p.strip() for p in s.split('||')]
                    total = 0
                    for p in parts:
                        m = re.search(r'(-?\d+)', p)
                        if m:
                            try:
                                total += int(m.group(1))
                            except Exception:
                                continue
                    return total
                m = re.search(r'(-?\d+)', s)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        return s
                return s

            for col in ('crowd', 'score_away', 'score_home'):
                if col in combined.columns:
                    combined[col] = combined[col].apply(coerce_int_or_na)

            # ensure all canonical columns exist (fill missing with empty string)
            for c in cols_order:
                if c not in combined.columns:
                    combined[c] = ''

            # place canonical columns first, keep any additional columns after
            remaining = [c for c in combined.columns if c not in cols_order]
            combined = combined[cols_order + remaining]
            combined.to_csv(data_path, index=False, encoding='utf-8-sig')
            logging.info('Saved combined file %s (%d rows)', data_path, len(combined))

    except WebDriverException:
        logging.exception('WebDriver failed')
    finally:
        try:
            lf.close()
        except Exception:
            pass
        if driver:
            driver.quit()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Crawl per-player daily records from KBO site (HitterDetail and PitcherDetail).

Usage examples:
  python scripts\crawl_player_daily_records.py --year 2024 --limit 50

Default input player id file: `data/player_ids_from_crawler.csv` (column: playerId or player_id)
Outputs:
  data/<year>/player_stats/<year>_hitter_daily.csv
  data/<year>/player_stats/<year>_pitcher_daily.csv

Behavior:
 - For each playerId, tries HitterDetail/Daily.aspx first. If it finds rows for the requested
   year and 'KBO 정규시즌' competition, it records them into the hitter CSV.
 - If hitter page has no data, tries PitcherDetail/Daily.aspx and writes to pitcher CSV.
 - The script appends results per-player to the CSVs and makes a timestamped backup if file does not exist.
 - Includes small delay between requests and a limit parameter for testing.

Note: The KBO pages use ASP.NET postbacks for changing year/competition. This script attempts to
select year and competition by submitting the form values (including viewstate fields) and
parsing returned HTML. If the site changes structure, the script may need adjustments.
"""
import argparse
import csv
import os
import re
import time
from datetime import datetime
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd


BASE = "https://www.koreabaseball.com/Record/Player"


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def read_player_ids(input_path: str) -> List[str]:
    """Read a CSV of player attributes or id list and return list of player ids.

    Supports files with columns: 'playerId', 'player_id', 'playerid', or Korean '선수_ID'.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    df = pd.read_csv(input_path, dtype=str)
    # Try common English columns
    for col in ("playerId", "player_id", "playerid"):
        if col in df.columns:
            return df[col].dropna().astype(str).str.strip().unique().tolist()
    # Try Korean header used in this repo
    for col in ("선수_ID", "선수id", "선수Id"):
        if col in df.columns:
            return df[col].dropna().astype(str).str.strip().unique().tolist()
    # fallback: first column
    first = df.columns[0]
    return df[first].dropna().astype(str).str.strip().unique().tolist()


def backup_if_missing(file_path: str):
    # Ensure the file exists. Do not create backups.
    if not os.path.exists(file_path):
        open(file_path, "a").close()


def parse_player_records_table(soup: BeautifulSoup, year: int, role: str) -> Optional[pd.DataFrame]:
    """Parse `.player_records` tables according to role ('hitter' or 'pitcher').

    For hitters: collect all tables with class `tbl-type02 tbl-type02-pd0` and parse only `tbody` rows.
    For pitchers: collect tables with class `tbl-type02` but exclude monthly pd0 tables (use selector
    `table.tbl-type02:not(.tbl-type02-pd0)`) and parse `tbody` rows.

    Returns DataFrame with standardized columns (excluding `playerId`) or None if no rows.
    """
    container = soup.select_one(".player_records")
    if not container:
        return None

    if role == "hitter":
        tables = container.select("table.tbl-type02.tbl-type02-pd0")
    else:
        # pitcher: select tables of class tbl-type02 but not the monthly pd0 tables
        tables = container.select("table.tbl-type02:not(.tbl-type02-pd0)")

    if not tables:
        # fallback to any table under container
        tables = container.find_all("table")

    rows_all = []
    for table in tables:
        for tr in table.select("tbody tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not cols:
                continue
            rows_all.append(cols)

    if not rows_all:
        return None

    # Define standardized columns (excluding playerId which is added later)
    if role == "hitter":
        std_cols = [
            "상대",
            "AVG1",
            "PA",
            "AB",
            "R",
            "H",
            "2B",
            "3B",
            "HR",
            "RBI",
            "SB",
            "CS",
            "BB",
            "HBP",
            "SO",
            "GDP",
            "AVG2",
        ]
    else:
        std_cols = [
            "상대",
            "구분",
            "결과",
            "ERA1",
            "TBF",
            "IP",
            "H",
            "HR",
            "BB",
            "HBP",
            "SO",
            "R",
            "ER",
            "ERA2",
        ]

    # Build DataFrame by position-mapping rows to std_cols (pad/truncate as needed)
    records = []
    for r in rows_all:
        # remove empty leading/trailing
        r = [c for c in r]
        if len(r) >= len(std_cols):
            r2 = r[: len(std_cols)]
        else:
            r2 = r + [""] * (len(std_cols) - len(r))
        records.append(r2)

    df = pd.DataFrame(records, columns=std_cols)

    # Normalize date-like column: find any column that contains "일자|날짜|Date|기준일"
    date_col = None
    for c in df.columns:
        if re.search(r"(일자|날짜|Date|기준일)", c, re.I):
            date_col = c
            break

    if date_col:
        # Extract year from date values, but some may be like '2024.03.22' or '23/03/22'
        def extract_year(v):
            if not v or v == "-":
                return None
            m = re.search(r"(20\d{2})", v)
            if m:
                return int(m.group(1))
            # try two-digit year
            m2 = re.search(r"(\d{2})[./-]\d{1,2}[./-]\d{1,2}", v)
            if m2:
                y = int(m2.group(1))
                return 2000 + y
            return None

        df["_rec_year"] = df[date_col].apply(extract_year)
        df = df[df["_rec_year"] == year]
        df = df.drop(columns=["_rec_year"])

    # Also try to filter competition column to 'KBO 정규시즌' if present
    comp_col = None
    for c in df.columns:
        if re.search(r"(경기명|경기구분|구분|Competition)", c, re.I):
            comp_col = c
            break
    if comp_col:
        df = df[df[comp_col].str.contains("KBO", na=False)]

    if df.empty:
        return None
    return df


def read_player_positions(input_path: str) -> dict:
    """Read player attributes CSV and return mapping playerId -> position string.

    Recognizes Korean `선수_ID` and `포지션` columns. If `포지션` contains '투수', it's a pitcher.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    df = pd.read_csv(input_path, dtype=str)
    id_col = None
    pos_col = None
    for col in df.columns:
        if col in ("playerId", "player_id", "playerid", "선수_ID", "선수id", "선수Id"):
            id_col = col
        if col in ("position", "포지션", "Position"):
            pos_col = col
    if id_col is None:
        id_col = df.columns[0]
    if pos_col is None:
        # if no position column, assume all hitters (conservative)
        pos_col = None

    mapping = {}
    for _, row in df.iterrows():
        pid = str(row.get(id_col, "")).strip()
        if not pid or pid.lower() in ("nan", "none"):
            continue
        pos = row.get(pos_col, "") if pos_col is not None else ""
        pos = str(pos) if pos is not None else ""
        mapping[pid] = pos
    return mapping


def make_postback(session: requests.Session, url: str, year: int) -> Optional[BeautifulSoup]:
    """Load page and attempt to post back selecting year and 'KBO 정규시즌'.
    Returns soup of resulting page.
    This tries to find select elements and emulate submit using hidden fields.
    """
    r = session.get(url, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    form = soup.find("form", id=re.compile(r"aspnetForm", re.I)) or soup.find("form")
    if not form:
        return soup

    data = {}
    # collect hidden inputs
    for inp in form.select("input[type=hidden]"):
        name = inp.get("name")
        if not name:
            continue
        data[name] = inp.get("value", "")

    # find year select and competition/series select (include ddlSeries)
    year_select = form.find("select", attrs={"name": re.compile(r"Year|year|ddlYear|ddlSeason", re.I)})
    comp_select = form.find(
        "select",
        attrs={
            "name": re.compile(
                r"GameType|Comp|ddlGameType|ddlLeagueType|ddlGubun|ddlSeries|Series", re.I
            )
        },
    )

    if year_select:
        # try to find option matching year
        opt_val = None
        for opt in year_select.find_all("option"):
            text = opt.get_text(strip=True)
            if str(year) in text:
                opt_val = opt.get("value") or text
                break
        if opt_val is None:
            # fallback to last option
            last = year_select.find_all("option")[-1]
            opt_val = last.get("value")
        data[year_select.get("name")] = opt_val

    if comp_select:
        opt_val = None
        # prefer exact '정규' match first
        for opt in comp_select.find_all("option"):
            text = opt.get_text(strip=True)
            if "정규" in text:
                opt_val = opt.get("value") or text
                break
        # fallback: any KBO option
        if opt_val is None:
            for opt in comp_select.find_all("option"):
                text = opt.get_text(strip=True)
                if "KBO" in text:
                    opt_val = opt.get("value") or text
                    break
        # final fallback: first option
        if opt_val is None:
            opt_val = comp_select.find_all("option")[0].get("value")
        data[comp_select.get("name")] = opt_val

    # try to emulate event target if selects cause postback
    # set __EVENTTARGET to the select name so server triggers postback.
    # Prefer comp_select (series/competition) because pages often require that postback.
    if comp_select and data.get(comp_select.get("name")):
        data["__EVENTTARGET"] = comp_select.get("name")
        data["__EVENTARGUMENT"] = ""
    elif year_select and data.get(year_select.get("name")):
        data["__EVENTTARGET"] = year_select.get("name")
        data["__EVENTARGUMENT"] = ""

    headers = {"Referer": url, "User-Agent": "Mozilla/5.0 (compatible)"}
    post_url = form.get("action") or url
    if post_url and not post_url.lower().startswith("http"):
        # make absolute
        post_url = requests.compat.urljoin(url, post_url)

    resp = session.post(post_url, data=data, headers=headers, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


def crawl_year(year: int, positions_map: dict, input_src: str, limit: Optional[int], delay: float):
    out_dir = os.path.join("data", str(year), "player_stats")
    ensure_dir(out_dir)

    hitter_file = os.path.join(out_dir, f"{year}_hitter_daily.csv")
    pitcher_file = os.path.join(out_dir, f"{year}_pitcher_daily.csv")

    backup_if_missing(hitter_file)
    backup_if_missing(pitcher_file)

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    processed = 0
    total = len(positions_map)
    for pid, pos in positions_map.items():
        if limit and processed >= limit:
            break
        pid = str(pid).strip()
        if not pid:
            continue
        # decide by position: if 포지션 contains '투수' -> pitcher, else hitter
        pos_text = str(pos or "")
        is_pitcher = True if "투수" in pos_text else False

        hitter_url = f"{BASE}/HitterDetail/Daily.aspx?playerId={pid}"
        pitcher_url = f"{BASE}/PitcherDetail/Daily.aspx?playerId={pid}"

        wrote_any = False
        if not is_pitcher:
            # treat as hitter only — do not fallback to pitcher
            try:
                soup = make_postback(session, hitter_url, year)
                df = parse_player_records_table(soup, year, role="hitter")
                if df is not None:
                    # insert player id as first column named '선수id' per user request
                    df.insert(0, "선수id", pid)
                    # enforce exact column order for hitter CSV
                    hitter_cols = [
                        "선수id",
                        "상대",
                        "AVG1",
                        "PA",
                        "AB",
                        "R",
                        "H",
                        "2B",
                        "3B",
                        "HR",
                        "RBI",
                        "SB",
                        "CS",
                        "BB",
                        "HBP",
                        "SO",
                        "GDP",
                        "AVG2",
                    ]
                    # ensure all columns exist; add missing as empty
                    for c in hitter_cols:
                        if c not in df.columns:
                            df[c] = ""
                    df = df[hitter_cols]
                    header = os.path.getsize(hitter_file) == 0
                    df.to_csv(hitter_file, mode="a", index=False, header=header, encoding="utf-8-sig")
                    wrote_any = True
            except Exception:
                pass
        else:
            # pitcher: try pitcher page only
            try:
                soup = make_postback(session, pitcher_url, year)
                df = parse_player_records_table(soup, year, role="pitcher")
                if df is not None:
                    # insert player id as first column named '선수id' per user request
                    df.insert(0, "선수id", pid)
                    # enforce exact column order for pitcher CSV
                    pitcher_cols = [
                        "선수id",
                        "상대",
                        "구분",
                        "결과",
                        "ERA1",
                        "TBF",
                        "IP",
                        "H",
                        "HR",
                        "BB",
                        "HBP",
                        "SO",
                        "R",
                        "ER",
                        "ERA2",
                    ]
                    for c in pitcher_cols:
                        if c not in df.columns:
                            df[c] = ""
                    df = df[pitcher_cols]
                    header = os.path.getsize(pitcher_file) == 0
                    df.to_csv(pitcher_file, mode="a", index=False, header=header, encoding="utf-8-sig")
                    wrote_any = True
            except Exception:
                pass

        processed += 1
        print(f"[{processed}/{total}] playerId={pid} pos={pos_text!r} wrote={wrote_any}")
        time.sleep(delay)

    print(f"Done. Processed {processed} players. Outputs in {out_dir}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True, help="Year to collect (e.g. 2024)")
    p.add_argument("--input", default="data/player_info/player_attributes.csv", help="CSV file with player attributes (선수_ID, 포지션) or ids list")
    p.add_argument("--limit", type=int, default=0, help="Limit number of players (0 = all)")
    p.add_argument("--delay", type=float, default=0.25, help="Delay between requests (seconds)")
    args = p.parse_args()

    positions_map = read_player_positions(args.input)
    # apply limit if requested
    if args.limit and args.limit > 0:
        # keep insertion order but slice
        items = list(positions_map.items())[: args.limit]
        positions_map = dict(items)

    crawl_year(args.year, positions_map, args.input, args.limit or None, args.delay)


if __name__ == "__main__":
    main()

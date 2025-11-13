#!/usr/bin/env python3
"""크롤러: data/player_attributes.csv의 id 컬럼을 사용해
KBO 선수 기본정보(.player_basic)를 크롤링해 컬럼으로 추가해서 저장합니다.

동작:
 - data/player_attributes.csv 읽기
 - id 컬럼 이름(id, playerId, player_id 중 하나)을 자동탐지
 - 각 id에 대해 HitterDetail 먼저 시도, 없으면 PitcherDetail 시도
 - .player_basic 내부의 dt/dd, th/td, p 항목 등을 파싱해 key/value 추출
 - 추출한 key들을 모두 컬럼으로 만들어 원본과 병합
 - 원본 파일은 백업을 만들고 덮어씀 (파일명에 타임스탬프 추가)

사용법: python scripts/enrich_player_attributes.py
"""

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import pandas as pd

LOG = logging.getLogger("enrich_player_attributes")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

BASE = "https://www.koreabaseball.com"
INPUT_CSV = Path("data") / "player_attributes.csv"

def detect_id_column(df: pd.DataFrame):
    for col in df.columns:
        if "id" in col.lower():
            return col
    return None

def parse_player_basic(soup: BeautifulSoup):
    div = soup.select_one(".player_basic")
    if not div:
        return None

    data = {}

    # dt/dd pairs
    dts = div.select("dt")
    dds = div.select("dd")
    if dts and dds and len(dts) == len(dds):
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True).replace(" ", "_")
            val = dd.get_text(" ", strip=True)
            data[key] = val

    # th/td rows
    for tr in div.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if th and td:
            key = th.get_text(strip=True).replace(" ", "_")
            val = td.get_text(" ", strip=True)
            data[key] = val

    # paragraphs like <p>Key : Value</p>
    for p in div.select("p"):
        text = p.get_text(" ", strip=True)
        if ":" in text:
            parts = [s.strip() for s in text.split(":", 1)]
            if len(parts) == 2:
                key = parts[0].replace(" ", "_")
                data[key] = parts[1]

    # list items (li) - sometimes used
    for li in div.select("li"):
        text = li.get_text(" ", strip=True)
        if ":" in text:
            parts = [s.strip() for s in text.split(":", 1)]
            if len(parts) == 2:
                key = parts[0].replace(" ", "_")
                data[key] = parts[1]

    # Fallback: raw text lines
    if not data:
        text = div.get_text("\n", strip=True)
        for line in [l.strip() for l in text.splitlines() if l.strip()]:
            if ":" in line:
                k, v = [s.strip() for s in line.split(":", 1)]
                data[k.replace(" ", "_")] = v

    return data

def fetch_player(id_):
    session = requests.Session()
    urls = [f"{BASE}/Record/Player/HitterDetail/Basic.aspx?playerId={id_}",
            f"{BASE}/Record/Player/PitcherDetail/Basic.aspx?playerId={id_}"]
    for url in urls:
        try:
            r = session.get(url, timeout=15)
        except Exception as e:
            LOG.debug("fetch error %s %s", url, e)
            continue
        if r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        parsed = parse_player_basic(soup)
        if parsed:
            parsed["_source_url"] = url
            return parsed
    return None

def main():
    parser = argparse.ArgumentParser(description="Enrich player_attributes.csv by crawling player basic info")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of players to process (for testing)")
    args = parser.parse_args()

    if not INPUT_CSV.exists():
        LOG.error("Input CSV not found: %s", INPUT_CSV)
        return

    df = pd.read_csv(INPUT_CSV)
    id_col = detect_id_column(df)
    if not id_col:
        LOG.error("Could not detect id column in %s", INPUT_CSV)
        return

    LOG.info("Detected id column: %s", id_col)

    # 강제 문자열로 통일 (병합 시 타입 불일치 방지)
    df[id_col] = df[id_col].astype(str)
    ids = df[id_col].fillna("")
    results = []
    failed = []

    total = len(ids)
    if args.limit and args.limit > 0:
        total = min(total, args.limit)
        ids = ids.iloc[:total]

    LOG.info("Starting enrichment for %d players", total)

    # Backup original before any writes
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = INPUT_CSV.with_name(f"player_attributes_backup_{ts}.csv")
    INPUT_CSV.replace(backup)
    LOG.info("Backed up original to %s", backup)

    for i, pid in enumerate(ids, start=1):
        pid = pid.strip()
        if not pid:
            LOG.warning("Empty id at row %d", i)
            failed.append((i, pid, "empty id"))
            results.append({id_col: pid})
            # write intermediate even for empty
            parsed_df_so_far = pd.DataFrame(results)
            if id_col not in parsed_df_so_far.columns:
                parsed_df_so_far[id_col] = parsed_df_so_far.index.map(lambda _: "")
            parsed_df_so_far[id_col] = parsed_df_so_far[id_col].astype(str)
            merged = df.merge(parsed_df_so_far, how="left", on=id_col)
            merged.to_csv(INPUT_CSV, index=False)
            LOG.info("Wrote intermediate enriched CSV after %d players to %s", i, INPUT_CSV)
            continue

        LOG.info("[%d/%d] Fetching playerId=%s", i, total, pid)
        try:
            parsed = fetch_player(pid)
        except Exception as e:
            LOG.exception("Error fetching %s: %s", pid, e)
            parsed = None

        if parsed:
            parsed[id_col] = pid
            results.append(parsed)
        else:
            failed.append((i, pid, "not found"))
            results.append({id_col: pid})

        # 즉시 중간 결과로 병합 후 원본 CSV 덮어쓰기 (한 선수씩 갱신)
        parsed_df_so_far = pd.DataFrame(results)
        if id_col not in parsed_df_so_far.columns:
            parsed_df_so_far[id_col] = parsed_df_so_far.index.map(lambda _: "")
        parsed_df_so_far[id_col] = parsed_df_so_far[id_col].astype(str)

        merged = df.merge(parsed_df_so_far, how="left", on=id_col)
        # 덮어쓰기
        merged.to_csv(INPUT_CSV, index=False)
        LOG.info("Wrote intermediate enriched CSV after %d players to %s", i, INPUT_CSV)

        time.sleep(0.2)

    # 최종 보고
    LOG.info("Players parsed: %d, failed: %d", len(results) - len(failed), len(failed))
    if failed:
        LOG.info("Sample failures (up to 10): %s", failed[:10])


if __name__ == "__main__":
    main()

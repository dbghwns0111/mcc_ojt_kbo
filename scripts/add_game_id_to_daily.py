#!/usr/bin/env python3
"""Add `game_id` column to year hitter/pitcher daily files using player_team_history and games_all mapping.

Usage: python scripts\add_game_id_to_daily.py

This will process years 2021..2025 and overwrite the daily CSVs with an additional `game_id` column as the first column.
"""
import os
import csv
from datetime import datetime
from collections import defaultdict, Counter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PTH_PLAYER_HISTORY = os.path.join(ROOT, 'data', 'player_info', 'player_team_history.csv')
PTH_ALL_GAMES = os.path.join(ROOT, 'data_etl', 'all_years_Games_all.csv')
PTH_ALL_HITTER = os.path.join(ROOT, 'data_etl', 'all_years_hitter_daily.csv')
PTH_ALL_PITCHER = os.path.join(ROOT, 'data_etl', 'all_years_pitcher_daily.csv')
YEARS = [2021, 2022, 2023, 2024, 2025]


def load_player_history(path):
    # returns dict player_id -> list of (start_date, end_date, team)
    out = defaultdict(list)
    if not os.path.exists(path):
        return out
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for r in reader:
            pid = r.get('player_id')
            team = r.get('team')
            sd = r.get('start_date')
            ed = r.get('end_date')
            try:
                sd_dt = datetime.fromisoformat(sd).date()
                ed_dt = datetime.fromisoformat(ed).date()
            except Exception:
                continue
            out[pid].append((sd_dt, ed_dt, team))
    # sort intervals per player
    for pid in out:
        out[pid].sort()
    return out


def build_team_abbrev_map(games_all_path):
    # returns mapping team_name -> most common 2-letter abbrev found in game_id
    mapping_counts = defaultdict(Counter)
    if not os.path.exists(games_all_path):
        return {}
    with open(games_all_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for r in reader:
            gid = r.get('game_id', '')
            if len(gid) < 12:
                continue
            away_abbr = gid[8:10]
            home_abbr = gid[10:12]
            team_away = r.get('team_away')
            team_home = r.get('team_home')
            if team_away:
                mapping_counts[team_away][away_abbr] += 1
            if team_home:
                mapping_counts[team_home][home_abbr] += 1
    result = {}
    for team, counter in mapping_counts.items():
        if not counter:
            continue
        # pick most common abbrev
        abbr, _ = counter.most_common(1)[0]
        result[team] = abbr
    return result


def build_games_index_and_abbr(etl_games_path):
    """Load the ETL games file and return:
    - team_abbr_map: team_name -> abbr
    - games_index: dict of (ymd, away_abbr, home_abbr) -> game_id
    - abbr_set: set of all seen abbreviations
    """
    mapping_counts = defaultdict(Counter)
    games_index = {}
    abbr_set = set()
    if not os.path.exists(etl_games_path):
        return {}, {}, set()
    with open(etl_games_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for r in reader:
            gid = r.get('game_id', '')
            date_s = r.get('date') or r.get('game_date')
            if not date_s:
                continue
            # extract abbr from game_id if possible
            if len(gid) >= 12:
                away_abbr = gid[8:10]
                home_abbr = gid[10:12]
            else:
                away_abbr = ''
                home_abbr = ''
            team_away = r.get('team_away')
            team_home = r.get('team_home')
            if team_away and away_abbr:
                mapping_counts[team_away][away_abbr] += 1
                abbr_set.add(away_abbr)
            if team_home and home_abbr:
                mapping_counts[team_home][home_abbr] += 1
                abbr_set.add(home_abbr)
            # normalize date to yyyymmdd
            try:
                ymd = datetime.fromisoformat(date_s).strftime('%Y%m%d')
            except Exception:
                continue
            if away_abbr and home_abbr:
                games_index[(ymd, away_abbr, home_abbr)] = gid

    team_abbr = {}
    for team, counter in mapping_counts.items():
        if not counter:
            continue
        abbr, _ = counter.most_common(1)[0]
        team_abbr[team] = abbr

    return team_abbr, games_index, abbr_set


def find_player_team_at(player_history, pid, date_obj):
    intervals = player_history.get(pid)
    if not intervals:
        return None
    for sd, ed, team in intervals:
        if sd <= date_obj <= ed:
            return team
    return None


def process_daily_file(path, player_history, team_abbr_map):
    # read and write with game_id as first column
    if not os.path.exists(path):
        return 0
    tmp_path = path + '.tmp'
    rows_processed = 0
    # support passing combined tuple (team_map, games_index, abbr_set)
    if isinstance(team_abbr_map, tuple):
        team_map, games_index, abbr_set = team_abbr_map
    else:
        team_map = team_abbr_map or {}
        games_index = None
        abbr_set = set()
    with open(path, newline='', encoding='utf-8-sig') as fr, open(tmp_path, 'w', newline='', encoding='utf-8-sig') as fw:
        reader = csv.DictReader(fr)
        fieldnames = reader.fieldnames.copy() if reader.fieldnames else []
        if 'game_id' in fieldnames:
            # already processed
            return 0
        out_fields = ['game_id'] + fieldnames
        writer = csv.DictWriter(fw, fieldnames=out_fields)
        writer.writeheader()
        for r in reader:
            pid = str(r.get('PLAYER_ID') or r.get('player_id') or r.get('선수_ID') or '').strip()
            # support multiple date/opponent column names
            date_s = r.get('GAME_DATE') or r.get('date') or r.get('일자') or r.get('game_date')
            opp = r.get('OPPONENT') or r.get('opponent') or r.get('상대') or ''
            try:
                dobj = datetime.fromisoformat(date_s).date()
            except Exception:
                game_id = ''
                newrow = {k: v for k, v in r.items()}
                newrow['game_id'] = game_id
                writer.writerow(newrow)
                continue
            # find player's team at date
            player_team = find_player_team_at(player_history, pid, dobj)
            game_id = ''
            ymd = dobj.strftime('%Y%m%d')
            # try to resolve abbreviations
            opp_key = (opp or '').strip()
            # direct map from team name -> abbr
            opp_abbr = team_map.get(opp_key)
            player_abbr = team_map.get(player_team) if player_team else None
            # fallback: if opponent already looks like an abbr (2-3 uppercase), use it
            if not opp_abbr and isinstance(opp_key, str) and opp_key.isupper() and 1 < len(opp_key) <= 3:
                opp_abbr = opp_key
            # if we have both abbrs, try both possible orderings and verify against games_index if available
            if player_abbr and opp_abbr:
                # prefer opp as away, player as home
                candidate1 = f"{ymd}{opp_abbr}{player_abbr}0"
                candidate2 = f"{ymd}{player_abbr}{opp_abbr}0"
                # if games_index was attached to team_abbr_map (tuple), handle accordingly
                game_id = ''
                # if games_index is available (we unpacked it earlier), prefer verified lookup
                if games_index is not None:
                    k1 = (ymd, opp_abbr, player_abbr)
                    k2 = (ymd, player_abbr, opp_abbr)
                    game_id = games_index.get(k1) or games_index.get(k2) or ''
                else:
                    # best-effort: assume game exists and use candidate1
                    game_id = candidate1
            newrow = {k: v for k, v in r.items()}
            newrow['game_id'] = game_id
            # maintain order
            ordered = {fn: newrow.get(fn, '') for fn in out_fields}
            writer.writerow(ordered)
            rows_processed += 1
    # replace original
    os.replace(tmp_path, path)
    return rows_processed


def main():
    player_history = load_player_history(PTH_PLAYER_HISTORY)
    total = 0
    # build team abbr and games index from ETL combined games file
    team_abbr_map, games_index, abbr_set = build_games_index_and_abbr(PTH_ALL_GAMES)
    combined_team_map = (team_abbr_map, games_index, abbr_set)

    # process combined ETL daily files first
    for fn in (PTH_ALL_HITTER, PTH_ALL_PITCHER):
        n = process_daily_file(fn, player_history, combined_team_map)
        print(f'Processed {n} rows for {fn}')
        total += n

    # also attempt per-year files (backwards compatible)
    for y in YEARS:
        games_all = os.path.join(ROOT, 'data', str(y), 'game_info', f'{y}_Games_all.csv')
        team_abbr = build_team_abbrev_map(games_all)
        for kind in ('hitter', 'pitcher'):
            fn = os.path.join(ROOT, 'data', str(y), 'player_stats', f'{y}_{kind}_daily.csv')
            # prefer using combined games index when available
            n = process_daily_file(fn, player_history, combined_team_map if team_abbr_map else team_abbr)
            print(f'Processed {n} rows for {fn}')
            total += n

    print('Done. Total rows processed:', total)

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Insert `player_id` into advanced hitter/pitcher CSVs using `data/player_info/player_attributes.csv`.

Rules:
- Only add IDs for names that are unique in `player_attributes.csv` (no 동명이인).
- If a name maps to multiple IDs, skip and leave `player_id` empty for those rows.
- Processes files matching `data/*/player_stats/*_hitter_advanced.csv` and `*_pitcher_advanced.csv`.
"""
import os
import glob
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PLAYER_ATTR = os.path.join(ROOT, 'data', 'player_info', 'player_attributes.csv')


def load_attributes(path):
    df = pd.read_csv(path, dtype=str, encoding='utf-8-sig')
    # normalize column names
    cols = {c: c.strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)
    # expect columns: '선수_ID' and '선수명' or similar
    id_col = None
    name_col = None
    for c in df.columns:
        if '선수' in c and ('ID' in c or '_ID' in c or 'ID' == c or '선수_ID' == c):
            id_col = c
        if '선수' in c and ('명' in c or '명' == c):
            name_col = c
    # fallback common English
    if not id_col and 'player_id' in df.columns:
        id_col = 'player_id'
    if not name_col and 'player_name' in df.columns:
        name_col = 'player_name'

    if not id_col or not name_col:
        raise RuntimeError(f'Cannot find id/name columns in {path}: cols={df.columns.tolist()}')

    # strip whitespace and normalize ids to integer-like strings
    df[name_col] = df[name_col].astype(str).str.strip()
    df[id_col] = df[id_col].astype(str).str.strip()

    # build mapping name -> set of normalized ids (deduplicated)
    mapping = {}
    for _, row in df.iterrows():
        name = row[name_col]
        pid_raw = row[id_col]
        if pd.isna(name) or name == 'nan' or name == '':
            continue
        if pd.isna(pid_raw) or pid_raw == 'nan' or str(pid_raw).strip() == '':
            continue
        pid = str(pid_raw).strip()
        # normalize numeric ids like '55460.0' -> '55460'
        try:
            if '.' in pid:
                pid = str(int(float(pid)))
            else:
                pid = str(int(pid))
        except Exception:
            # non-numeric ids are kept as-is
            pid = pid
        mapping.setdefault(name, set()).add(pid)

    # convert sets to lists and build unique/duplicate maps
    mapping_lists = {name: sorted(list(ids)) for name, ids in mapping.items()}
    unique_map = {name: ids[0] for name, ids in mapping_lists.items() if len(ids) == 1}
    dup_names = {name: ids for name, ids in mapping_lists.items() if len(ids) > 1}
    logging.info('Loaded %d players from attributes, unique names=%d, duplicate names=%d', len(mapping_lists), len(unique_map), len(dup_names))
    return mapping_lists, unique_map, dup_names


def scan_roles(root):
    """Scan all player_stats CSVs to collect observed roles per player_id.
    Returns dict: player_id -> set of roles (e.g., {'hitter','pitcher'})
    """
    roles = {}
    pattern = os.path.join(root, 'data', '*', 'player_stats', '**', '*.csv')
    files = glob.glob(pattern, recursive=True)
    logging.info('Scanning %d player_stats files to build id->roles map', len(files))
    for f in files:
        fname = os.path.basename(f).lower()
        role = None
        if 'hitter' in fname:
            role = 'hitter'
        elif 'pitcher' in fname:
            role = 'pitcher'
        elif 'defense' in fname:
            role = 'defense'
        elif 'runner' in fname:
            role = 'runner'
        else:
            # skip unrelated files
            continue

        try:
            df = pd.read_csv(f, dtype=str, encoding='utf-8-sig')
        except Exception:
            try:
                df = pd.read_csv(f, dtype=str, encoding='cp949')
            except Exception:
                continue

        # heuristically find id column
        id_col = None
        for c in df.columns:
            cc = c.strip().lower()
            if 'id' == cc or 'player_id' == cc or '선수_id' == cc or '선수id' == cc or cc.endswith('id'):
                id_col = c
                break
        if not id_col:
            # try first column as fallback
            if len(df.columns) > 0:
                id_col = df.columns[0]
        if not id_col:
            continue

        ids = df[id_col].dropna().astype(str).str.strip()
        for pid_raw in ids:
            if pid_raw == '' or str(pid_raw).lower() == 'nan':
                continue
            pid = str(pid_raw)
            try:
                if '.' in pid:
                    pid = str(int(float(pid)))
                else:
                    pid = str(int(pid))
            except Exception:
                pid = pid
            roles.setdefault(pid, set()).add(role)

    logging.info('Scanned roles for %d unique player ids', len(roles))
    return roles


def process_file(path, name_to_ids, unique_map, roles_map):
    logging.info('Processing %s', path)
    try:
        df = pd.read_csv(path, dtype=str, encoding='utf-8-sig')
    except Exception:
        df = pd.read_csv(path, dtype=str, encoding='cp949')

    # find Name column (case-sensitive 'Name' in these files)
    name_col = None
    for c in df.columns:
        if c.strip().lower() == 'name' or c.strip() == 'Name' or '이름' in c:
            name_col = c
            break
    if not name_col:
        logging.warning('No Name column found in %s, skipping', path)
        return False, 0, 0

    names = df[name_col].fillna('').astype(str).str.strip()
    ids = []
    added = 0

    # determine desired role from filename
    fname = os.path.basename(path).lower()
    desired_role = 'hitter' if 'hitter' in fname else ('pitcher' if 'pitcher' in fname else None)

    for n in names:
        chosen = ''
        if not n:
            ids.append('')
            continue

        # exact unique mapping
        if n in unique_map:
            chosen = unique_map[n]
        else:
            # multiple candidate ids for this name
            candidates = name_to_ids.get(n, [])
            if len(candidates) == 1:
                chosen = candidates[0]
            elif len(candidates) > 1 and desired_role:
                # prefer candidate that has the desired role
                role_matches = [pid for pid in candidates if desired_role in roles_map.get(pid, set())]
                if len(role_matches) == 1:
                    chosen = role_matches[0]
                else:
                    # ambiguous or none matched -> skip
                    chosen = ''
            else:
                chosen = ''

        if chosen:
            ids.append(chosen)
            added += 1
        else:
            ids.append('')

    # insert player_id as first column
    df.insert(0, 'player_id', ids)

    # write back, preserving utf-8-sig
    df.to_csv(path, index=False, encoding='utf-8-sig')
    logging.info('Wrote %s (added %d ids out of %d rows)', path, added, len(df))
    return True, added, len(df)


def main():
    name_to_ids, unique_map, dup_names = load_attributes(PLAYER_ATTR)
    roles_map = scan_roles(ROOT)

    pattern1 = os.path.join(ROOT, 'data', '*', 'player_stats', '*_hitter_advanced.csv')
    pattern2 = os.path.join(ROOT, 'data', '*', 'player_stats', '*_pitcher_advanced.csv')
    files = sorted(glob.glob(pattern1) + glob.glob(pattern2))
    if not files:
        logging.warning('No advanced hitter/pitcher files found with patterns: %s , %s', pattern1, pattern2)
        return

    summary = []
    for p in files:
        ok, added, total = process_file(p, name_to_ids, unique_map, roles_map)
        summary.append((p, ok, added, total))

    logging.info('Summary:')
    for p, ok, added, total in summary:
        logging.info('%s -> success=%s added=%d/%d', p, ok, added, total)

    if dup_names:
        logging.warning('There are %d duplicated names in attributes; these were skipped unless role-based disambiguation applied.', len(dup_names))


if __name__ == '__main__':
    main()

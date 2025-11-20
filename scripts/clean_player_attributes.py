#!/usr/bin/env python3
"""Clean and normalize `data/player_info/player_attributes.csv`.

Operations:
- Normalize uniform number column (remove 'No.' / '#' and keep digits only).
- Split position column to keep only position (투수/포수/내야수/외야수) and extract 투타 (e.g. 우투우타) into new `투타` column.
- Split height/weight into `신장_cm` and `체중_kg` integer columns, removing units like 'cm' and 'kg'.

The script overwrites the existing CSV with UTF-8-SIG encoding and prints a brief summary.
"""
import os
import re
import glob
import logging
import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PLAYER_ATTR = os.path.join(ROOT, 'data', 'player_info', 'player_attributes.csv')


def read_csv_flexible(path):
    try:
        return pd.read_csv(path, dtype=str, encoding='utf-8-sig')
    except Exception:
        return pd.read_csv(path, dtype=str, encoding='cp949')


def find_column(df, candidates):
    for c in df.columns:
        for cand in candidates:
            if cand == c or cand in c:
                return c
    return None


def normalize_number(val: str):
    if pd.isna(val):
        return ''
    s = str(val).strip()
    # remove common prefixes
    s = re.sub(r'^(No\.?\s*#?:?\s*)', '', s, flags=re.IGNORECASE)
    s = s.replace('#', '').strip()
    m = re.search(r'(\d+)', s)
    if m:
        return str(int(m.group(1)))
    return ''


def split_position_and_batta(val: str):
    """Return (position, batthrow) where batthrow is like 우투우타 or '' if missing."""
    if pd.isna(val):
        return '', ''
    s = str(val).strip()
    if s == '' or s.lower() == 'nan':
        return '', ''

    # extract content in parentheses as possible 투타
    batta = ''
    m = re.search(r'\(([^)]+)\)', s)
    if m:
        cand = m.group(1).strip()
        # if the parentheses content is exactly 4 chars (e.g. '우언우타' or '우투양타'),
        # take it as the 투타 value directly
        if len(cand) == 4:
            batta = cand
        else:
            # common pattern: 우투우타, 우투좌타, 좌투우타, 좌투좌타 etc
            if re.search(r'[우좌].*투.*[우좌].*타', cand):
                batta = cand
        # remove parentheses part from position
        s = re.sub(r'\([^)]*\)', '', s).strip()

    # normalize position keywords to one of 투수/포수/내야수/외야수
    pos_map = {
        '투수': '투수', '포수': '포수', '내야수': '내야수', '외야수': '외야수',
        'INF': '내야수', 'IF': '내야수', 'OF': '외야수'
    }
    pos = ''
    for k in pos_map.keys():
        if k in s:
            pos = pos_map[k]
            break
    if pos == '':
        # heuristics: if contains '내야' or '외야' or '포' or '투'
        if '내야' in s or 'IF' in s:
            pos = '내야수'
        elif '외야' in s or 'OF' in s:
            pos = '외야수'
        elif '포' in s:
            pos = '포수'
        elif '투' in s:
            pos = '투수'
        else:
            pos = s

    return pos, batta


def split_height_weight(val: str):
    """Return (height_cm:int or '', weight_kg:int or '')"""
    if pd.isna(val):
        return '', ''
    s = str(val).strip()
    if s == '' or s.lower() == 'nan':
        return '', ''

    # common patterns: '185cm/85kg', '185 / 85', '185cm 85kg', '185cm/ 85 kg'
    # extract all integers
    nums = re.findall(r'(\d{2,3})', s)
    if len(nums) >= 2:
        h = int(nums[0])
        w = int(nums[1])
        return str(h), str(w)
    # fallback: maybe '185cm' only
    m1 = re.search(r'(\d{2,3})\s*cm', s)
    m2 = re.search(r'(\d{2,3})\s*kg', s)
    h = str(int(m1.group(1))) if m1 else ''
    w = str(int(m2.group(1))) if m2 else ''
    return h, w


def main():
    path = PLAYER_ATTR
    if not os.path.exists(path):
        logging.error('player_attributes file not found: %s', path)
        return

    df = read_csv_flexible(path)
    orig_cols = df.columns.tolist()

    # identify columns
    id_col = find_column(df, ['선수_ID', 'player_id', 'ID', '선수id'])
    name_col = find_column(df, ['선수명', '이름', 'name'])
    num_col = find_column(df, ['등번호', '번호', 'No', 'no'])
    pos_col = find_column(df, ['포지션', '포지션/포지션', 'position'])
    hw_col = find_column(df, ['신장', '체중', '신장/체중', 'height', 'weight'])

    # prepare new columns
    added_cols = []

    # process number
    if num_col:
        logging.info('Normalizing number column `%s`', num_col)
        df['등번호'] = df[num_col].apply(normalize_number)
        added_cols.append('등번호')
    else:
        df['등번호'] = ''
        added_cols.append('등번호')

    # process position and bat/throw
    if pos_col:
        logging.info('Splitting position and 투타 from `%s`', pos_col)
        pos_batta = df[pos_col].apply(lambda x: split_position_and_batta(x))
        df['포지션'] = [p for p, b in pos_batta]
        df['투타'] = [b for p, b in pos_batta]
        added_cols.extend(['포지션', '투타'])
    else:
        df['포지션'] = ''
        df['투타'] = ''
        added_cols.extend(['포지션', '투타'])

    # process height/weight
    if hw_col:
        logging.info('Splitting height/weight from `%s`', hw_col)
        hw = df[hw_col].apply(lambda x: split_height_weight(x))
        df['신장_cm'] = [h for h, w in hw]
        df['체중_kg'] = [w for h, w in hw]
        added_cols.extend(['신장_cm', '체중_kg'])
    else:
        # try to find separate columns
        h_col = find_column(df, ['신장'])
        w_col = find_column(df, ['체중'])
        if h_col:
            df['신장_cm'] = df[h_col].astype(str).str.extract(r'(\d{2,3})', expand=False).fillna('').astype(str)
        else:
            df['신장_cm'] = ''
        if w_col:
            df['체중_kg'] = df[w_col].astype(str).str.extract(r'(\d{2,3})', expand=False).fillna('').astype(str)
        else:
            df['체중_kg'] = ''
        added_cols.extend(['신장_cm', '체중_kg'])

    # ensure id and name columns exist
    if id_col:
        df[id_col] = df[id_col].astype(str).str.strip()
    if name_col:
        df[name_col] = df[name_col].astype(str).str.strip()

    # re-order columns: keep id, name, then our standardized columns, then the rest
    final_cols = []
    if id_col:
        final_cols.append(id_col)
    if name_col:
        final_cols.append(name_col)
    # add the new standardized cols
    final_cols += ['등번호', '포지션', '투타', '신장_cm', '체중_kg']
    # append other original columns that are not in final_cols
    for c in orig_cols:
        if c not in final_cols:
            final_cols.append(c)

    df = df[final_cols]

    # convert numeric columns to integers where possible
    df['등번호'] = df['등번호'].apply(lambda x: int(x) if str(x).isdigit() else '')
    df['신장_cm'] = df['신장_cm'].apply(lambda x: int(x) if str(x).isdigit() else '')
    df['체중_kg'] = df['체중_kg'].apply(lambda x: int(x) if str(x).isdigit() else '')

    # backup original file
    bak = path + '.bak'
    try:
        if not os.path.exists(bak):
            os.replace(path, bak)
            logging.info('Backed up original file to %s', bak)
        else:
            logging.info('Backup already exists: %s', bak)
    except Exception:
        logging.warning('Could not create backup, will overwrite original directly')

    # write processed file
    try:
        df.to_csv(path, index=False, encoding='utf-8-sig')
        logging.info('Wrote cleaned player attributes to %s', path)
    except Exception as e:
        logging.error('Failed to write cleaned file: %s', e)


if __name__ == '__main__':
    main()

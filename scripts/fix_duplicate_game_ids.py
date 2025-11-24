import pandas as pd
from pathlib import Path
import re


def fix_file(fp: Path) -> int:
    """Fix duplicate GAME_IDs and correct dates for GAME_IDs ending with '_2'.

    - For duplicate (PLAYER_ID, GAME_ID) pairs, change last digit '1'->'2' when appropriate,
      otherwise append '_2'.
    - After that, for rows where GAME_ID ends with '_2', if the `game_date` day is
      '01','02' or '03', change it to '10','20','30' respectively, remove the '_2',
      and update any YYYYMMDD/ YYYY-MM-DD found inside GAME_ID to match the corrected date.

    :param fp: Path to CSV file
    :return: number of changes applied
    """
    print(f"Processing: {fp}")
    if not fp.exists():
        print(f"File not found, skipping: {fp}")
        return 0

    df = pd.read_csv(fp, dtype=str)

    # Ensure required columns exist
    if 'GAME_ID' not in df.columns or 'PLAYER_ID' not in df.columns:
        raise ValueError(f"Required columns GAME_ID or PLAYER_ID not found in {fp}")

    seen = set()
    changes = 0

    # First pass: resolve duplicates
    for idx, row in df.iterrows():
        pid = row['PLAYER_ID']
        gid = str(row['GAME_ID'])
        pair = (pid, gid)

        if pair not in seen:
            seen.add(pair)
            continue

        # duplicate found -> adjust GAME_ID
        s = gid
        if len(s) == 13:
            if s[-1] == '1':
                new_gid = s[:-1] + '2'
            else:
                new_gid = s + '_2'
        else:
            new_gid = s + '_2'

        df.at[idx, 'GAME_ID'] = new_gid
        seen.add((pid, new_gid))
        changes += 1

    # Second pass: correct dates for GAME_IDs that end with '_2'
    mapping = {'01': '10', '02': '20', '03': '30'}
    date_col = next((c for c in df.columns if c.lower() == 'game_date'), None)

    if date_col:
        mask = df['GAME_ID'].astype(str).str.endswith('_2')
        for idx in df[mask].index:
            gid = str(df.at[idx, 'GAME_ID'])
            gd = str(df.at[idx, date_col])

            m = re.search(r"(\d{4})[-/\.]?(\d{2})[-/\.]?(\d{2})", gd)
            if not m:
                continue
            year, month_str, day = m.group(1), m.group(2), m.group(3)
            if day not in mapping:
                continue

            new_day = mapping[day]
            new_gd = f"{year}-{month_str}-{new_day}"
            df.at[idx, date_col] = new_gd

            # remove trailing '_2' and update any date inside GAME_ID
            new_gid = gid[:-2]

            # replace compact YYYYMMDD first
            new_gid = re.sub(r"(\d{4})(\d{2})(\d{2})",
                             lambda mo: mo.group(1) + mo.group(2) + new_day,
                             new_gid, count=1)

            # replace dashed YYYY-MM-DD if present
            new_gid = re.sub(r"(\d{4})[-/\.](\d{2})[-/\.](\d{2})",
                             lambda mo: mo.group(1) + mo.group(2) + new_day,
                             new_gid, count=1)

            df.at[idx, 'GAME_ID'] = new_gid
            changes += 1

    # write back
    df.to_csv(fp, index=False)
    print(f"Finished {fp}: applied {changes} changes")
    return changes


def main():
    try:
        base = Path(__file__).resolve().parents[1] / "data_etl"
    except Exception:
        base = Path.cwd().parent / "data_etl"

    hitter = base / "all_years_hitter_daily.csv"
    pitcher = base / "all_years_pitcher_daily.csv"

    total_changes = 0
    print(f"Base data directory assumed: {base.resolve()}")

    for f in (hitter, pitcher):
        if f.exists():
            total_changes += fix_file(f)
        else:
            print(f"File not found, skipping: {f}")

    print(f"Total changes applied: {total_changes}")


if __name__ == '__main__':
    main()

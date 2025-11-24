from pathlib import Path
import pandas as pd

fp = Path(__file__).resolve().parents[1] / 'data_etl' / 'all_years_hitter_daily.csv'
print('Target file:', fp)

if not fp.exists():
    print('File not found, aborting')
    raise SystemExit(1)

# load
df = pd.read_csv(fp, dtype=str)

mask = df['GAME_ID'] == '20210603SSSK0'
count = mask.sum()
print('Rows to change:', int(count))

if count:
    df.loc[mask, 'GAME_ID'] = '20210630SSSK0'
    # find game_date column case-insensitive
    date_col = next((c for c in df.columns if c.lower() == 'game_date'), 'GAME_DATE')
    df.loc[mask, date_col] = '2021-06-30'
    df.to_csv(fp, index=False)
    print('Applied changes and saved file')
else:
    print('No matching rows found')

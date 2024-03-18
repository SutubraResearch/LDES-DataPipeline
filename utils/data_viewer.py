import sqlite3
import pandas as pd
import numpy as np
def load_sqlite_table_into_dataframe(database_file, table_name):
    # Create a connection
    conn = sqlite3.connect(database_file)

    # Execute the query
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)

    # Close connection
    conn.close()

    return df

def create_season_day_to_hourly_map():
    """Creates a mapping from (season_name, time_of_day) to hour numbers (1-8760)."""
    from itertools import product
    months_days = [(month, day) for month in range(1, 13) for day in range(1, 32)
                   if not (month == 2 and day > 28)
                   if not (month in [4, 6, 9, 11] and day > 30)]
    hours = range(1, 25)

    # Create (MM-DD, HH) combinations
    season_day_combinations = list(product(months_days, hours))

    # Map (season_name, time_of_day) to hour numbers
    season_day_map = {}
    for hour_number, ((month, day), hour) in enumerate(season_day_combinations, start=1):
        season_name = f"{month:02}-{day:02}"
        time_of_day = f"{hour:02}"
        season_day_map[(season_name, time_of_day)] = hour_number

    return season_day_map

def add_zeros_to_df(df):
    # Get all unique combinations of 'tech' and 'Hour'
    tech_hour_df = pd.DataFrame(np.array(np.meshgrid(df['tech'].unique(), np.arange(1, 8761))).T.reshape(-1, 2),
                                columns=['tech', 'Hour'])

    tech_hour_df['Hour'] = tech_hour_df['Hour'].astype(int)

    # Merge with the original dataframe
    merged_df = pd.merge(tech_hour_df, df, on=['tech', 'Hour'], how='left')

    # Fill NaNs in 'vflow_out' with 0
    merged_df['vflow_out'].fillna(0, inplace=True)
    return merged_df

# Load the table
df = load_sqlite_table_into_dataframe("../output_dbs/solved/Test_Lite.sqlite", "Output_VFlow_Out")
df = df.query("(sector == 'generation' | sector == 'storage') & t_periods == 2030")
season_day_to_hourly_map = create_season_day_to_hourly_map()
df['Hour'] = df.apply(lambda row: season_day_to_hourly_map[(row['t_season'], row['t_day'])], axis=1)
#df2 = add_zeros_to_df(df)
pivot_df = df.pivot_table(index='Hour', columns='tech', values='vflow_out', fill_value=0)
pivot_df.to_csv('./flow_out.csv', index=True)
print(pivot_df.mean())

df = load_sqlite_table_into_dataframe("../output_dbs/solved/Test_Lite.sqlite", "Output_VFlow_In")
df = df.query("(sector == 'storage') & t_periods == 2030")
season_day_to_hourly_map = create_season_day_to_hourly_map()
df['Hour'] = df.apply(lambda row: season_day_to_hourly_map[(row['t_season'], row['t_day'])], axis=1)
#df2 = add_zeros_to_df(df)
pivot_df_in = df.pivot_table(index='Hour', columns='tech', values='vflow_in', fill_value=0)
pivot_df_in.to_csv('./flow_in.csv', index=True)
print(pivot_df_in.mean())



import pandas as pd
from utils.time_utils import create_hourly_to_season_day_map


def load_and_filter_data(csv_path, planning_years):
    df = pd.read_csv(csv_path)
    df_filtered = df[df['Year'].isin(planning_years)]
    return df_filtered


def prepare_demand_data(df, config):
    for index, row in df.iterrows():
        year = row['Year']
        for region in config['Geography']['regions']:
            if region in df.columns:
                # Assuming demand values are directly in the region columns
                demand = row[region]
                yield region, year, 'demand_elec', demand, 'MWh', ''


def populate_demand_table(db_manager, config, csv_path):
    planning_years = config['Time']['model_years']
    df_filtered = load_and_filter_data(csv_path, planning_years)
    query = '''
    INSERT INTO Demand (regions, periods, demand_comm, demand, demand_units, demand_notes)
    VALUES (?, ?, ?, ?, ?, ?)
    '''
    # Generate data in a list
    demand_data = list(prepare_demand_data(df_filtered, config))
    # Use executemany instead of execute_query
    db_manager.execute_query(query, demand_data, many=True)


def normalize_df(df):
    # Assume 'df' is your DataFrame after filtering for the relevant regions
    return df.div(df.sum(axis=0), axis=1)


def prepare_and_populate_distribution(db_manager, config, csv_path):
    hourly_map = create_hourly_to_season_day_map()
    df = pd.read_csv(csv_path)
    df_normalized = normalize_df(df)
    query = '''
    INSERT INTO DemandSpecificDistribution (regions, season_name, time_of_day_name, demand_name, dds, dds_notes)
    VALUES (?, ?, ?, ?, ?, ?)
    '''
    distribution_data = []
    for hour in range(1, 8761):
        season, time_of_day = hourly_map[hour]
        for region in config['Geography']['regions']:
            if region in df_normalized.columns:
                dds = df_normalized.loc[hour - 1, region]
                distribution_data.append((region, season, time_of_day, 'demand_elec', dds, ''))

    # Use executemany instead of execute_query
    db_manager.execute_query(query, distribution_data, many=True)
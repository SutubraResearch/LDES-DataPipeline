from utils.time_utils import create_hourly_to_season_day_map
import time

def initialize_scenario_dependent_sets(db_manager, config):
    populate_regions(db_manager, config)
    populate_commodities(db_manager)
    populate_GDR(db_manager, config)
    populate_time_periods(db_manager, config)
    populate_myopic_baseyear(db_manager, config)

def initialize_scenario_independent_sets(db_manager):
    populate_commodity_labels(db_manager)
    populate_sector_labels(db_manager)
    populate_technology_labels(db_manager)
    populate_time_of_day(db_manager)
    populate_time_season(db_manager)
    populate_segfrac(db_manager)

def populate_table_with_query(db_manager, query, data):
    for item in data:
        db_manager.execute_query(query, item)


def populate_sector_labels(conn):
    sectors = ['storage', 'generation', 'imports', 'transmission', 'distribution', 'accounting', 'dummy']
    query = 'INSERT INTO sector_labels (sector) VALUES (?)'
    populate_table_with_query(conn, query, [(sector,) for sector in sectors])


def populate_commodity_labels(conn):
    commodity_labels_data = [
        ("p", "physical"),
        ("e", "emission"),
        ("s", "source"),
        ("d", "demand")
    ]
    query = 'INSERT INTO commodity_labels (comm_labels, comm_labels_desc) VALUES (?, ?)'
    populate_table_with_query(conn, query, commodity_labels_data)


def populate_technology_labels(conn):
    technology_labels_data = [
        ("r", "resource"),
        ("p", "production"),
        ("pb", "production (baseload)"),
        ("ps", "production (storage)")
    ]
    query = 'INSERT INTO technology_labels (tech_labels, tech_labels_desc) VALUES (?, ?)'
    populate_table_with_query(conn, query, technology_labels_data)


def populate_time_of_day(conn):
    time_of_day_data = [(f"{i:02}",) for i in range(1, 25)]
    query = 'INSERT INTO time_of_day (t_day) VALUES (?)'
    populate_table_with_query(conn, query, time_of_day_data)


def populate_time_season(conn):
    time_season_data = [(f"{month:02}-{day:02}",) for month in range(1, 13) for day in range(1, 32) if
                        not (month == 2 and day > 28) and not (month in [4, 6, 9, 11] and day > 30)]
    query = 'INSERT INTO time_season (t_season) VALUES (?)'
    populate_table_with_query(conn, query, time_season_data)


def populate_segfrac(conn):
    query = 'INSERT INTO SegFrac (season_name, time_of_day_name, segfrac) VALUES (?, ?, ?)'
    hourly_map = create_hourly_to_season_day_map()
    segfrac_data = [(season_name, time_of_day_name, 1 / 8760) for _,
    (season_name, time_of_day_name) in hourly_map.items()]

    conn.populate_table_with_query_bulk(query, segfrac_data)

def populate_commodities(db_manager):
    # retrieve all unique commodities in the 'in_comm', 'out_comm', and 'emis_comm'
    commodities = set(db_manager.get_all_values_from_columns('Efficiency', ['input_comm', 'output_comm']))
    emis_commodities = set(db_manager.get_all_values_from_columns('EmissionActivity', ['emis_comm']))

    # Create demand_comms, ethos_comms, and emis_comms lists and update commodities set
    demand_comms = [comm for comm in commodities if 'demand' in comm]
    ethos_comms = [comm for comm in commodities if 'ethos' in comm]
    commodities = [comm for comm in commodities if
                   'demand' not in comm and 'ethos' not in comm and comm not in emis_commodities]

    # Distinguish between different types of commodities and assign flags
    emis_data = [(comm, 'e', '') for comm in emis_commodities]
    ethos_data = [(comm, 's', '') for comm in ethos_comms]
    demand_data = [(comm, 'd', '') for comm in demand_comms]
    other_data = [(comm, 'p', '') for comm in commodities]

    # Combine all data
    data = emis_data + ethos_data + demand_data + other_data

    # Now continue processing as before with the refined commodities set
    query = 'INSERT INTO commodities (comm_name, flag, comm_desc) VALUES (?, ?, ?)'
    populate_table_with_query(db_manager, query, data)

def populate_regions(db_manager, config):
    regions = config['Geography']['regions']
    query = 'INSERT INTO regions (regions, region_note) VALUES (?, ?)'
    populate_table_with_query(db_manager, query, [(region, '') for region in regions])

def populate_GDR(db_manager, config):
    gdr = config['Financials']['social_discount_rate']
    query = 'INSERT INTO GlobalDiscountRate (rate) VALUES (?)'
    populate_table_with_query(db_manager, query, [(float(gdr),)])


def populate_time_periods(db_manager, config):
    model_years = config['Time']['model_years']
    model_years.append(
        model_years[-1] + (model_years[-1] - model_years[-2]) if len(model_years) > 1 else model_years[0] + 1)
    query = 'INSERT INTO time_periods (t_periods, flag) VALUES (?, ?)'
    populate_table_with_query(db_manager, query, [(year, 'f') for year in model_years])

    populate_table_with_query(db_manager, query, [(1900, 'e') for year in model_years])


    query = 'INSERT INTO time_period_labels (t_period_labels, t_period_labels_desc) VALUES (?, ?)'
    populate_table_with_query(db_manager, query, [('e', 'existing')])
    populate_table_with_query(db_manager, query, [('f', 'future')])

def populate_myopic_baseyear(db_manager, config):
    base_year = config['Time']['model_years'][0]
    query = 'INSERT INTO MyopicBaseyear (year) VALUES (?)'
    populate_table_with_query(db_manager, query, [(base_year,)])
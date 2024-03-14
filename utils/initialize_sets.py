from utils.time_utils import create_hourly_to_season_day_map
import time


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

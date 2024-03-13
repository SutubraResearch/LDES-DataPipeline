from readers.config_reader import read_config
from utils.db_utils import copy_and_rename_sqlite_template, SQLiteDBManager
from utils.initialize_sets import initialize_scenario_independent_sets
from readers.demands import populate_demand_table, prepare_and_populate_distribution
from readers.new_generators import populate_new_generators


def main():
    config_file_path = "./data_files/configuration/lite.toml"
    config = read_config(config_file_path)

    template_path = "./data_files/sqlite_template/schema.sqlite"
    destination_directory = "./output_dbs/"
    db_name = config['General']['file_name']

    db_path = copy_and_rename_sqlite_template(
        template_path,
        destination_directory,
        db_name
    )
    db_manager = SQLiteDBManager()
    db_manager.create_connection(db_path)

    initialize_scenario_independent_sets(db_manager)

    # Now, populate the Demand table with data from the CSV file
    csv_path = "./data_files/loads/load_forecasts.csv"  # Adjust the path as necessary
    populate_demand_table(db_manager, config, csv_path)
    csv_path = "./data_files/loads/hourly_loads.csv"  # Adjust the path as necessary
    prepare_and_populate_distribution(db_manager, config, csv_path)
    populate_new_generators(db_manager, config, "data_files/generators/new/atb-2023-gens.toml")
    db_manager.close_connection()

    print(f"Database initialized successfully at {db_path}.")



if __name__ == "__main__":
    main()

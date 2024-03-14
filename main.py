from readers.config_reader import read_config
from utils.db_utils import copy_and_rename_sqlite_template, SQLiteDBManager
from utils.initialize_sets import initialize_scenario_independent_sets
from readers.demands import populate_demand_table, prepare_and_populate_distribution
from readers.new_generators import populate_new_generators
import constants

def init_database_from_config(db_manager, config):
    db_name = config['General']['file_name']
    db_path = copy_and_rename_sqlite_template(
        constants.TEMPLATE_PATH,
        constants.DESTINATION_DIRECTORY,
        db_name
    )
    db_manager.create_connection(db_path)
    initialize_scenario_independent_sets(db_manager)

    populate_demand_table(db_manager, config, constants.DEMAND_CSV_PATH)
    prepare_and_populate_distribution(db_manager, config, constants.HOURLY_LOADS_CSV_PATH)
    populate_new_generators(db_manager, config, constants.NEW_GENERATORS_FILE_PATH)

    db_manager.close_connection()
    print(f"Database initialized successfully at {db_path}.")


def main():
    config = read_config(constants.CONFIG_FILE_PATH)
    db_manager = SQLiteDBManager()
    init_database_from_config(db_manager, config)


if __name__ == "__main__":
    main()

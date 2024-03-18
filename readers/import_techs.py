# Constants
# Python 3.11.7
import toml
from utils.data_utils import get_conversion_factor, convert_units, determine_import_techs, construct_import_data, determine_distribution_techs
import constants

# SQL query constant top-level declaration
INSERT_TECHNOLOGIES_QUERY = """
    INSERT INTO technologies (tech, flag, sector, tech_desc, tech_category, unlim_cap)
    VALUES (?, ?, ?, ?, ?, ?)
"""
INSERT_TECH_RESERVE_QUERY = """
    INSERT INTO tech_reserve (tech, notes)
    VALUES (?, ?)
"""
INSERT_EFFICIENCY_QUERY = """
    INSERT INTO Efficiency (regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
INSERT_COST_VARIABLE_QUERY = """
    INSERT INTO CostVariable (regions, periods, tech, vintage, cost_variable, cost_variable_units, cost_variable_notes)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
INSERT_LIFETIME_TECH_QUERY = """
    INSERT INTO LifetimeTech (regions, tech, life, life_notes)
    VALUES (?, ?, ?, ?)
"""
INSERT_EMISSION_ACTIVITY_QUERY = """
    INSERT INTO EmissionActivity (regions, emis_comm, input_comm, tech, vintage, output_comm, emis_act, emis_act_units, emis_act_notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
def read_new_generator_data(file_path):
    with open(file_path, 'r') as file:
        data = toml.load(file)
    return data

def get_converted_value(value, from_unit, to_unit, conversion_type):
    if conversion_type == "activity":
        filepath = constants.ACTIVITY_CONVERSION_FILEPATH
    elif conversion_type == "capacity":
        filepath = constants.CAPACITY_CONVERSION_FILEPATH
    elif conversion_type == "emissions":
        filepath = constants.EMISSIONS_CONVERSION_FILEPATH
    elif conversion_type == "currency":
        filepath = constants.CURRENCY_CONVERSION_FILEPATH

    else:
        return value

    conversion_table = get_conversion_factor(filepath)
    return convert_units(value, from_unit, to_unit, conversion_table)


def populate_technologies(db_manager, gen_data, config, sector="generation", flag="p"):
    unique_technologies = set()

    for resource in gen_data:
        if resource not in unique_technologies:
            resource_data = gen_data[resource]
            if sector == "import":
                note = "technology to import " + resource_data.get('output_comm')
            elif sector == "distribution":
                note = "intra-regional transmission and distribution"
            data_tuple = (
                resource_data.get('name'),
                flag,
                sector,
                note,
                "",
                1
            )
            db_manager.execute_query(INSERT_TECHNOLOGIES_QUERY, data_tuple)
            unique_technologies.add(resource_data.get('name'))


def populate_efficiency_table(db_manager, gen_data, config):
    for (generator_name, generator_info) in gen_data.items():
        data_tuple = (
            generator_info['region'],
            generator_info['input_comm'],
            generator_info['name'],
            generator_info['vintage'],
            generator_info['output_comm'],
            generator_info['efficiency'],
            ""
        )
        db_manager.execute_query(INSERT_EFFICIENCY_QUERY, data_tuple)


def populate_cost_variable_table(db_manager, gen_data, config):
    for (generator_name, generator_info) in gen_data.items():
        for idx, year in enumerate(config['Time']['model_years']):
            data_tuple = (
                generator_info['region'],
                year,
                generator_info['name'],
                generator_info['vintage'],
                generator_info['variable_costs'][idx],
                "USD/MMBtu",
                generator_info['data_source']
            )
            db_manager.execute_query(INSERT_COST_VARIABLE_QUERY, data_tuple)

def populate_lifetime_tech_table(db_manager, gen_data, config):

    for (generator_name, generator_info) in gen_data.items():
        for idx, year in enumerate(config['Time']['model_years']):
            data_tuple = (
                generator_info['region'],
                generator_info['name'],
                generator_info['lifetime'],
                ""
            )
            db_manager.execute_query(INSERT_LIFETIME_TECH_QUERY, data_tuple)

def populate_emission_activity_table(db_manager, gen_data, config):
    for (generator_name, generator_info) in gen_data.items():
        if generator_info['emissions'] > 0:
            data_tuple = (
                generator_info['region'],
                'co2',
                generator_info['input_comm'],
                generator_info['name'],
                generator_info['vintage'],
                generator_info['output_comm'],
                generator_info['emissions'] / 1000, # TODO: generalize.
                "t/MMBtu",
                'NREL Data'
            )
            db_manager.execute_query(INSERT_EMISSION_ACTIVITY_QUERY, data_tuple)

def populate_import_techs(db_manager, config, import_filename, fuels_filename):
    import_dict = determine_import_techs(db_manager, fuels_filename)
    imp_data = construct_import_data(config, import_dict, import_filename, fuels_filename)
    populate_technologies(db_manager, imp_data, config,  sector="import", flag="r")
    populate_efficiency_table(db_manager, imp_data, config)
    populate_cost_variable_table(db_manager, imp_data, config)
    populate_lifetime_tech_table(db_manager, imp_data, config)
    populate_emission_activity_table(db_manager, imp_data, config)

def populate_distribution_tech(db_manager, config, dist_filename):
    dist_data = determine_distribution_techs(config, dist_filename)
    populate_technologies(db_manager, dist_data, config, sector="distribution", flag="p")
    populate_efficiency_table(db_manager, dist_data, config)
    # populate_cost_variable_table(db_manager, dist_data, config)
    populate_lifetime_tech_table(db_manager, dist_data, config)
    # populate_emission_activity_table(db_manager, dist_data, config)
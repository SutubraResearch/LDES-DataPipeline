# Constants
# Python 3.11.7
import toml
from utils.data_utils import get_conversion_factor, convert_units, capacity_to_activity_conversion
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
INSERT_COST_INVEST_QUERY = """
    INSERT INTO CostInvest (regions, tech, vintage, cost_invest, cost_invest_units, cost_invest_notes)
    VALUES (?, ?, ?, ?, ?, ?)
"""
INSERT_COST_VARIABLE_QUERY = """
    INSERT INTO CostVariable (regions, periods, tech, vintage, cost_variable, cost_variable_units, cost_variable_notes)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

INSERT_COST_FIXED_QUERY = """
    INSERT INTO CostFixed (regions, periods, tech, vintage, cost_fixed, cost_fixed_units, cost_fixed_notes)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

INSERT_CAPACITY_CREDIT_QUERY = """
    INSERT INTO CapacityCredit (regions, periods, tech, vintage, cf_tech, cf_tech_notes)
    VALUES (?, ?, ?, ?, ?, ?)
"""

INSERT_DISCOUNT_RATE_QUERY = """
    INSERT INTO DiscountRate (regions, tech, vintage, tech_rate, tech_rate_notes)
    VALUES (?, ?, ?, ?, ?)
"""
INSERT_CAPACITY_TO_ACTIVITY_QUERY = """
    INSERT INTO CapacityToActivity (regions, tech, c2a, c2a_notes)
    VALUES (?, ?, ?, ?)
"""

INSERT_LIFETIME_TECH_QUERY = """
    INSERT INTO LifetimeTech (regions, tech, life, life_notes)
    VALUES (?, ?, ?, ?)
"""

INSERT_LIFETIME_LOAN_QUERY = """
    INSERT INTO LifetimeLoanTech (regions, tech, loan, loan_notes)
    VALUES (?, ?, ?, ?)
"""

INSERT_STORAGE_DURATION_QUERY = """
    INSERT INTO StorageDuration (regions, tech, duration, duration_notes)
    VALUES (?, ?, ?, ?)
"""
def read_new_generator_data(file_path):
    with open(file_path, 'r') as file:
        data = toml.load(file)
    return data


def get_applicable_generators(config, region):
    regional_generators = config['Storage']['New']['Regional'].get(region, [])
    global_generators = config['Storage']['New']['Regional'].get('global', [])
    return {**regional_generators, **global_generators} # merge the two dictionaries

def convert_generator_name(name, duration):
    return name + "_" + str(duration) + "HR"
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


def populate_technologies(db_manager, gen_data, config, sector="storage", flag="ps"):
    unique_technologies = set()
    for region in config['Geography']['regions']:
        for (resource, durations) in config['Storage']['New']['Regional'][region].items():
            if resource in gen_data and resource not in unique_technologies:
                resource_data = gen_data[resource]
                for duration in durations:
                    data_tuple = (
                        convert_generator_name(resource_data.get('name'), duration),
                        flag,
                        sector,
                        str(resource),
                        "",
                        None
                    )
                    db_manager.execute_query(INSERT_TECHNOLOGIES_QUERY, data_tuple)

                    data_tuple_tech_reserve = (
                        convert_generator_name(resource_data.get('name'), duration),
                        None
                    )
                    db_manager.execute_query(INSERT_TECH_RESERVE_QUERY, data_tuple_tech_reserve)
                    unique_technologies.add(resource_data.get('name'))


def populate_efficiency_table(db_manager, gen_data, config):
    defaults = gen_data.get('defaults', {})

    for region in config['Geography']['regions']:
        for (generator_name, durations) in get_applicable_generators(config, region).items():
            generator = gen_data.get(generator_name, {})
            for duration in durations:
                tech = convert_generator_name(generator.get('name'), duration)
                eff_notes = generator.get('data_source', defaults.get('data_source'))
                start_year = generator.get('start_year', defaults.get('start_year'))

                for year in config['Time']['model_years']:
                    efficiency = generator.get('roundtrip_efficiency', defaults.get('roundtrip_efficiency'))
                    if isinstance(efficiency, list):
                        efficiency = efficiency[year - start_year]
                    data_tuple = (
                        region,
                        "electricity",
                        tech,
                        year,
                        "electricity",
                        efficiency,
                        eff_notes
                    )
                    db_manager.execute_query(INSERT_EFFICIENCY_QUERY, data_tuple)



def populate_cost_variable_table(db_manager, gen_data, config):
    data_list = []
    defaults = gen_data.get('defaults', {})

    for region in config['Geography']['regions']:
        for (generator_name, durations) in get_applicable_generators(config, region).items():
            generator = gen_data.get(generator_name, {})
            for duration in durations:
                tech = convert_generator_name(generator.get('name'), duration)
                cost_variable_notes = generator.get('data_source', defaults.get('data_source'))
                lifetime = generator.get('lifetime', defaults.get('lifetime'))
                start_year = generator.get('start_year', defaults.get('start_year'))
                activity_unit_given = generator.get('activity_unit', defaults.get('activity_unit'))
                cost_unit_given = generator.get('cost_unit', defaults.get('cost_unit'))

                # Get the required units from config
                activity_unit = config['Units']['activity']
                currency_unit = config['Units']['currency']

                # Convert the units if necessary
                cost_converter_numerator = get_converted_value(1, cost_unit_given, currency_unit, 'currency')
                cost_converter_denominator = get_converted_value(1, activity_unit_given, activity_unit, 'activity')
                cost_scalar = cost_converter_numerator / cost_converter_denominator
                if currency_unit == 'none':
                    cost_variable_unit = config['Financials']['currency'] + '/' + activity_unit
                else:
                    cost_variable_unit = currency_unit + ' ' + config['Financials']['currency'] + '/' + activity_unit

                for vintage in config['Time']['model_years']:
                    cost_variable = generator.get('variable_costs', defaults.get('variable_costs'))
                    if cost_variable == 0:
                        continue # Skip if the variable cost is 0
                    if isinstance(cost_variable, list):
                        cost_variable = cost_variable[vintage - start_year]

                    for period in range(vintage, vintage + lifetime):
                        if period not in config['Time']['model_years']:
                            continue

                        data_tuple = (
                            region,
                            period,
                            tech,
                            vintage,
                            cost_variable * cost_scalar,
                            cost_variable_unit,
                            cost_variable_notes
                        )
                        data_list.append(data_tuple)
    db_manager.populate_table_with_query_bulk(INSERT_COST_VARIABLE_QUERY, data_list)

def populate_cost_invest_and_fixed_tables(db_manager, gen_data, config):
    defaults = gen_data.get('defaults', {})
    data_tuple_invest_list = []
    data_tuple_fixed_list = []

    for region in config['Geography']['regions']:
        for (generator_name, durations) in get_applicable_generators(config, region).items():
            generator = gen_data.get(generator_name, {})
            for duration in durations:
                tech = convert_generator_name(generator.get('name'), duration)
                cost_notes = generator.get('data_source', defaults.get('data_source'))
                start_year = generator.get('start_year', defaults.get('start_year'))
                lifetime = generator.get('lifetime', defaults.get('lifetime'))
                capacity_unit_given = generator.get('capacity_unit', defaults.get('capacity_unit'))
                cost_unit_given = generator.get('cost_unit', defaults.get('cost_unit'))

                # Get the required units from config
                capacity_unit = config['Units']['capacity']
                currency_unit = config['Units']['currency']

                # Convert the units if necessary
                cost_converter_numerator = get_converted_value(1, cost_unit_given, currency_unit, 'currency')
                cost_converter_denominator = get_converted_value(1, capacity_unit_given, capacity_unit, 'capacity')
                cost_scalar = cost_converter_numerator / cost_converter_denominator
                if currency_unit == 'none':
                    cost_invest_unit = config['Financials']['currency'] + '/' + capacity_unit
                    cost_fixed_unit = config['Financials']['currency'] + '/' + capacity_unit + '-year'
                else:
                    cost_invest_unit = currency_unit + ' ' + config['Financials']['currency'] + '/' + capacity_unit
                    cost_fixed_unit = currency_unit + ' ' + config['Financials'][
                        'currency'] + '/' + capacity_unit + '-year'

                for vintage in config['Time']['model_years']:
                    cost_invest_cap = generator.get('capital_costs_capacity', defaults.get('capital_costs_capacity'))
                    if isinstance(cost_invest_cap, list):
                        cost_invest_cap = cost_invest_cap[vintage - start_year]
                    cost_invest_en = generator.get('capital_costs_energy', defaults.get('capital_costs_energy'))
                    if isinstance(cost_invest_en, list):
                        cost_invest_en = cost_invest_en[vintage - start_year]
                    cost_invest = cost_invest_cap + cost_invest_en * duration


                    data_tuple = (
                        region,
                        tech,
                        vintage,
                        cost_invest * cost_scalar,
                        cost_invest_unit,
                        cost_notes
                    )
                    data_tuple_invest_list.append(data_tuple)


                    cost_fixed_percent = generator.get('fixed_costs_percent', defaults.get('fixed_costs_percent'))
                    if isinstance(cost_fixed_percent, list):
                        cost_fixed_percent = cost_fixed_percent[vintage - start_year]
                    cost_fixed = cost_invest * cost_fixed_percent

                    for period in range(vintage, vintage + lifetime):
                        if period not in config['Time']['model_years']:
                            continue

                        data_tuple = (
                            region,
                            period,
                            tech,
                            vintage,
                            cost_fixed * cost_scalar,
                            cost_fixed_unit,
                            cost_notes
                        )
                        data_tuple_fixed_list.append(data_tuple)

    db_manager.populate_table_with_query_bulk(INSERT_COST_INVEST_QUERY, data_tuple_invest_list)
    db_manager.populate_table_with_query_bulk(INSERT_COST_FIXED_QUERY, data_tuple_fixed_list)


def populate_capacity_credit_table(db_manager, gen_data, config):
    defaults = gen_data.get('defaults', {})
    for region in config['Geography']['regions']:
        for (generator_name, durations) in get_applicable_generators(config, region).items():
            generator = gen_data.get(generator_name, {})
            for duration in durations:
                tech = convert_generator_name(generator.get('name'), duration)
                capacity_credit_notes = generator.get('data_source', defaults.get('data_source'))
                lifetime = generator.get('lifetime', defaults.get('lifetime'))
                start_year = generator.get('start_year', defaults.get('start_year'))

                for vintage in config['Time']['model_years']:
                    capacity_credit = generator.get('capacity_credit', defaults.get('capacity_credit'))
                    if isinstance(capacity_credit, list):
                        capacity_credit = capacity_credit[vintage - start_year]

                    for period in range(vintage, vintage + lifetime):
                        if period not in config['Time']['model_years']:
                            continue

                        data_tuple = (
                            region,
                            period,
                            tech,
                            vintage,
                            capacity_credit,
                            capacity_credit_notes
                        )
                        db_manager.execute_query(INSERT_CAPACITY_CREDIT_QUERY, data_tuple)


def get_discount_rate(generator, default, config_overrides):
    """
    Get the discount rate or Weighted Average Cost of Capital (WACC) for a generator.
    The discount rate is retrieved from the generator data or default data,
    with a possibility of being overridden in the configuration file.
    The default value is 'wacc' field or 0 if it doesn't exist.

    :param generator: dict containing generator data
    :param default: dict containing default data
    :param config_overrides: dict containing configuration overrides
    :return: wacc value
    """
    return config_overrides.get('wacc', generator.get('wacc', default.get('wacc', 0)))


def populate_discount_rate_table(db_manager, gen_data, config):
    # Get the default items from the gen_data
    defaults = gen_data.get('defaults', {})
    # Loop through each region in the configuration
    for region in config['Geography']['regions']:
        region_specific_overrides = config['Storage']['New']['Overrides'].get(region, {})
        for (generator_name, durations) in get_applicable_generators(config, region).items():
            generator = gen_data.get(generator_name, {})
            for duration in durations:
                tech = convert_generator_name(generator.get('name'), duration)
                # Compute wacc (discount rate)
                wacc = get_discount_rate(generator, defaults, region_specific_overrides)

                # If no wacc is found, we continue to the next generator
                if wacc is None:
                    continue

                notes = defaults.get('data_source')

                # Execute appropriate SQL query for each model year
                for year in config['Time']['model_years']:
                    data_tuple = (region, tech, year, wacc, notes)
                    db_manager.execute_query(INSERT_DISCOUNT_RATE_QUERY, data_tuple)


def populate_storage_duration_table(db_manager, gen_data, config):

    # Loop through each region in the configuration
    for region in config['Geography']['regions']:
        for (generator_name, durations) in get_applicable_generators(config, region).items():
            generator = gen_data.get(generator_name, {})
            for duration in durations:
                tech = convert_generator_name(generator.get('name'), duration)

                # Execute appropriate SQL query for each model year
                data_tuple = (region, tech, duration, '')
                db_manager.execute_query(INSERT_STORAGE_DURATION_QUERY, data_tuple)

def populate_capacity_to_activity_table(db_manager, gen_data, config):
    """
    Populate the 'CapacityToActivity' table in the database.
    Given a generator and a configuration, compute the capacity_to_activity ratio,
    and the notes for the capacity to activity ratio, and insert them into the database

    :param db_manager: an instance of the DatabaseManager class, providing DB functionality
    :param gen_data: dictionary containing generator data
    :param config: dictionary containing configuration data
    """
    defaults = gen_data.get('defaults', {})

    for region in config['Geography']['regions']:
        for (generator_name, durations) in get_applicable_generators(config, region).items():
            generator = gen_data.get(generator_name, {})
            for duration in durations:
                tech = convert_generator_name(generator.get('name'), duration)

                # Compute c2a (capacity to activity ratio)
                capacity_unit = config['Units']['capacity']  # get capacity unit from config
                activity_unit = config['Units']['activity']  # get activity unit from config
                c2a = capacity_to_activity_conversion(capacity_unit, activity_unit)

                if c2a is None:
                    continue

                c2a_notes = f"1 {capacity_unit} operated with a 100% capacity factor generates {c2a} {activity_unit}"
                data_tuple = (region, tech, c2a, c2a_notes)
                db_manager.execute_query(INSERT_CAPACITY_TO_ACTIVITY_QUERY, data_tuple)


def populate_lifetime_table(db_manager, gen_data, config, type="tech"):
    """
    Populate the 'LifetimeTech' table in the database.
    Given a generator and a configuration, retrieve the technology life and its data source,
    and insert them into the database.

    :param db_manager: an instance of the DatabaseManager class, providing DB functionality
    :param gen_data: dictionary containing generator data
    :param config: dictionary containing configuration data
    :param type: string indicating the type of lifetime to populate (tech, loan, or both)
    """
    defaults = gen_data.get('defaults', {})

    for region in config['Geography']['regions']:
        for (generator_name, durations) in get_applicable_generators(config, region).items():
            generator = gen_data.get(generator_name, {})
            for duration in durations:
                tech = convert_generator_name(generator.get('name'), duration)

                # Retrieve lifespan and data source directly from generator data
                if type == "tech" or type == "both":
                    life_t = generator.get('lifetime', defaults.get('lifetime'))
                if type == "loan" or type == "both":
                    life_l = generator.get('capital_recovery_period', defaults.get('capital_recovery_period'))

                data_source = generator.get('data_source', defaults.get('data_source'))

                if type == "tech" or type == "both":
                    data_tuple = (region, tech, life_t, data_source)
                    db_manager.execute_query(INSERT_LIFETIME_TECH_QUERY, data_tuple)
                if type == "loan" or type == "both":
                    if life_l == None:
                        continue
                    data_tuple = (region, tech, life_l, data_source)
                    db_manager.execute_query(INSERT_LIFETIME_LOAN_QUERY, data_tuple)


def populate_new_storage(db_manager, config, filename):
    gen_data = read_new_generator_data(filename)
    populate_technologies(db_manager, gen_data, config, sector="storage", flag="ps")
    populate_efficiency_table(db_manager, gen_data, config)
    populate_cost_invest_and_fixed_tables(db_manager, gen_data, config)
    populate_cost_variable_table(db_manager, gen_data, config)
    populate_capacity_credit_table(db_manager, gen_data, config)
    populate_discount_rate_table(db_manager, gen_data, config)
    populate_capacity_to_activity_table(db_manager, gen_data, config)
    populate_lifetime_table(db_manager, gen_data, config, type="both")
    populate_storage_duration_table(db_manager, gen_data, config)

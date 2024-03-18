import pandas as pd
import toml
import copy

def get_conversion_factor(file_path):
    conversion_table = pd.read_csv(file_path, index_col=0)
    # strip any leading or tailing spaces
    conversion_table.columns = conversion_table.columns.str.strip()
    conversion_table.index = conversion_table.index.str.strip()
    return conversion_table


def convert_units(value, from_unit, to_unit, conversion_table):
    factor = conversion_table.loc[from_unit, to_unit]
    return value * factor

def capacity_to_activity_conversion(capacity_unit, activity_unit):
    # Constants for hours in a year and capacity unit conversion to kW
    HOURS_IN_YEAR = 8760
    POWER_UNITS = {'kW': 1, 'MW': 1000, 'GW': 10 ** 6, 'TW': 10 ** 9}

    # Energy units are represented in the same base but with different power to represent scale
    ENERGY_UNITS = {'kWh': 1, 'MWh': 10 ** 3, 'GWh': 10 ** 6, 'TWh': 10 ** 9,
                    'GJ': 277.78, 'TJ': 277.78 * 10 ** 3, 'PJ': 277.78 * 10 ** 6}

    # Convert the capacity to energy produced in kWh (if the generator is run for an entire year)
    energy_produced = POWER_UNITS[capacity_unit] * HOURS_IN_YEAR

    # Convert the energy produced to the correct activity unit
    c2a = energy_produced / ENERGY_UNITS[activity_unit]

    return c2a


def determine_import_techs(db_manager, fuels_filename):
    fuels = toml.load(fuels_filename)

    query = "SELECT * FROM Efficiency"
    efficiencies = pd.read_sql_query(query, db_manager.conn)
    result_dict = {}

    for _, eff in efficiencies.iterrows():
        region, input_comm = eff['regions'], eff['input_comm']
        if input_comm in fuels:
            if region not in result_dict:
                result_dict[region] = set()  # creates a set if region is not yet in the dictionary
            result_dict[region].add(input_comm)  # adds input_comm to the set; if it's already there, it will be ignored
        else:
            print(f"Warning: {input_comm} not found in fuels.toml")
    return result_dict


def construct_import_data(config, import_dict, import_filename, fuels_filename):
    # Reading the 'import-tech' template and fuels data
    import_template = toml.load(import_filename)['import-tech']
    fuels_data = toml.load(fuels_filename)

    # We will store the import objects here
    import_objs = {}

    for region, fuels in import_dict.items():
        for fuel in fuels:
            # Copy the 'import-tech' object and customize it
            import_obj = copy.deepcopy(import_template)
            import_obj['name'] = import_obj['name'].replace('<FUEL>', fuel)
            import_obj['region'] = region
            import_obj['output_comm'] = fuel

            # Select only the costs data for the relevant years
            fuel_data = fuels_data[fuel]
            start_year = fuel_data['start_year']
            end_year = fuel_data['end_year']
            all_years = range(start_year, end_year + 1)
            all_costs = fuel_data['costs']['NS']
            year_costs_dict = dict(zip(all_years, all_costs))
            relevant_costs = [year_costs_dict[year] for year in config['Time']['model_years'] if year in year_costs_dict]
            import_obj['variable_costs'] = relevant_costs
            import_obj['emissions'] = fuel_data['emis_co2']
            import_obj['vintage'] = min(config["Time"]["model_years"])
            import_obj['data_source'] = fuel_data['data_source']
            # Add the customized import object to our dictionary
            import_objs[f"{region}_{fuel}"] = import_obj
    with open("temp.toml", "w") as file:
        toml.dump(import_objs, file)
    return import_objs

def determine_distribution_techs(config, dist_filename):
    dist_techs = toml.load(dist_filename)

    regions = config['Geography']['regions']
    result_dict = {}

    for r in regions:
        # Iterate over each distribution tech
        for tech_name, tech_contents in dist_techs.items():
            # Copy the tech
            new_tech = tech_contents.copy()

            # Modify the name and region fields
            new_tech['name'] = new_tech['name']
            new_tech['region'] = r
            new_tech['vintage'] = min(config["Time"]["model_years"])

            # Add the new tech to the result dict
            result_dict[r + '_' + tech_name] = new_tech

    return result_dict


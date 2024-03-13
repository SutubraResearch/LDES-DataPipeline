import pandas as pd


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
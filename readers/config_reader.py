import toml


def read_config(file_path):
    """Read a TOML config file and return the configuration as a dictionary.
    :param file_path: Path to the TOML config file.
    :return: Dictionary with the configuration.
    """
    with open(file_path, 'r') as config_file:
        config = toml.load(config_file)
    return config

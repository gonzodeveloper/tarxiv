# Misc. utility functions

import yaml


def read_config(config_file):
    """
    Read in yaml configuration file for modules.
    :param config_file: filename for config yaml; str
    :return: module configuration; dict
    """
    # Read in configuration
    with open(config_file) as stream:
        return yaml.safe_load(stream)



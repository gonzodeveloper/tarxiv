# Misc. utility functions
import logging
import yaml
import sys


def read_config(config_file):
    """Read in yaml configuration file for modules.

    :param config_file: filename for config yaml; str
    :return: module configuration; dict
    """
    # Read in configuration
    with open(config_file) as stream:
        return yaml.safe_load(stream)


def get_logger(module_name, level, log_file):
    # Use logging module
    logger = logging.getLogger(module_name)

    if level == "info":
        logger.setLevel(logging.INFO)
    elif level == "debug":
        logger.setLevel(logging.DEBUG)
    else:
        raise ValueError("invalid logging level, not in  ['info', 'debug']")

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Add logfile if needed
    if log_file is not None:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

#!/usr/bin/env python3

from common.client import Client
from common.file_reader import FileReader
import logging
import os


def initialize_config():
    """ Parse env variables to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a dict with config parameters
    """

    config_params = {}
    
    config_params["gateway_host"] = os.getenv('GATEWAY_HOST')
    config_params["gateway_port"] = int(os.getenv('GATEWAY_PORT'))
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if config_params["gateway_host"] is None or config_params["gateway_port"] is None:
        raise ValueError("Expected value not found. Aborting gateway.")
    
    return config_params


def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    gateway_host = config_params["gateway_host"]
    gateway_port = config_params["gateway_port"]

    initialize_log(logging_level)

    # Initialize client
    client = Client(gateway_host, gateway_port)
    client.run()

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


if __name__ == "__main__":
    main()
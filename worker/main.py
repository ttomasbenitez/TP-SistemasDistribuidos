#!/usr/bin/env python3

import logging
import os
from Middleware.middleware import MessageMiddlewareQueue
from filter.year import FilterYearNode


def initialize_config():
    """ Parse env variables to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a dict with config parameters
    """

    config_params = {}
    
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue"] = os.getenv('INPUT_QUEUE_1')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')

    if config_params["rabbitmq_host"] is None or config_params["input_queue"] is None:
        raise ValueError("Expected value not found. Aborting filter.")
    
    return config_params


def main():
    config_params = initialize_config()

    initialize_log(config_params["logging_level"])

    input_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["input_queue"])
    # output_queue = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_queue_1"])
    # output_exchange = MessageMiddlewareQueue(config_params["rabbitmq_host"], config_params["output_exchange_filter_year"])
    
    filter = FilterYearNode(input_queue, None, None, {2024, 2025})
    filter.start()

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
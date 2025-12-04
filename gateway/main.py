#!/usr/bin/env python3

from common.gateway import Gateway
import logging
import os
from Middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from pkg.message.constants import MESSAGE_TYPE_USERS, MESSAGE_TYPE_MENU_ITEMS, MESSAGE_TYPE_STORES, MESSAGE_TYPE_TRANSACTIONS, MESSAGE_TYPE_TRANSACTION_ITEMS, MESSAGE_TYPE_EOF


def initialize_config():
    """ Parse env variables to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a dict with config parameters
    """

    config_params = {}

    config_params["port"] = int(os.getenv('PORT'))
    config_params["listen_backlog"] = int(os.getenv('LISTEN_BACKLOG'))
    config_params["logging_level"] = os.getenv('LOGGING_LEVEL', 'INFO')
    config_params["exchange_name"] = os.getenv('EXCHANGE_NAME')
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue"] = os.getenv('INPUT_QUEUE_1')
    config_params["output_exchange_name"] = os.getenv('OUTPUT_EXCHANGE_NAME')

    config_params["q1_replicas"] = int(os.getenv('Q1_REPLICAS', 3))

    if config_params["port"] is None or config_params["listen_backlog"] is None or config_params["exchange_name"] is None or config_params["rabbitmq_host"] is None:
        raise ValueError("Expected value not found. Aborting gateway.")

    return config_params

def main():
    config_params = initialize_config()
    logging_level = config_params["logging_level"]
    port = config_params["port"]
    listen_backlog = config_params["listen_backlog"]

    initialize_log(logging_level)

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.info(f"action: config | result: success | port: {port} | "
                  f"listen_backlog: {listen_backlog} | logging_level: {logging_level}")

    # Initialize gateway and start gateway loop
    gateway = Gateway(port, listen_backlog, config_params["exchange_name"], config_params["input_queue"], config_params["output_exchange_name"], config_params["rabbitmq_host"], config_params["q1_replicas"])
    gateway.run()

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
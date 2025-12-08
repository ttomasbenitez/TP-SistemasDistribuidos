

import os
from utils.custom_logging import initialize_log

def initialize_config():
    config_params = {}
    config_params["rabbitmq_host"] = os.getenv('RABBITMQ_HOST')
    config_params["input_queue_1"] = os.getenv('INPUT_QUEUE_1')
    config_params["input_queue_2"] = os.getenv('INPUT_QUEUE_2')
    config_params["output_middleware"] = os.getenv('OUTPUT_MIDDLEWARE')
    config_params["logging_level"] = os.getenv('LOG_LEVEL', 'INFO')
    config_params["container_name"] = os.getenv('CONTAINER_NAME', 'joiner_stores_q4')
    config_params["storage_dir"] = os.getenv('STORAGE_DIR', '/tmp/joiner_q4_storage')
    config_params["expected_eofs"] = int(os.getenv('EXPECTED_EOFS'))

    if None in [config_params["rabbitmq_host"], config_params["input_queue_1"],
                config_params["input_queue_2"]]:
        raise ValueError("Expected value not found. Aborting.")
    initialize_log(config_params["logging_level"])
    
    return config_params

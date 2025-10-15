import logging, sys
import multiprocessing as mp
from logging.handlers import QueueHandler, QueueListener


def initialize_log(logging_level = logging.INFO):
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
    
# logging_utils.py
import logging, sys, os
from datetime import datetime

FMT = "%(asctime)s | %(processName)s[%(process)d] | %(levelname)s | %(name)s | %(message)s"

def setup_process_logger(name: str, level: str = "INFO"):
    """
    Configura logging solo para el proceso actual.
    name -> etiqueta del proceso (e.g. 'data', 'eof', 'final', 'eofsvc')
    to_file=True -> escribe a /logs/<name>.<pid>.log
    """
    root = logging.getLogger()
    # limpiar handlers previos en este proceso
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    formatter = logging.Formatter(FMT)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    root.addHandler(sh)

    # opcional: ver warnings como logs
    logging.captureWarnings(True)
    logging.info("Logger inicializado para '%s'", name)

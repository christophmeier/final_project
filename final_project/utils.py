""" This module contains utility functions for final project

The module contains the following functions:
* dec_logger: decorator for logging
* dec_validation: decorator for error handling
* configure_logger: creates and configures a custom logger
"""
import logging
import multiprocessing as mp
import os
import sys
from functools import wraps


def dec_logger(func):
    """ Decorator for logging start and finish of a function

    :param func: function to be decorated
    :return: wrapper function
    """
    @wraps(func)
    def wrapper_logger(*args, **kwargs):
        # Get logger and set level
        logger = mp.get_logger()
        logger.setLevel(logging.INFO)

        # Call function with logger at the start and after finish
        logger.info("> Start of function '{}'...".format(func.__name__))
        output = func(*args, **kwargs)
        logger.info("+ Function '{}' successfully finished.".format(func.__name__))

        return output

    return wrapper_logger


def dec_validation(func):
    """ Decorator for error handling which stops the entire function in case of
    an exception.

    :param func: function to be decorated
    :return: wrapper function
    """
    @wraps(func)
    def wrapper_validation(*args, **kwargs):
        # Get logger and set level
        logger = mp.get_logger()
        logger.setLevel(logging.ERROR)

        try:
            # Call decorated function
            return func(*args, **kwargs)

        except Exception as e:
            # Logging of exception and stop function
            logger.error("!! Exception occured: {}".format(e))
            raise Exception

    return wrapper_validation


def configure_logger(dict_config):
    """ Create and configure a logger

    :param dict_config: config data
    :type dict_config: Dictionary
    :return: a customized logger
    """
    # Set level of fbprophet logger to ERROR
    logging.getLogger("fbprophet").setLevel(logging.ERROR)
    logging.getLogger("fbprophet").handlers = []

    # Create a custom logger
    logger = mp.get_logger()
    logger.setLevel(logging.INFO)

    # Console output
    c_handler = logging.StreamHandler(sys.stderr)
    c_handler.setLevel(logging.INFO)
    c_format = logging.Formatter(
        fmt="[%(levelname)s/%(processName)s] %(asctime)s | %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
    )
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    # File output
    fp = os.path.join(
        dict_config["dir_logs"], f"summary_[{mp.current_process().name}].log"
    )
    f_handler = logging.FileHandler(fp, mode="w")

    f_handler.setLevel(logging.INFO)
    f_format = logging.Formatter(
        fmt="[%(levelname)s/%(processName)s] %(asctime)s | %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
    )
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

    # File output in case of exceptions
    fp = os.path.join(
        dict_config["dir_logs"], f"summary_error_[{mp.current_process().name}].log"
    )
    f_handler_error = logging.FileHandler(fp, mode="w")
    f_handler_error.setLevel(logging.ERROR)
    f_handler_error.setFormatter(f_format)
    logger.addHandler(f_handler_error)

    return logger

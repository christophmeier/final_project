""" This module contains utility functions for final project

The module contains the following functions:
* dec_logger - decorator for logging
* dec_validation - decorator for error handling
"""
import logging
import multiprocessing as mp
import sys


def dec_logger(func):
    """ Decorator for logging start and finish of a function

    :param func: function to be decorated
    :return: wrapper function
    """

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
            return

    return wrapper_validation


def configure_logger():
    """ Creates and configures a logger using Python logging module.

    :return: a customized logger
    """
    # Disable fbprophet logger
    logging.getLogger("fbprophet").setLevel(logging.ERROR)
    logging.getLogger("fbprophet").handlers = []

    # Create a custom logger
    # mp.log_to_stderr()
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
    f_handler = logging.FileHandler(
        f"../logs/summary_[{mp.current_process().name}].log", mode="w"
    )
    f_handler.setLevel(logging.INFO)
    f_format = logging.Formatter(
        fmt="[%(levelname)s/%(processName)s] %(asctime)s | %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
    )
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

    # File output in case of exceptions
    f_handler_error = logging.FileHandler(
        f"../logs/summary_error_[{mp.current_process().name}].log", mode="w"
    )
    f_handler_error.setLevel(logging.ERROR)
    f_handler_error.setFormatter(f_format)
    logger.addHandler(f_handler_error)

    return logger

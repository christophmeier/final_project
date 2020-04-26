""" This module contains utility functions for final project

The module contains the following functions:
* dec_logger - decorator for logging
* dec_validation - decorator for error handling
"""
import logging
import multiprocessing as mp


def dec_logger(func):
    """ Decorator for logging a function start and end

    :param func: function to be decorated
    :return: wrapper function
    """
    def wrapper_logger(*args, **kwargs):
        # Get process ID
        process_id = mp.current_process().name
        # Get DTF logger
        dtf_logger = logging.getLogger("dtf_logger")
        # Logging of function start
        dtf_logger.info(
            "|{}| > Start of function '{}'...".format(process_id, func.__name__)
        )
        # Call decorated function
        output = func(*args, **kwargs)
        # Logging of function end
        dtf_logger.info(
            "|{}| + Function '{}' successfully finished.".format(
                process_id, func.__name__
            )
        )
        return output
    return wrapper_logger


def dec_validation(func):
    """ Decorator for error handling which stops the entire function in case of
    an exception.

    :param func: function to be decorated
    :return: wrapper function
    """
    def wrapper_validation(*args, **kwargs):
        # Get process ID
        process_id = mp.current_process().name
        # Get DTF logger
        dtf_logger = logging.getLogger("dtf_logger")
        try:
            # Call decorated function
            return func(*args, **kwargs)
        except Exception as e:
            # Logging of exception and stop function
            dtf_logger.error("|{}| Exception occured: {}".format(process_id, e))
            return
    return wrapper_validation


def configure_logger():
    """ Creates and configures a logger using Python logging module.

    :return: a customized logger
    """

    # Disable handlers of root logger
    logging.getLogger().handlers = []

    # Create a custom logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler("../summary.log", mode="w")
    f_handler_error = logging.FileHandler("../summary_error.log", mode="w")

    c_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.INFO)
    f_handler_error.setLevel(logging.ERROR)

    # Create formatters and add it to handlers
    c_format = logging.Formatter(
        fmt="[%(asctime)s] %(message)s", datefmt="%d-%b-%y %H:%M:%S"
    )
    f_format = logging.Formatter(
        fmt="[%(asctime)s] %(message)s", datefmt="%d-%b-%y %H:%M:%S"
    )
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)
    f_handler_error.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    logger.addHandler(f_handler_error)

    return logger

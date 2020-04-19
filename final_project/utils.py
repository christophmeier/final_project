""" This module contains utility functions for final project

The module contains the following functions:
* dec_logger - decorator for logging
* dec_validation - decorator for error handling
"""
import logging
import multiprocessing as mp


def dec_logger(func):
    """ Decorator for logging

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

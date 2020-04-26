"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -m final_project` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``final_project.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``final_project.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import argparse
import datetime
from .data import *
from .utils import *
from .process import *


parser = argparse.ArgumentParser()
parser.add_argument("-d", dest="n_days", type=int, default=1095)


def main():
    # Get number of days to be forecasted
    # args = parser.parse_args()
    # fcst_days = args.n_days
    fcst_days = 1095

    # Create dictionary with respect to configuration data
    dict_config = {'fcst_days': fcst_days}

    # Init time for program start
    t_start = datetime.datetime.now()

    # Create and configure logger
    logger = configure_logger()
    logger.info("Program start")

    # Load clustering and traffic data
    dict_so_cluster, df_traffic = load_data()

    # Start worker processes
    start_process(df_traffic, dict_so_cluster, dict_config)

    # Export data

    # Measure final time and display overall time
    t_end = datetime.datetime.now()
    logger.info("Program end\nProgram duration: {}".format(t_end - t_start))

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
from .process import *

parser = argparse.ArgumentParser()
parser.add_argument("-d", dest="fcst_days", type=int, default=365)


def main():
    # Get number of days to be forecasted which is at least 0 days
    args = parser.parse_args()

    # Get config data
    dict_config = get_config_data()
    dict_config["fcst_days"] = max(args.fcst_days, 0)

    # Init time for program start
    t_start = datetime.datetime.now()

    # Create and configure logger
    logger = configure_logger(dict_config)
    logger.info("Program start")

    # Download data from AWS, import clustering and traffic data
    download_data_aws(dict_config)
    dict_so_cluster, df_traffic = load_data(dict_config)

    # Start worker processes
    df_fcst_results = start_process(df_traffic, dict_so_cluster, dict_config)

    # Export data
    export_fcst_results_hdf5(df_fcst_results, dict_config)

    # Measure final time and display overall time
    logger.info(f"Program end\nProgram duration: {(datetime.datetime.now() - t_start)}")

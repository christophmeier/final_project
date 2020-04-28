"""
This module contains functions related to multiprocessing

The module contains the following functions:
* start_process:
* get_number_processes:
* get_cluster_chunks:
* mp_run:
"""
import concurrent.futures
import multiprocessing as mp
from functools import partial
from .preprocessing import *
from .forecasting import *
from .data import *


MAX_PROCESSES = 32


@dec_validation
@dec_logger
def start_process(df_traffic, dict_so_cluster, dict_config):
    """ Initialize the worker processes for pre-processing and forecasting

    :param df_traffic: DataFrame with traffic related data and DateTimeIndex
    :type df_traffic: pandas DataFrame
    :param dict_so_cluster: dictionary mapping site numbers to cluster IDs
    :type dict_so_cluster: Dictionary
    :param dict_config: Dictionary with config data
    :type dict_config: Dictionary
    :return: the original and forecasted time series for every cluster
    :rtype pandas DataFrame
    """
    # Determine max number of processes to be initialized
    n_processes = get_number_processes()

    # Determine cluster IDs to be forecasted
    cluster_ids = list(set(val for val in dict_so_cluster.values()))
    cluster_ids = cluster_ids[:2]

    # Create result DataFrame
    df_fcst_results = prepare_fcst_df()

    # Create a partial function to pass multiple arguments
    func = partial(mp_run, df_traffic, dict_so_cluster, dict_config)

    # Serial implementation
    # for cluster in cluster_ids:
    #     fcst_results = mp_run(df_traffic, dict_so_cluster, dict_config, cluster)
    #
    #     # Combine all individual forecast results to one DataFrame
    #     df_fcst_results = df_fcst_results.append(fcst_results)

    # Use context manager for ProcessPoolExecutor and iterate over cluster_ids
    with concurrent.futures.ProcessPoolExecutor(max_workers=n_processes) as executor:
        fcst_results = executor.map(func, cluster_ids)

        # Combine all individual forecast results to one DataFrame
        for fcst in fcst_results:
            df_fcst_results = df_fcst_results.append(fcst)

    return df_fcst_results


@dec_validation
@dec_logger
def get_number_processes():
    """ Calculate number of processes used for forecasting

    :return: number of processes used for forecasting
    :rtype: int
    """
    # Check number of CPU cores
    if MAX_PROCESSES <= mp.cpu_count():
        return MAX_PROCESSES

    return mp.cpu_count()


@dec_validation
@dec_logger
def get_cluster_chunks(cluster_ids, n_processes):
    """ Split list of cluster IDs in n_process-disjoint lists

    :param cluster_ids: list of cluster IDs
    :type cluster_ids: list of ints
    :param n_processes: number of processes for forecasting
    :type n_processes: int
    :return: list with independent cluster ID chunks
    :rtype: list of lists of ints
    """
    cluster_ids_section = []
    section_size = int(len(cluster_ids) / n_processes)

    # Split all cluster IDs in equally large chunks
    for i in range(n_processes):
        if i >= n_processes - 1:
            section = cluster_ids[(i * section_size):]
        else:
            section = cluster_ids[(i * section_size):((i + 1) * section_size)]
        cluster_ids_section.append(section)

    return cluster_ids_section


def mp_run(df_traffic, dict_so_cluster, dict_config, clu_id):
    """ Implementation of a single process / worker

    :param df_traffic: DataFrame with traffic related data and DateTimeIndex
    :type df_traffic: pandas DataFrame
    :param dict_so_cluster: dictionary mapping site numbers to cluster IDs
    :type dict_so_cluster: Dictionary
    :param dict_config: Dictionary with config data
    :type dict_config: Dictionary
    :param clu_id: cluster ID to be processed
    :type clu_id: int
    :return the original & forecasted time series for the cluster
    :rtype pandas DataFrame
    """

    # Configure logger for each individual process and get process ID
    logger = configure_logger()

    # Run pre-processing
    df_ts_cluster = preprocess_data(df_traffic, dict_so_cluster, clu_id)

    # Run forecasting to get a DataFrame with forecasted time series
    logger.info(f"> Start forecast cluster ID {clu_id}")
    df_fcst = forecast(df_ts_cluster, dict_config)
    logger.info(f"+ End forecast cluster ID: {clu_id}")

    # Add cluster identifier for the time series
    df_fcst["cluster_id"] = clu_id

    return df_fcst[["ds", "cluster_id", "y", "yhat"]]

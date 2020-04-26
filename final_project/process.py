"""
This module contains functions related to multiprocessing

The module contains the following functions:
* start_process:
* get_number_processes:
* get_cluster_chunks:
* mp_run:
"""
from functools import partial
from multiprocessing import Pool
from .utils import *
from .preprocessing import *
from .forecasting import *


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
    :return:
    """
    # Determine number of processes to be initialized
    n_processes = get_number_processes()
    n_processes = 1
    # Determine cluster IDs to be forecasted
    cluster_ids = list(set(val for val in dict_so_cluster.values()))
    cluster_ids = [0]
    # Split cluster IDs in equal batches which will be assigned to processes
    cluster_ids_section = get_cluster_chunks(cluster_ids, n_processes)

    # Start multiprocessing and generate a pool with n_processes as #workers
    pool = Pool(processes=n_processes)

    # Create partial function and pass constant parameters to pool workers
    func = partial(mp_run, df_traffic, dict_so_cluster, dict_config)

    # Map-reduce of pool workers
    pool.map(func, cluster_ids_section)

    # Wait for workers until they are all finished
    pool.close()
    pool.join()


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


def mp_run(df_traffic, dict_so_cluster, dict_config, cluster_ids):
    """ Implementation of a single process / worker for

    :param df_traffic: DataFrame with traffic related data and DateTimeIndex
    :type df_traffic: pandas DataFrame
    :param dict_so_cluster: dictionary mapping site numbers to cluster IDs
    :type dict_so_cluster: Dictionary
    :param dict_config: Dictionary with config data
    :type dict_config: Dictionary
    :param cluster_ids: list of clusters to be processed
    :type cluster_ids: list of ints
    :return
    :rtype
    """

    # Configure logger for each individual process
    logger = configure_logger()

    # Get process ID
    process_id = mp.current_process().name

    # Run pre-processing and forecasting for every selected cluster
    for clu_id in cluster_ids:
        # Run pre-processing
        df_ts_cluster = preprocess_data(df_traffic, dict_so_cluster, clu_id)

        # Run forecasting to get a DataFrame with forecasted time series
        logger.info("|{}| > Start cluster forecast: {}".format(process_id, clu_id))
        df_fcst = forecast(df_ts_cluster, dict_config)
        logger.info("|{}| > End cluster forecast: {}".format(process_id, clu_id))

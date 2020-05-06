"""
This module contains functions related to multiprocessing

The module contains the following functions:
* start_process: triggers all functions necessary for the forecasting workflow
* get_number_processes: determines number of processes used for multi-processing
* get_cluster_chunks: splits up a list of cluster IDs into n-disjoint chunks
* mp_run: implementation of a single process
"""
import concurrent.futures
from functools import partial
from .preprocessing import *
from .forecasting import *
from .data import *


@dec_validation
@dec_logger
def start_process(
    df_traffic, dict_so_cluster, dict_config, max_processes=32, subset=True
):
    """ Initialize the worker processes for pre-processing and forecasting

    :param df_traffic: DataFrame with traffic related data and DateTimeIndex
    :type df_traffic: pandas DataFrame
    :param dict_so_cluster: dictionary mapping site numbers to cluster IDs
    :type dict_so_cluster: Dictionary
    :param dict_config: Dictionary with config data
    :type dict_config: Dictionary
    :param max_processes: upper bound for the number of process
    :type max_processes: int
    :param subset: flag indicating if all cluster IDs shall be considered
    :type subset: bool
    :return: the original and forecasted time series for every cluster
    :rtype pandas DataFrame
    """
    # Determine max number of processes to be initialized
    n_processes = get_number_processes(max_processes)

    # Determine cluster IDs to be forecasted and set ID -1 for total Germany
    # Note: IDs are not consecutive numbers
    cluster_ids = list(set(val for val in dict_so_cluster.values()))
    cluster_ids.append(-1)

    # Check if only a subset of cluster IDs shall be considered
    if subset:
        cluster_ids = cluster_ids[:10]

    # Create result DataFrame
    df_fcst_results = prepare_fcst_df()

    # Create chunks with cluster ID
    cluster_chunks = get_cluster_chunks(cluster_ids, n_processes)

    # Create a partial function to pass multiple arguments
    func = partial(mp_run, df_traffic, dict_so_cluster, dict_config)

    # Use context manager for ProcessPoolExecutor and iterate over cluster chunks
    with concurrent.futures.ProcessPoolExecutor(max_workers=n_processes) as executor:
        fcst_results = executor.map(func, cluster_chunks)

        # Combine forecast results of all chunks to one DataFrame
        for fcst in fcst_results:
            df_fcst_results = df_fcst_results.append(fcst)

    return df_fcst_results


@dec_validation
@dec_logger
def get_number_processes(max_processes):
    """ Calculate number of processes used for forecasting

    :param max_processes: upper bound for the number of process
    :type max_processes: int
    :return: number of processes used for forecasting
    :rtype: int
    """
    # Check number of CPU cores
    if max_processes <= mp.cpu_count():
        return max_processes

    return mp.cpu_count()


@dec_validation
@dec_logger
def get_cluster_chunks(cluster_ids, n_processes):
    """ Split list of cluster IDs in n_process-disjoint lists if  n_processes >
    number of IDs, otherwise create only one chunk (note: other rules would be
    possible in this case)

    :param cluster_ids: list of cluster IDs
    :type cluster_ids: list of ints
    :param n_processes: number of processes for forecasting
    :type n_processes: int
    :return: list with independent cluster ID chunks
    :rtype: list of lists of ints
    """
    # Create only one process if n_processes > number IDs
    if n_processes > len(cluster_ids):
        return [cluster_ids]

    cluster_ids_section = []
    section_size = int(len(cluster_ids) / n_processes)

    # Split all cluster IDs in equally large chunks
    for i in range(n_processes):
        if i >= n_processes - 1:
            section = cluster_ids[(i * section_size) :]
        else:
            section = cluster_ids[(i * section_size) : ((i + 1) * section_size)]
        cluster_ids_section.append(section)

    return cluster_ids_section


def mp_run(df_traffic, dict_so_cluster, dict_config, cluster_chunk):
    """ Implementation of a single process / worker

    :param df_traffic: DataFrame with traffic related data and DateTimeIndex
    :type df_traffic: pandas DataFrame
    :param dict_so_cluster: dictionary mapping site numbers to cluster IDs
    :type dict_so_cluster: Dictionary
    :param dict_config: Dictionary with config data
    :type dict_config: Dictionary
    :param cluster_chunk: chunk of cluster IDs to be processed
    :type cluster_chunk: list of ints
    :return the original & forecasted time series for all clusters in the chunk
    :rtype pandas DataFrame
    """

    # Configure logger for each individual process and get process ID
    logger = configure_logger(dict_config)

    # Create result DataFrame for cluster chunk
    df_fcst_results = prepare_fcst_df()

    # Iterate over all cluster IDs in chunk
    for clu_id in cluster_chunk:
        # Run pre-processing
        df_ts_cluster = preprocess_data(
            df_traffic, dict_so_cluster, clu_id, dict_config
        )

        # Note: a cluster time series must have at least 2 NaN rows. This is a
        # constraint of fbprophet
        if len(df_ts_cluster.index) > 2:
            # Run forecasting to get a DataFrame with forecasted time series
            logger.info(f"> Start forecast cluster ID {clu_id}")
            df_fcst, model = forecast(df_ts_cluster, dict_config)
            logger.info(f"+ End forecast cluster ID: {clu_id}")

            # Add cluster identifier for the time series
            df_fcst["cluster_id"] = clu_id
            df_fcst_results = df_fcst_results.append(
                df_fcst[["ds", "cluster_id", "y", "yhat"]]
            )

            # Plot the forecasting result and save it
            fp = os.path.join(dict_config["dir_plot"], f"fcst_cluster_{clu_id}.png")
            model.plot(df_fcst).savefig(fp)

            # Plot the forecasting components and save them
            fp = os.path.join(
                dict_config["dir_plot"], f"components_cluster_{clu_id}.png"
            )
            model.plot_components(df_fcst).savefig(fp)

    return df_fcst_results

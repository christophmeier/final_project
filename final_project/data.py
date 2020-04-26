"""
This module contains functions related to input / output of data

The module contains the following functions:
* load_data: calls
* load_clustering: loads clustering data from a csv file to a dictionary
* load_traffic: loads data traffic info from hdf file to pandas DataFrame
"""
import os
import pandas as pd
from .utils import *


@dec_validation
@dec_logger
def load_data(f_clustering="../data/clustering.csv", f_traffic="../data/traffic.h5"):
    """ Loads all the relevant data

    :param f_clustering: name of clustering related input file
    :param f_traffic: name of traffic related input file
    :return: a dictionary with site-to-cluster mapping and a DataFrame with
             DateTimeIndex containing data traffic information for a certain date
    :rtype tuple (dictionary, DataFrame)
    """
    dict_so_cluster = load_clustering(f_clustering)
    df_traffic = load_traffic(f_traffic)

    return dict_so_cluster, df_traffic


@dec_validation
@dec_logger
def load_clustering(filename):
    """ Load a file containing a list of site numbers assigned to cluster IDs
    and return dictionary with site_number-->cluster_id

    use case: data/clustering.csv

    :param filename: path/name to file to load
    :type filename: str
    :return dict_so_cluster: dictionary mapping site numbers to cluster IDs
    :rtype: dictionary
    """
    # Check if file exists
    if not os.path.exists(filename):
        raise FileNotFoundError("File {} does not exist".format(filename))
    # Read raw data
    df_clustering = pd.read_csv(filename, sep=';')
    # Make dictionary out of site numbers and cluster IDs
    dict_so_cluster = pd.Series(
        df_clustering["cluster"].values, index=df_clustering["so_number"]
    ).to_dict()

    return dict_so_cluster


@dec_validation
@dec_logger
def load_traffic(filename, key="df"):
    """ Load a hdf file containing data traffic related information with respect
    to a certain date and site

    use case: data/traffic.h5

    :param filename: path/name to file to load
    :type filename: str
    :param key: a key to access the hdf file
    :return a pandas DataFrame with DateTimeIndex and traffic information for
            a certain date
    :rtype: pandas DataFrame
    """
    # Check if file exists
    if not os.path.exists(filename):
        raise FileNotFoundError("File {} does not exist".format(filename))
    # Read raw traffic data and return it as DataFrame
    return pd.read_hdf(filename, key)

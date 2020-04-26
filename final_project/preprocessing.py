"""
This module contains functions related to the pre-processing of data in advance
of forecasting

The module contains the following functions:
* load_data: calls
* load_clustering: loads clustering data from a csv file to a dictionary
* load_traffic: loads data traffic info from hdf file to pandas DataFrame
* make_ts: constructs a time series for a given cluster
* make_clean_ts: cleans the time series of a cluster
"""
from .utils import *


TS_START = "2017-07-01"
TS_END = "2019-12-31"


@dec_validation
@dec_logger
def preprocess_data(df_traffic, dict_so_cluster, cluster_id):
    """ Pre-processes all input data before forecasting, e.g. time series
    generation, data cleaning and data imputation

    :param df_traffic: DataFrame with traffic related data and DateTimeIndex
    :param dict_so_cluster: dictionary mapping site numbers to cluster IDs
    :param cluster_id: a specific cluster ID
    :return: a cleaned time series for the cluster
    :rtype: pandas DataFrame
    """
    # Create time series for cluster
    df_ts = make_ts(df_traffic, dict_so_cluster, cluster_id)

    # Clean time series
    df_ts_clean = make_clean_ts(df_ts)

    return df_ts_clean


@dec_validation
@dec_logger
def make_ts(df_traffic, dict_so_cluster, cluster_id):
    """ Makes a time series as pandas DataFrame for a certain cluster assuming
    traffic input data for all clusters

    :param df_traffic: DataFrame with traffic related data and DateTimeIndex
    :type df_traffic: pandas DataFrame
    :param dict_so_cluster: dictionary mapping site numbers to cluster IDs
    :type dict_so_cluster: Dictionary
    :param cluster_id: a specific cluster ID
    :type cluster_id: int
    :return: a DataFrame with daily data traffic for the cluster
    :rtype pandas DataFrame with DateTimeIndex and one column
    """
    # Get all sites of cluster
    so_cluster = [
        so for so in dict_so_cluster.keys() if dict_so_cluster[so] == cluster_id
    ]

    # Generate DataFrames related to data traffic of cluster
    df_ts_cluster = df_traffic.loc[df_traffic["so_number"].isin(so_cluster)]

    # Consider only dates within specified period
    df_ts_cluster = df_ts_cluster.loc[TS_START:TS_END]

    # Rename index and aggregate traffic data on cluster level by date / index
    df_ts_cluster.index.names = ["dt"]
    df_ts_cluster_agg = df_ts_cluster.groupby(['dt']).agg({"gb": "sum"})

    return df_ts_cluster_agg


@dec_validation
@dec_logger
def make_clean_ts(df_ts_cluster):
    """ Cleans the raw input time series, i.e. replacing missing dates and data
    imputation

    Note: outlier handling is inherently accomplished by fbprophet during
    forecasting

    :param df_ts_cluster: time series of a cluster with respect to data traffic
    :type df_ts_cluster: pandas DataFrame
    :return: a cleaned time series for the cluster with respect to data traffic
    :rtype pandas DataFrame
    """
    # Fill missing dates with a Day and value '0'
    df_ts_cluster = df_ts_cluster.asfreq("D", fill_value=0)

    # Replace all NaN values with '0'
    df_ts_cluster.fillna(0, inplace=True)

    return df_ts_cluster

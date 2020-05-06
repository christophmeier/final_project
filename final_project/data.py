"""
This module contains functions related to input / output of data

The module contains the following functions:
* load_data: calls download from AWS S3 and put it into DataFrame & Dictionary
* download_data_aws: downloads clustering and traffic data from AWS S3
* load_clustering: loads clustering data from a csv file to a dictionary
* load_traffic: loads data traffic info from hdf file to pandas DataFrame
* export_fcst_results_hdf5: exports a DataFrame as hdf file
* prepare_fcst_df: creates a DataFrame in the format necessary for export
* get_config_data: returns configuration data for the app
"""
import boto3
import pandas as pd
from .utils import *


@dec_validation
@dec_logger
def load_data(dict_config):
    """ Loads all the relevant data

    :param dict_config: config data
    :type dict_config: Dictionary
    :return: a dictionary with site-to-cluster mapping and a DataFrame with
             DateTimeIndex containing data traffic information for a certain date
    :rtype tuple (dictionary, DataFrame)
    """
    # Read site-to-cluster assignments
    dict_so_cluster = load_clustering(dict_config)
    assert dict_so_cluster is not None, "Import of clustering data is None."

    # Read data traffic
    df_traffic = load_traffic(dict_config)
    assert df_traffic is not None, "Import of traffic data is None."

    return dict_so_cluster, df_traffic


@dec_validation
@dec_logger
def download_data_aws(dict_config):
    """ Download data from AWS S3 bucket into local data folder
    :param dict_config: config data
    :type dict_config: Dictionary
    """
    # Access S3 bucket
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    # Download clustering data
    fp_clustering = os.path.join(dict_config["dir_local"], dict_config["f_clustering"])
    s3.download_file(
        dict_config["aws_bucket"],
        f"{dict_config['dir_bucket']}/{dict_config['f_clustering']}",
        fp_clustering,
    )

    # Download traffic data
    fp_traffic = os.path.join(dict_config["dir_local"], dict_config["f_traffic"])
    s3.download_file(
        dict_config["aws_bucket"],
        f"{dict_config['dir_bucket']}/{dict_config['f_traffic']}",
        fp_traffic,
    )


@dec_validation
@dec_logger
def load_clustering(dict_config):
    """ Load a file containing a list of site numbers assigned to cluster IDs
    and return dictionary with site_number-->cluster_id

    use case: data/clustering.csv

    :param dict_config: config data
    :type dict_config: Dictionary
    :return dict_so_cluster: dictionary mapping site numbers to cluster IDs
    :rtype: dictionary
    """
    # Read raw data
    df_clustering = pd.read_csv(
        os.path.join(dict_config["dir_local"], dict_config["f_clustering"]), sep=";"
    )

    # Make dictionary out of site numbers and cluster IDs
    dict_so_cluster = pd.Series(
        df_clustering["cluster"].values, index=df_clustering["so_number"]
    ).to_dict()

    return dict_so_cluster


@dec_validation
@dec_logger
def load_traffic(dict_config, key="df"):
    """ Load a hdf file containing data traffic related information with respect
    to a certain date and site

    use case: data/traffic.h5

    :param dict_config: config data
    :type dict_config: Dictionary
    :param key: a key to access the hdf file
    :return a pandas DataFrame with DateTimeIndex and traffic information for
            a certain date
    :rtype: pandas DataFrame
    """
    # Read raw traffic data and return it as DataFrame
    return pd.read_hdf(
        os.path.join(dict_config["dir_local"], dict_config["f_traffic"]), key
    )


@dec_validation
@dec_logger
def export_fcst_results_hdf5(df_fcst_results, dict_config, filename="forecast.h5"):
    """ Writes the content of a DataFrame containing the forecasting results
    for each cluster into a HDF5 file

    :param df_fcst_results: DataFrame containing the forecasting results of
                            each cluster
    :type df_fcst_results: pandas DataFrame
    :param dict_config: config data
    :type dict_config: Dictionary
    :param filename: name of output file
    :type filename: str
    """
    # Replace NaN values with 0 and export as hdf file
    fp_results = os.path.join(dict_config["dir_results_local"], filename)
    df_fcst_results.fillna(0).to_hdf(fp_results, key="df", mode="w")


@dec_validation
@dec_logger
def prepare_fcst_df():
    """ Creates a DataFrame in the format used for export of forecast results

    :return: empty DataFrame with desired forecast format
    :rtype pandas DataFrame
    """
    # Generate DataFrame for Oracle export
    df_export = pd.DataFrame(columns=["ds", "cluster_id", "y", "yhat"])

    return df_export


@dec_validation
@dec_logger
def get_config_data():
    """ Get configuration data (could be achieved via *yml file or database import)

    :return: configuration related data
    :rtype Dictionary
    """
    dict_config = {
        "f_clustering": "clustering.csv",
        "f_traffic": "traffic_small.h5",
        "aws_bucket": "cmeier-csci-e-29",
        "dir_bucket": "final_project",
        "dir_local": "./data",
        "dir_results_local": "./data/fcst_results",
        "dir_plot": "./data/fcst_images",
        "dir_logs": "./logs",
        "ts_input_start": "2017-07-01",
        "ts_input_end": "2019-12-31",
    }

    return dict_config

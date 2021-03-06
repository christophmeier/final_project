"""
This module contains functions related to the forecasting process

The module contains the following functions:
* forecast: calls all functions related to forecasting
* prepare_forecast: prepares the input time series of a cluster in a format
                    necessary for forecasting
* make_forecast: builds and fits the forecasting model and subsequently makes
                 the forecast
"""
import pandas as pd
from .utils import *
from fbprophet import Prophet


@dec_validation
@dec_logger
def forecast(df_ts_cluster, dict_config):
    """ Make a forecast for a time series of a cluster for certain number days

    :param df_ts_cluster: time series of cluster to be forecasted with respect
                          to traffic data
    :type df_ts_cluster: pandas DataFrame with DateTimeIndex
    :param dict_config: Dictionary with config data
    :type dict_config: Dictionary
    :return: the forecasted time series as well as original time series
    :rtype: tuple (pandas DataFrame, fbprophet model)
    """
    # Prepare input data for forecast
    df_ts_prophet = prepare_forecast(df_ts_cluster)

    # Make forecast
    df_fcst, model = make_forecast(df_ts_prophet, dict_config)

    return df_fcst, model


@dec_validation
@dec_logger
def prepare_forecast(df_ts_cluster):
    """ Prepare input data for forecast with fbprophet (which requires a certain
    input format)

    :param df_ts_cluster: time series of cluster to be forecasted with respect
                          to traffic data
    :type df_ts_cluster: pandas DataFrame
    :return: the time series to be forecasted in the required fbprophet format
    :rtype: pandas DataFrame
    """
    # Create empty DataFrame with column names "ds" and "y" as required
    df_ts_prophet = pd.DataFrame(columns=["ds", "y"])

    # Set target variable
    df_ts_prophet["y"] = df_ts_cluster["gb"].iloc[:]

    # Set dates
    df_ts_prophet.loc[(df_ts_prophet["y"] < 0), "y"] = None
    df_ts_prophet["ds"] = df_ts_cluster.index
    df_ts_prophet = df_ts_prophet.reset_index(drop=True)

    return df_ts_prophet


@dec_validation
@dec_logger
def make_forecast(df_ts_prophet, dict_config):
    """ Make forecast for cluster time series using fbprophet package

    :param df_ts_prophet: time series of cluster in the required fbprophet format
    :type df_ts_prophet: pandas DataFrame
    :param dict_config: Dictionary with config data
    :type dict_config: Dictionary
    :return:  the original and forecasted time series of a cluster as well as
              the corresponding fbporphet model
    :rtype: tuple (pandas DataFrame, fbprophet model)
    """
    # Build fbprophet model with most parameters set as default
    model = Prophet(seasonality_mode="multiplicative", yearly_seasonality=True,)

    # Use country-wide German holidays
    model.add_country_holidays(country_name="DE")

    # Fit model
    model.fit(df_ts_prophet)

    # Construct future DataFrame with number of days to be predicted
    df_future = model.make_future_dataframe(
        periods=dict_config["fcst_days"], freq="D"
    )

    # Make forecast
    df_fcst_prophet = model.predict(df_future)

    # Add original values
    df_fcst_prophet["y"] = df_ts_prophet["y"]

    return df_fcst_prophet, model

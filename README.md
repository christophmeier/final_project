# Concurrent mobile data traffic forecasting of regional clusters in Germany 

[![Build Status](https://travis-ci.com/christophmeier/final_project.svg?branch=master)](https://travis-ci.com/christophmeier/final_project)

[![Maintainability](https://api.codeclimate.com/v1/badges/75a0b89f10149b643151/maintainability)](https://codeclimate.com/github/christophmeier/final_project/maintainability)

[![Test Coverage](https://api.codeclimate.com/v1/badges/75a0b89f10149b643151/test_coverage)](https://codeclimate.com/github/christophmeier/final_project/test_coverage)

## Final project CSCI-E-29
Extending the network infrastructure constitutes a problem of major practical relevance for telecommunication companies. 
As resources (budget, time) are limited, this problem can be regarded as prioritization problem: which parts of the network 
(i.e. which cities/regions) should be prioritized for extension in order to maximize some objective? Thereby, potential 
objectives include maximization of customer satisfaction (in order to reduce churn) or an increase of revenues (e.g. 
through build-up of new sites in regions with low number of customers / low coverage). Beside certain legal requirements 
in Germany (e.g. minimum coverage along highways, rivers etc.), the telco companies may upgrade their network with a 
relatively high degree of freedom. In order to support decision making for network extension, one pivotal information – 
regardless of the business objective – is the expected growth of mobile data traffic over the next couple of years on a 
granularity as low as possible, i.e. not only a forecast for the entire country but also on a city-level or even just for 
some (clustered) set of sites. Such a KPI would facilitate the decision process as we would intuitively argue that 
investments should be prioritized in regions with high future traffic growth. Certainly, more KPIs (e.g. the customer 
structure, financial issues etc.) are necessary for a sophisticated network extension plan but expected data traffic 
growth is one of the most important one. 

Therefore, I like to develop a Python script for mobile data traffic forecasting of clustered regions in Germany with 
variable umber of forecast days (can be specified by the user). Due to the scope of this work, I will significantly reduce 
the complexity of the problem at some points and use a clustering of Germany in 1.363 regions. As the forecast of an 
individual cluster is independent of the forecast of other clusters, I can significantly speed-up overall process time by 
calculating the cluster forecasts in parallel. 

In the following I like to describe the individual tasks a little bit more in detail: 
•	Data input from AWS S3
Basically, this task shall import necessary data stored in two files from an AWS S3 bucket:
1.	Data related to mobile data traffic in Germany which is stored in a HDF5 file format (https://en.wikipedia.org/wiki/Hierarchical_Data_Format). 
It contains the daily data traffic of each site (approximately 30k) in the network for the last 2.5 years (start is July 2017). 
The output is a pandas Dataframe with DateTimeIndex. 
2.	Clustering related data stored as *.csv file. In this case, spectral clustering was used to decompose Germany in 
1.363 meaningful regions but clustering itself is not part of the process in this project (out of scope). Basically, the 
output is a dictionary mapping each site in the network to a cluster ID.
•	Worker process 1 – n
In order to achieve a parallel calculation of the individual forecasts I will assign all cluster IDs to be forecasted to 
n batches (depending on the number of CPUs available on the system). Thereby, each batch represents a worker process in 
the built-in Python multiprocessing package (https://docs.python.org/3.4/library/multiprocessing.html? highlight=process#module-multiprocessing) 
which can be executed in parallel to other worker processes. Within a worker process, the forecasting pipeline will be 
executed iteratively for each cluster. 
•	Time series generation
This task is part of the pipeline within each worker process. Using the imported raw data, this task generates a “clean” 
daily time series with respect to data traffic for a cluster ID as pandas DataFrame with DateTimeIndex. For that purpose, 
this task needs to aggregate the input data of multiple sites assigned to the cluster ID. 
•	Time series cleaning
Once a time series is available for a cluster, it needs to be cleaned. Therefore, this task deals with missing data in the 
time series and applies an outlier management (i.e. outlier detection and handling). Regarding outlier detection, I will 
decompose the time series using STL (https://en.wikipedia.org/wiki/Decomposition_of_time_series) and make a test on the residuals.  
•	Forecasting
With the cleaned time series of a cluster ID as input, this task performs a forecast for the desired number of days. 
In general, multiple strategies for time series forecasting exist. However, as the forecast should be automated (no manual 
fine-tuning) and scalable I decided to use a relatively new open-source package from Facebook for that task: fbprophet 
(https://facebook.github.io/prophet/). The output is a time series as pandas DataFrame with DateTimeIndex containing data 
traffic values for the input days as well as for the forecast period. Besides, a simple line chart visualizing the time 
series will be produced and saved as *.png file.   
•	Process output
The forecasting results of all clusters in any worker process need to be somehow saved until all worker processes are 
finished and the final output is generated. This task is responsible for that job.
•	Data output of results to AWS S3
This task waits until all worker processes are finished, aggregates their results (i.e. a pandas DataFrame with time series 
for all associated clusters) to one final DataFrame with time series (input data as well as forecasting data) for all 
cluster IDs, writes it to an Excel-file and uploads it to an AWS S3 bucket. Additionally, this task will upload the *.png 
files for all clusters.

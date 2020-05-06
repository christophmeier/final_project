# Final project CSCI-E-29: Concurrent mobile data traffic forecasting of regional clusters in Germany 

[![Build Status](https://travis-ci.com/christophmeier/final_project.svg?branch=master)](https://travis-ci.com/christophmeier/final_project)

[![Maintainability](https://api.codeclimate.com/v1/badges/75a0b89f10149b643151/maintainability)](https://codeclimate.com/github/christophmeier/final_project/maintainability)

[![Test Coverage](https://api.codeclimate.com/v1/badges/75a0b89f10149b643151/test_coverage)](https://codeclimate.com/github/christophmeier/final_project/test_coverage)

## Important Notes

1. The application can be started via `python -m final_project` with `-d` as additional argument for the number of forecast days 
for every cluster, i.e. if the user likes to forecast 100 days the command would be `python -m final_project -d 100`. 
The default number of forecasting days (wihtout `-d` argument) is 365. 

2. To run the application, two files are necessary: `clustering.csv` and `traffic_small.h5`. These two files will downloaded 
from AWS S3 using my credentials. The corresponding Travis build is available at [Travis final project](https://travis-ci.com/github/christophmeier/final_project).

3. Due to the size of the original input data, the app is using only 10% of the original mobile data traffic information. 
Besides, only a subset of the cluster IDs, more precisely the first 10 cluster IDs, will be processed. 

4. The folders `logs`, `data`, `data/fcst_images`, `data/fcst_results` contain a `.gitkeep` file and will be used save 
logging messages as well as input & output files.  

## Project Description
#### Background information
Extending the network infrastructure constitutes a problem of major technical and business related relevance for telecommunication 
companies. As resources (budget, time) are limited, this problem can be regarded as prioritization problem: which parts of the 
network (i.e. which cities/regions) should be prioritized for extension in order to maximize some objective? Thereby, 
potential objectives include maximization of customer satisfaction (in order to reduce churn) or an increase of revenues 
(e.g. through build-up of new sites in regions with low number of customers / low coverage). Beside certain legal 
requirements in Germany (e.g. minimum coverage along highways, rivers etc.), the telco companies may upgrade their network 
with a relatively high degree of freedom. In order to support decision making for network extension, one pivotal information – 
regardless of the business objective – is the expected growth of mobile data traffic over the next couple of years on a 
granularity as low as possible, i.e. not only a forecast for the entire country but also on a city-level or even just for 
some (clustered) set of sites. Such a KPI would facilitate the decision process with respect to location of new site 
installations (roughly $250k per site) as we would intuitively argue that investments should be prioritized in regions 
with high future traffic growth. Certainly, more KPIs (e.g. the customer structure, financial issues etc.) are necessary 
for a sophisticated network extension plan but expected data traffic growth is one of the most important one.

Therefore, I developed a Python app for mobile data traffic forecasting of clustered regions in Germany with variable 
number of forecast days (can be specified by the user). Due to the scope of this work, I significantly reduced the complexity 
of the problem at many points and used an existing clustering (which comes from one of my former projects) of Germany in 
roughly 1.400 regions. As output, the app will also generate plots for the time series and its components (trend, seasonality) 
for a specific cluster: 
![Result visualiaztion](https://i.ibb.co/mHY1XJL/visualization-final.png)

#### Workflow
As the forecast of an individual cluster is independent of the forecast of other clusters, overall process time can be 
reduced through concurrency. Using concurrency in Python is actually the main motivation of my work. For that purpose, 
Python basically offers two options: multiprocessing and threading. As the task to be parallelized is more CPU bound, 
multiprocessing is expected to be the best strategy. The overall process workflow looks as follows:
![Workflow](https://i.ibb.co/K0ZVRMv/workflow.png)

#### Results
I used the ProcessPoolExecutor / ThreadPoolExecutor classes in concurrent.futures and compared their times with a sequential 
execution of all 1.400 forecasts on my ThinkPad notebook with 4 CPUs. The results are as follows:
* Multiprocessing execution: 1h 20min
* Sequential execution: 3h 40min (**2.75x slower** than multiprocessing)
* Multithreading execution (with GIL): 5h 53min (**4.41x slower** than multiprocessing)
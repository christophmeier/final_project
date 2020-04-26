.PHONY: data

data: data/clustering.csv
data: data/traffic.h5

data/clustering.csv:
	aws s3 cp s3://cmeier-csci-e-29/final_project/clustering.csv data/ --request-payer=requester
data/traffic.h5:
	aws s3 cp s3://cmeier-csci-e-29/final_project/traffic.h5 data/ --request-payer=requester

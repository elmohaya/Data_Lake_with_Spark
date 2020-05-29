# Data_Lake_with_Spark

A music streaming startup named Sparkify wanted to move thier data warehouse to a data lake. Their data resides in S3, in
a directory of JSON logs on user activity on the app, as well as a JSON metadata on the songs in their app. This project 
resembles an attempt to build an ETL pipeline that extracts their data from S3, process them using Spark, and loads the data 
back into S3 as a set of dimensional tables. This will allow easier analytics for finding insights in what songs their users
are listening to.

This repository contains three files:

etl.py ----- This file loades data from S3, processes it using Spark, creates dimensional tables, and loades it back
dl.cfg ----- It contains the AWS credentials
README.md -- The current file
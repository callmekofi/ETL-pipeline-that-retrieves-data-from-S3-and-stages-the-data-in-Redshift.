INTRODUCTION:
The goal of this project is to develp an ETL pipeline that retrieves data from S3 and stages the data in Redshift. The data is transformed into a group of dimension tables to analise songs and user activity data collected by Sparkify. This makes use of models to design and create tables to optimise queries on datasets made available by Sparkify as log data and song data in JSON formats stored in an S3 bucket. At the end of this project, the Sparkify analysis team is able to retrieve results based on queries on what songs its users are listening to. In summary, data is retrieved from S3 to staging tables on Redshift. A "facts" and "dimensions" tables is created based on SQL queries to load data from staging tables into the analytics tables.

STEPS:
Files made available to execute this project are: 
- create_tables.py: This file makes a connection to the Redshift cluster, executes create and drop table queries and should be executed prior to running other scripts.
- sql_queries.py: This file is made up of "create" and "insert into" facts and dim table queries to the dB. 
- etl.py: This file contains all processes for an ETL procedure for all fact and dim tables considered.
- dwh.cfg: This file contains information on the data warehouse cluster created, path to the dataset given,iam role credentials and iam user keys.

IMPORTANT PROCESSES:
- Create resources, IAM role, Redshift cluster and cluster connections using IaC.
- Create tables in sql_queries.py. Make use of distribution keys for table columns to allow efficient query processing. 
- Build ETL processes and Pipelines in etl.py
- Test queries in Redshift.


TABLES CREATED:
- Facts: Songplays = Made up of songplay events for the datasets given to inform business decision.
- Dim: users = Made up of users registered to the Sparkify app.
       songs = Made up of all available songs in the Sparkify music dB
     artists = Made up of all artists of the songs in the Sparkify misuc dB
        time = Made up of the timestamps of song plays by users of the Sparkify app.
        
QUERIES:
All select, join, aggregation and known available postgres queries can be applied on the Redshift cluster.
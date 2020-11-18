# Purpose of this Database

Sparkify has grown their business and therefore increased their user base and song database. 
They  would like to move their data from a data warehouse to a data lake.
Currently their data is stored in S3 in a directory of JSON logs, but they would like to 
move to a Redshift cloud database. As a data engineer my job is to create an ETL/ELT pipelines which 
would...
  * extract data from S3 -> process using Spark -> load data back into S3 as set of dimensional 
 tables

# Justification of Database Schema Design and ETL pipeline

[Imgur](https://i.imgur.com/Y2fKlEi.png) <- this an ERD graphic for my database schema
* Database Schema Design
    * data is in a star schema which is optimal for queries on `song_plays`
* ETL pipeline
    * stages two tables (`events` and `songs`) from the two datasets previously stored in S3 
    * the staging of the two tables allows for creation of the fact and dimension tables which 
    make up the Redshift database
        * **fact table:** `song_plays`
        * **dimension tables:** `users`, `songs`, `artists`, `time`
## Files in Repository
* `etl.py`
    * This file reads data from S3, processes data using Spark and then writes them back 
    into an S3 hosted data lake.
        * the process_song_data and process_log_data function are used to create the 
        fact and dimension tables via pyspark
* `dl.cfg`
    * this file contains AWS credentials
 
## How To Run Files
1. Run `etl.py` to make the ETL process occur.

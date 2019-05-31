# DEND Project 4: Data Lake

## Project Summary

A fictional music streaming startup, Sparkify, has grown their user base and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I have been tasked with building an ETL pipeline that extracts the data from S3, processes it using Spark, and loads the data back into S3 as a set of dimensional tables. I have written a script in python that uses Spark SQL and Spark Data Frames to execute the following:

* create a Spark session using the Apache Hadoop Amazon Web Services Support module
* load AWS access keys into environment variables
* ingest log files and song data files from S3
* clean and process the data:
  * add unique row identifiers to all fact and dimension tables
  * remove duplicate rows
  * impute nulls to desired values
  * parse timestamps into time and date components
  * create tables for
* write the final set of tables to S3

This project was submitted for Udacity's Data Engineering Nanodegree (DEND) in Spring 2019.

## How to Use

Run etl.py from terminal or python console.

## Files in Repository

The tables produced by the ETL process are called songplays, users, artists, songs and time. Each of the five tables are written to parquet files in a separate analytics directory on S3. Each table has its own folder within the directory. Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.

#### Fact Table

**songplays** - records in log data associated with song plays i.e. records with page NextSong
* *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables

**users** - users in the app
* *user_id, first_name, last_name, gender, level*

**songs** - songs in music database
* *song_id, title, artist_id, year, duration*

**artists** - artists in music database
* *artist_id, name, location, lattitude, longitude*

**time** - timestamps of records in songplays broken down into specific units
*start_time, hour, day, week, month, year, weekday*

## Discussion

One of the primary challenges in this assignment was learning to deal with extremely long times for data transfer during development and testing. My first attempt to use S3 failed to complete a table write of less than 20MB over a period of eight hours.

Some strategies that I used to deal with slow loading times included:
* Developing ETL initially on a small set of sample files in a standalone workspace offered by the course provider.  
* Printing frequent updates to the console to indicate where in the ETL process progress was getting stuck.
* Testing the script on a small subset of the log and songplays data in the S3 repository

Other students/mentors suggested running the script from Amazon EMR, a managed service for data processing that supports Spark as well as other big data frameworks. This would have been a good approach for a production project.

For future reference, Amazon provides [guidelines](https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-perforance-guidelines.html) for optimizing data transfer performance when using S3.

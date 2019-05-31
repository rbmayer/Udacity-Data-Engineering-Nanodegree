import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create the Spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Create songs and artists tables from song data.

    Load song files from S3, process data into songs and artists tables, and write tables to partitioned parquet files on S3.

    Parameters: 
    spark - an active Spark session
    input_data - path to S3 bucket with input data
    output_data - path to S3 bucket to store output tables

    Returns: None

    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    print('starting read of song_data json files: ' + str(datetime.now()))
    df = spark.read.json(song_data)
    print('loading of song_data files complete: ' + str(datetime.now()))

    # extract columns to create songs
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_path = os.path.join(output_data, 'songs')
    print('writing songs table to S3: ' + str(datetime.now()))
    songs_table.write.parquet(songs_path, mode='overwrite', partitionBy=["year", "artist_id"])
    print('write of songs table to S3 complete: ' + str(datetime.now()))

    # extract columns to create artists table artist_id, name, location, lattitude, longitude
    artists_table = df.selectExpr(["artist_id",
                                   "artist_name",
                                   "coalesce(nullif(artist_location, ''), 'N/A') as location",
                                   "coalesce(artist_latitude, 0.0) as latitude",
                                   "coalesce(artist_longitude, 0.0) as longitude"]) \
                      .dropDuplicates()
    
    # write artists table to parquet files
    artists_path = os.path.join(output_data, 'artists')
    print('writing artists table to S3: ' + str(datetime.now()))
    artists_table.write.parquet(artists_path, mode='overwrite')
    print('write of artists table to S3 complete: ' + str(datetime.now()))


def process_log_data(spark, input_data, output_data):
    """Create users, time and songplays tables.

    Load log files and input tables from S3, process data into output table formats, and write tables to partitioned parquet files on S3.

    Parameters: 
    spark - an active Spark session
    input_data - path to S3 bucket with input data
    output_data - path to S3 bucket to store output tables

    Returns: None

    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file
    print('reading log data from S3: ' + str(datetime.now()))
    df = spark.read.json(log_data)
    print('loading of log data from S3 complete: ' + str(datetime.now()))
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(["userId as user_id", 
                                 "firstName as first_name", 
                                 "lastName as last_name", 
                                 "gender", 
                                 "level"]) \
                    .dropDuplicates()
    
    # write users table to parquet files
    users_path = os.path.join(output_data, 'users')
    print('writing users table to S3: ' + str(datetime.now()))
    users_table.write.parquet(users_path, mode='overwrite')
    print('write of users table to S3 complete: ' + str(datetime.now()))

    # create timestamp column from original timestamp column
    df = df.withColumn("log_timestamp", F.to_timestamp(df.ts/1000))
    
    # create datetime column from original timestamp column
    df = df.withColumn("log_datetime", F.to_date(df.log_timestamp))
    
    # extract columns to create time table start_time, hour, day, week, month, year, weekday
    time_table = df.selectExpr(["log_timestamp as start_time", 
                                "hour(log_datetime) as hour", 
                                "dayofmonth(log_datetime) as day", 
                                "weekofyear(log_datetime) as week", 
                                "month(log_datetime) as month", 
                                "year(log_datetime) as year", 
                                "dayofweek(log_datetime) as weekday"]) \
                   .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_path = os.path.join(output_data, 'time')
    print('writing time table to S3 partitioned by year and month: ' + str(datetime.now()))
    time_table.write.parquet(time_path, mode='append', partitionBy=["year", "month"])
    print('write of time table to S3 complete: ' + str(datetime.now()))

    # read in song data to use for songplays table
    print('reading songs table from S3: ' + str(datetime.now()))
    songs_path = os.path.join(output_data, 'songs')
    song_df = spark.read.parquet(songs_path)
    print('loading of songs table from S3 complete: ' + str(datetime.now()))
    song_df = song_df.withColumnRenamed("artist_id", "songs_artist_id")
    
    # read in artists data to use for songplays table
    artists_path = os.path.join(output_data, 'artists')
    print('reading artists table from S3: ' + str(datetime.now()))
    artists_df = spark.read.parquet(artists_path)
    print('loading of artists table form S3 complete: ' + str(datetime.now()))
    artists_df = artists_df.withColumnRenamed("artist_id", "artists_artist_id") \
                           .withColumnRenamed("location", "artist_location")

    # extract columns from joined song and log datasets to create songplays table 
    print('creating songplays table: ' + str(datetime.now()))
    songplays_table = df.select(df.log_timestamp.alias("start_time"), 
                                 df.userId.alias("user_id"), 
                                 "level", 
                                 "song", 
                                 "artist", 
                                 df.sessionId.alias("session_id"), 
                                 "location", 
                                 df.userAgent.alias("user_agent")) \
                        .join(song_df, df.song==song_df.title, 'left_outer')   \
                        .join(artists_df, df.artist==artists_df.artist_name, 'left_outer') \
                        .selectExpr("start_time",
                                    "user_id",
                                    "level",
                                    "song_id",
                                    "coalesce(artists_artist_id, songs_artist_id) as artist_id",
                                    "session_id",
                                    "location",
                                    "user_agent",
                                    "year(start_time) as year",
                                    "month(start_time) as month") \
                        .dropDuplicates() \
                        .withColumn('songplay_id', F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_path = os.path.join(output_data, 'songplays')
    
    print('writing songplays table to S3: ' + str(datetime.now()))
    songplays_table.write.parquet(songplays_path, mode='overwrite', partitionBy=["year", "month"])
    print('write of songplays table to S3 complete: ' + str(datetime.now()))


def main():
    print('creating spark session: ' + str(datetime.now()))
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-rbmayer/"
    
    print('starting song data processing: ' + str(datetime.now()))
    process_song_data(spark, input_data, output_data)
    print('song data processing complete: ' + str(datetime.now()))
    print('starting log data processing: ' + str(datetime.now()))
    process_log_data(spark, input_data, output_data)
    print('ETL complete: ' + str(datetime.now()))


if __name__ == "__main__":
    main()

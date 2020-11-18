import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Description: The function loads song_data from S3 the extracts the songs and artists tables
    
    Parameters
        spark: Spark session
        input_data: where file is loaded from to process
        output_data: where data is aftered it's processed, where the results are stored"""
    # get filepath to song data file
    song_data =input_data + 'song_data/*/*/*/*.json'
    
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create temporary view to run SQL queries
    df.createOrReplaceTempView("songdata_table")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISINCT songs.song_id,
        songs.title,
        songs.artist_id,
        songs.year,
        songs.duration
        FROM songdata_table songs
        WHERE song_id IS NOT NULL
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT artists.artist_id,
        artists.artist_name,
        artists.artist_location
        artists.artist_longitude
        FROM songdata_table artists
        WHERE artists.artist_id IS NOT NULL
        """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
        Description: The function loads song_data from S3 the extracts the songs and artists tables
        
        Parameters
        spark: Spark session
        input_data: where file is loaded from to process
        output_data: where data is aftered it's processed, where the results are stored
    """
    # get filepath to log data file
    log_data =input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # create temporary view to run SQL queries
    df.createOrReplaceTempView('logdata_table')
    
    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT 
        users.userId as user_id,
        users.firstName as first_name,
        users.lastName as last_name,
        users.gender as gender,
        users.level as level,
        FROM logdata_table users
        WHERE users.userId is NOT NULL
    """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')
    
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINT
        A.start_time_sub as start_time,
        hour(A.start_time_sub) as hour,
        daysofmonth(A.start_time_sub) as day,
        weekofyear(A.start_tiime_sub) as week,
        month(A.start_time_sub) as month,
        year(A.start_time_sub) as weekday
        FROM
        (SELECT to_timestamp(timeSt.ts/1000) as start_time
        FROM logdata_table time
        WHERE timeSt.ts IS NOT NULL) A
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    songplays_df = spark.read.parquet(output_data+'songplays_table/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT monotonically_increase_id() as songplay_id,
        to_timestamp(LT.ts/1000) as start_time,
        month(to_timestamp(LT.ts/1000)) as month,
        year(to_timestamp(LT.ts/1000)) as year,
        LT.userId as user_id,
        LT.level as level,
        ST.song_id as song_id,
        ST.artist_id as artist_id,
        LT.sessionId as session_id,
        LT.userAgent as user_agent,
        FROM logdata_table LT
        JOIN songdata_table ST 
        ON LT.artist = ST.artist_name AND LT.song and ST.title
    
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/dloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

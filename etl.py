import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = 
    df.createOrReplaceTempView('songs')

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id, title, artist_id, year, duration
                            FROM songs
                 """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table

    # extract columns to create artists table
    artists_table = spark.sql("""
                              SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                              FROM songs
                 """)
    
    # write artists table to parquet files
    artists_table


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 
    df.createOrReplaceTempView('logs')
    
    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT userId, firstName, lastName, gender, level
                            FROM logs
               """)
    
    # write users table to parquet files
    users_table

    # create timestamp column from original timestamp column
    spark.udf.register('get_timestamp', lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = 
    
    # create datetime column from original timestamp column
    spark.udf.register('get_datetime', lambda x: datetime.fromtimestamp(x/1000), DateType())
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

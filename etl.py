import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates the Spark Session
    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process all the songs data from the json files specified from the input_data
    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('songs')

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id, title, artist_id, year, duration
                            FROM songs
                 """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data + 'songs.parquet', mode = 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""
                              SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                              FROM songs
                 """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists.parquet', mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Process all event logs of the Sparkify app usage, specifically the 'NextSong' event.
    :param spark:
    :param input_data:
    :param output_data:
    :return:
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView('logs')
    sql = """SELECT * FROM logs
         WHERE page = 'NextSong'
    """
    df = spark.sql(sql)
    df.createOrReplaceTempView('logs')
    
    # extract columns for users table
    sql = """SELECT DISTINCT userId, firstName, lastName, gender, level
             FROM logs
    """
    users_table = spark.sql(sql)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'artists.parquet', mode = 'overwrite')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.fromtimestamp(x/1000), TimestampType())#for dataframe(add one more column)
    df = df.withColumn('timestamp', get_timestamp('ts'))
    spark.udf.register('get_timestamp', lambda x: datetime.fromtimestamp(x/1000), TimestampType())#for SparkSQL(extract columns to create time table)
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:datetime.fromtimestamp(x/1000), DateType())#for dataframe(add one more column)
    df = df.withColumn('datetime', get_datetime('ts'))
    spark.udf.register('get_datetime', lambda x: datetime.fromtimestamp(x/1000), DateType())#for SparkSQL(extract columns to create time table)
    
    # extract columns to create time table
    sql = """SELECT get_timestamp(ts) AS start_time,
                    DATE_TRUNC('hour', timestamp) AS hour,
                    DATE_TRUNC('day', timestamp) AS days,
                    DATE_TRUNC('week',timestamp) AS week,
                    DATE_TRUNC('month', timestamp) AS month,
                    DATE_TRUNC('year', timestamp) AS year,
                    DAYOFWEEK(timestamp) AS weekday
            FROM logs
          """
    time_table = spark.sql(sql)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data + 'time.parquet', mode = 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs.parquet')
    song_df.createOrReplaceTempView('songs')

    # extract columns from joined song and log datasets to create songplays table
    sql = """SELECT timestamp AS start_time,
                userId AS user_id,
                level,
                song_id,
                artist_id,
                sessionId AS session_id,
                location,
                userAgent
         FROM logs 
         JOIN song ON song.title = logs.song
         """
    songplays_table = spark.sql(sql)
    songplays_table.withColumn('songplay_id', F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + 'songplays.parquet', mode = 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-aws/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

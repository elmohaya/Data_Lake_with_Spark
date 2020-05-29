import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).where(df.page = 'NextSong').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs/' + 'songs.parquet', partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_lattitude", "artist_longitude"]) \
                    .withColumnRenamed('artist_name', 'name') \
                    .withColumnRenamed('artist_location', 'location') \
                    .withColumnRenamed('artist_lattitude', 'lattitude') \
                    .withColumnRenamed('artist_longitude', 'longitude').distinct()
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/' + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.select(['userId','firstName','lastName','gender','level'])\
                    .withColumnRenamed('userId','user_id') \
                    .withColumnRenamed('firstName','first_name') \
                    .withColumnRenamed('lastName','last_name') \
                    
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/' + 'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    time_table = df.select('ts', 'time') \
                   .withColumn('year', F.year('time')) \
                   .withColumn('month', F.month('time')) \
                   .withColumn('week', F.weekofyear('time')) \
                   .withColumn('weekday', F.dayofweek('time')) \
                   .withColumn('day', F.dayofyear('time')) \
                   .withColumn('hour', F.hour('time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time/' +' time.parquet', partitionBy=['year','month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT l.time,
                                       t.year,
                                       t.month,
                                       l.userid,
                                       l.level,
                                       q.song_id,
                                       q.artist_id,
                                       l.sessionid,
                                       l.location,
                                       l.useragent
                                  FROM log_data l
                                  JOIN time_table t ON (l.time = t.time)
                                 LEFT JOIN (
                                           SELECT s.song_id,
                                                  s.title,
                                                  a.artist_id,
                                                  a.artist_name
                                             FROM songs_table s
                                             JOIN artists_table a ON (s.artist_id = a.artist_id)
                                          ) AS q ON (l.song = q.title AND l.artist = q.artist_name)
                                  """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays_table")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-company/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

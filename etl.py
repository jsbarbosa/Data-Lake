import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import udf, to_date, dayofweek, \
    monotonically_increasing_id, year, month, dayofmonth, hour, weekofyear

ALLOW_READ: bool = True  #: read from local or S3
ALLOW_WRITE: bool = True  #: write to S3

config = configparser.ConfigParser()

config.read(
    "dl.cfg"
)

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def read_song_data(spark: SparkSession, input_data: str):
    """
    Function that reads the song data from the S3 bucket into the Spark Session

    Parameters
    ----------
    spark
        spark session
    input_data
        path to S3 data
    """

    if ALLOW_READ:
        song_data = f"{input_data}/song-data/A/A/*/*.json"
    else:
        song_data = "data/song-data/A/A/*/*.json"

    # read song data file
    return spark.read.json(
        song_data
    )


def read_log_data(spark: SparkSession, input_data: str):
    """
    Function that reads the log data from the S3 bucket into the Spark Session

    Parameters
    ----------
    spark
        spark session
    input_data
        path to S3 data
    """

    if ALLOW_READ:
        log_data = f"{input_data}/log-data/A/A/*.json"
    else:
        log_data = "data/log-data/*.json"

    # read log data file
    return spark.read.json(
        log_data
    )


def create_spark_session():
    """
    Function that builds the Spark session with the following configuration:
        - spark.jars.packages
        - org.apache.hadoop:hadoop-aws:2.7.0
    """

    return SparkSession.builder.config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()


def process_song_data(
        spark: SparkSession,
        input_data: str,
        output_data: str
):
    """
    Function that process the song data and builds two parquet tables:
        - songs/songs_table
        - artists/artists_table
    These tables are uploaded to S3

    Parameters
    ----------
    spark
        Spark Session
    input_data
        Path to S3 input data
    output_data
        Path to S3 Parquet output
    """

    # read song data file
    df = read_song_data(
        spark,
        input_data
    )

    # extract columns to create songs table
    songs_table = df[
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ].dropDuplicates(
        ["song_id"]
    )

    # write songs table to parquet files partitioned by year and artist
    if ALLOW_WRITE:
        songs_table.write.partitionBy("year", "artist_id").parquet(
            f"{output_data}songs/songs_table.parquet"
        )

    # extract columns to create artists table
    artists_table = df[
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ].distinct()

    # write artists table to parquet files
    if ALLOW_WRITE:
        artists_table.write.parquet(
            f"{output_data}artists/artists_table.parquet"
        )


def process_log_data(
        spark: SparkSession,
        input_data: str,
        output_data: str
):
    """
    Function that process the log data and builds two parquet tables:
        - users/users_table
        - time/time_table
        - songplays/songplays_table
    These tables are uploaded to S3

    Parameters
    ----------
    spark
        Spark Session
    input_data
        Path to S3 input data
    output_data
        Path to S3 Parquet output
    """

    # get filepath to log data file
    df = read_log_data(
        spark,
        input_data
    )

    # filter by actions for song plays
    df = df.withColumn(
        "user_id",
        df.userId.cast(
            types.IntegerType()
        )
    ).filter(
        df.page == "NextSong"
    )

    # extract columns for users table
    user_table = df[
        "firstName",
        "lastName",
        "gender",
        "level",
        "userId"
    ].distinct()

    # write users table to parquet files
    if ALLOW_WRITE:
        user_table.write.parquet(
            f"{output_data}users/users_table.parquet"
        )

    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda t: datetime.fromtimestamp(t / 1000).isoformat()
    )

    df = df.withColumn(
        "start_time",
        get_timestamp("ts").cast(
            types.TimestampType()
        )
    )

    # create datetime column from original timestamp column
    # get_datetime = udf(
    #     lambda x: to_date(x)
    # )

    df = df.withColumn(
        "datetime",
        to_date("start_time").cast(
            types.TimestampType()
        )
    )

    # extract columns to create time table
    df = df.withColumn(
        "hour",
        hour("start_time")
    ).withColumn(
        "day",
        dayofmonth("start_time")
    ).withColumn(
        "month",
        month("start_time")
    ).withColumn(
        "year",
        year("start_time")
    ).withColumn(
        "week",
        weekofyear("start_time")
    ).withColumn(
        "weekday",
        dayofweek("start_time")
    )

    time_table = df[
        "start_time",
        "hour",
        "day",
        "week",
        "month",
        "year",
        "weekday"
    ].distinct()

    # write time table to parquet files partitioned by year and month
    if ALLOW_WRITE:
        time_table.write.partitionBy("year", "month").parquet(
            f"{output_data}time/time_table.parquet"
        )

    # read in song data to use for songplays table
    song_df = read_song_data(
        spark,
        input_data
    )

    # extract columns from joined song and log datasets to create
    songplays_table = df.join(
        song_df,
        song_df.artist_name == df.artist,
        "inner"
    )[
        "start_time",
        "userId",
        "level",
        "sessionId",
        "location",
        "userAgent",
        "song_id",
        "artist_id"
    ].distinct().withColumn(
        "songplay_id",
        monotonically_increasing_id()
    )

    # write songplays table to parquet files partitioned by year and month
    if ALLOW_WRITE:
        songplays_table.write.partitionBy(
            "year",
            "month"
        ).parquet(
            f"{output_data}songplays/songplays_table.parquet"
        )


def main():
    """
    Function that runs the complete Spark job
    """

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-logs-368346057517-us-west-2/elasticmapreduce/"

    process_song_data(
        spark,
        input_data,
        output_data
    )

    process_log_data(
        spark,
        input_data,
        output_data
    )


if __name__ == "__main__":
    main()

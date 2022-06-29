""" This module provides helper functions for ETL """
import os
from pyspark.sql import SparkSession, DataFrame
import requests
import pyspark.sql.functions as F
from pyspark.sql.functions import when

LOCAL_DIR_RAWDATA = "../data/raw/"


def get_dataframe() -> DataFrame:
    ''' Provides the Pyspark Dataframe data structure ready to use '''
    if get_dataframe.cachedFrame is not None:
        return get_dataframe.cachedFrame
    provide_rawcsv()
    new_dataframe = make_dataframe_from_rawcsv()
    transformed_dataframe = transform_dataframe(new_dataframe)
    get_dataframe.cachedFrame = transformed_dataframe
    return transformed_dataframe

get_dataframe.cachedFrame = None

def provide_rawcsv():
    ''' Makes sure we have the *.csv dataset we need '''
    print('providing file...')
    if os.path.exists(LOCAL_DIR_RAWDATA+'place_tiles.csv') is False:
        print('not found. need to download '+LOCAL_DIR_RAWDATA+'place_tiles.csv ...')
        download_dataset_fromsource()
    else:
        print(LOCAL_DIR_RAWDATA+'place_tiles.csv is already in data/raw')

def transform_dataframe(df_input: DataFrame) -> DataFrame:
    ''' Transforms a dataframe from its source structure into something more usable '''
    df_transform = transform_dataframe_timestamp(df_input)
    df_transform = transform_dataframe_normalize_seconds(df_transform)
    df_transform = transform_dataframe_colums(df_transform)
    return df_transform

def make_dataframe_from_rawcsv() -> DataFrame:
    ''' takes all csv files from the raw data dir and loads it into a dataFrame '''
    raw_csvs = LOCAL_DIR_RAWDATA+'*.csv'

    spark = SparkSession.builder.appName('placegroups').getOrCreate()
    spark.sparkContext.setCheckpointDir('../data/interim/checkpoints')
    data_frame = spark.read.option('header', True).csv(raw_csvs)
    return data_frame

def transform_dataframe_timestamp(df_input: DataFrame) -> DataFrame:
    ''' Transforms timestamp \'yyyy-mm-dd HH:MM:ss.SSSS UTC\' into Unix Epoch seconds '''
    df_output = df_input.withColumn(
        "ts", F.unix_timestamp(df_input.ts.substr(0, 19)))
    return df_output

def transform_dataframe_normalize_seconds(df_input: DataFrame) -> DataFrame:
    ''' normalizes the timestamp column so it starts with 0 seconds
    ONLY USE THIS AFTER TIMESTAMP FORMAT WAS TRANSFORMED '''
    mints = 0
    if(df_input.select('ts').rdd.isEmpty is False):
        mints = df_input.select('ts').rdd.min()[0]
    df_output = df_input.withColumn(
    'ts', (df_input['ts'] - mints))
    return df_output

def transform_dataframe_colums(df_input: DataFrame) -> DataFrame:
    ''' Transforms columns from [\'ts\',\'user_hash\',\'x_coordinate\',\'y_coordinate,\'color\']
    into [\'user_id\',\'x\',\'y\',\'t\',\'pixel_color\']'''

    df_output = df_input.select(F.col('user_hash').alias('user_id'),
                                F.col('x_coordinate').alias('x'),
                                F.col('y_coordinate').alias('y'),
                                F.col('ts').alias('t'),
                                F.col('color').alias('pixel_color')
                                )
    #das geht bestimmt auch einfacher, ich konnte es bis jetzt aber auch noch nicht richtig testen  
    #teilweise kommen auch noch Fehlermeldungen zu den Farben auf

    df = df_output.withColumn("pixel_color", when(df_output.pixel_color == 0, "#FFFFFF")
      .when(df_output.pixel_color == 1, "#E4E4E4")
      .when(df_output.pixel_color == 2, "#888888")
      .when(df_output.pixel_color == 3, "#222222")
      .when(df_output.pixel_color == 4, "#FFA7D1")
      .when(df_output.pixel_color == 5, "#E50000")
      .when(df_output.pixel_color == 6, "#E5D900")
      .when(df_output.pixel_color == 7, "#A06A42")
      .when(df_output.pixel_color == 8, "#E5D900")
      .when(df_output.pixel_color == 9, "#94E044")
      .when(df_output.pixel_color == 10, "#02BE01")
      .when(df_output.pixel_color == 11, "#00E5F0")
      .when(df_output.pixel_color == 12, "#0083C7")
      .when(df_output.pixel_color == 13, "#0000EA")
      .when(df_output.pixel_color == 14, "#E04AFF")
      .when(df_output.pixel_color == 15, "#820080"))

    return df

def download_dataset_fromsource():
    ''' Downloading the reddit place dataset from source '''
    url = 'https://storage.googleapis.com/place_data_share/place_tiles.csv'
    localfilename = LOCAL_DIR_RAWDATA+'place_tiles.csv'
    download_file(url,localfilename)


def download_file(url: str, localfilepath: str):
    ''' Helper to download a single file'''
    print('downloading from '+url + ' to '+localfilepath)
    # stream = True , iter_content() - Daten werden immer nur Stückweise
    # runtergeladen bis das Stück weiterverarbeitet wurde
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(localfilepath, "wb") as file_handle:
            for chunk in response.iter_content(chunk_size=1024):
                file_handle.write(chunk)
    file_handle.close()        
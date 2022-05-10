""" This module provides helper functions for ETL """
import gzip
import shutil
import os
from pyspark.sql import SparkSession, DataFrame
import requests
import numpy as np
import pyspark.sql.functions as F

LOCAL_DIR_RAWDATA = "../data/raw/"


def get_dataframe(sample=False) -> DataFrame:
    ''' Provides the Pyspark Dataframe data structure ready to use '''
    if 'TEST_ENVIRONMENT' in os.environ:
        sample = True
    provide_rawcsv(sample)
    new_dataframe = make_dataframe_from_rawcsv()
    transformed_dataframe = transform_dataframe(new_dataframe)
    return transformed_dataframe


def provide_rawcsv(sample=False):
    ''' Makes sure we have the *.csv dataset we need '''
    start = 0
    finish = 78
    if sample:
        start = 50
        finish = 50
    for i in np.arange(start, finish):
        file_prefix = '2022_place_canvas_history-0000000000'+i
        if os.path.exists(LOCAL_DIR_RAWDATA+file_prefix+'.csv') is False:
            provide_and_unpack_gzip(file_prefix)


def provide_and_unpack_gzip(file_prefix: str):
    ''' Provides a previously missing .csv file. '''
    if os.path.exists(LOCAL_DIR_RAWDATA+file_prefix+'.csv.gzip') is False:
        download_dataset_fromsource(file_prefix)
    unpack_gzip(file_prefix)


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
    data_frame = spark.read.option('header', True).csv(raw_csvs)
    return data_frame


def transform_dataframe_timestamp(df_input: DataFrame) -> DataFrame:
    ''' Transforms timestap \'yyyy-mm-dd HH:MM:ss.SSSS UTC\' into Unix Epoch seconds '''
    df_output = df_input.withColumn(
        "timestamp", F.unix_timestamp(df_input.timestamp.substr(0, 19)))
    return df_output


def transform_dataframe_normalize_seconds(df_input: DataFrame) -> DataFrame:
    ''' normalizes the timestamp column so it starts with 0 seconds
    ONLY USE THIS AFTER TIMESTAMP FORMAT WAS TRANSFORMED '''
    mints = df_input.select('timestamp').rdd.min()[0]
    df_output = df_input.withColumn(
        'timestamp', (df_input['timestamp'] - mints))
    return df_output


def transform_dataframe_colums(df_input: DataFrame) -> DataFrame:
    ''' Transforms columns from [\'timestamp\',\'user_id\',\'pixel_color\',\'coordinate\']
    into [\'user_id\',\'x\',\'y\',\'t\',\'pixel_color\']'''
    df_output = df_input.select('user_id',
                                F.split('coordinate', ',').getItem(
                                    0).cast('int').alias('x'),
                                F.split('coordinate', ',').getItem(
                                    1).cast('int').alias('y'),
                                F.col('timestamp').alias('t'),
                                'pixel_color')
    return df_output


def unpack_gzip(file_prefix: str):
    ''' dataset is downloaded as gzip. we need to unpack everything '''
    with gzip.open(LOCAL_DIR_RAWDATA+file_prefix+'.csv.gzip', 'rb') as f_in:
        with open(LOCAL_DIR_RAWDATA+file_prefix+'.csv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(LOCAL_DIR_RAWDATA+file_prefix+'.csv.gzip')


def download_dataset_fromsource(file_prefix: str):
    ''' Downloading the reddit place dataset from source '''
    # stream = True , iter_content() - Daten werden immer nur Stückweise
    # runtergeladen bis das Stück weiterverarbeitet wurde
    with requests.get('https://placedata.reddit.com/data/canvas-history/'
                      + file_prefix+'.csv.gzip', stream=True) as response:
        response.raise_for_status()
        with open(LOCAL_DIR_RAWDATA+file_prefix+'.csv.gzip', "wb") as file_handle:
            for chunk in response.iter_content(chunk_size=1024):
                file_handle.write(chunk)
            file_handle.close()

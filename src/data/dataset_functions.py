""" This module provides helper functions for ETL """
import gzip
import shutil
import os
from pyspark.sql import SparkSession, DataFrame
import requests
import numpy as np
import pyspark.sql.functions as F

LOCAL_DIR_RAWDATA = "../data/raw/"

get_dataframe.cachedFrame = None


def get_dataframe(sample=False) -> DataFrame:
    ''' Provides the Pyspark Dataframe data structure ready to use '''
    if get_dataframe.cachedFrame is not None:
        return cachedFrame
    if 'TEST_ENVIRONMENT' in os.environ:
        sample = True
    provide_rawcsv(sample)
    new_dataframe = make_dataframe_from_rawcsv()
    transformed_dataframe = transform_dataframe(new_dataframe)
    get_dataframe.cachedFrame = transformed_dataframe
    return transformed_dataframe


def provide_rawcsv(sample=False):
    ''' Makes sure we have the *.csv dataset we need '''
    start = 0
    finish = 78
    if sample:
        start = 50
        finish = 51
    for i in np.arange(start, finish):
        print('providing '+filename_csv(i)+' ...')
        if os.path.exists(filename_csv(i)) is False:
            print('not found. need to download '+filename_gzip(i)+' ...')
            provide_and_unpack_gzip(i)
        else:
            print(filename_csv(i)+' is already in data/raw')


def provide_and_unpack_gzip(file_nr: int):
    ''' Provides a previously missing .csv file. '''
    gz_name = filename_gzip(file_nr)
    if os.path.exists(gz_name) is False:
        download_dataset_fromsource(file_nr)
    unpack_gzip(file_nr)


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


def unpack_gzip(file_nr: int):
    ''' dataset is downloaded as gzip. we need to unpack everything '''
    csv_name = filename_csv(file_nr)
    gz_name = filename_gzip(file_nr)
    print('unpacking '+gz_name+' into '+csv_name)
    with gzip.open(gz_name, 'rb') as f_in:
        with open(csv_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print('deleting '+gz_name)
    os.remove(gz_name)


def download_dataset_fromsource(file_nr: int):
    ''' Downloading the reddit place dataset from source '''
    filename = filename_gzip_nopath(file_nr)
    url = 'https://placedata.reddit.com/data/canvas-history/'+filename
    localfilename = filename_gzip(file_nr)
    download_file(url, localfilename)


def filename_csv(file_nr: int) -> str:
    ''' Helper to build correct csv filename with local path'''
    return LOCAL_DIR_RAWDATA+filename_csv_nopath(file_nr)


def filename_gzip(file_nr: int) -> str:
    ''' Helper to build correct gzip filename with local path'''
    return filename_csv(file_nr) + '.gzip'


def filename_csv_nopath(file_nr: int) -> str:
    ''' Helper to build correct csv filename'''
    return reddit_filename_no_type(file_nr)+'.csv'


def filename_gzip_nopath(file_nr: int) -> str:
    ''' Helper to build correct gzip filename'''
    return filename_csv_nopath(file_nr)+'.gzip'


def reddit_filename_no_type(file_nr: int) -> str:
    ''' Helper to build correct reddit filename'''
    prefix = '2022_place_canvas_history-0000000000'
    # Normierung: geht von 0 - 78
    if file_nr < 0:
        file_nr = 0
    if file_nr > 78:
        file_nr = 78
    if file_nr < 10:
        prefix = prefix+'0'
    return prefix + str(file_nr)


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

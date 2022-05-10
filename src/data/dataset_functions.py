""" This module provides helper functions for ETL """
# pyspark hat nach der Installation Probleme
# und wird nicht automatisch von Python zum importieren gefunden
# dafür gibt es dann extra das findspark tool
import gzip
import shutil
import os
from pyspark.sql import SparkSession
import requests
import numpy as np

LOCAL_DIR_RAWDATA = "../data/raw/"


def make_dataframe_from_rawcsv():
    ''' takes all csv files from the raw data dir and loads it into a dataFrame '''
    raw_csvs = LOCAL_DIR_RAWDATA+'*.csv'

    spark = SparkSession.builder.appName('placegroups').getOrCreate()
    data_frame = spark.read.option('header', True).csv(raw_csvs)
    return data_frame


def unpack_rawdata():
    ''' dataset is downloaded as gzip. we need to unpack everything '''
    start = 0
    finish = 78
    for i in np.arange(start, finish):
        filename = '2022_place_canvas_history-0000000000'+i+'.csv'
        with gzip.open(LOCAL_DIR_RAWDATA+filename+'.gzip', 'rb') as f_in:
            with open(LOCAL_DIR_RAWDATA+filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(LOCAL_DIR_RAWDATA+filename+'.gzip')


def download_dataset_fromsource(small=False):
    ''' Downloading the reddit place dataset from source '''
    # stream = True , iter_content() - Daten werden immer nur Stückweise
    # runtergeladen bis das Stück weiterverarbeitet wurde
    start = 0
    finish = 78
    if(small):
        start = 50
        finish = 50
    for i in np.arange(start, finish):
        filename = '2022_place_canvas_history-0000000000'+i+'.csv.gzip'
        with requests.get('https://placedata.reddit.com/data/canvas-history/'
                          + filename, stream=True) as response:
            response.raise_for_status()
            with open(LOCAL_DIR_RAWDATA+filename, "wb") as file_handle:
                for chunk in response.iter_content(chunk_size=1024):
                    file_handle.write(chunk)
                file_handle.close()

""" This module provides helper functions for feature calculation """
import pyspark.sql.functions as F
from pyspark.sql import DataFrame,Window,SparkSession
import itertools


def group_dataframe_by_columns(df_input: DataFrame, columns_to_group: list, add_count=True) -> DataFrame:
    ''' groups a dataframe by a list of columns. all other columns get aggregatet as a list ov their values. adds a count as first aggregated column '''
    agg_columns = [
        colname for colname in df_input.columns if colname not in columns_to_group]
    exprs = [F.collect_list(colName) for colName in agg_columns]
    if add_count:
        exprs.insert(0, F.count(columns_to_group[0]).alias('count'))
    df_grouped = df_input.groupby(columns_to_group).agg(
        *exprs).orderBy(F.col('count').desc())
    return df_grouped


def group_dataframe_by_user(df_input):
    ''' shorthand helper func'''
    return group_dataframe_by_columns(df_input, ['user_id'])


def group_dataframe_by_pixel(df_input):
    '''shorthand helper func'''
    return group_dataframe_by_columns(df_input, ['x','y'])

def group_dataframe_by_color(df_input):
    '''shorthand helper func'''
    return group_dataframe_by_columns(df_input, ['pixel_color'])

def group_dataframe_by_time(df_input):
    '''shorthand helper func'''
    return group_dataframe_by_columns(df_input, ['t'])

def group_dataframe_by_time_and_user(df_input):
    '''shorthand helper func'''
    return group_dataframe_by_columns(df_input, ['user_id', 't'])

def get_latestpixels_from_box(dataFrame,x1,y1,x2,y2,t):
    # generiere relevante Pixelkoordinaten in einem neuen Frame
    x_list = list(range(x1,x2+1))
    y_list = list(range(y1,y2+1))

    generated_data = list(itertools.product(x_list,y_list))

    spark = SparkSession.builder.appName('placegroups').getOrCreate()

    box_schema = ["x","y"]
    generated_frame = spark.createDataFrame(data = generated_data, schema = box_schema)

    # join mit dem Hauptdatensatz -> Frame mit Datensatz für die Pixel in der Box
    relevant_data = generated_frame.alias("gf").join(dataFrame.alias("df"), F.col("gf.x") == F.col("df.x"))
    relevant_data = relevant_data.where(F.col("gf.y") == F.col("df.y"))
    relevant_data = relevant_data.where(F.col("df.t") < tz)

    # finde den neuesten Wert pro Pixel und lösche alle anderen x,y Duplikate
    window = Window.partitionBy("gf.x","gf.y").orderBy(F.col("t").desc())
    sorted_data = relevant_data.withColumn('steps',F.row_number().over(window))
    dropped_data = sorted_data.where(F.col('steps') == 1).orderBy('gf.x','gf.y')

    return dropped_data
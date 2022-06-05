""" This module provides helper functions for feature calculation """
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


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

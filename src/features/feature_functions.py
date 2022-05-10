""" This module provides helper functions for feature calculation """
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def group_dataframe_by_columns(df_input: DataFrame, columns_to_group: list) -> DataFrame:
    ''' groups a dataframe by a list of columns. all other columns get aggregatet as a list ov their values '''
    agg_columns = [
        colname for colname in df_input.columns if colname not in columns_to_group]
    exprs = [F.collect_list(colName) for colName in agg_columns]
    exprs.insert(0, F.count(columns_to_group[0]).alias('count'))
    df_grouped = df_input.groupby(columns_to_group).agg(*exprs)
    return df_grouped

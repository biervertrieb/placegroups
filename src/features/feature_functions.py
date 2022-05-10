""" This module provides helper functions for feature calculation """
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def group_dataframe_by_user(df_input: DataFrame) -> DataFrame:
    ''' takes a dataframe with sourcedata and groups the pixeldata by user '''
    agg_list = []
    for columnname in df_input.columns:
        if columnname == 'user_id':
            agg_list.append(F.count('*').alias('count'))
        else:
            agg_list.append(F.collect_list(columnname))
    df_output = df_input.groupBy('user_id').agg(
        agg_list).orderBy(F.count('x').desc())
    return df_output

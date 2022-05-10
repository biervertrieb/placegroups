""" This module provides helper functions for feature calculation """
from datetime import datetime
import time


def stamp2time(stamp):
    ''' Helperfunction to calculate EPOCH seconds from timestamp in dataset'''
    dtime_obj = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S.%f UTC")
    return int(time.mktime(dtime_obj.timetuple()))

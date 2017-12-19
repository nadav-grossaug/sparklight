#! /usr/bin/env python
# -*- coding: UTF-8 -*-
from __future__ import print_function
import sys
import argparse
import os
from operator import add 
from collections import defaultdict
import arrow
import time
import numpy as np
from six.moves import xrange
import ciso8601 # pip install ciso8601
import datetime
import types
"""

    Timeliner is a helper class that converts a timeline rdd to a numpy matrix(es) 
    Example usage:

    # ### Read data:
    full_rdd = sc.textFile(INPUT)\
            .map(parse_line)

    # ### One hot encoding of "location":
    one_hot_list = ( full_rdd
            .map(lambda x:x["location"])
            .distinct()
            .collect()
            )
    ohl=dict(zip(one_hot_list, range(len(one_hot_list))))

    # ### Generate a time-aggregated matrix of access to "location" for each "user"  for day (24-hours):

    timeliner  = (Timeliner(timewindow_minutes = 60*24) 
        .rdd(full_rdd) 
        .cache()
        .date_func( lambda x: x["date_t"] )
        .hour_func( lambda x: x["time_t"] )
        .key_func( lambda x:  x["username"] )
        .vectorizer( 
            lambda x: x["location"],
            lambda value: to_one_hot(ohl, value, count=1) 
        )
    )

    # ### is to_bool=True, matrix elements are either 0 or 1, indicating access
    # Returns a list of (user, numpy-matrix, time-labels)
    all_users = list(timeliner.to_groupby_key_matrix(to_bool=True))
    all_users[0] = (
        'John smith', 
        array([[ 0.,  0.,  0., ...,  0.,  0.,  0.],
            [ 0.,  0.,  0., ...,  0.,  0.,  0.],
            [ 0.,  0.,  0., ...,  0.,  0.,  0.],
            ..., 
            [ 0.,  0.,  0., ...,  0.,  0.,  0.],
            [ 0.,  0.,  0., ...,  0.,  0.,  0.],
            [ 0.,  0.,  0., ...,  0.,  0.,  0.]]), 
     ['2017-03-19 00:00:00',
      '2017-03-20 00:00:00',
      '2017-03-21 00:00:00',
        ... ]
    )
"""

DELTA_WINDOW=60

def to_time_quant_old(date, hour, timewindow_mins):
    """ return arrow time object after quantization by timewindow_mins.
    Example usage:
    to_time_quant("2017-04-30", "13:23:02.23",timewindow_mins=0.5 ) 
    # => <Arrow [2017-04-30T13:23:00.230000+00:00]>
    """
    dt = arrow.get(date+" "+hour)
    minutes_since_midnight =  timewindow_mins * \
                    ((60*dt.hour+ dt.minute )// timewindow_mins)
    new_date = dt \
                .replace(hour=0, minute=0, second=0) \
                .replace(minutes=int(minutes_since_midnight))
    return new_date.format("YYYY-MM-DD HH:mm:ss")

def to_time_quant(date, hour, timewindow_mins):
    """ return arrow time object after quantization by timewindow_mins.
    Example usage:
    to_time_quant("2017-04-30", "13:23:02.23",timewindow_mins=0.5 ) 
    # => <Arrow [2017-04-30T13:23:00.230000+00:00]>
    """
    # dt = arrow.get(date+" "+hour)
    dt = ciso8601.parse_datetime(date+"T"+hour)
    minutes_since_midnight =  timewindow_mins * \
                    ((60*dt.hour+ dt.minute )// timewindow_mins)
    new_date = datetime.datetime(dt.year, dt.month, dt.day) + \
            datetime.timedelta(minutes=minutes_since_midnight)
    return "%04d-%02d-%02d %02d:%02d:%02d" % (new_date.year, new_date.month, new_date.day,
            new_date.hour, new_date.minute, new_date.second)
def to_one_hot(ohl, name, count=1):
    vec = np.zeros(len(ohl), dtype=float)
    if name in ohl:
        index = ohl[name]
        vec[index]+=count
    return vec

def matrix_plus( mat, column, vec):
    mat[column,:]+=vec

def vectorize_with_weights(groupby_tuple, vectorizer_func):
    key, arr =  groupby_tuple
    return np.sum([ vectorizer_func(value)*count for key,value,count in arr], axis=0)

class Timeliner(object):
    def __init__(self,timewindow_minutes=DELTA_WINDOW):
        self.timewindow_mins(timewindow_minutes)
        self.date_func_=lambda x:x[0]
        self.hour_func_=lambda x:x[1]
        self.key_func_=lambda x:None
        self.reducer_func_=lambda x:1
        self.cache_=False
        self.reduced_rdd_ = None
        self.partition_counts_ = 1
    def timewindow_mins(self, minutes):
        self.timewindow_mins_ = minutes
        return self
    def cache(self, is_cache=True):
        self.cache_=is_cache
        return self
    def rdd(self, rdd):
        self.rdd_ = rdd
        return self
    def date_func(self, func):
        self.date_func_ = func
        return self
    def hour_func(self, func):
        self.hour_func_ = func
        return self
    def vectorizer(self, value_func, vectorizer_func):
        self.value_func_ = value_func
        self.vectorizer_func_=vectorizer_func
        return self
    def key_func(self, func):
        self.key_func_=func
        return self
    def reducer_func(self, func):
        self.reducer_func_=func
        return self
    def to_reduced_rdd(self):
        if self.reduced_rdd_:
            return self.reduced_rdd_
        timewindow_mins  = self.timewindow_mins_
        date_f =self.date_func_
        hour_f= self.hour_func_
        value_f = self.value_func_
        key_f = self.key_func_
        partition_counts = self.partition_counts_
        reducer_f = self.reducer_func_
        if date_f==None or hour_f==None or value_f==None or key_f==None:
            sys.stderr.write("ERROR: some of the date,hour, vectorizer_func or key_func are not set. Exiting. \n")
        self.reduced_rdd_ =( self.rdd_
            .map(
                lambda x:( 
                    (   key_f(x), 
                        to_time_quant(date_f(x),hour_f(x), timewindow_mins),
                        value_f(x) ),
                    reducer_f(x)
                )
            )
            .reduceByKey(add) 
            .map(lambda x:  ( (x[0][0], x[0][1]), x[0][2], x[1]  ))
            .repartition(partition_counts)
        )
        """ # reduced_rdd_.first() => (('general', '2017-01-12 15:00:00'),
 'example-1', 18) """
        if self.cache_:
            self.reduced_rdd_=self.reduced_rdd_.cache()
        return self.reduced_rdd_
    def to_vector_rdd(self):
        partition_counts = self.partition_counts_

        rdd = self.to_reduced_rdd()
        vectorizer_f = self.vectorizer_func_ 
        vec_rdd  = (rdd
            .groupBy(lambda x:x[0])
            .repartition(partition_counts)
            .map(lambda x: (x[0], vectorize_with_weights(x,vectorizer_f)))
        )
        return vec_rdd
    def to_matrix(self,rdd=None, to_bool=False, start_date=None,end_date=None ):
        if rdd==None:
            rdd =self.to_vector_rdd()
        is_rdd = not isinstance(rdd, (list, tuple, types.GeneratorType))

        date_func = lambda x:x[0][1].split(" ")[0]
        vec_func = lambda x: x[1]
        
        # Generate time mapping:
        
        if start_date==None:
            if is_rdd:
                start_date = rdd.map(date_func).min()
            else:
                start_date = min(map(date_func, rdd))
        if end_date==None:
            if is_rdd:
                end_date = rdd.map(date_func).max()
            else:
                end_date = max(map(date_func, rdd))

        mapping = {}
        date = arrow.get(start_date)
        end_date_t =  arrow.get(end_date).replace(days=1) # last day + 1
        slot = 0
        while(date<end_date_t):
            mapping[date.format("YYYY-MM-DD HH:mm:ss")]= slot
            slot+=1
            date=date.replace(minutes=self.timewindow_mins_)
        # generate matrix:
        if is_rdd:
            n = rdd.map(vec_func).first().shape[0]
        else:
            n = vec_func(rdd[0]).shape[0]
        mat = np.zeros((len(mapping), n), dtype=float)
        if is_rdd:
            for x in rdd.toLocalIterator():
                matrix_plus(mat, column=mapping[x[0][1]], vec=x[1])
        else:
            for x in rdd:
                matrix_plus(mat, column=mapping[x[0][1]], vec=x[1])
        times = sorted(mapping.keys())
        if to_bool:
            mat=1.0*(mat>0)
        self.matrix_ = mat
        self.time_lables_ = times
        return mat,times
    def to_groupby_key_matrix(self,to_bool=False):
        rdd = self.to_vector_rdd()
        date_func = lambda x:x[0][1].split(" ")[0]
        start_date = rdd.map(date_func).min()
        end_date = rdd.map(date_func).max()
        for key, groupby_elem in rdd.groupBy(lambda x:x[0][0]).toLocalIterator():
            mat_times = self.to_matrix( groupby_elem, to_bool, start_date, end_date)
            yield key, mat_times[0], mat_times[1]
#     def to_groupby_key_matrix(self,to_bool=False):
#         rdd = self.to_vector_rdd()
#         date_func = lambda x:x[0][1].split(" ")[0]
#         start_date = rdd.map(date_func).min()
#         end_date = rdd.map(date_func).max()
#         results_rdd = rdd.groupBy(lambda x:x[0][0]) \
#             .map(lambda x: (x[0],
#                     self.to_matrix(list(x[1]), to_bool, start_date, end_date)
#                            )   )
#         for key, mat_times in results_rdd.toLocalIterator():
#             yield key, mat_times[0], mat_times[1]

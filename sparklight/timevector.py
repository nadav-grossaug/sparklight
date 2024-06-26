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

EPOCH=1970
DELTA_WINDOW=60


class TimeVector:
    def __init__(self, start_date, end_date, start_hour="00:00:00", end_hour="23:59:00", timewindow_mins=DELTA_WINDOW):
        self.start_date = start_date
        self.end_date = end_date
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.delta = timewindow_mins
        self.deltas_per_day = 24.0*60/self.delta
        self.start_index = self.to_timeindex(self.start_date, self.start_hour, False)
        self.len=self.to_timeindex(self.end_date, self.end_hour)+1
        self.vec = np.zeros(self.len)
    def vector(self):
        return self.vec
    def inc(self, date, hour, increment=1):
        if increment!=0:
            index = self.to_timeindex(date, hour)
            self.vec[index]+=increment
    def to_timeindex(self, date, hour, relative_to_start_time=True):
        hour = hour.split(".")[0] # omit_milliseconds
        #     timestamp = time.strptime(hour,"%H:%M:%S") # <= '23:58:21.574703'
        timestamp = time.strptime(date+" "+hour,"%Y-%m-%d %H:%M:%S")
        hour_index = (timestamp.tm_hour*60.0+timestamp.tm_min)/self.delta
        date_index = ((timestamp.tm_year-EPOCH)*365+timestamp.tm_yday) * self.deltas_per_day
        index= int(date_index + hour_index)
        if relative_to_start_time:
            index -= self.start_index
        return index
    def from_tuples(self, tuples, value_func=None):
        for date, hour, value in tuples:
            if value_func:
                value = value_func(value)
            self.inc(date, hour, value)
        return self.vec
    @staticmethod 
    def to_extent(rdd, date_func):
        min_date = rdd.map(date_func).min()
        max_date = rdd.map(date_func).max()
        return (min_date,max_date)
    @staticmethod 
    def to_v(tuples, value_func=None, start_date=None, end_date=None,timewindow_mins=DELTA_WINDOW):
        if start_date==None:
            start_date=min([date for date,hour,value in tuples])
        if end_date==None:
            end_date=max([date for date,hour,value in tuples])
        v = TimeVector( start_date, end_date, timewindow_mins=timewindow_mins )
        return v.from_tuples(tuples, value_func)

def timevector_to_v(tuples, value_func=None, start_date=None, end_date=None,timewindow_mins=DELTA_WINDOW):
    if start_date==None:
        start_date=min([date for date,hour,value in tuples])
    if end_date==None:
        end_date=max([date for date,hour,value in tuples])
    v = TimeVector( start_date, end_date, timewindow_mins=timewindow_mins )
    return v.from_tuples(tuples, value_func)
    
    
    
class HashTimeVector:
    def __init__(self, start_date=None, end_date=None, start_hour="00:00:00", end_hour="23:59:00", timewindow_mins=DELTA_WINDOW):
        self.start_date = start_date
        self.end_date = end_date
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.delta = timewindow_mins
        self.deltas_per_day = 24.0*60/self.delta
        self.hash = defaultdict(int)
    def vector(self):
        if self.start_date is None and self.end_date is None:
            self.start_date, self.end_date = self.to_extent()
        tv = TimeVector(self.start_date, self.end_date, timewindow_mins=self.delta)
        tv.from_tuples([ (x[0][0], x[0][1], x[1]) for x in six.iteritems(self.hash)  ])
        return tv.vector()
    def to_axis(self, as_int=False):
        vec=self.vector()
        axis=[]
        min_date, ma_date = self.to_extent()
        date = arrow.get(min_date, 'YYYY-MM-DD')
        for i in xrange(vec.shape[0]):
            if as_int:
                axis.append(date.timestamp)
            else:
                axis.append(date.format('YYYY-MM-DD HH:mm:ss'))
            date = date.shift(minutes=self.delta)
        return axis,vec
    def to_extent(self):
        min_date = min([x[0] for x in six.iterkeys(self.hash)])
        max_date = max([x[0] for x in six.iterkeys(self.hash)])
        return (min_date,max_date)
    def force_extent(self,start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
    def inc(self, date, hour, increment=1):
        if increment!=0:
            ts = self.to_timequantization(date, hour)
            self.hash[ts]+=increment
    def to_timequantization(self, date, hour):
        hour = hour.split(".")[0] # omit_milliseconds
        ts = arrow.get("%s %s" % (date, hour), 'YYYY-MM-DD HH:mm:ss')
        ts_quantization = ts.replace(second=0).shift(minutes= -1*((ts.hour*60+ts.minute) % self.delta))
        return (ts_quantization.format('YYYY-MM-DD'),ts_quantization.format('HH:mm:ss'))



class TimeMatrix:
    """
    generate a time vectorss, where each data point is mean on vectors
    """
    def __init__(self, start_date, end_date, start_hour="00:00:00", end_hour="23:59:00", timewindow_mins=DELTA_WINDOW, vector_len=None):
        self.start_date = start_date
        self.end_date = end_date
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.delta = timewindow_mins
        self.deltas_per_day = 24.0*60/self.delta
        self.start_index = self.to_timeindex(self.start_date, self.start_hour, False)
        self.len=self.to_timeindex(self.end_date, self.end_hour)+1
        self.sums = np.zeros([self.len, vector_len])
        self.n = np.ones(self.len)
    def vector(self):
        return (self.sums.T/self.n).T
        # return self.sums
    def inc(self, date, hour, vec):
        index = self.to_timeindex(date, hour)
        self.sums[index]= self.sums[index] + vec
        self.n[index]+=1
    def to_timeindex(self, date, hour, relative_to_start_time=True):
        hour = hour.split(".")[0] # omit_milliseconds
        #     timestamp = time.strptime(hour,"%H:%M:%S") # <= '23:58:21.574703'
        timestamp = time.strptime(date+" "+hour,"%Y-%m-%d %H:%M:%S")
        hour_index = (timestamp.tm_hour*60.0+timestamp.tm_min)/self.delta
        date_index = ((timestamp.tm_year-EPOCH)*365+timestamp.tm_yday) * self.deltas_per_day
        index= int(date_index + hour_index)
        if relative_to_start_time:
            index -= self.start_index
        return index
    def from_tuples(self, tuples, map_func=None):
        for date, hour, vec in tuples:
            if map_func:
                vec = map_func(vec)
            self.inc(date, hour, vec)
        return self.vector()
    @staticmethod 
    def to_extent(rdd, date_func):
        min_date = rdd.min(date_func)
        max_date = rdd.max(date_func)
        return (min_date,max_date)
    @staticmethod 
    def to_v(tuples, map_func=None, start_date=None, end_date=None,timewindow_mins=DELTA_WINDOW):
        if start_date==None:
            start_date=min([date for date,hour,vec in tuples])
        if end_date==None:
            end_date=max([date for date,hour,vec in tuples])
        first_vector = tuples[0][2]
        v = TimeMatrix( start_date, end_date, timewindow_mins=timewindow_mins, vector_len=len(first_vector))
        return v.from_tuples(tuples, map_func)
    @staticmethod 
    def to_series(tuples, map_func=None, start_date=None, end_date=None,timewindow_mins=DELTA_WINDOW):
        if start_date==None:
            start_date=min([date for date,hour,vec in tuples])
        if end_date==None:
            end_date=max([date for date,hour,vec in tuples])
        first_vector = tuples[0][2]
        n = len(first_vector)
        v = TimeMatrix( start_date, end_date, timewindow_mins=timewindow_mins, vector_len=n)
        vec = v.from_tuples(tuples, map_func)
        
        start_time=arrow.get(start_date)
        end_time=arrow.get(end_date).shift(days=1)
        m = vec.shape[0]
        delta_time = (end_time-start_time) / m
        date_axis = np.array([
            (start_time + delta_time * timeslot).format('YYYY-MM-DD HH:mm:ss')
            for timeslot in xrange(m)
        ], dtype='datetime64')
        return vec,date_axis
    @staticmethod 
    def to_series_ex(tuples_rdd, map_func=None, start_date=None, end_date=None,timewindow_mins=DELTA_WINDOW):
        if map_func==None:
            map_func=lambda z:z
        if start_date==None:
            start_date=tuples_rdd.map(lambda x:x[0]).min()
        if end_date==None:
            end_date=tuples_rdd.map(lambda x:x[0]).max()
        first_vector = tuples_rdd.first()[2]

        n = len(first_vector)
        v = TimeMatrix( start_date, end_date, timewindow_mins=timewindow_mins, vector_len=n)
        
        tuples_rdd \
            .foreach(lambda x: (x[0], x[1], map_func(x[2]))) #v.inc( x[0], x[1], map_func(x[2]) ) )
        vec = v.vector()
        
        start_time=arrow.get(start_date).replace(hour=0, minute=0, second=0)
        end_time=arrow.get(end_date).shift(days=1).replace(hour=0, minute=0, second=0)
        m = vec.shape[0]
        delta_time = (end_time-start_time) / m
        print(start_time); print(end_time) ;print("delta_time="); print( delta_time)
        date_axis = np.array([
            (start_time + delta_time * timeslot).format('YYYY-MM-DD HH:mm:ss')
            for timeslot in xrange(m)
        ], dtype='datetime64')
        return vec,date_axis
        
    @staticmethod 
    def from_rdd(rdd, date_func, time_func, vec_func, start_date=None, end_date=None,timewindow_mins=DELTA_WINDOW):
        tuples = rdd.map(lambda x:(date_func(x), time_func(x), vec_func(x))).collect()
        return TimeMatrix.to_v(tuples, 
                start_date=start_date, 
                end_date=end_date,
                timewindow_mins=timewindow_mins
         )
    @staticmethod 
    def from_rdd_to_series(rdd, date_func, time_func, vec_func, start_date=None, end_date=None,timewindow_mins=DELTA_WINDOW):
        # tuples = rdd.map(lambda x:(date_func(x), time_func(x), vec_func(x))).collect()
        # return TimeMatrix.to_series(tuples,
        #         start_date=start_date,
        #         end_date=end_date,
        #         timewindow_mins=timewindow_mins
        #  )
        tuples_rdd = rdd.map(lambda x:(date_func(x), time_func(x), vec_func(x)))
        return TimeMatrix.to_series_ex(tuples_rdd, 
                start_date=start_date, 
                end_date=end_date,
                timewindow_mins=timewindow_mins
         )

def timevector_to_v(tuples, value_func=None, start_date=None, end_date=None,timewindow_mins=DELTA_WINDOW):
    if start_date==None:
        start_date=min([date for date,hour,value in tuples])
    if end_date==None:
        end_date=max([date for date,hour,value in tuples])
    v = TimeVector( start_date, end_date, timewindow_mins=timewindow_mins )
    return v.from_tuples(tuples, value_func)
    


SPARKLINES_CHARS= u"▁▂▃▄▅▆▇█"

def numpy_to_sparkline_string(vec):
        max_value=vec.max()
        min_value=vec.min()
        if max_value==min_value:
            return SPARKLINES_CHARS[0]*len(vec)
        sparkline=[SPARKLINES_CHARS[
                int(7.0*(i-min_value)/(max_value-min_value))
            ] for i in vec
        ]
        return "".join(sparkline)

def number_to_sparkline_string(value, max_value, min_value=0, width=10):
    if value is None or value==float('Inf') or value==float('Nan'):
        return " "*width
    if max_value==min_value:
        return SPARKLINES_CHARS[-1]*width
    if value>max_value:
        value = max_value
    if value<min_value:
        value=min_value
    ratio = 1.0*(value-min_value)/(max_value-min_value)
    spaces = int(ratio*width)
    return SPARKLINES_CHARS[-1]*spaces + " "*(width-spaces)

def date_string_to_date_time(time_str, hour_shift=0):
    # time_str='2015-09-07 10:50:14.445'
    if isinstance(time_str, int) or isinstance(time_str, float):
        ts=arrow.get(time_str)
    else:
        ts=arrow.get(time_str, 'YYYY-MM-DD HH:mm:ss')
    ts = ts.shift(hours=hour_shift)
    date = ts.format('YYYY-MM-DD')
    hour = ts.format('HH:mm:ss')
    return date, hour
if __name__ == "__main__":
    pass

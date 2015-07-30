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
import pandas as pd
import vincent # pip install vincent 
# ### Vincent in ipython requires the following:
# %matplotlib inline
# vincent.core.initialize_notebook()

from timevector import DELTA_WINDOW,TimeVector,timevector_to_v,numpy_to_sparkline_string
class LifeLine:
    def __init__(self, rdd, date_func ,time_func, key_func, filter_func=None, timewindow_mins=DELTA_WINDOW):
        self.rdd=rdd
        self.key_func = key_func
        if filter_func:
            self.rdd=rdd.filter(filter_func)
        self.date_func=date_func
        self.time_func=time_func
        self.timewindow_mins=timewindow_mins
    def to_top_k(self, value_func=lambda x:1, top_k=9):
        key_func = self.key_func
        top_k_rdd = self.rdd.map(lambda x: (key_func(x), value_func(x))) \
            .reduceByKey(add) \
            .map(lambda x: (x[1],x[0]) ) \
            .sortByKey(False) \
            .map(lambda x:x[1])
        if top_k is None:
            top_k_list=top_k_rdd.collect()
        else:
            top_k_list=top_k_rdd.take(top_k)
        return top_k_list
    def to_extent(self):
        self.min_date, self.max_date = TimeVector.to_extent(self.rdd, self.date_func)
        return self.min_date, self.max_date
    def to_lifeline(self,  value_func=lambda x:1, top_k=9, extent=None):
        top_k_list = set(self.to_top_k(value_func=value_func, top_k=top_k))
        if extent is None:
            min_date, max_date = self.to_extent()
        else:
            min_date, max_date = extent
        key_func = self.key_func
        date_func = self.date_func
        time_func = self.time_func
        timewindow_mins = self.timewindow_mins
        timeline_rdd  = self.rdd \
            .map(lambda x: (key_func(x) if key_func(x) in top_k_list else "Other", (date_func(x), time_func(x), 1))) \
            .groupByKey() \
            .map(lambda x: (x[0],TimeVector(min_date, max_date, timewindow_mins=timewindow_mins).from_tuples(x[1], lambda y:int(y>0) ))) \
            .cache()
        return timeline_rdd
    def plot(self,  value_func=lambda x:1, top_k=9, normalize_scale=True):
        lifeline_rdd = self.to_lifeline(value_func=value_func, top_k=top_k)
        data = lifeline_rdd.collect()
        if normalize_scale:
            vmax = (0,max([ np.max(d[1]) for d in data]))
        else:
            vmax=None
        for d in data:
            pd.DataFrame(data={d[0]:d[1]}).plot(
                figsize=[14,0.8], 
                kind="area",
                ylim=vmax
            )
        return lifeline_rdd
    
    
    def plot_stacked(self,  value_func=lambda x:1, top_k=9, title="Breakdown by group", use_bar_graph=False):
        lifeline_rdd = self.to_lifeline(value_func=value_func, top_k=top_k)
        # vlen=lifeline_rdd.first()[1].shape[0]
        # d = pd.DataFrame(dict(lifeline_rdd.collect()), index=np.arange(vlen)*24.0/timewindow_mins)
        d = pd.DataFrame( dict(lifeline_rdd.collect()) )

        if use_bar_graph=="group":
            stacked = vincent.GroupedBar(d)
        elif use_bar_graph:
            stacked = vincent.StackedBar(d)
        else:
            stacked = vincent.StackedArea(d)
        stacked.axis_titles(x=title, y="")
        stacked.legend(title="T")
        stacked.height=150
        stacked.width=800
        stacked.colors(brew='Spectral')
        stacked.display()
            
if __name__ == "__main__":
    from sparklight import SparklightContext,SparklightRdd
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--input", type=str, dest='input', nargs='+', default=None,help="Input files")
    parser.add_argument("-c","--client", type=str, dest='client', default=None,help="client IP")
    start_options = parser.parse_args()
    
    sc=SparklightContext()
    
    FIELD_CLIENT=1
    FIELD_TIME=0
    FIELD_USERNAME=17
    FIELD_HOST=5
    
    # If no --client <ip> givem pick first client:
    if start_options.client==None:
        start_options.client=sc.textFile(start_options.input) \
            .map(lambda line: line.split("\t")) \
            .map(lambda parts: parts[FIELD_CLIENT]) \
            .first()
        print(start_options.client)
    
    
    rdd=sc.textFile(start_options.input) \
            .repartition(100) \
            .map(lambda line: line.split("\t"))
            
    lifeline = LifeLine(rdd, 
                date_func=lambda x: x[FIELD_TIME].split(" ")[0],
                time_func=lambda x: x[FIELD_TIME].split(" ")[1],
                key_func = lambda x: x[FIELD_HOST], 
                filter_func=lambda x: x[FIELD_CLIENT]==start_options.client
    )
    
    lifeline.plot()

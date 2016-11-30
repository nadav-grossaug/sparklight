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

from IPython.display import display
import bearcart # pip install git+git://github.com/wrobstory/bearcart.git

# #### Important: the y-axis width is too big:
# $ vi /Users/ami/anaconda/lib/python2.7/site-packages/bearcart/templates/y_axis.js
# ### Add the following line
# var y_axis = new Rickshaw.Graph.Axis.Y( {
#         graph: graph,
#         orientation: 'left',
#         height: {{ height }},
#         width: 40, # <-- Add this line
#         tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
#         element: d3.select("#{{ y_axis_id }}").node()
# } );


# ### Bearcart in ipython requires the following:
# bearcart.bearcart.initialize_notebook()


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
    def to_top_k(self, value_func=lambda x:1, top_k=9, metric_max=True):
        if top_k==0:
            return []
        key_func = self.key_func
        if metric_max:
            top_k_rdd = self.rdd.map(lambda x: (key_func(x), value_func(x))) \
                .reduceByKey(add) \
                .sortBy(lambda x:x[1], ascending=False) \
                .map(lambda x:x[0])
        else: # metric mean
            top_k_rdd = self.rdd.map(lambda x: (key_func(x), value_func(x))) \
                .groupByKey() \
                .map(lambda x: (np.mean(x[1]),x[0]) ) \
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
        if min_date is None or max_date is None:
            return self.rdd.filter(False) # return empty rdd if no data
        key_func = self.key_func
        date_func = self.date_func
        time_func = self.time_func
        timewindow_mins = self.timewindow_mins
        timeline_rdd  = self.rdd \
            .map(lambda x: (key_func(x) if key_func(x) in top_k_list else "Other", (date_func(x), time_func(x), value_func(x)))) \
            .groupByKey() \
            .map(lambda x: (x[0],TimeVector(min_date, max_date, timewindow_mins=timewindow_mins).from_tuples(x[1], lambda x:x))) \
            .cache()
        return timeline_rdd
    def to_numpy_array(self, value_func=lambda x:1, top_k=9, extent=None):
        rdd = self.to_lifeline(value_func, top_k, extent)
        numpy_arr =  np.array(rdd.map(lambda x: x[1]).collect())
        return numpy_arr
    def to_datetime_array(self, value_func=lambda x:1, top_k=9, extent=None):
        if extent is None:
            min_date, max_date = self.to_extent()
        else:
            min_date, max_date = extent
        numpy_matrix=self.to_numpy_array(value_func=value_func, top_k=top_k, extent=extent)
        if numpy_matrix.shape==(0,):
            return None, None
        n = numpy_matrix.shape[1]
        start_time=arrow.get(min_date)
        end_time=arrow.get(max_date).replace(days=1)
        delta_time = (end_time-start_time) / n
        date_axis = [
            (start_time + delta_time * timeslot).format('YYYY-MM-DD HH:mm:ss')
            for timeslot in xrange(n)
        ]
        return np.array(date_axis, dtype='datetime64'), numpy_matrix
    def yield_date_hour(self, value_func=lambda x:1, top_k=9, extent=None):
        if extent is None:
            min_date, max_date = self.to_extent()
        else:
            min_date, max_date = extent
        numpy_matrix=self.to_numpy_array(value_func=value_func, top_k=top_k, extent=extent)
        if numpy_matrix.shape==(0,):
            return
        n = numpy_matrix.shape[1]
        start_time=arrow.get(min_date)
        end_time=arrow.get(max_date).replace(days=1)
        delta_time = (end_time-start_time) / n
        for timeslot in xrange(n):
            now = (start_time + delta_time * timeslot)
            date = now.format('YYYY-MM-DD')
            hour = now.format('HH:mm:ss')
            yield date,hour
    def to_labels(self, value_func=lambda x:1, top_k=9, extent=None):
        rdd = self.to_lifeline(value_func, top_k, extent)
        labels =  np.array(rdd.map(lambda x: x[0]).collect())
        return labels
    def to_pandas_series(self, value_func=lambda x:1, extent=None):
        datetime_vector,numpy_matrix = self.to_datetime_array(value_func=value_func, top_k=0, extent=extent)
        if datetime_vector is None:
            return pd.Series()
        return pd.Series(numpy_matrix[0], index=datetime_vector)
    def to_pandas_dataframe(self, value_func=lambda x:1,top_k=9, extent=None):
        datetime_vector,numpy_matrix = self.to_datetime_array(value_func=value_func, top_k=top_k, extent=extent)
        labels = self.to_labels(value_func=value_func, top_k=top_k)
        if datetime_vector is None:
            return pd.DataFrame()
        all_series={}
        for i in xrange(len(labels)):
            name = labels[i]
            if name != "Other":
                all_series[name]=numpy_matrix[i]
        return pd.DataFrame(all_series, index=datetime_vector)
    def to_extent_dates(self, value_func=lambda x:1, top_k=9, extent=None):
        if extent is None:
            min_date, max_date = self.to_extent()
        else:
            min_date, max_date = extent
    def plot(self,  value_func=lambda x:1, top_k=9, normalize_scale=True, kind="area"):
        lifeline_rdd = self.to_lifeline(value_func=value_func, top_k=top_k)
        data = lifeline_rdd.collect()
        if normalize_scale:
            vmax = (0,max([ np.max(d[1]) for d in data]))
        else:
            vmax=None
        for d in data:
            pd.DataFrame(data={d[0]:d[1]}).plot(
                figsize=[14,0.8], 
                kind=kind,
                ylim=vmax
            )
        return lifeline_rdd
    
    
    def plot_stacked(self,  value_func=lambda x:1, top_k=9, title="Breakdown by group", use_bar_graph=False, order_by_score=False, omit_other=False):
        lifeline_rdd = self.to_lifeline(value_func=value_func, top_k=top_k)
        # vlen=lifeline_rdd.first()[1].shape[0]
        # d = pd.DataFrame(dict(lifeline_rdd.collect()), index=np.arange(vlen)*24.0/timewindow_mins)

        if order_by_score:
            if lifeline_rdd.count()==0:
                return
            datetime_vector,numpy_matrix = self.to_datetime_array(value_func=value_func, top_k=top_k)
            labels = self.to_labels(value_func=value_func, top_k=top_k)

            reordered_lables =[ label for label in  self.to_top_k(value_func=value_func, top_k=top_k)]
            if not omit_other:
                reordered_lables.append("Other")

            reordered_matrix = np.zeros(( len(reordered_lables), numpy_matrix.shape[1]))
            for v,label in zip(numpy_matrix, labels):
                if label in reordered_lables:
                    index = reordered_lables.index(label)
                    reordered_matrix[index]=v
            d = pd.DataFrame(
                reordered_matrix.T,
                index=datetime_vector,
                columns=reordered_lables
                )
        else:
            d = pd.DataFrame( dict(lifeline_rdd.collect()) )
        # d = pd.DataFrame( dict(lifeline_rdd.collect()) )

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


    def plot_bearcart(self,  value_func=lambda x:1, top_k=9, plt_type=None, legend=False, width=700, order_by_score=False):
        # plt_type: 'bar, 'area', 'line', 'scatterplot'
        
        lifeline_rdd = self.to_lifeline(value_func=value_func, top_k=top_k)
        if lifeline_rdd.count()==0:
            return
        datetime_vector,numpy_matrix = self.to_datetime_array(value_func=value_func, top_k=top_k)
        labels = self.to_labels(value_func=value_func, top_k=top_k)
        
        if order_by_score:
            if omit_other:
                omit_other="Other"
            reordered_lables =[ label for label in  self.to_top_k(value_func=value_func, top_k=top_k) if label!=omit_other]

            reordered_matrix = np.zeros(( len(reordered_lables), numpy_matrix.shape[1]))
            for v,label in zip(numpy_matrix, labels):
                if label in reordered_lables:
                    index = reordered_lables.index(label)
                    reordered_matrix[index]=v
            print(labels) #REMREM
            print(numpy_matrix.shape) # RMEREM
            print(reordered_matrix.shape) # RMEREM
            print(len(datetime_vector)) # REMREM
            print(len(reordered_lables)) # REMREM
            d = pd.DataFrame(
                reordered_matrix.T,
                index=datetime_vector,
                columns=reordered_lables
                )

                    
        df = pd.DataFrame( 
            numpy_matrix.T,
            index=datetime_vector,
            columns=labels
            )

        if plt_type is None:
            plt_type="bar"
        
        vis = bearcart.Chart(df, 
                width=width-200*legend, 
                height=150, 
                y_zero=True, 
                x_time=True, 
                legend=legend, 
                plt_type=plt_type, 
                y_axis=True)
        vis.create_chart()
        display(vis)

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

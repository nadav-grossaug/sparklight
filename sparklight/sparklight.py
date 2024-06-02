#! /usr/bin/env python
# -*- coding: UTF-8 -*-
from __future__ import print_function
import sys
import argparse
import os
import gzip
import glob2
from operator import add 
import codecs
import errno
import itertools
import collections
import six
import math
import random
import tqdm
import types
# Fix Python 2.x unicode : source http://stackoverflow.com/questions/6812031/how-to-make-unicode-string-with-python3
try:
    UNICODE_EXISTS = bool(type(unicode))
except NameError:
    unicode = lambda s, encoding=None: str(s,encoding)

try:
    from collections.abc import Callable # Python 3.6
except ImportError:
    from collections import Callable


class Row(object):
    def __init__(self,**kwargs):
        for key,value in kwargs.items():
            self.__setattr__(key,value)
    def asDict(self):
        return self.__dict__
    def __repr__(self):
        out=str(self.__dict__)
        return "Row("+out[1:-1]+")"

SPARKLIGHT_VERBOSE=False
class SparklightContext:
    def __init__(self):
        pass
    def textFile(self, paths):
        rdd = SparklightRdd()
        if "," in paths: 
            rdd.paths=paths.split(",")
        else:
            rdd.paths=paths
        return rdd
    def parallelize(self, data, numSlices=None):
        rdd = SparklightRdd(data=data)
        return rdd
    @staticmethod
    def each_line(inputs, limit=-1):
        if inputs!=None and isinstance(inputs,(list, tuple)) or isinstance(inputs, types.GeneratorType):
            count = 0
            for input in inputs:
                for line in SparklightContext.each_line(input):
                    yield line
                    count+=1
                    if count==limit:
                        return
            return
    
        filename = inputs
        if filename==None or filename=="-" or filename=="stdin":
            stream = sys.stdin
        elif "*" in filename:
            for line in SparklightContext.each_line(glob2.glob(filename), limit):
                yield line
            return
        elif filename.endswith(".gz") or filename.endswith(".Z"):
            if SPARKLIGHT_VERBOSE:
                sys.stderr.write("RDD Open gz file for reading: "+filename+"\n")
            stream = gzip.open(filename,'rb')
        else:
            if SPARKLIGHT_VERBOSE:
                sys.stderr.write("RDD Open file for reading: "+filename+"\n")
            stream = open(filename, "rb")

        count = 0
        try:
            for line in stream:
                try:
                    line = unicode(line, 'utf8')
                    if line.endswith("\n"):
                        line=line[:-1]
                    yield line
                    count+=1
                    if count==limit:
                        break
                except UnicodeDecodeError as ex:
                    if SPARKLIGHT_VERBOSE:
                        sys.stderr.write("Exception UnicodeDecodeError "+str(ex)+" for line: "+line+"\n")
        except EOFError as ex:
            if SPARKLIGHT_VERBOSE:
                sys.stderr.write("Exception EOFError "+str(ex)+" for line: "+line+"\n")
                
        stream.close()

class SparklightRdd:
    def __init__(self, parent_rdd=None, data=None):
        self.data=data
        self.paths=None
        self.mapper=None
        self.tqdm_mapper=None
        self.flat_mapper=None
        self.parent_rdd=parent_rdd
        self.zip_with_index=None
        self.group_by_key=None
        self.group_by_value=None
        self.filter_func=None
        self.sample_func=None
        self.reduce_by_key=None
        self.top_tuple=None
        self.flag_distinct=False
        self.sort_by=None
        self.sort_by_ascending=True
        self.union_list=None
    # @TODO: add implementation for:
    # flatMapValues
    # foldByKey
    # countByKey
    # countByValue
    # aggregate
    # aggregateByKey
    
    def repartition(self, partitions):
        return self
    def cache(self):
        self.data=self.collect()
        return self
    def collect(self):
        return self.take(-1)
    def collectAsMap(self):
        return dict(self.collect())
    def toLocalIterator(self):
        return self.yield_rdd(-1)
    def first(self):
        out = self.take(1)
        if len(out)==0:
            return None
        return out[0]
    def take(self, top_k):
        return [x for x in self.yield_rdd(top_k)]
    def takeOrdered(self, num, key=None):
        if key==None:
            key=lambda x:x
        return self.sortBy(key, ascending=True).take(num)
    def top(self, num, key=None):
        if key==None:
            key=lambda x:x
        return self.sortBy(key, ascending=False).take(num)
    def foreach(self, f):
        for x in self.yield_rdd():
            f(x)
    def count(self):
        if self.data and not isinstance(self.data, Callable):
            return len(self.data)
        count = 0
        for x in self.yield_rdd():
            count+=1
        return count
    def yield_rdd(self, top_k=-1):
        # cache():
            
        if self.data is not None:
            if isinstance(self.data, Callable):
                data=self.data()
            else:
                data =self.data
            
            if top_k>0:
                for line in itertools.islice(data, 0, top_k):
                    yield line
            else:
                for line in data:
                    yield line
            return
        # sort_by :
        if self.sort_by:
            lines=[line for line in self.yield_raw()]
            sorted_lines = sorted(lines, 
                key=self.sort_by, reverse= not self.sort_by_ascending)
            count=0
            for line in sorted_lines:
                yield line
                count+=1
                if count==top_k:
                    return
            return
        # ### top
        if self.top_tuple:
            n, key_func = self.top_tuple
            if key_func is None:
                key_func = lambda x: x
            lines=[line for line in self.yield_raw()]
            sorted_lines = sorted(lines, 
                key=key_func, reverse=True)
            count=0
            for line in sorted_lines:
                yield line
                count+=1
                if count==n:
                    return
            return
        # ### distinct
        if self.flag_distinct:
            hash={}
            for line in self.yield_raw():
                if line not in hash:
                    hash[line]=0
                hash[line]+=1
            count=0
            for key in six.iterkeys(hash):
                yield key
                count+=1
                if count==top_k:
                    return
            return
        # ### reduce_by_key:
        if self.reduce_by_key:
            hash={}
            for line in self.yield_raw():
                key=line[0]
                value=line[1]
                if key not in hash:
                    hash[key]=value
                else:
                    hash[key]=self.reduce_by_key(hash[key],value)
            count=0
            for key, value in six.iteritems(hash):
                yield (key,value)
                count+=1
                if count==top_k:
                    return
            return
        # ### zipWithIndex
        if self.zip_with_index:
            for index, value in enumerate(self.yield_raw()):
                yield value, index 
            return
        # ### group-by
        if self.group_by_key:
            hash={}
            for line in self.yield_raw():
                key=self.group_by_key(line)
                if key not in hash:
                    hash[key]=[]
                hash[key].append(self.group_by_value(line))
            count=0
            for key, arr in six.iteritems(hash):
                yield (key,arr)
                count+=1
                if count==top_k:
                    return
            return
        elif self.tqdm_mapper:
            # ### map
            for line in tqdm.tqdm(self.yield_raw(top_k), total=self.tqdm_mapper):
                yield line
            return
            # ### flat-map
        elif self.mapper:
            # ### map
            for line in self.yield_raw(top_k):
                yield self.mapper(line)
            return
            # ### flat-map
        elif self.flat_mapper:
            count=0
            for line in self.yield_raw():
                iters = self.flat_mapper(line)
                if iters:
                    for o in iters:
                        if o!=None:
                            count+=1
                            yield o
                            if count==top_k:
                                return
            return
        # ### filter
        elif self.filter_func:
            count=0
            for line in self.yield_raw():
                if self.filter_func(line):
                    count+=1
                    yield line
                    if count==top_k:
                        return
            return
        # ### sample
        elif self.sample_func:
            count=0
            for line in self.yield_raw():
                if self.sample_func(line):
                    count+=1
                    yield line
                    if count==top_k:
                        return
            return
        else:
            for line in self.yield_raw(top_k):
                yield line
                
    def yield_raw(self, top_k=-1):
        if isinstance(self.data, Callable):
            data=self.data()
        else:
            data =self.data
        
        if data is not None:
            if top_k>0:
                for line in itertools.islice(data, 0, top_k):
                    yield line
            else:
                for line in data:
                    yield line
                    
        elif self.union_list is not None:
             for an_rdd in self.union_list:
                 for line in an_rdd.yield_rdd():
                     yield line
        elif self.paths is not None:
            for line in SparklightContext.each_line(self.paths, top_k):
                yield line
        elif self.parent_rdd is not None:
            for line in self.parent_rdd.yield_rdd(top_k):
                yield line

    def filter(self, func):
        rdd = SparklightRdd(self)
        rdd.filter_func=func
        return rdd
    def sample(self, withReplacement, fraction, seed=None):
        # withReplacement is not implemented
        if seed:
            random.seedseed
        rdd = SparklightRdd(self)
        rdd.sample_func=lambda x: random.random()<=fraction
        return rdd
    def tqdm_map(self, ):
        rdd = SparklightRdd(self)
        rdd.tqdm_mapper=self.count()
        return rdd
    def map(self, func):
        rdd = SparklightRdd(self)
        rdd.mapper=func
        return rdd
    def flatMap(self, func):
        rdd = SparklightRdd(self)
        rdd.flat_mapper=func
        return rdd
    def groupByKey(self,numPartitions=None):
        return self.groupBy(None, numPartitions)
    def zipWithIndex(self):
        rdd = SparklightRdd(self)
        rdd.zip_with_index=True
        return rdd
    def groupBy(self, func,numPartitions=None):
        rdd = SparklightRdd(self)
        if func==None: # groupByKey:
            rdd.group_by_key=lambda x:x[0]
            rdd.group_by_value=lambda x:x[1]
        else:
            rdd.group_by_key=func
            rdd.group_by_value=lambda x:x 
        return rdd
    def reduceByKey(self, func):
        rdd = SparklightRdd(self)
        rdd.reduce_by_key=func
        return rdd
    def distinct(self):
        rdd = SparklightRdd(self)
        rdd.flag_distinct=True
        return rdd
    def union(self, other):
        rdd = SparklightRdd(self)
        rdd.union_list=(self, other)
        return rdd
    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        if keyfunc==None:
            keyfunc=lambda x:x[0]
        rdd = SparklightRdd(self)
        rdd.sort_by=keyfunc
        rdd.sort_by_ascending=(ascending==True)
        return rdd
    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=None):
        return self.sortBy(keyfunc,ascending,numPartitions)
    def min(self, key=None):
        if key is None:
            values = self.collect()
        else:
            values = self.map(key).collect()
        if len(values)==0:
            return None
        return min(values)
    def max(self, key=None):
        if key is None:
            values = self.collect()
        else:
            values = self.map(key).collect()
        if len(values)==0:
            return None
        return max(values)
    def saveAsTextFile(self, path, compressionCodecClass=None):
        try:
            os.makedirs(path)
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else: raise
        if compressionCodecClass=="org.apache.hadoop.io.compress.GzipCodec":
            stream = gzip.open(path+"/part-000000.gz", 'wt')
        else:
            stream = codecs.open(path+"/part-000000", "wt", "utf-8")
        for line in self.yield_rdd():
            # stream.write(unicode(line+"\n", "utf-8").encode('utf-8'))
            #stream.write(unicode(line, "utf-8")+"\n")
            stream.write(line+"\n")
        stream.close()
        return self
    def reduce(self,func):
        first=True
        value=None
        for x in self.yield_rdd():
            if first:
                value=x
                first=False
            else:
                value=func(value,x)
        return value
    def sum(self):
        value=0
        for x in self.yield_rdd():
            value+=x
        return value
    def mean(self):
        value=0
        count=0
        for x in self.yield_rdd():
            value+=x
            count+=1
        return 1.0*value/count
    def stdev(self):
        mean_f = self.mean()
        value=0
        count=0
        for x in self.yield_rdd():
            value+= (x-mean_f)**2
            count+=1
        return math.sqrt(value/count)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--input", type=str, dest='input', nargs='+', default=None,help="Input files")
    start_options = parser.parse_args()
    
    
    sc=SparklightContext()
    for x in sc.textFile(start_options.input) \
            .repartition(100) \
            .map(lambda line: line.split("\t")) \
            .take(4):
        print(x)
    

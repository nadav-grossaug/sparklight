#! /usr/bin/env python
# -*- coding: UTF-8 -*-
from __future__ import print_function
import sys
import argparse
import os
import gzip
import glob
from operator import add 
import codecs
import errno

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
    @staticmethod
    def each_line(inputs, limit=-1):
        if inputs!=None and isinstance(inputs,list):
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
            for line in SparklightContext.each_line(glob.glob(filename), limit):
                yield line
            return
        elif filename.endswith(".gz"):
            if SPARKLIGHT_VERBOSE:
                sys.stderr.write("RDD Open gz file for reading: "+filename+"\n")
            stream = gzip.open(filename,'rb')
        else:
            if SPARKLIGHT_VERBOSE:
                sys.stderr.write("RDD Open file for reading: "+filename+"\n")
            stream = open(filename, "rb")

        count = 0
        for line in stream:
            try:
                line = unicode(line, 'utf8').strip()
                yield line
                count+=1
                if count==limit:
                    break
            except UnicodeDecodeError, ex:
                if SPARKLIGHT_VERBOSE:
                    sys.stderr.write("Exception UnicodeDecodeError "+str(ex)+" for line: "+line+"\n")
                
        stream.close()

class SparklightRdd:
    def __init__(self, parent_rdd=None):
        self.data=None
        self.paths=None
        self.mapper=None
        self.flat_mapper=None
        self.parent_rdd=parent_rdd
        self.group_by_key=None
        self.group_by_value=None
        self.filter_func=None
        self.reduce_by_key=None
        self.flag_distinct=False
        self.sort_by=None
        self.sort_by_ascending=True
    def repartition(self, partitions):
        return self
    def cache(self):
        self.data=self.collect()
        return self
    def collect(self):
        return self.take(-1)
    def first(self):
        out = self.take(1)
        if len(out)==0:
            return None
        return out[0]
    def take(self, top_k):
        return [x for x in self.yield_rdd(top_k)]
    def count(self):
        if self.data:
            return len(self.data)
        count = 0
        for x in self.yield_rdd():
            count+=1
        return count
    def yield_rdd(self, top_k=-1):
        # cache():
        if self.data is not None:
            if top_k>0:
                for line in self.data[:top_k]:
                    yield line
            else:
                for line in self.data:
                    yield line
            return
        # sort_by :
        if self.sort_by:
            lines=[line for line in self.yield_raw()]
            sorted_lines = sorted(lines, 
                key=self.sort_by, reverse=self.sort_by_ascending)
            count=0
            for line in sorted_lines:
                yield line
                count+=1
                if count==top_k:
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
            for key, count in hash.iteritems():
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
            for key, value in hash.iteritems():
                yield (key,value)
                count+=1
                if count==top_k:
                    return
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
            for key, arr in hash.iteritems():
                yield (key,arr)
                count+=1
                if count==top_k:
                    return
            return
        elif self.mapper:
            # ### map
            for line in self.yield_raw(top_k):
                yield self.mapper(line)
            return
            # ### flat-map
        elif self.flat_mapper:
            count=0
            for line in self.yield_raw():
                for o in self.flat_mapper(line):
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
        else:
            for line in self.yield_raw(top_k):
                yield line
                
    def yield_raw(self, top_k=-1):
        if self.data is not None:
            for line in self.data[0:top_k]:
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
    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        if keyfunc==None:
            keyfunc=lambda x:x[0]
        rdd = SparklightRdd(self)
        rdd.sort_by=keyfunc
        rdd.sort_by_ascending=(ascending==True)
        return rdd
    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=None):
        return self.sortBy(keyfunc,ascending,numPartitions)
    def min(self):
        return min(self.collect())
    def max(self):
        return max(self.collect())
    def saveAsTextFile(self, path, compressionCodecClass=None):
        try:
            os.makedirs(path)
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else: raise
        if compressionCodecClass=="org.apache.hadoop.io.compress.GzipCodec":
            stream = gzip.open(path+"/part-000000.gz", 'wb')
        else:
            stream = codecs.open(path+"/part-000000", "w", "utf-8")
        for line in self.yield_rdd():
            stream.write(unicode(line).encode('utf-8'))
        stream.close()
        return self
            

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
    
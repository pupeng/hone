# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Debug the formation of the aggregation tree

from hone_lib import *

def query():
    q = (Select(['app', 'cpu']) *
         From('AppStatus') *
         Where([('app', '==', 'python')]) *
         Every(1000))
    return q

def localSum(table):
    table = map(lambda x: float(x[1]), table)
    print 'local sum'
    return sum(table)

def agg(data):
    print 'middle level {0}'.format(data)
    return data

def myPrint(data):
    print 'global sum'

def main():
    return (query() >> \
            MapStreamSet(localSum) >> \
            TreeMerge(agg) >> \
            Print(myPrint))


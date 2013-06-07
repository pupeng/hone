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

def LocalSum(table):
    table = map(lambda x: float(x[1]), table)
    print 'local sum'
    return sum(table)

def IntermediateSum(data):
    print 'intermediate level {0}'.format(data)
    return sum(data)

def FinalSum(data):
    print 'final level {0}'.format(data)
    print 'sum: {0}'.format(sum(data))

def main():
    return (query() >>
            MapStreamSet(LocalSum) >>
            TreeMerge(IntermediateSum) >>
            MapStream(FinalSum))


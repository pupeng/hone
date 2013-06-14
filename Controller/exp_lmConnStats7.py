# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Test lazy materialization for conn measurement
# match test in exp2: connStats7

from hone_lib import *

def query():
    q = (Select(['app', 'srcIP', 'srcPort', 'dstIP', 'dstPort', 'BytesWritten', 'BytesSentOut']) *
         From('HostConnection') *
         Where([('app', '==', 'test_prog')]) *
         Every(1000))
    return q

def DisplayStats(x):
    print 'number of connections: {0}'.format(len(x))
    if x:
        print 'the statistics are:'
        for data in x:
            print data
    return x

def main():
    return (query() >>
            MapStreamSet(DisplayStats) >>
            MergeHosts() >>
            MapStream(DisplayStats))

# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# calculate traffic matrix

from hone_lib import *

import time

def TrafficMatrixQuery():
    return (Select(['srcIP','srcPort','dstIP','dstPort','BytesSentOut']) *
            From('HostConnection') *
            Groupby(['srcIP','dstIP']) *
            Every(5000))

def PrintHelper(x):
    print time.time()
    print x
    print '*******************\n'

def main():
    stream = TrafficMatrixQuery() >> Print(PrintHelper)
    return stream
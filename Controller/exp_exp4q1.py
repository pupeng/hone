# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Sum throughputs of iperf across hosts

from hone_lib import *
from subprocess import check_output
from math import *
import time

def connQuery():
    q = (Select(['app','srcIP','srcPort','dstIP','dstPort','BytesSentOut','StartTimeSecs','ElapsedSecs','StartTimeMicroSecs','ElapsedMicroSecs']) *
         From('HostConnection') *
         Where([('app', '==', 'test_prog')]) *
         Every(1000))
    return q

def CalcTM(newData, tpData):
    openConn = []
    print 'Num of connections: {0}'.format(len(newData))
    for conn in newData:
        [app, srcIP, srcPort, dstIP, dstPort, newAccumBS, startSecs, elapsedSecs, startMicrosecs, elapsedMicrosecs] = conn
        newTS = startSecs + elapsedSecs+startMicrosecs / 1000000.0 + elapsedMicrosecs / 1000000.0
        thetuple = (srcIP,srcPort,dstIP,dstPort)
        if thetuple in tpData:
            (lastTS, lastAccumBS, lastTP) = tpData[thetuple]
            if newTS>lastTS:
                newTP = float((newAccumBS-lastAccumBS))/(newTS-lastTS)
            else:
                newTP = 0
        else:
            newTP = float(newAccumBS)/newTS
        tpData[thetuple] = (newTS, newAccumBS, newTP)
        openConn.append(thetuple)
    closeConn = []
    for key in tpData.iterkeys():
        if not key in openConn:
            closeConn.append(key)
    for key in closeConn:
        del tpData[key]
    return tpData

def LocalAgg(tpData):
    return sum(map(lambda x: x[2], tpData.values()))

def GlobalAgg(data):
    print 'Global agg throughput {0} bps'.format(sum(data) * 8.0)
    return sum(data)

def main():
    connStream = (connQuery() >> 
                  ReduceStreamSet(CalcTM, {}) >> 
                  MapStreamSet(LocalAgg) >> 
                  MergeHosts() >> 
                  MapStream(GlobalAgg))
    return connStream

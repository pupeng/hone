# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# monitor the throughputs across hosts of application trafclient

from hone_lib import *
from math import *
import time

K = 0.2

def query():
    q = (Select(['app','srcHost', 'srcIP','srcPort','dstIP','dstPort','BytesSentOut','StartTimeSecs','ElapsedSecs','StartTimeMicroSecs','ElapsedMicroSecs']) *
         From('HostConnection') *
         Where([('app', '==', 'trafclient')]) *
         Every(1000))
    return q

# tpData[(srcIP,srcPort,dstIP,dstPort)] = (lastTimestamp, lastAccumulativeBytesSent, lastThroughput)
def CalThroughput(newData, oldData):
    (hostId, tpData) = oldData
    openConn = []
    for conn in newData:
        [app, newHostId, srcIP, srcPort, dstIP, dstPort, newAccumBS, startSecs, elapsedSecs, startMicrosecs, elapsedMicrosecs] = conn
        if hostId is None:
            hostId = newHostId
        newTS = startSecs + elapsedSecs+startMicrosecs / 1000000.0 + elapsedMicrosecs / 1000000.0
        thetuple = (srcIP,srcPort,dstIP,dstPort)
        if thetuple in tpData:
            (lastTS, lastAccumBS, lastTP) = tpData[thetuple]
            if newTS > lastTS:
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
    return [hostId, tpData]

def LocalSum(data):
    (hostId, tpData) = data
    sumTP = []
    avgTS = []
    for (ts, accumBS, tp) in tpData.itervalues():
        # tp unit is bytes per second now
        sumTP.append(tp)
        avgTS.append(ts)
    if avgTS:
        return [hostId, sum(avgTS)/len(avgTS), sum(sumTP) * 8.0 / 1000.0] # now throughput change to Kbps

# exponentially weighted moving average
def EWMA(newData, lastData):
    (newHostId, newTime, newTP) = newData
    (lastHostId, lastTime, lastRate) = lastData
    timeDiff = newTime - lastTime
    if timeDiff > 0:
        newRate = (1.0 - exp(-timeDiff / K)) * newTP + exp(-timeDiff / K) * lastRate
    else:
        newRate = 0.0
    return [newHostId, newTime, newRate]

def MonitorThroughput(x):
    outputFile = open('logs/throughput.txt', 'a')
    sumRate = []
    timestamps = []
    for (hostId, timestamp, rate) in x:
        sumRate.append(rate)
        timestamps.append(timestamp)
        print >> outputFile, 'host {0} {1} {2}'.format(hostId, timestamp, rate)
    print >> outputFile, 'aggregate {0} {1}'.format(min(timestamps), sum(sumRate))
    outputFile.close()

def main():
    return (query() >>
            ReduceStreamSet(CalThroughput, [None, {}]) >>
            MapStreamSet(LocalSum) >>
            ReduceStreamSet(EWMA, [None, time.time(), 100]) >>
            MergeHosts() >>
            MapStream(MonitorThroughput))


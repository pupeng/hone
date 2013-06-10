# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# simple diagnosis job
# detect connections with low congestion window, and identify their shared links (if exist) as the bottleneck link

from hone_lib import *

def ConnQuery():
    return (Select(['src']))


def TrafficMatrixQuery():
    return (Select(['srcIP','dstIP','BytesSentOut','StartTimeSecs','ElapsedSecs','StartTimeMicroSecs','ElapsedMicroSecs']) *
            From('HostConnection') *
            Groupby(['srcIP','dstIP']) *
            Every(5000))

def SumBytesSentForHostPair(connsOfOneHostPair):
    hostAIP = None
    hostBIP = None
    timestamps = []
    bytesSum = []
    for (srcIP, dstIP, bytesSent, startSecs, elapsedSecs, startMicrosecs, elapsedMicrosecs) in connsOfOneHostPair:
        hostAIP = srcIP
        hostBIP = dstIP
        bytesSum.append(bytesSent)
        timestamp = startSecs + elapsedSecs + startMicrosecs / 1000000.0 + elapsedMicrosecs / 1000000.0
        timestamps.append(timestamp)
    return [hostAIP, hostBIP, sum(bytesSum), average(timestamps)]

def CalThroughput(newData, tpData):
    activeHostPair = []
    for hostPairData in newData:
        (hostAIP, hostBIP, newAccumBytes, newTimestamp) = hostPairData
        if (hostAIP, hostBIP) in tpData:
            (lastTimestamp, lastAccumBytes, lastRate) = tpData[(hostAIP, hostBIP)]
            if newTimestamp > lastTimestamp:
                newRate = float(newAccumBytes - lastAccumBytes) / (newTimestamp - lastTimestamp)
            else:
                newRate = 0.0
        else:
            newRate = float(newAccumBytes) / newTimestamp
        tpData[(hostAIP, hostBIP)] = (newTimestamp, newAccumBytes, newRate)
        activeHostPair.append((hostAIP, hostBIP))
    inactiveHostPair = []
    for key in tpData.iterkeys():
        if key not in activeHostPair:
            inactiveHostPair.append(key)
    for key in inactiveHostPair:
        del tpData[key]
    return tpData

# exponentially weighted moving average
def EWMA(newTpData, lastTpData):
    for hostPair, newData in newTpData.iteritems():
        (newTimestamp, _, newRate) = newData
        if hostPair in lastTpData:
            (lastTimestamp, lastRate) = lastTpData[hostPair]
        else:
            (lastTimestamp, lastRate) = (0.0, 0.0)
        timeDiff = newTimestamp - lastTimestamp
        if timeDiff > 0:
            newRate = (1.0 - math.exp(-timeDiff / K)) * newRate + math.exp(-timeDiff / K) * lastRate
        else:
            newRate = 0.0
        lastTpData[hostPair] = (newTimestamp, newRate)
    return lastTpData

def FormatThroughputData(tpData):
    ret = {}
    for key, value in tpData.iteritems():
        ret[key] = value[1]
    return ret

def AggTrafficMatrix(x):
    globalTrafficMatrix = {}
    for dataFromEachHost in x:
        for hostPair, rate in dataFromEachHost.iteritems():
            globalTrafficMatrix[hostPair] = rate
    return globalTrafficMatrix

def DisplayTrafficMatrix(x):
    for hostPair, rate in x.iteritems():
        print 'IP1:{0} IP2:{1} Rate:{2} Kbps'.format(hostPair[0], hostPair[1], rate * 8 / 1000.0)
    print '*******************\n'

def main():
    stream = TrafficMatrixQuery()
    stream = stream >> MapStreamSet(MapList(SumBytesSentForHostPair))
    stream = stream >> ReduceStreamSet(CalThroughput, {})
    stream = stream >> ReduceStreamSet(EWMA, {})
    stream = stream >> MapStreamSet(FormatThroughputData)
    stream = stream >> MergeHosts()
    stream = stream >> MapStream(AggTrafficMatrix)
    stream = stream >> MapStream(DisplayTrafficMatrix)
    return stream
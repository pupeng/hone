# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Elephant flow detection and scheduling

from hone_lib import *
import math

K = 0.2 # for moving average

def ElephantQuery():
    return (Select(['srcIP','srcPort','dstIP','dstPort','BytesWritten','BytesSentOut']) *
            From('HostConnection') *
            Every(2000))

def LinkQuery():
    return (Select(['BeginDevice', 'BeginPort', 'EndDevice', 'EndPort']) *
            From('LinkStatus') *
            Every(2000))

def TrafficMatrixQuery():
    return (Select(['srcIP','dstIP','BytesSentOut','StartTimeSecs','ElapsedSecs','StartTimeMicroSecs','ElapsedMicroSecs']) *
            From('HostConnection') *
            Groupby(['srcIP','dstIP']) *
            Every(2000))

def HostInfoQuery():
    return (Select(['hostId', 'IP']) *
            From('HostStatus') *
            Every(2000))

def IsElephant(row):
    [sip, sport, dip, dport, bw, bs] = row
    return (bw - bs) > 100

def DetectElephant(table):
    return FilterList(IsElephant)(table)

def average(x):
    if len(x) == 0:
        return None
    else:
        return sum(x) / float(len(x))

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

def FormatHostInfoToDict(x):
    result = {}
    for (hostId, ip) in x:
        result[hostId] = ip
    return result

# local is stable
def FindRoutesForHostPair(links):
    links = links[0]
    hosts = filter(lambda x : x[1] is None, links)
    hosts.sort()
    result = {}
    for i in range(len(hosts)):
        for j in range(i+1, len(hosts)):
            result[(hosts[i], hosts[j])] = links
    # for each host pair, find all equal-cost routes between them
    return result

def ReplaceHostIDwithHostIP(x):
    (hostInfoDict, hostPairRoutes) = x
    results = {}
    for hostIdPair, routes in hostPairRoutes.iteritems():
        (hostAId, hostBId) = hostIdPair
        hostAIp = hostInfoDict[hostAId]
        hostBIp = hostInfoDict[hostBId]
        results[(hostAIp, hostBIp)] = routes
    return results

# local is stable
def Schedule(x):
    ((trafficMatrix, hostPairRoutes), elephantFlows) = x
    ruleSet = []
    for (sip, dip, sport, dport, _, _) in elephantFlows:
        # placeholder
        route = hostPairRoutes[(sip, dip)][0]
        criterion = {'srcIp': sip, 'dstIp': dip}
        action = {'forward':route}
        rule = [criterion, action]
        ruleSet.append(rule)
    # first: schedule elephant flows
    # second: for remaining flows in traffic matrix, randomly pick a route for them not interfering with elephants
    return ruleSet

def main():
    elephantStream = (ElephantQuery() >>
                      MapStreamSet(DetectElephant) >>
                      MergeHosts())
    trafficMatrixStream = (TrafficMatrixQuery() >>
                           MapStreamSet(MapList(SumBytesSentForHostPair)) >>
                           ReduceStreamSet(CalThroughput, {}) >>
                           ReduceStreamSet(EWMA, {}) >>
                           MapStreamSet(FormatThroughputData) >>
                           MergeHosts() >>
                           MapStream(AggTrafficMatrix))
    hostInfoStream = HostInfoQuery() >> MergeHosts() >> MapStream(FormatHostInfoToDict)
    routesStream = (LinkQuery() >>
                    MapStream(FindRoutesForHostPair))
    hostRoutesStream = MergeStreams(hostInfoStream, routesStream) >> MapStream(ReplaceHostIDwithHostIP)
    stream = (MergeStreams(MergeStreams(trafficMatrixStream, hostRoutesStream), elephantStream) >>
              MapStream(Schedule) >>
              RegisterPolicy())
    return stream

# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Distributed Rate Limiting

from hone_lib import *
from math import *
import time

_DRL_DEBUG_ = True

K = 0.2
totalBudget = 10000 # KBps

def query():
    q = (Select(['app','srcHost', 'srcIP','srcPort','dstIP','dstPort','BytesSentOut','StartTimeSecs','ElapsedSecs','StartTimeMicroSecs','ElapsedMicroSecs']) *
         From('HostConnection') *
         Where([('app', '==', 'test_prog')]) *
         Every(2000))
    return q

''' tpData[(srcIP,srcPort,dstIP,dstPort)] = (lastTimestamp, lastAccumulativeBytesSent, lastThroughput) '''
def CalThroughput(newData, oldData):
    (hostId, tpData) = oldData
    if _DRL_DEBUG_:
        fileOutput = open('tmp_drl.log', 'a')
        print >>fileOutput, 'CalThroughput'
        print >>fileOutput, 'before start'
        print >>fileOutput, 'newData'
        print >>fileOutput, newData
        print >>fileOutput, 'tpData'
        print >>fileOutput, tpData
        fileOutput.close()
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
        sumTP.append(tp)
        avgTS.append(ts)
    if avgTS:
        return [hostId, sum(avgTS)/len(avgTS), sum(sumTP)]

def EWMA(newData, lastData):
    if _DRL_DEBUG_:
        fileOutput = open('tmp_drl.log', 'a')
        print >>fileOutput,  'in ewma'
        print >>fileOutput, 'newData'
        print >>fileOutput,  newData
        print >>fileOutput, 'lastData'
        print >>fileOutput,  lastData
        fileOutput.close()
    (newHostId, newTime, newTP) = newData
    (lastHostId, lastTime, lastRate) = lastData
    timeDiff = newTime - lastTime
    if timeDiff > 0:
        newRate = (1.0 - exp(-timeDiff / K)) * newTP + exp(-timeDiff / K) * lastRate
        if _DRL_DEBUG_:
            fileOutput = open('tmp_drl.log', 'a')
            print >>fileOutput, 'newRate'
            print >>fileOutput,  newRate
            fileOutput.close()
    else:
        newRate = 0.0
    return [newHostId, newTime, newRate]

def GenRateLimitPolicy(x):
    # sumDemand = 0
    print x
    # localDemand = {}
    # for (hostID, seq, data) in x:
    #     localDemand[hostID] = data[1]
    #     sumDemand += data[1]
    # rs = []
    # if sumDemand>0:
    #     for (hostID, localRate) in localDemand.iteritems():
    #         if localRate<0.1:
    #             localBudget = totalBudget
    #         else:
    #             localBudget = float(localRate)/float(sumDemand)*totalBudget
    #         cr = {'app':'trafclient', 'srcHost':hostID}
    #         action = {'rateLimit':localBudget}
    #         rs.append([cr, action])
    #         print hostID+' '+str(localBudget)
    #     print 'Distributed Rate Limiting One Round'
    # return rs

def DebugPrint(x):
    for (hostID, timestamp, data) in x:
        print 'hostID:{0}. timestamp:{1}. data:{2}'.format(hostID, timestamp, data)
    print '******************************************'

def main():
    return (query() >>
            ReduceStreamSet(CalThroughput, [None, {}]) >>
            MapStreamSet(LocalSum) >>
            ReduceStreamSet(EWMA, [None, time.time(), 100]) >>
            MergeHosts() >>
            Print(DebugPrint))
            # MapStream(GenRateLimitPolicy) >>
            # RegisterPolicy())
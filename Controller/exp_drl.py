# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Distributed Rate Limiting

from hone_lib import *
from math import *
import time

_DRL_DEBUG_ = False

K = 0.2
totalBudget = 10000 # KBps

def query():
    q = (Select(['app','srcIP','srcPort','dstIP','dstPort','TimeStamps','BytesSentOut']) *
        From('HostConnection') *
        Where([('app','==','test_prog')]) *
        Every(2000))
    return q

''' tpData[(srcIP,srcPort,dstIP,dstPort)] = (lastTimestamp, lastAccumulativeBytesSent, lastThroughput) '''
def calThroughput(newData, tpData):
    if _DRL_DEBUG_:
        fileOutput = open('tmp_drl.log', 'a')
        print >>fileOutput, 'calThroughput'
        print >>fileOutput, 'before start'
        print >>fileOutput, 'newData'
        print >>fileOutput, newData
        print >>fileOutput, 'tpData'
        print >>fileOutput, tpData
        fileOutput.close()
    openConn = []
    for conn in newData:
        srcIP = conn[1]
        srcPort = conn[2]
        dstIP = conn[3]
        dstPort = conn[4]
        newTS = conn[5]
        newAccumBS = conn[6]
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
    return tpData

def localSum(x):
    sumTP = []
    avgTS = []
    for (ts, accumBS, tp) in x.itervalues():
        sumTP.append(tp)
        avgTS.append(ts)
    if avgTS:
        return [sum(avgTS)/len(avgTS), sum(sumTP)]

def ewma(newData, lastData):
    if _DRL_DEBUG_:
        fileOutput = open('tmp_drl.log', 'a')
        print >>fileOutput,  'in ewma'
        print >>fileOutput, 'newData'
        print >>fileOutput,  newData
        print >>fileOutput, 'lastData'
        print >>fileOutput,  lastData
        fileOutput.close()
    newTime = newData[0]
    newTP = newData[1]
    lastTime = lastData[0]
    lastRate = lastData[1]
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
    return [newTime, newRate]

def genRateLimitPolicy(x):
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

def myPrint(x):
    for (hostID, timestamp, data) in x:
        print 'hostID:'+hostID+' collect timestamp:'+str(timestamp)+' data:'+repr(data)
    print '******************************************'

def main():
    return (query() >>
            ReduceStreamSet(calThroughput, {}) >>
            MapStreamSet(localSum) >>
            ReduceStreamSet(ewma, [time.time(), 100]) >>
            MergeHosts() >>
            MapStream(genRateLimitPolicy) >>
            RegisterPolicy())
            





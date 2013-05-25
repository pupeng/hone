# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Calculate throughputs with EWMA

from hone_lib import *
from math import *
import time, sys

_DRL_DEBUG_ = True

K = 0.2
totalBudget = 10000 # KBps

def query():
    q = (Select(['app','srcIP','srcPort','dstIP','dstPort','timestamp','BytesSentOut']) *
        From('HostConnection') *
        Where([('app','==','test_prog')]) *
        Every(1))
    return q

''' bsData[(srcIP,srcPort,dstIP,dstPort)] = (bytesSentInLastPeriod, lastAccumulativeBytesSent) '''
def calBytesSentBetweenMeaure(newData, bsData):
    openConn = []
    for conn in newData:
        srcIP = conn[1]
        srcPort = conn[2]
        dstIP = conn[3]
        dstPort = conn[4]
        timestamp = conn[5]
        newAccumBS = conn[6]
        thetuple = (srcIP,srcPort,dstIP,dstPort)
        if bsData.has_key(thetuple):
            (lastBS, lastAccumBS, lastTimestamp) = bsData[thetuple]
            newBS = newAccumBS - lastAccumBS
        else:
            newBS = newAccumBS
        bsData[thetuple] = (newBS, newAccumBS, timestamp)
        openConn.append(thetuple)
    closeConn = []
    for key in bsData.iterkeys():
        if not key in openConn:
            closeConn.append(key)
    for key in closeConn:
        del bsData[key]
    return bsData
        
def localSum(x):
    if _DRL_DEBUG_:
        fileOutput = open('tmp_eval1.log', 'a')
        print >>fileOutput, 'in localSum'
        print >>fileOutput, len(x.keys())
        fileOutput.close()
    sumBS = []
    avgTS = []
    for (bs, accumBS, ts) in x.itervalues():
        sumBS.append(bs)
        avgTS.append(ts)
    if avgTS and sumBS:
        return [float(sum(avgTS))/len(avgTS), sum(sumBS), len(avgTS)]

def ewma(newData, lastData):
    newTime = newData[0]
    newBS = newData[1]
    newNum = newData[2]
    lastTime = lastData[0]
    lastRate = lastData[1]
    lastNum = lastData[2]
    timeDiff = newTime - lastTime
    if timeDiff > 0:
        newRate = (1.0 - exp(-timeDiff/K))*newBS/timeDiff+exp(-timeDiff/K)*lastRate
    else:
        newRate = 0.0
    return [newTime, newRate, newNum]

def myPrint(x):
    for (hostID, seq, [timestamp,rate,number]) in x:
        print 'hostID:'+hostID+',seq:'+str(seq)+',rate:'+str(rate)+',number:'+str(number)
    print '******************************************'

def main():
    return (query()>>ReduceSet(calBytesSentBetweenMeaure, {})>>\
            MapSet(localSum)>>ReduceSet(ewma, [time.time(),100,0])>>\
            MergeHosts()>>JoinHostsBySeq()>>\
            Print(myPrint))





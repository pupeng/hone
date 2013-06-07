# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# evaluate host-controller communication overhead (i.e., used bandwidth)

from hone_lib import *

def query():
    q = (Select(['app','srcIP','srcPort','dstIP','dstPort','BytesSentOut','StartTimeSecs','ElapsedSecs','StartTimeMicroSecs','ElapsedMicroSecs']) *
         From('HostConnection') *
         Where([('dstPort','==','8866')]) *
         Every(1000))
    return q

# tpData[(srcIP,srcPort,dstIP,dstPort)] = (lastTimestamp, lastAccumulativeBytesSent, lastThroughput)
def CalThroughput(newData, tpData):
    openConn = []
    for conn in newData:
        [app, srcIP, srcPort, dstIP, dstPort, newAccumBS, startSecs, elapsedSecs, startMicrosecs, elapsedMicrosecs] = conn
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
    return tpData

def LocalSum(x):
    sumTP = []
    avgTS = []
    for (ts, accumBS, tp) in x.itervalues():
        sumTP.append(tp)
        avgTS.append(ts)
    if avgTS:
        return [sum(avgTS)/len(avgTS), sum(sumTP)]

def PrintHelper(x):
    outputFile = open('eval_ctrl_vol.txt', 'a')
    for (timestamp, rate) in x:
        print >> outputFile, '{0} {1}'.format(timestamp, rate)
    print >> outputFile, 'one round done.'
    outputFile.close()

def main():
    return (query() >>
            ReduceStreamSet(CalThroughput, {}) >>
            MapStreamSet(LocalSum) >>
            MergeHosts() >>
            Print(PrintHelper))


#
#def query():
#    q = (Select(['dstPort','BytesSentOut','currentTime'])*
#         From('HostConnection')*
#         Where([('dstPort','==','8866')])*
#         Every(1))
#    return q
#
#def extract(x):
#    if x:
#        [dport, bs, ct] = x[0]
#        return [ct, bs]
#    else:
#        return None
#
#def isValid(x):
#    if x:
#        return True
#    else:
#        return False
#
#def check(x):
#    if len(x)>threshold:
#        return True
#    else:
#        return False
#
#def aggregate(x):
#    sumBS = []
#    for (hostID, seq, [ct, bs]) in x:
#        sumBS.append(bs)
#    return [seq, sum(sumBS)]
#
#def calculateRate(newData, lastData):
#    [seq, bs] = newData
#    if lastData:
#        [lastSeq, lastBS, lastRate] = lastData
#        newRate = float(lastBS-bs)/(lastSeq-seq)
#        return [seq, bs, newRate]
#    else:
#        return [seq, bs, bs]
#
#def myPrint(x):
#    [seq, bs, rate] = x
#    fileOutput = open('eval_hostctrl.txt', 'a')
#    print 'seq:'+str(seq)+',bs:'+str(bs)+' rate:'+str(rate)
#    print >>fileOutput, str(seq)+' '+str(rate)

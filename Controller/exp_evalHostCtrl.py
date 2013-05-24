# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# evaluate host-controller communication overhead

from hone_lib import *

def query():
    q = (Select(['app','srcIP','srcPort','dstIP','dstPort','timestamp','BytesSentOut']) *
        From('HostConnection') *
        Where([('srcPort','==','8866')]) *
        Every(1))
    return q

def calThroughput(newData, tpData):
    openConn = []
    for conn in newData:
        srcIP = conn[1]
        srcPort = conn[2]
        dstIP = conn[3]
        dstPort = conn[4]
        newTS = conn[5]
        newAccumBS = conn[6]
        thetuple = (srcIP,srcPort,dstIP,dstPort)
        if tpData.has_key(thetuple):
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

def localSum(x):
    sumTP = []
    avgTS = []
    for (ts, accumBS, tp) in x.itervalues():
        sumTP.append(tp)
        avgTS.append(ts)
    if avgTS:
        return [sum(avgTS)/len(avgTS), sum(sumTP)]

def myPrint(x):
    volOut = open('eval_ctrl_vol.txt', 'a')
    (hostID, seq, [ts, tp]) = x
    ct = time.time()
    print >>volOut, str(seq)+' '+str(tp)+' '+str(ct)+' '+str(ts)
    volOut.close()
    print 'seq:'+str(seq)+',tp:'+str(tp)+',ct:'+str(ct)+',ts:'+str(ts)

def main():
    return (query()>>
            ReduceSet(calThroughput, {})>>
            MapSet(localSum)>>
            MergeHosts()>>
            Print(myPrint))



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

'''
Peng Sun
Elephant flow detection and scheduling
'''

from hone_lib import *
from math import *
import time, sys

_DRL_DEBUG_ = False

def ElephantQuery():
    return (Select(['srcIP','srcPort','dstIP','dstPort','BytesWritten','BytesSentOut']) *
            From('HostConnection') *
            Every(1000))

def LinkQuery():
    return (Select(['BeginDevice','EndDevice']) *
            From('LinkStatus') *
            Every(1000))

def TrafficMatrixQuery():
    return (Select(['srcIP','srcPort','dstIP','dstPort','BytesSentOut','timestamp']) *
            From('HostConnection') *
            #Groupby(['srcIP','dstIP']) *
            Every(1000))

def IsElephant(row):
    [sip, sport, dip, dport, bw, bs] = row
    return ((bw - bs) > 100)

def DetectElephant(table):
    return filter(IsElephant, table)

def BuildTopo(table):
    return table

def CalcTM(newData, tpData):
    openConn = []
    for conn in newData:
        [srcIP, srcPort, dstIP, dstPort, newAccumBS, newTS] = conn
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
    agg = {}
    for (conn, value) in tpData.iteritems():
        (sip, sport, dip, dport) = conn
        if (sip, dip) not in agg:
            agg[(sip, dip)] = []
        agg[(sip, dip)].append(value[2])
    for conn in agg.iterkeys():
        agg[conn] = sum(agg[conn])
    return agg

def GlobalAgg(table):
    agg = {}
    for hostData in table:
        for (conn, tp) in hostData.iteritems():
            agg[conn] = tp
    return agg

def Schedule(x):
    return x
#    sumDemand = 0
#    localDemand = {}
#    for (hostID, seq, data) in x:
#        localDemand[hostID] = data[1]
#        sumDemand += data[1]
#    rs = []
#    if sumDemand>0:
#        for (hostID, localRate) in localDemand.iteritems():
#            if localRate<0.1:
#                localBudget = totalBudget
#            else:
#                localBudget = float(localRate)/float(sumDemand)*totalBudget
#            cr = {'app':'trafclient', 'srcHost':hostID}
#            action = {'rateLimit':localBudget}
#            rs.append([cr, action])
#            print hostID+' '+str(localBudget)
#        print 'Distributed Rate Limiting One Round'
#    return rs

def main():
    EStream = ElephantQuery() >> MapStreamSet(DetectElephant) >> \
              MergeHosts()
    TopoStream = LinkQuery() >> MapStream(BuildTopo)
    TmStream = (TrafficMatrixQuery() >> \
                ReduceStreamSet(CalcTM, {}) >> \
                MapStreamSet(LocalAgg) >> \
                MergeHosts() >> \
                MapStream(GlobalAgg))
    return (MergeStreams(EStream, TmStream) >> \
            MapStream(Schedule) >> \
            Print())


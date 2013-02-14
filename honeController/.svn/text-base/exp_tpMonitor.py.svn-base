'''
Peng Sun
DRL
'''

from hone_lib import *
from math import *
import time, sys

_DRL_DEBUG_ = False


def query():
    q = (Select(['app','srcIP','srcPort','dstIP','dstPort','timestamp','BytesSentOut']) *
        From('HostConnection') *
        Where([('app','==','test_prog')]) *
        Every(1))
    return q

''' tpData[(srcIP,srcPort,dstIP,dstPort)] = (lastTimestamp, lastAccumulativeBytesSent, lastThroughput) '''
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
            if not (newTS>0):
                print 'Found weird connection'
                print conn
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
    if sumTP and avgTS:
        return [float(sum(avgTS))/len(avgTS), sum(sumTP)]

def checkHost(newData, lastDict):
    (hostID, seq, data) = newData
    if not lastDict.has_key(hostID):
        lastDict[hostID] = []
    lastDict[hostID].append((seq, data))
    if len(lastDict[hostID])>50:
        lastDict[hostID].pop()
    return lastDict

def globalAgg(x):
    sumHosts = []
    for (hostID, seq, data) in x:
        timestamp = data[0]
        throughput = data[1]/1000.0
        sumHosts.append(throughput)
    return [x, sum(sumHosts)]

def myPrint(x):
    fileOutput = open('eval_ctrl_number.txt', 'a')
    num = len(x.keys())
    print 'number of hosts now:'+str(num)
    print >>fileOutput, str(time.time())+' '+str(num)
    fileOutput.close()
    print '******************************************'

def dummyPrint(x):
    print ''

def main():
    return (query()>>
            ReduceSet(calThroughput, {})>>
            MapSet(localSum)>>
            MergeHosts()>>
            ReduceStream(checkHost, {})>>
            Print(myPrint))
    #return (query()>>ReduceSet(calThroughput, {})>>\
    #        MergeHosts()>>Print())
            





'''
HONE application
Author: Peng Sun
Purpose:
Exp4 measure cpu, memory, and throughput of python
'''

from hone_lib import *
from subprocess import check_output
from math import *
import time
import psutil

def cpuMemQuery():
    q = (Select(['app','cpu','memory'])*
         From('AppStatus')*
         Where([('app','==','python')])*
         Every(1000))
    return q

def sumCpuMem(table):
    cpuSum = []
    memSum = []
    for [app, cpu, mem] in table:
        cpuSum.append(cpu)
        memSum.append(mem)
    return (sum(cpuSum), sum(memSum))

def CpuMemPrint(cpuMem):
    (cpu, mem) = cpuMem
    output = open('logs/agentCpuMem.txt', 'a')
    print >>output, '{0:6f} {1} {2}'.format(time.time(), cpu, mem)
    output.close()
    return cpuMem

def connQuery():
    q = (Select(['app','srcIP','srcPort','dstIP','dstPort','BytesSentOut','StartTimeSecs', 'ElapsedSecs', 'StartTimeMicroSecs', 'ElapsedMicroSecs']) *
         From('HostConnection') *
         Where([('app', '==', 'python')]) *
         Every(1000))
    return q

def CalcTM(newData, tpData):
    openConn = []
    for conn in newData:
        [app, srcIP, srcPort, dstIP, dstPort, newAccumBS, startSecs, elapsedSecs, startMicrosecs, elapsedMicrosecs] = conn
        newTS = startSecs+elapsedSecs+startMicrosecs/1000000.0+elapsedMicrosecs/1000000.0
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
    return sum(agg.values())

def TpPrint(throughput):
    output = open('logs/agentThroughput.txt', 'a')
    print >>output, "{0:6f} {1}".format(time.time(), throughput)
    output.close()
    return throughput

def HeartBeat(data):
    return 'HeartBeat'

def CtrlCpuMem(data):
    print data
    try:
        pids = check_output('ps -a | grep python | cut -b1-5', shell=True, executable='/bin/bash')
        pids = pids.rstrip('\n').split('\n')
        pids = map(lambda x: int(x.lstrip(' ').rstrip(' ')), pids)
        cpuUsage = []
        memUsage = []
        for pid in pids:
            proc = psutil.Process(pid)
            proc.get_cpu_percent(interval=None)
            cpu = proc.get_cpu_percent(interval=0.05)
            mem = proc.get_memory_percent()
            cpuUsage.append(cpu)
            memUsage.append(mem)
        output = open('logs/ctrlCpuMem.txt', 'a')
        print >>output, '{0:6f} {1} {2}'.format(time.time(), sum(cpuUsage), sum(memUsage))
        output.close()
    except Exception, msg:
        print msg

def main():
    cpuStream = (cpuMemQuery() >> \
                 MapStreamSet(sumCpuMem) >> \
                 MapStreamSet(CpuMemPrint))
    connStream = (connQuery() >> \
                  ReduceStreamSet(CalcTM, {}) >> \
                  MapStreamSet(LocalAgg) >> \
                  MapStreamSet(TpPrint))
    #stream = (MergeStreamsForSet(cpuStream, connStream) >> \
    stream = (cpuStream >> \
              MapStreamSet(HeartBeat) >> \
              MergeHosts() >> \
              MapStream(CtrlCpuMem))
    return stream


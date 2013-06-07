# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# measure cpu, memory, and throughput

from hone_lib import *
from subprocess import check_output
from math import *
import time
import psutil

def cpuMemQuery():
    q = (Select(['app','cpu','memory']) *
         From('AppStatus')*
         Where([('app','==','python')]) *
         Every(1000))
    return q

def SumCpuMem(table):
    cpuSum = []
    memSum = []
    for [app, cpu, mem] in table:
        cpuSum.append(cpu)
        memSum.append(mem)
    return (sum(cpuSum), sum(memSum))

def AgentCpuMem(cpuMem):
    (cpu, mem) = cpuMem
    output = open('logs/agentCpuMem.txt', 'a')
    print >>output, '{0:6f} {1} {2}'.format(time.time(), cpu, mem)
    output.close()
    return cpuMem

def HeartBeat(data):
    return 'HeartBeat'

def CtrlCpuMem(data):
    print data
    try:
        pids = check_output('ps -A | grep python | cut -b1-5', shell=True, executable='/bin/bash')
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
    agentCpuMemStream = (cpuMemQuery() >>
                         MapStreamSet(SumCpuMem) >>
                         MapStreamSet(AgentCpuMem))
    stream = (agentCpuMemStream >>
              MapStreamSet(HeartBeat) >>
              MergeHosts() >>
              MapStream(CtrlCpuMem))
    return stream

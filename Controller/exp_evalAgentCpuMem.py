# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Calculate cpu and memory usage of HONE agent

import time

from hone_lib import *

def CpuMemQuery():
    q = (Select(['app','cpu','memory']) *
         From('AppStatus') *
         Where([('app','==','python')]) *
         Every(1000))
    return q

def SumCpuMem(table):
    cpuSum = []
    memSum = []
    for [_, cpu, mem] in table:
        cpuSum.append(cpu)
        memSum.append(mem)
    return (sum(cpuSum), sum(memSum))

def HostPrint(cpuMem):
    cpuMemOutput = open('logs/agentCpuMem.txt', 'a')
    print >>cpuMemOutput, "{0:6f} {1} {2}".format(time.time(), cpuMem[0], cpuMem[1])
    cpuMemOutput.close()
    return cpuMem

def main():
    return (CpuMemQuery() >>
              MapStreamSet(SumCpuMem) >>
              MapStreamSet(HostPrint))

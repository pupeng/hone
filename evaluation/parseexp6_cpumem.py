# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# parse log files in exp6data to generate data for plotting

import sys
import math
import re
import os
import json

def average(x):
    return sum(x) / float(len(x))

def stddev(x):
    avg = average(x)
    var = map(lambda x: (x - avg)**2, x)
    return math.sqrt(average(var))

def GetDiff(x):
    x = map(lambda x : x * 1000.0, x)
    for i in reversed(range(1, len(x))):
        x[i] = x[i] - x[i-1]
    x[0] = 0.0
    return x

def accum(x, y):
    x.append(y)
    return x

def parse(number):
    logFile = open('exp6data/{0}/controller-cpumem.txt'.format(number), 'r')
    ctrlLogs = logFile.read().split('\n')
    logFile.close()
    # process controller logs
    ctrlLogs.pop(len(ctrlLogs) - 1)
    ctrlLogs = map(lambda x : x.split(' '), ctrlLogs)
    ctrlCpu = average(reduce(lambda x, y : accum(x, float(y[1])), ctrlLogs, []))
    ctrlMem = average(reduce(lambda x, y : accum(x, float(y[2])), ctrlLogs, []))
    # read in host data
    files = os.listdir('exp6data/{0}'.format(number))
    files = filter(lambda x : re.search("^host", x), files)
    files = filter(lambda x : re.search("cpumem", x), files)
    hostCpu = []
    hostMem = []
    for hostLog in files:
        logFile = open('exp6data/{0}/{1}'.format(number, hostLog), 'r')
        hostData = logFile.read().split('\n')
        logFile.close()
        hostData.pop(len(hostData) - 1)
        hostData = map(lambda x : x.split(' '), hostData)
        cpu = average(reduce(lambda x, y : accum(x, float(y[1])), hostData, []))
        mem = average(reduce(lambda x, y : accum(x, float(y[2])), hostData, []))
        hostCpu.append(cpu)
        hostMem.append(mem)
        print hostLog
    # output results
    outputFile = open('exp6data/cpumem_{0}.txt'.format(number), 'w')
    print >> outputFile, 'ctrl cpu:{0} mem:{1}'.format(ctrlCpu, ctrlMem)
    for i in range(len(hostCpu)):
        print >> outputFile, 'host cpu:{0} mem:{1}'.format(hostCpu[i], hostMem[i])
    print >> outputFile, 'host max cpu:{0} max mem:{1}'.format(max(hostCpu), max(hostMem))
    outputFile.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Give number of expected hosts'
        sys.exit(0)
    parse(int(sys.argv[1]))
# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# parse log files in exp4task2data to generate data for plotting

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
    x = map(lambda x : float(x) * 1000.0, x)
    for i in reversed(range(1, len(x))):
        x[i] = x[i] - x[i-1]
    x[0] = 0.0
    return x

def CtrlSequenceIsCorrect(entry):
    sequence = {}
    for event in entry:
        if event[0] == 'NewStatsIn':
            sequence[event[4]] = None
    if len(sequence.keys()) == 1:
        return True
    else:
        return False

def FilterHostData(data):
    if (data[0][0] == 'JobExecutionLoop') and (len(data) == 8):
        return True
    else:
        return False

def FilterCtrlDataByLength(numberOfHosts, data):
    if (numberOfHosts == 4) or (numberOfHosts == 16) or (numberOfHosts == 64):
        return len(data) == 12
    elif (numberOfHosts == 128) or (numberOfHosts == 32):
        return len(data) == 8
    else:
        return False

def parseTimestamp():
    logFile = open('exp4task2data/controller-timestamp.log', 'r')
    logs = logFile.read().split('\n')
    logFile.close()
    # process controller logs
    logs.pop(len(logs) - 1)
    logs = map(lambda x: x.split(','), logs)
    ctrlLogs = filter(lambda x : x[0] == 'ControllerExecution', logs)
    ctrlLogs = map(lambda x : x[1].split('#'), ctrlLogs)
    ctrlLogs = map(lambda y: map(lambda x : x.split('$'), y), ctrlLogs)
    ctrlLogs = filter(lambda x : len(x) == 6, ctrlLogs)
    netLogs = filter(lambda x : x[0] == 'NetworkRun', logs)
    netLogs = map(lambda x : x[1].split('#'), netLogs)
    netLogs = map(lambda y : map(lambda x : x.split('$'), y), netLogs)
    netLogs = filter(lambda x : len(x) == 10, netLogs)
    # process controller data
    timeSeries = {}
    for log in ctrlLogs:
        flowId = log[1][3]
        sequence = log[1][4]
        if sequence not in timeSeries:
            timeSeries[sequence] = {}
        timeSeries[sequence][flowId] = [log[1][1], log[5][1]]
    # process network data
    for log in netLogs:
        sequence = log[3][4]
        temp = []
        for i in [0, 2, 5, 6, 9]:
            temp.append(log[i][1])
        if sequence in timeSeries:
            timeSeries[sequence]['network'] = temp
    # remove incomplete data
    sequenceToRemove = []
    for sequence, data in timeSeries.iteritems():
        if ('network' not in data) or ('6' not in data) or ('7' not in data):
            sequenceToRemove.append(sequence)
    for sequence in sequenceToRemove:
        del timeSeries[sequence]
    results = []
    for sequence, data in timeSeries.iteritems():
        network = float(data['network'][4]) * 1000.0 - float(data['network'][0]) * 1000.0
        controller = max(map(lambda x : float(x[1]) * 1000.0 - float(x[0]) * 1000.0, [data['6'], data['7']]))
        e2e = float(data['7'][1]) * 1000.0 - float(data['network'][0]) * 1000.0
        results.append([network, controller, e2e])
    # output results
    outputFile = open('exp4task2data/latency.txt', 'w')
    for result in results:
        print >> outputFile, '{0} {1} {2}'.format(result[0], result[1], result[2])
    outputFile.close()

def parseCpuMem():
    logFile = open('exp4task2data/ctrlCpuMem.txt', 'r')
    ctrlCpuMem = logFile.read().split('\n')
    logFile.close()
    ctrlCpuMem.pop(len(ctrlCpuMem) - 1)
    ctrlCpuMem = map(lambda x : x.split(' '), ctrlCpuMem)
    cpuSum = []
    memSum = []
    for (_, cpu, mem) in ctrlCpuMem:
        cpuSum.append(float(cpu))
        memSum.append(float(mem))
    print 'controller cpu:{0} mem:{1}'.format(average(cpuSum), average(memSum))

if __name__ == '__main__':
    parseTimestamp()
    parseCpuMem()

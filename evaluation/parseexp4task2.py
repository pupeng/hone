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
    ctrlLogs = filter(lambda x : len(x) == 5, ctrlLogs)
    netLogs = filter(lambda x : x[0] == 'NetworkRun', logs)
    netLogs = map(lambda x : x[1].split('#'), netLogs)
    netLogs = map(lambda y : map(lambda x : x.split('$'), y), netLogs)
    netLogs = filter(lambda x : len(x) == 10, netLogs)
    # get the sequence numbers and the controller data
    timeSeries = {}
    for log in ctrlLogs:
        sequence = log[1][4]
        timeSeries[sequence] = {}
        temp = []
        for i in [1, 4]:
            temp.append(log[i][1])
        timeSeries[sequence]['controller'] = temp
    # process network data
    for log in netLogs:
        try:
            sequence = log[3][4]
            temp = []
            for i in [0, ]
    files = os.listdir('exp4task1data')
    files = filter(lambda x : re.search("^host", x) and re.search("log$", x), files)
    for hostLog in files:
        logFile = open('exp4task1data/{0}'.format(hostLog))
        hostData = logFile.read().split('\n')
        logFile.close()
        hostData.pop(len(hostData) - 1)
        hostData = map(lambda x : x.split(','), hostData)
        hostData = map(lambda x : [x[0]] + x[1].split('#'), hostData)
        hostData = map(lambda y: map(lambda x : x.split('$'), y), hostData)
        hostData = filter(FilterHostData, hostData)
        for data in hostData:
            temp = []
            try:
                sequence = data[7][4]
            except Exception:
                continue
            for i in [1, 4, 5, 7]:
                temp.append(data[i][1])
            if sequence in timeSeries:
                if 'host' not in timeSeries[sequence]:
                    timeSeries[sequence]['host'] = []
                timeSeries[sequence]['host'].append(temp)
        print hostLog
    # calculate e2e latency
    sequenceToRemove = []
    for sequence, data in timeSeries.iteritems():
        if ('host' not in data) or (len(data['host']) != 4):
            sequenceToRemove.append(sequence)
    for sequence in sequenceToRemove:
        del timeSeries[sequence]
    results = []
    for sequence, data in timeSeries.iteritems():
        firstHostStart = min(map(lambda x : x[0], data['host']))
        lastHostEnd = max(map(lambda x : x[3], data['host']))
        lastStatsIn = data['controller'][3]
        controllerDone = data['controller'][5]
        results.append([firstHostStart, lastHostEnd, lastStatsIn, controllerDone])
    results = map(GetDiff, results)
    # output results
    outputFile = open('exp4task1data/breakdown.txt', 'w')
    for result in results:
        print >> outputFile, '{0} {1} {2}'.format(result[1], result[2], result[3])
    outputFile.close()

def parseCpuMem():
    logFile = open('exp4task1data/ctrlCpuMem.txt', 'r')
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
    files = os.listdir('exp4task1data')
    files = filter(lambda x : re.search("^host", x) and re.search("txt$", x), files)
    cpuSum = []
    memSum = []
    for hostLog in files:
        logFile = open('exp4task1data/{0}'.format(hostLog))
        hostData = logFile.read().split('\n')
        logFile.close()
        hostData.pop(len(hostData) - 1)
        hostData = map(lambda x : x.split(' '), hostData)
        for (_, cpu, mem) in hostData:
            cpuSum.append(float(cpu))
            memSum.append(float(mem))
        print hostLog
    print 'agent cpu:{0} mem:{1}'.format(average(cpuSum), average(memSum))

if __name__ == '__main__':
    parseTimestamp()
    parseCpuMem()

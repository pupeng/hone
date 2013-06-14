# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# parse log files in exp4task1data to generate data for plotting

import sys
import math
import re
import os
import json

def average(x):
    x = map(lambda x : float(x), x)
    return sum(x) / float(len(x))

def stddev(x):
    avg = average(x)
    var = map(lambda x: (x - avg)**2, x)
    return math.sqrt(average(var))

def GetDiff(x):
    x = map(lambda x : float(x) * 1000.0, x)
    ret = []
    ret.append(max(x[1] - x[0], x[3] - x[2]))
    ret.append(x[4] - min(x[3], x[1]))
    ret.append(x[5] - x[4])
    ret.append(x[5] - x[0])
    return ret

def CtrlSequenceIsCorrect(entry):
    numbers = {'12':10, '9':6, '7':10}
    sequence = {}
    for event in entry:
        if event[0] == 'NewStatsIn':
            sequence[event[4]] = None
    if len(sequence.keys()) == 1:
        flowIds = {}
        for event in entry:
            if event[0] == 'NewStatsIn':
                flowIds[event[3]] = None
        if len(flowIds.keys()) == 1:
            flowId = flowIds.keys()[0]
            if len(entry) == numbers[flowId]:
                return True
            else:
                return False
        else:
            return False
    else:
        return False

def FilterHostData(data):
    if (data[0][0] == 'JobExecutionLoop') and (len(data) == 12):
        return True
    else:
        return False

def parseTimestamp():
    logFile = open('exp4task3data/controller-timestamp.log', 'r')
    logs = logFile.read().split('\n')
    logFile.close()
    # process controller logs
    logs.pop(len(logs) - 1)
    logs = map(lambda x: x.split(','), logs)
    ctrlLogs = filter(lambda x : x[0] == 'ControllerExecution', logs)
    ctrlLogs = map(lambda x : x[1].split('#'), ctrlLogs)
    ctrlLogs = map(lambda y: map(lambda x : x.split('$'), y), ctrlLogs)
    x = ctrlLogs[1:400]
    ctrlLogs = filter(CtrlSequenceIsCorrect, ctrlLogs)
    netLogs = filter(lambda x : x[0] == 'NetworkRun', logs)
    netLogs = map(lambda x : x[1].split('#'), netLogs)
    netLogs = map(lambda y : map(lambda x : x.split('$'), y), netLogs)
    netLogs = filter(lambda x : len(x) == 6, netLogs)
    # get the sequence numbers and the controller data
    timeSeries = {}
    for log in ctrlLogs:
        sequence = log[1][4]
        if sequence not in timeSeries:
            timeSeries[sequence] = {}
        flowId = log[1][3]
        if flowId == '7':
            temp = []
            for i in [1, 3, 5, 7, 9]:
                temp.append(log[i][1])
            timeSeries[sequence]['hostinfo'] = temp
        elif flowId == '9':
            temp = []
            for i in [1, 3, 5]:
                temp.append(log[i][1])
            timeSeries[sequence]['network'] = temp
        elif flowId == '12':
            temp = []
            for i in [1, 3, 5, 7, 9]:
                temp.append(log[i][1])
            timeSeries[sequence]['conn'] = temp
    for log in netLogs:
        sequence = log[3][4]
        if sequence not in timeSeries:
            timeSeries[sequence] = {}
        temp = []
        for i in [0, 3, 4, 5]:
            temp.append(log[i][1])
        timeSeries[sequence]['netRun'] = temp
    # read in host data
    files = os.listdir('exp4task3data')
    files = filter(lambda x : re.search("^host", x) and re.search("log$", x), files)
    for hostLog in files:
        logFile = open('exp4task3data/{0}'.format(hostLog))
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
                sequence = data[11][4]
            except Exception:
                continue
            for event in data:
                if event[0] == 'Begin' or (event[0] == 'DoneToCtrl' and event[2] != '101'):
                    temp.append(event[1])
            if sequence in timeSeries:
                if 'host' not in timeSeries[sequence]:
                    timeSeries[sequence]['host'] = []
                timeSeries[sequence]['host'].append(temp)
        print hostLog
    # calculate e2e latency
    sequenceToRemove = []
    for sequence, data in timeSeries.iteritems():
        if ('host' not in data) or (len(data['host']) != 3) or ('hostinfo' not in data) or ('netRun' not in data) or ('network' not in data) or ('conn' not in data):
            sequenceToRemove.append(sequence)
    for sequence in sequenceToRemove:
        del timeSeries[sequence]
    results = []
    for sequence, data in timeSeries.iteritems():
        firstHostStart = min(map(lambda x : x[0], data['host']))
        lastHostEnd = max(map(lambda x : x[2], data['host']))
        netRunStart = data['netRun'][0]
        netRunEnd = data['netRun'][2]
        lastStatsIn = average([data['hostinfo'][2], data['network'][0], data['conn'][2]])
        controllerDone = max([data['hostinfo'][4], data['network'][2], data['conn'][4]])
        results.append([firstHostStart, lastHostEnd, netRunStart, netRunEnd, lastStatsIn, controllerDone])
    results = map(GetDiff, results)
    # output results
    outputFile = open('exp4task3data/breakdown.txt', 'w')
    for result in results:
        print >> outputFile, '{0} {1} {2} {3}'.format(result[0], result[1], result[2], result[3])
    outputFile.close()

def parseCpuMem():
    logFile = open('exp4task3data/ctrlCpuMem.txt', 'r')
    ctrlCpuMem = logFile.read().split('\n')
    logFile.close()
    ctrlCpuMem.pop(len(ctrlCpuMem) - 1)
    ctrlCpuMem = map(lambda x : x.split(' '), ctrlCpuMem)
    cpuSum = []
    memSum = []
    for (_, cpu, mem) in ctrlCpuMem:
        cpuSum.append(float(cpu))
        memSum.append(float(mem))
    print 'controller cpu:{0} mem:{1}'.format(average(cpuSum) / 2.0, average(memSum) * 12.0 / 30.0)
    files = os.listdir('exp4task3data')
    files = filter(lambda x : re.search("^host", x) and re.search("txt$", x), files)
    cpuSum = []
    memSum = []
    for hostLog in files:
        logFile = open('exp4task3data/{0}'.format(hostLog))
        hostData = logFile.read().split('\n')
        logFile.close()
        hostData.pop(len(hostData) - 1)
        hostData = map(lambda x : x.split(' '), hostData)
        for (_, cpu, mem) in hostData:
            cpuSum.append(float(cpu))
            memSum.append(float(mem))
        print hostLog
    print 'agent cpu:{0} mem:{1}'.format(average(cpuSum) / 2.0, average(memSum) * 12.0 / 30.0)

if __name__ == '__main__':
    parseTimestamp()
    parseCpuMem()

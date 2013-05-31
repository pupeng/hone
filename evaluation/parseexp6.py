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

def GetLatency(x):
    a = x[len(x)-6] - x[0] + x[len(x)-1] - x[len(x)-3]
    return a

def CtrlSequenceIsCorrect(entry):
    sequence = {}
    for event in entry:
        if event[0] == 'NewStatsIn':
            sequence[event[4]] = None
    if len(sequence.keys()) == 1:
        return True
    else:
        return False

def MiddleSeqIsCorrect(entry):
    sequence = {}
    for event in entry:
        if event[0] == 'NewMiddleStats':
            sequence[event[5]] = None
    if len(sequence.keys()) == 1:
        return True
    else:
        return False

def FilterHostData(data):
    if (data[0][0] == 'JobExecutionLoop') and (len(data) == 8):
        return True
    elif (data[0][0] == 'MiddleExecution') and (len(data) == 8):
        return MiddleSeqIsCorrect(data)
    else:
        return False

def parse(number):
    logFile = open('exp6data/{0}/controller-temp.log'.format(number), 'r')
    ctrlLogs = logFile.read().split('\n')
    logFile.close()
    # process controller logs
    ctrlLogs.pop(len(ctrlLogs) - 1)
    ctrlLogs = map(lambda x: x.split(','), ctrlLogs)
    ctrlLogs = filter(lambda x : x[0] == 'ControllerExecution', ctrlLogs)
    ctrlLogs = map(lambda x : x[1].split('#'), ctrlLogs)
    ctrlLogs = map(lambda y: map(lambda x : x.split('$'), y), ctrlLogs)
    ctrlLogs = filter(lambda x : len(x) == 12, ctrlLogs)
    ctrlLogs = filter(CtrlSequenceIsCorrect, ctrlLogs)
    # get the sequence numbers and the controller data
    timeSeries = {}
    for log in ctrlLogs:
        sequence = log[1][4]
        timeSeries[sequence] = {}
        temp = []
        for i in range(1, len(log), 2):
            temp.append(log[i][1])
        timeSeries[sequence]['controller'] = temp
    # read in host data
    files = os.listdir('exp6data/{0}'.format(number))
    files = filter(lambda x : re.search("^host", x), files)
    for hostLog in files:
        logFile = open('exp6data/{0}/{1}'.format(number, hostLog), 'r')
        hostData = logFile.read().split('\n')
        logFile.close()
        hostData.pop(len(hostData) - 1)
        hostData = map(lambda x : x.split(','), hostData)
        hostData = map(lambda x : [x[0]] + x[1].split('#'), hostData)
        hostData = map(lambda y: map(lambda x : x.split('$'), y), hostData)
        hostData = filter(FilterHostData, hostData)
        for data in hostData:
            if data[0][0] == 'JobExecutionLoop':
                temp = []
                sequence = data[7][4]
                for i in [1, 4, 5, 7]:
                    temp.append(data[i][1])
                if sequence in timeSeries:
                    if 'source' not in timeSeries[sequence]:
                        timeSeries[sequence]['source'] = []
                    timeSeries[sequence]['source'].append(temp)
            elif data[0][0] == 'MiddleExecution':
                temp = []
                sequence = data[6][5]
                level = data[6][4]
                for i in [2, 3, 4, 5, 6, 7]:
                    temp.append(data[i][1])
                if sequence in timeSeries:
                    if level not in timeSeries[sequence]:
                        timeSeries[sequence][level] = []
                    timeSeries[sequence][level].append(temp)
        print hostLog
    # calculate e2e latency
    totalLevels = int(math.log(number, 4))
    sequenceToRemove = []
    for sequence, data in timeSeries.iteritems():
        checkResult = True
        if ('source' not in data) or (len(data['source']) != number):
            checkResult = False
        for i in range(totalLevels - 1):
            expectNum = int(math.pow(4, totalLevels - 1 - i))
            if (str(i) not in data) or (len(data[str(i)]) != expectNum):
                checkResult = False
        if not checkResult:
            sequenceToRemove.append(sequence)
    for sequence in sequenceToRemove:
        del timeSeries[sequence]
    results = []
    for sequence, data in timeSeries.iteritems():
        firstHostStart = min(map(lambda x : x[0], data['source']))
        ctrlDone = data['controller'][5]
        results.append(float(ctrlDone) * 1000.0 - float(firstHostStart) * 1000.0)
    # output results
    outputFile = open('exp6data/treemerge_{0}.txt'.format(number), 'w')
    for latency in results:
        print >> outputFile, latency
    outputFile.close()
    outputFile = open('exp6data/alldata_{0}.txt'.format(number), 'w')
    print >> outputFile, json.dumps(timeSeries, sort_keys=True, indent=2)
    outputFile.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Give number of expected hosts'
        sys.exit(0)
    parse(int(sys.argv[1]))

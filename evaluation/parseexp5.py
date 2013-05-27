# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# parse log files in exp5data to generate data for plotting

import sys
import math

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

def parse(number):
    logFile = open('exp5data/{0}/controller-temp.log'.format(number), 'r')
    ctrlLogs = logFile.read().split('\n')
    logFile.close()
    # preprocess controller logs
    ctrlLogs.pop(len(ctrlLogs) - 1)
    ctrlLogs = map(lambda x: x.split(','), ctrlLogs)
    ctrlLogs = filter(lambda x : x[0] == 'ControllerExecution', ctrlLogs)
    ctrlLogs = map(lambda x : x[1].split('#'), ctrlLogs)
    ctrlLogs = map(lambda y: map(lambda x : x.split('$'), y), ctrlLogs)
    ctrlLogs = filter(lambda x : len(x) == 12, ctrlLogs)
    mergeTime = []
    for log in ctrlLogs:
        log.pop(0)
        result = map(lambda x : float(x[1]), log)
        sequence = int(log[0][4])
        timeDiff = result[8] * 1000.0 - result[0] * 1000.0
        mergeTime.append((sequence, timeDiff))
    # output the data
    output = open('exp5data/mergehosts_{0}.txt'.format(number), 'w')
    for (sequence, timeDiff) in mergeTime:
        print >>output, '{0} {1}'.format(sequence, timeDiff)
    output.close()
#    for log in ctrlLogs:
#        timestamp = map(lambda x: float(x.split('$')[3])*1000, log)
#        latency = GetLatency(timestamp)
#        timestamp = GetDiff(timestamp)
#        line = ''
#        for x in timestamp:
#            line += '{0:3f} '.format(x)
#        print >>output, line
#        print >>output2, latency
#    output.close()
#    output2.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Give number of expected hosts'
        sys.exit()
    parse(sys.argv[1])

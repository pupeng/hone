# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# parse log files in exp2data to generate data for plotting

import sys
import math

def average(x):
    return sum(x) / float(len(x))

def stddev(x):
    avg = average(x)
    var = map(lambda x: (x - avg)**2, x)
    return math.sqrt(average(var))

def removePrefix(x):
    x = x.split('$')
    return x[len(x)-1]

def parse(testname):
    logFileName = 'exp2data/' + testname + '.log'
    logFile = open(logFileName)
    logs = logFile.read().split('\n')
    logFile.close()
    logs.pop(len(logs) - 1)
    logs = map(lambda x: x.split(','), logs)
    logs = map(lambda x: [x[1], x[2]], logs)
    logs = map(lambda x: [x[0]] + x[1].split('#'), logs)
    logs = filter(lambda x: len(x) > 2, logs)
    results = []
    for log in logs:
        log = map(removePrefix, log)
        event = int(log[0])
        if event == 127:
            results.append((float(log[len(log)-1])*1000 - float(log[1])*1000))
    print "total number: {0}".format(len(results))
    print "avg:{0} min:{1} max:{2} std:{3}".format(average(results), \
          min(results), max(results), stddev(results))
    output = open('exp2data/result_' + testname + '.txt', 'w')
    print >>output, "{0} {1} {2} {3}".format(average(results), \
                    min(results), max(results), stddev(results))
    output.close()

def main():
    testname = sys.argv[1]
    parse(testname)

if __name__ == '__main__':
    main()

'''
Peng Sun
parse log files to generate data for plotting
'''
import sys
import math

def average(x):
    return sum(x) / float(len(x))

def stddev(x):
    avg = average(x)
    var = map(lambda x: (x - avg)**2, x)
    return math.sqrt(average(var))

def parseForConnMeasureOverhead(logFileName):
    logFile = open(logFileName)
    logs = logFile.read().split('\n')
    logFile.close()
    context = logFileName.split('/')
    context = context[len(context) - 1]
    context = context.split('.')
    context = context[len(context) - 2]
    logs.pop(len(logs) - 1)
    logs = map(lambda x: x.split(','), logs)
    logs = map(lambda x: [x[1], x[2]], logs)
    logs = map(lambda x: [x[0]] + x[1].split('#'), logs)
    logs = filter(lambda x: len(x) == 5, logs)
    results = []
    for log in logs:
        event = int(log[0])
        if event == 114:
            result = []
            for timestamp in log[1:]:
                if (len(timestamp.split('$')) == 1):
                    result.append(float(timestamp))
                else:
                    result.append(float(timestamp.split('$')[1]))
            results.append(result)            
    results = map(getDifference, results)
    prepare = map(lambda x: x[1], results)
    measure = map(lambda x: x[2], results)
    distrib = map(lambda x: x[3], results)
    print "total number: {0}".format(len(results))
    print "avg:{0} min:{1} max:{2} std:{3}".format(average(prepare), \
          min(prepare), max(prepare), stddev(prepare))
    print "avg:{0} min:{1} max:{2} std:{3}".format(average(measure), \
          min(measure), max(measure), stddev(measure))
    print "avg:{0} min:{1} max:{2} std:{3}".format(average(distrib), \
          min(distrib), max(distrib), stddev(distrib))
    output = open('exp1/connOverhead_' + context + '.txt', 'w')
    for result in results:
        print >>output, '{0} {1} {2} {3}'.format(result[0], result[1], result[2], result[3])
    output.close()

def getDifference(x):
    x = map(lambda x: x*1000, x)
    for i in reversed(range(1, len(x))):
        x[i] = x[i] - x[i - 1]
    x[0] = 0.0
    return x

def main():
    logFileName = sys.argv[1]
    parseForConnMeasureOverhead(logFileName)

if __name__ == '__main__':
    main()

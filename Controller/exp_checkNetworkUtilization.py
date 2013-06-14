# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# calculate the link utilization of current network

from hone_lib import *

def LinkQuery():
    return (Select(['BeginDevice', 'BeginPort', 'EndDevice', 'EndPort']) *
            From('LinkStatus') *
            Every(2000))

def SwitchStatsQuery():
    return (Select(['switchId', 'portNumber', 'transmitBytes', 'receiveBytes', 'capacity', 'timestamp']) *
            From('SwitchStatus') *
            Every(2000))

def JoinTables(x):
    (links, switchStats) = x
    links = links[0]
    switchStats = switchStats[0]
    switchDataDict = {}
    for (device, port, transmitBytes, receiveBytes, capacity, timestamp) in switchStats:
        if device not in switchDataDict:
            switchDataDict[device] = {}
        switchDataDict[device][port] = [(transmitBytes + receiveBytes), capacity, timestamp]
    results = {}
    for (deviceA, portA, deviceB, portB) in links:
        results[(deviceA, portA, deviceB, portB)] = switchDataDict[deviceB][portB]
    return results

def CalculateRate(newData, tpData):
    for link, linkStats in newData.iteritems():
        (newAccumBytes, capacity, newTimestamp) = linkStats
        if link in tpData:
            (lastTimestamp, lastAccumBytes, lastRate, _) = tpData[link]
            if newTimestamp > lastTimestamp:
                newRate = float((newAccumBytes - lastAccumBytes) / (newTimestamp - lastTimestamp))
            else:
                newRate = 0.0
        else:
            newRate = float(newAccumBytes) / float(newTimestamp)
        tpData[link] = (newTimestamp, newAccumBytes, newRate, capacity)
    return tpData

def DisplayUtilization(x):
    for link, linkRate in x.iteritems():
        (_, _, rate, capacity) = linkRate
        print 'link {0}'.format(link)
        print 'Rate:{0} Kbps Capacity:{1} Kbps Utilization:{2}%'.format(rate * 8.0 / 1000.0, capacity, rate * 8.0 / 10.0 / capacity)
        print '***************************************************'
    print '###############################\n\n'

def main():
    stream = MergeStreams(LinkQuery(), SwitchStatsQuery())
    stream = stream >> MapStream(JoinTables)
    stream = stream >> ReduceStream(CalculateRate, {})
    stream = stream >> MapStream(DisplayUtilization)
    return stream

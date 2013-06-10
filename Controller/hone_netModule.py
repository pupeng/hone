# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_netModule.py
# Network module of HONE to communicate with the network
# Provide a concrete implementation to chat with a FloodLight controller

import json
import multiprocessing
import logging
import time
import heapq
import sys
import traceback
import urllib2
from threading import Timer, Thread

import hone_freLib as freLib
from hone_util import *
from hone_message import *
from hone_sndModule import NetToControllerSndSocket

FloodlightAddress = '127.0.0.1'
FloodlightPort = 8080

class JobFlowMinQueue:
    def __init__(self):
        self.queue = []

    def push(self, deadline, jobFlowKey):
        heapq.heappush(self.queue, (deadline, jobFlowKey))

    def pop(self):
        (_, jobFlowKey) = heapq.heappop(self.queue)
        return jobFlowKey

    def isEmpty(self):
        if self.queue:
            return False
        return True

    def minDeadline(self):
        if self.isEmpty():
            return None
        return self.queue[0][0]

    def debug(self):
        return self.queue

minRunInterval = 15.0 # seconds
netJobTable = {}
jobFlowQueue = JobFlowMinQueue()
stopSchedule = False
eventAndGoFunc = {}

class NetworkJob:
    def __init__(self, jobId, flowId, progName, createTime, exePlan):
        self.jobId = jobId
        self.flowId = flowId
        self.progName = progName
        self.createTime = createTime
        self.query = exePlan[0]
        self.computePart = exePlan[1:]
        self.period = self.query.ev
        self.deadline = None
        self.lastSequence = None
        if self.query.ft == 'LinkStatus':
            self.measureType = 'link'
        elif self.query.ft == 'SwitchStatus':
            self.measureType = 'switch'
        elif self.query.ft == 'HostRoute':
            self.measureType = 'route'
        self.measureStats = self.query.se


    def updateDeadline(self):
        self.deadline += self.period / 1000
        self.lastSequence += 1

class NetworkModuleProcess(multiprocessing.Process):
    def __init__(self, passedNetworkJobQueue):
        super(NetworkModuleProcess, self).__init__()
        self._stopEvent = multiprocessing.Event()
        self.networkJobQueue = passedNetworkJobQueue

    def stop(self):
        self._stopEvent.set()

    def shouldStop(self):
        return self._stopEvent.is_set()

    def run(self):
        logging.info('network module starts')
        print 'hone network module starts to run.'
        global minRunInterval
        scheduleLoop = Timer(minRunInterval, scheduleLoopRun)
        scheduleLoop.start()
        time.sleep(minRunInterval / 2.0)
        while not self.shouldStop():
            currentTime = time.time()
            while not self.networkJobQueue.empty():
                (itemType, itemContent) = self.networkJobQueue.get_nowait()
                if itemType == 'NewNetworkJob':
                    (jobId, createTime, progName, exeFlow) = itemContent
                    LogUtil.DebugLog('network', 'new network job', jobId, createTime, progName, exeFlow)
                    for eachFlow in exeFlow:
                        newJob = NetworkJob(jobId, eachFlow.flowId, progName, createTime, eachFlow.exePlan)
                        registerNetworkJob(newJob)
                        key = ComposeKey(newJob.jobId, newJob.flowId)
                        if (len(netJobTable) == 0) or (minRunInterval > (newJob.period / 1000.0)):
                            minRunInterval = newJob.period / 1000.0
                        alreadyPassedPeriods = int(float(currentTime - newJob.createTime) / float(newJob.period / 1000.0))
                        newJob.deadline = newJob.createTime + newJob.period / 1000.0 * (alreadyPassedPeriods + 1)
                        newJob.lastSequence = alreadyPassedPeriods
                        jobFlowQueue.push(newJob.deadline, key)
                        netJobTable[key] = newJob
            LogUtil.DebugLog('network', 'netModule is alive')
            time.sleep(minRunInterval)
        global stopSchedule
        stopSchedule = True
        time.sleep(minRunInterval * 2)
        logging.info('netModule exits')
        print 'network module exits'

def registerNetworkJob(netJob):
    (e, go) = freLib.RawEvent()
    try:
        if not sys.modules.has_key(netJob.progName):
            __import__(netJob.progName)
    except ImportError, msg:
        logging.warning('hone application is invalid {0}'.format(netJob.progName))
        return
    for i in range(len(netJob.computePart)):
        operator = netJob.computePart[i]
        opType = operator[0]
        if opType == 'NetworkToController':
            opFunc = NetworkToController(netJob.jobId, netJob.flowId)
            e = e >> opFunc
        else:
            raise Exception('Only NetworkToController is supported now!')
    global eventAndGoFunc
    if netJob.jobId not in eventAndGoFunc:
        eventAndGoFunc[netJob.jobId] = {}
    eventAndGoFunc[netJob.jobId][netJob.flowId] = (e, go)

def scheduleLoopRun():
    LogUtil.DebugLog('network', 'a new schedule loop starts at {0}'.format(time.time()))
    if stopSchedule:
        logging.info('schedule loop should stop now')
        return
    currentTime = time.time()
    if jobFlowQueue.minDeadline():
        delta = currentTime - jobFlowQueue.minDeadline() - 0.1
    else:
        delta = 0.0
    if delta < minRunInterval:
        nextLoop = Timer((minRunInterval - delta), scheduleLoopRun)
    else:
        nextLoop = Timer(minRunInterval, scheduleLoopRun)
    nextLoop.start()
    # schedule job to run
    jobFlowToRun = []
    linkJobFlow = []
    switchJobFlow = []
    routeJobFlow = []
    while (not jobFlowQueue.isEmpty()) and (jobFlowQueue.minDeadline() < currentTime):
        jobFlowKey = jobFlowQueue.pop()
        jobFlowToRun.append(jobFlowKey)
        measureType = netJobTable[jobFlowKey].measureType
        if measureType == 'link':
            linkJobFlow.append(jobFlowKey)
        elif measureType == 'switch':
            switchJobFlow.append(jobFlowKey)
        elif measureType == 'route':
            routeJobFlow.append(jobFlowKey)
        netJobTable[jobFlowKey].updateDeadline()
        jobFlowQueue.push(netJobTable[jobFlowKey].deadline, jobFlowKey)
    LogUtil.DebugLog('network', 'network job flow to run', jobFlowToRun)
    if linkJobFlow:
        linkThread = Thread(target=linkMeasureRun, args=(linkJobFlow, None))
        linkThread.daemon = True
        linkThread.start()
    if switchJobFlow:
        switchThread = Thread(target=switchMeasureRun, args=(switchJobFlow, None))
        switchThread.daemon = True
        switchThread.start()
    if routeJobFlow:
        routeThread = Thread(target=routeMeasureRun, args=(routeJobFlow, None))
        routeThread.daemon = True
        routeThread.start()

FeatureToCapacity = { 192 : 10000000,
                      64  : 10000000,
                      0   : 10000} #Kbps
def DecodeCapacity(feature):
    return FeatureToCapacity[feature]

link_stats_location = {'BeginDevice' : 0,
                       'BeginPort' : 1,
                       'EndDevice' : 2,
                       'EndPort' : 3}
def linkMeasureRun(jobFlowToM, nothing):
    links = GetLinks()
    for jobFlow in jobFlowToM:
        (jobId, flowId) = DecomposeKey(jobFlow)
        netJob = netJobTable[jobFlow]
        results = []
        for link in links:
            result = []
            for name in netJob.measureStats:
                result.append(link[link_stats_location[name]])
            results.append(result)
        if results:
            (_, goFunc) = eventAndGoFunc[jobId][flowId]
            goThread = Thread(target=runGo, args=(goFunc, results, jobId, flowId))
            goThread.daemon = True
            goThread.start()

def switchMeasureRun(jobFlowToM, nothing):
    switchStats = GetSwitchStats('all', 'port')
    for jobFlow in jobFlowToM:
        (jobId, flowId) = DecomposeKey(jobFlow)
        netJob = netJobTable[jobFlow]
        if 'capacity' in netJob.measureStats:
            switchFeatures = GetSwitchStats('all', 'features')
            capacity = {}
            for switchId, features in switchFeatures.iteritems():
                if switchId not in capacity:
                    capacity[switchId] = {}
                for portFeature in features['ports']:
                    capacity[switchId][portFeature['portNumber']] = DecodeCapacity(portFeature['currentFeatures'])
        results = []
        for switchId, stats in switchStats.iteritems():
            for portStat in stats:
                result = []
                for name in netJob.measureStats:
                    if name == 'switchId':
                        result.append(switchId)
                    elif name == 'capacity':
                        result.append(capacity[switchId][portStat['portNumber']])
                    else:
                        result.append(portStat[name])
                results.append(result)
        if results:
            (_, goFunc) = eventAndGoFunc[jobId][flowId]
            goThread = Thread(target=runGo, args=(goFunc, results, jobId, flowId))
            goThread.daemon = True
            goThread.start()

def routeMeasureRun(jobFlowToM, nothing):
    hosts = {}
    url = ComposeUrl('device/')
    hostLinks = GetJsonFromUrl(url)
    for link in hostLinks:
        attachDevices = link['attachmentPoint']
        if attachDevices:
            hostId = str(link['mac'][0]).translate(None, ':')
            for device in attachDevices:
                switchId = str(device['switchDPID'])
                switchPort = device['port']
                hosts[hostId] = (switchId, switchPort)
    allHostIds = sorted(hosts.keys())
    routes = []
    for i in range(len(allHostIds) - 1):
        hostA = allHostIds[i]
        for j in range(i+1, len(allHostIds)):
            hostB = allHostIds[j]
            route = GetRoute(hosts[hostA][0], hosts[hostA][1], hosts[hostB][0], hosts[hostB][1])
            routes.append([hostA, hostB, route])
    for jobFlow in jobFlowToM:
        (jobId, flowId) = DecomposeKey(jobFlow)
        (_, goFunc) = eventAndGoFunc[jobId][flowId]
        goThread = Thread(target=runGo, args=(goFunc, routes, jobId, flowId))
        goThread.daemon = True
        goThread.start()

def runGo(goFunc, data, jobId, flowId):
    try:
        goFunc(data)
    except Exception, msg:
        logging.warning('go thread of jobId {0} flowId {1} caught exception {2}'.format(jobId, flowId, msg))
        print 'go thread caught exception'
        print msg
        traceback.print_exc()
    finally:
        pass

def NetworkToController(jobId, flowId):
    def push(x):
        if x:
            key = ComposeKey(jobId, flowId)
            sequence = netJobTable[key].lastSequence
            message = HoneMessage()
            message.messageType = HoneMessageType_NetworkStatsIn
            message.hostId = 'network'
            message.jobId = jobId
            message.flowId = flowId
            message.sequence = sequence
            message.content = x
            NetToControllerSndSocket().sendMessage(message)
    return freLib.FListener(push=push)

def GetJsonFromUrl(url):
    try:
        result = json.loads(urllib2.urlopen(url).read())
    except Exception, msg:
        logging.warning('Exception {0}'.format(msg))
        result = None
    finally:
        return result

def ComposeUrl(suffix):
    return 'http://{0}:{1}/wm/{2}'.format(FloodlightAddress, FloodlightPort, suffix)

def GetLinks():
    links = []
    url = ComposeUrl('topology/links/json')
    switchLinks = GetJsonFromUrl(url)
    if switchLinks:
        for link in switchLinks:
            sswitch = str(link['src-switch'])
            sport = link['src-port']
            dswitch = str(link['dst-switch'])
            dport = link['dst-port']
            links.append([sswitch, sport, dswitch, dport])
    url = ComposeUrl('device/')
    hostLinks = GetJsonFromUrl(url)
    if hostLinks:
        for link in hostLinks:
            attachDevices = link['attachmentPoint']
            if attachDevices:
                mac = str(link['mac'][0]).translate(None, ':')
                for device in attachDevices:
                    deviceId = str(device['switchDPID'])
                    devicePort = device['port']
                    links.append([mac, None, deviceId, devicePort])
    return links

def GetSwitchStats(switchId, statsType):
    url = ComposeUrl('core/switch/{0}/{1}/json'.format(switchId, statsType))
    stats = GetJsonFromUrl(url)
    if switchId == 'all':
        return stats
    else:
        return stats[switchId]

def GetRoute(switchIdA, portA, switchIdB, portB):
    url = ComposeUrl('topology/route/{0}/{1}/{2}/{3}/json'.format(switchIdA, portA, switchIdB, portB))
    routes = GetJsonFromUrl(url)
    return routes

def GetSwitchProperties():
    url = ComposeUrl('core/controller/switches/json')
    switches = GetJsonFromUrl(url)
    switchProperties = {}
    for switch in switches:
        switchId = str(switch['dpid'])
        switchProperties[switchId] = switch
    return switchProperties

if __name__ == '__main__':
    links = GetLinks()
    hosts = filter(lambda x: x[1] is None, links)
    print 'Links:'
    for link in links:
        print link
    # routes = GetRoute(hosts[0][2], hosts[0][3], hosts[2][2], hosts[2][3])
    # print 'Routes between {0} and {1}'.format(hosts[0][0], hosts[2][0])
    # print routes
    print 'switch stats:'
    print GetSwitchStats('all', 'port')
    print 'switch features'
    print GetSwitchStats('all', 'features')
    # switches = GetSwitchProperties()
    # for (key, value) in switches.iteritems():
        # print key
        # print value







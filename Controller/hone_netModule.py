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
from subprocess import check_output
from threading import Timer, Thread

import hone_freLib as freLib
from hone_util import *
from hone_message import *
from hone_sndModule import NetToControllerSndSocket

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
    while (not jobFlowQueue.isEmpty()) and (jobFlowQueue.minDeadline() < currentTime):
        jobFlowKey = jobFlowQueue.pop()
        jobFlowToRun.append(jobFlowKey)
        netJobTable[jobFlowKey].updateDeadline()
        jobFlowQueue.push(netJobTable[jobFlowKey].deadline, jobFlowKey)
    # TODO execute jobFlowToRun
    for jobFlow in jobFlowToRun:
        (jobId, flowId) = DecomposeKey(jobFlow)
        (_, goFunc) = eventAndGoFunc[jobId][flowId]
        goThread = Thread(target=runGo, args=(goFunc, [1, 2, 3], jobId, flowId))
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

def GetLinks():
    links = []
    result = check_output('curl -s http://localhost:8080/wm/topology/links/json', shell=True, executable='/bin/bash')
    switchLinks = json.loads(result)
    for link in switchLinks:
        sswitch = str(link['src-switch'])
        sport = link['src-port']
        dswitch = str(link['dst-switch'])
        dport = link['dst-port']
        links.append([sswitch, sport, dswitch, dport])
    result = check_output('curl -s http://localhost:8080/wm/device/',
                         shell=True, executable='/bin/bash')
    hostLinks = json.loads(result)
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
    command = 'curl -s http://localhost:8080/wm/core/switch/{0}/{1}/json'.format(switchId, statsType)
    result = check_output(command, shell=True, executable='/bin/bash')
    stats = json.loads(result)
    return stats[switchId]

def GetRoute(switchIdA, portA, switchIdB, portB):
    result = check_output('curl -s http://localhost:8080/wm/topology/route/{0}/{1}/{2}/{3}/json'.format(switchIdA, portA, switchIdB, portB), shell=True, executable='/bin/bash')
    routes = json.loads(result)
    return routes

def GetSwitchProperties():
    result = check_output('curl -s http://localhost:8080/wm/core/controller/switches/json', shell=True, executable='/bin/bash')
    switches = json.loads(result)
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
    routes = GetRoute(hosts[0][2], hosts[0][3], hosts[2][2], hosts[2][3])
    print 'Routes between {0} and {1}'.format(hosts[0][0], hosts[2][0])
    print routes
    print 'switch stats:'
    print GetSwitchStats(links[1][0], 'flow')

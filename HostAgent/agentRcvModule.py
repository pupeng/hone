# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# agentRcvModule
# Host agent receiver module

import sys
import traceback
import logging
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from cStringIO import StringIO
import cPickle as pickle
from threading import Thread

from agentUtil import *
from hone_message import *
from agentTypes import *
import agentLib
import agentFreLib
import agentMiddleLib

_honeHostListeningPort = 8877
middleJobTable = {}
sourceJobQueue = None
socketCriteriaQueue = None

class MergedData:
    def __init__(self, jobId, flowId, level):
        self.jobId = jobId
        self.flowId = flowId
        self.level = level
        self.lastSeq = None
        self.seenHosts = []
        self.bufferedData = []

    def addNewData(self, hostId, sequence, data, expectedNum):
        #debugLog('job', 'addNewData for merge. from hostId', hostId,\
        #    'sequence:', sequence,\
        #    'data:', data)
        #debugLog('job', 'MergedData class self check',\
        #    'lastSeq:', self.lastSeq,\
        #    'seenHosts', self.seenHosts,\
        #    'bufferedData', self.bufferedData)
        #EvalLog('{0:6f},84,add new data jobId {1} flowId {2} sequence {3}'.format(time.time(), self.jobId, self.flowId, sequence))
        if self.lastSeq is None:
            self.lastSeq = sequence
        if (sequence > self.lastSeq):
            self.releaseData()
            self.lastSeq = sequence
            self.seenHosts.append(hostId)
            self.bufferedData.append(data)
        elif (sequence == self.lastSeq):
            if hostId not in self.seenHosts:
                self.seenHosts.append(hostId)
                self.bufferedData.append(data)
            if (len(self.bufferedData) >= expectedNum):
                self.releaseData()

    def releaseData(self):
        #debugLog('job', 'releaseData', 'seq', self.lastSeq)
        key = composeMiddleJobKey(self.jobId, self.flowId, self.level)
        middleJobTable[key].lastSeq = self.lastSeq
        dataToRelease = self.bufferedData[:]
        #EvalLog('{0:6f},85,release data for jobId {1} flowId {2} lastSeq {3}'.format(time.time(), self.jobId, self.flowId, self.lastSeq))
        self.lastSeq = None
        del self.seenHosts[:]
        del self.bufferedData[:]
        if key in middleJobTable:
            middleJob = middleJobTable[key]
            go = middleJob.goFunc
            goThread = Thread(target=runGo, args=(go, dataToRelease, key))
            goThread.daemon = True
            goThread.start()

def runGo(goFunc, data, jobFlowLevel):
    try:
        #EvalLog('{0:6f},86,start go function for jobFlow {1}'.format(time.time(), jobFlow))
        #debugLog('job', 'go thread', 'jobFlowLevel', jobFlowLevel)
        goFunc(data)
    except Exception, msg:
        print 'go thread caught exception'
        print msg
        traceback.print_exc()
    finally:
        #EvalLog('{0:6f},87,done go function for jobFlow {1}'.format(time.time(), jobFlow))
        pass

def buildSourceJob(sourceJob, exePlan):
    #EvalLog('{0:6f},80,build source job exe plan jobId {1} flowId {2}'.format(time.time(), sourceJob.jobId, sourceJob.flowId))
    query = exePlan[0]
    if query.ft == 'HostConnection':
        sourceJob.measureType = 'conn'
    elif query.ft == 'AppStatus':
        sourceJob.measureType = 'proc'
    elif query.ft == 'HostStatus':
        sourceJob.measureType = 'machine'
    sourceJob.measureStats = query.se
    sourceJob.period = query.ev
    computePart = []
    if query.wh is not None:
        for (stat, op, value) in query.wh:
            if stat in ['app', 'srcIP', 'dstIP', 'srcPort', 'dstPort']:
                sourceJob.measureCriteria[stat] = value
            else:
                computePart.append(['WC', stat, op, value])
    if sourceJob.measureType == 'conn':
        key = composeKey(sourceJob.jobId, sourceJob.flowId)
        item = (IPCType['InstallSocketCriteria'], (key, sourceJob.measureCriteria))
        socketCriteriaQueue.put(item)
        #socketCriteria[key] = sourceJob.measureCriteria
    if query.gp is not None:
        groupStatsPosition = ['GB']
        for stat in query.gp:
            groupStatsPosition.append(sourceJob.measureStats.index(stat))
        computePart.append(groupStatsPosition)
    if query.agg is not None:
        aggregateOp = ['AGG']
        for (stat, op) in query.agg:
            aggregateOp.append((sourceJob.measureStats.index(stat), op))
        computePart.append(aggregateOp)
    computePart = computePart + exePlan[1:]
    #debugLog('rcvMod', 'stats: ', sourceJob.measureStats,\
    #    'type: ', sourceJob.measureType,\
    #    'measure criteria', sourceJob.measureCriteria,\
    #    'build compute part for source job: ', computePart)
    sourceJob.computePart = computePart
    item = (IPCType['NewSourceJob'], sourceJob)
    sourceJobQueue.put(item)
    #newSourceJobList.append(sourceJob)
    #EvalLog('{0:6f},81,done build source job for jobId {1} flowId {2}'.format(time.time(), sourceJob.jobId, sourceJob.flowId))

def buildMiddleJob(middleJob, flowExePlan):
    try:
        if not sys.modules.has_key(middleJob.progName):
            __import__(middleJob.progName)
    except ImportError, msg:
        # user-defined function not ready yet
        #debugLog('job', 'Error loading user module: ', middleJob.progName)
        #logging.error('Error loading user module {0}'.format(middleJob.progName))
        return
    #EvalLog('{0:6f},82,build middle job for jobId {1} flowId {2}'.format(time.time(), middleJob.jobId, middleJob.flowId))
    e, go = agentFreLib.RawEvent()
    for i in range(len(flowExePlan)):
        operator = flowExePlan[i]
        #debugLog('job', 'the operator:', operator)
        (ef, complete) = _processOp(operator, middleJob.jobId, middleJob.flowId, middleJob.level, middleJob.progName, e)
        #debugLog('job', 'process operator: ', operator, 'complete? ', complete)
        if complete:
            if (ef.__class__.__name__=='FEvent'):
                e = ef
            else:
                e = e >> ef
        else:
            #TODO: support merge in TreeMerge function
            return
    middleJob.event = e
    middleJob.goFunc = go
    middleJobTable[composeMiddleJobKey(middleJob.jobId, middleJob.flowId, middleJob.level)] = middleJob
    #EvalLog('{0:6f},83,done build middle job for jobId {1} flowId {2}'.format(time.time(), middleJob.jobId, middleJob.flowId))

def _processOp(operator, jobId, flowId, level, progName, e):
    #debugLog('job', 'operator: ', operator,\
    #    'jobId: ', jobId,\
    #    'flowId: ', flowId,\
    #    'event: ', e)
    if not operator:
        print 'return None'
        return (None, True)
    opType = operator[0]
    if (opType not in dir(agentLib)) and (opType not in dir(agentMiddleLib)):
        mgmtModule = sys.modules[progName]
        mgmtFunc = getattr(mgmtModule, opType)
        #debugLog('job', 'mgmtFunc: ', opType)
        return (mgmtFunc, True)
    elif opType=='WC':
        opFunc = getattr(agentLib, opType)
        attr = operator[1]
        op = operator[2]
        value = operator[3]
        #debugLog('job', 'where complex: ', attr, op, value)
        return (opFunc(attr,op,value), True)
    elif opType=='GB':
        opFunc = getattr(agentLib, opType)
        attr = operator[1:]
        #debugLog('lib', 'groupby: ', attr)
        return (opFunc(attr), True)
    elif opType=='AGG':
        opFunc = getattr(agentLib, opType)
        attr = operator[1:]
        #debugLog('lib', 'AGG', attr)
        return (opFunc(attr), True)
    elif opType=='ToCtrl':
        opFunc = getattr(agentLib, opType)
        #debugLog('lib', 'ToCtrl', )
        return (opFunc(jobId, flowId), True)
    elif opType == 'ToMiddle':
        opFunc = getattr(agentLib, opType)
        #debugLog('lib', 'ToMiddle')
        return (opFunc(jobId, flowId), True)
    elif opType == 'ToUpperLevel':
        opFunc = getattr(agentMiddleLib, opType)
        return (opFunc(jobId, flowId, level), True)
    elif opType=='MergeStreamsForSet':
        opFunc = getattr(agentLib, opType)
        subFlowId = operator[1]
        if (jobId in middleJobTable) and (subFlowId in middleJobTable[jobId]):
            subEvent = middleJobTable[jobId][subFlowId].event
            return (opFunc(e, subEvent), True)
        else:
            return (subFlowId, False)
    elif (opType=='ReduceStreamSet') or (opType=='ReduceList'):
        opFunc = getattr(agentLib, opType)
        init = operator[1]
        (f, subcomplete) = _processOp(operator[2:], jobId, flowId, level, progName, e)
        if subcomplete:
            #debugLog('lib', 'Reduce: ', opType, operator[2:])
            return (opFunc(f, init), True)
        else:
            return (f, False)
    elif opType=='RateLimit':
        #debugLog('lib', 'RateLimit')
        opFunc = getattr(agentLib, opType)
        return (opFunc(jobId), True)
    else:
        opFunc = getattr(agentLib,opType)
        (f, subcomplete) = _processOp(operator[1:], jobId, flowId, level, progName, e)
        if subcomplete:
            #debugLog('lib', 'other: ', opType, operator[1:])
            return (opFunc(f), True)
        else:
            return (f, False)


class HoneCommProtocolHost(LineReceiver):
    '''the protocol between controller and host agent'''
    def __init__(self, factory):
        self.typeActions = {
            HoneMessageType_InstallSourceJob : self.handleSourceJob,
            HoneMessageType_InstallMiddleJob : self.handleMiddleJob,
            HoneMessageType_UpdateSourceJob  : self.handleSourceJobUpdate,
            HoneMessageType_UpdateMiddleJob  : self.handleMiddleJobUpdate,
            HoneMessageType_SendFile         : self.handleFileTransfer,
            HoneMessageType_RelayStatsIn     : self.handleMiddleStatsIn }
        self.factory = factory
    
    def lineReceived(self, line):
        #debugLog('rcvMod', 'receive a new thing')
        #EvalLog('{0:6f},67,receive one new message'.format(time.time()))
        buf = StringIO(line)
        message = pickle.load(buf)
        buf.close()
        self.typeActions.get(message.messageType, self.handleUnknownType) \
                            (message)
    
    def handleSourceJob(self, message):
        #debugLog('rcvMod', 'new source job. jobId:', message.jobId, \
        #         'content:', message.content)
        #EvalLog('{0:6f},68,receive new source job of jobId {1}'.format(time.time(), message.jobId))
        (middleAddress, createTime, progName, exePlan) = message.content
        for flowExePlan in exePlan:
            #debugLog('rcvMod', 'new flow exe plan', flowExePlan.exePlan)
            sourceJob = SourceJob(message.jobId, flowExePlan.flowId)
            sourceJob.middleAddress = middleAddress
            sourceJob.createTime = createTime
            sourceJob.progName = progName
            buildSourceJob(sourceJob, flowExePlan.exePlan)
        logging.info('install source job. ID: {0}. middleAddress: {1}. createTime: {2}. progName: {3}.'.format(
            message.jobId, middleAddress, createTime, progName))
        #EvalLog('{0:6f},69,done handling source jobId {1}'.format(time.time(), message.jobId))

    def handleSourceJobUpdate(self, message):
        #debugLog('rcvMod', 'update source job. jobId:', message.jobId, \
        #         'content:', message.content)
        #EvalLog('{0:6f},70,start handleSourceJobUpdate jobId {1}'.format(time.time(), message.jobId))
        item = (IPCType['UpdateSourceJob'], (message.jobId, message.content))
        sourceJobQueue.put(item)
        #EvalLog('{0:6f},71,done handleSourceJobUpdate jobId {1}'.format(time.time(), message.jobId))

    def handleMiddleJob(self, message):
        #debugLog('rcvMod', 'new middle job. jobId:', message.jobId, \
        #         'content:', message.content)
        #EvalLog('{0:6f},72,start handleMiddleJob jobId {1}'.format(time.time(), message.jobId))
        (numOfChildren, parentAddress, progName, exePlan) = message.content
        for flowExePlan in exePlan:
            middleJob = MiddleJob(message.jobId, flowExePlan.flowId, message.level)
            middleJob.expectedNumOfChild = numOfChildren
            middleJob.progName = progName
            middleJob.parentAddress = parentAddress
            buildMiddleJob(middleJob, flowExePlan.exePlan)
        #EvalLog('{0:6f},73,done handleMiddleJob jobId {1}'.format(time.time(), message.jobId))
        logging.info('install middle job id {0} level {1}'.format(message.jobId, message.level))

    def handleMiddleJobUpdate(self, message):
        #debugLog('rcvMod', 'update middle job.', 'jobId:', message.jobId, \
        #         'level', message.level, \
        #         'content:', message.content)
        if message.level == 0:
            (_, parentAddress) = message.content
            item = (IPCType['UpdateSourceJob'], (message.jobId, parentAddress))
            sourceJobQueue.put(item)
        for key in middleJobTable.iterkeys():
            if middleJobKeyContainJobIdAndLevel(key, message.jobId, message.level):
                (numOfChildren, parentAddress) = message.content
                middleJobTable[key].expectedNumOfChild = numOfChildren
                if parentAddress:
                    middleJobTable[key].parentAddress = parentAddress
        logging.info('update middle job id {0} level {1} with content {2}'.format(message.jobId, message.level, message.content))

    def handleFileTransfer(self, message):
        #debugLog('rcvMod', 'transfer program file. jobId:', message.jobId)
        #EvalLog('{0:6f},76,start receiving file for jobId {1}'.format(time.time(), message.jobId))
        (moduleName, fileContent) = message.content
        fileName = moduleName + '.py'
        fileOutput = open(fileName, 'w')
        fileOutput.write(fileContent)
        fileOutput.close()
        if not sys.modules.has_key(moduleName):
            __import__(moduleName)
        logging.info('receive a new file for job {0} with name {1}'.format(message.jobId, moduleName))
        #EvalLog('{0:6f},77,done receiving file for jobId {1}'.format(time.time(), message.jobId))

    def handleMiddleStatsIn(self, message):
        #EvalLog('{0:6f},78,start handleMiddleStatsIn for jobId {1}'.format(time.time(), message.jobId))
        key = composeMiddleJobKey(message.jobId, message.flowId, message.level)
        #debugLog('rcvMod', 'new stats from child to middle. jobId:', message.jobId, \
        #         'flowId', message.flowId, 'level', message.level, \
        #         'peer', self.transport.getPeer()[1], \
        #         'content: ', message.content, \
        #         'expectNumOfChild', middleJobTable[key].expectedNumOfChild, \
        #         'middleJobTable', middleJobTable)
        if key in middleJobTable:
            expectNumOfChild = middleJobTable[key].expectedNumOfChild
            if message.jobId not in self.factory.statsBuffer:
                self.factory.statsBuffer[message.jobId] = {}
            if message.flowId not in self.factory.statsBuffer[message.jobId]:
                self.factory.statsBuffer[message.jobId][message.flowId] = {}
            if message.level not in self.factory.statsBuffer[message.jobId][message.flowId]:
                self.factory.statsBuffer[message.jobId][message.flowId][message.level] = MergedData(message.jobId, message.flowId, message.level)
            self.factory.statsBuffer[message.jobId][message.flowId][message.level].addNewData(message.hostId, message.sequence, message.content, expectNumOfChild)
        #EvalLog('{0:6f},79,done handleMiddleStatsIn for jobId {1}'.format(time.time(), message.jobId))
    
    def handleUnknownType(self, message):
        logging.warning('Agent receives unknown message. type: {0}. content: {1}'.format(message.messageType, repr(message.content)))


class HoneCommFactoryHost(Factory):
    def __init__(self):
        self.statsBuffer = {}

    def buildProtocol(self, addr):
        return HoneCommProtocolHost(self)


class RcvModuleProcess(StoppableProcess):
    def __init__(self, passedSourceJobQueue, passedSocketCriteriaQueue):
        super(RcvModuleProcess, self).__init__()
        global sourceJobQueue
        sourceJobQueue = passedSourceJobQueue
        global socketCriteriaQueue
        socketCriteriaQueue = passedSocketCriteriaQueue

    def run(self):
        logging.info('agentRcvModule starts')
        print 'hone agent rcvModule starts to run.'
        #EvalLog('{0:6f},65,rcvModule starts to run'.format(time.time()))
        try:
            reactor.listenTCP(_honeHostListeningPort, HoneCommFactoryHost())
            #debugLog('rcvMod', 'agentRcvModule starts on port ', \
            #         _honeHostListeningPort)
            reactor.run()
        except Exception as e:
            logging.warning('agentRcvModule got exception {0}'.format(e))
        finally:
            logging.info('Exit from agentRcvModule')
            print 'Exit from agentRcvModule.'

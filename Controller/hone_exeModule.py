# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_exeModule.py
# Execution module on the controller

import sys
import traceback
import logging
import time
from threading import Thread

from hone_util import LogUtil
import hone_exeLib
import hone_rts as rts

jobGoFun = {} # key: jobID, value: a dictionary: key: flowIndex, value: go
jobEvent = {} # key: jobID, value: a dictionary: key: flowIndex, value: event

# key: jobId, value: a dictionary: key: flowId, value: MergedData
statsBuffer = {}

class MergedData:
    def __init__(self, jobId, flowId):
        self.jobId = jobId
        self.flowId = flowId
        self.lastSeq = None
        self.seenHosts = []
        self.bufferedData = []

    def addNewData(self, hostId, sequence, data, expectedNum):
        # LogUtil.DebugLog('exeMod', 'sequence: {0}. data: {1}. expectedNum: {2}.'.format(sequence, data, expectedNum))
        # LogUtil.DebugLog('exeMod', 'MergedData class self check. lastSeq: {0}. seenHosts: {1}. bufferedData: {2}'.format(
        #     self.lastSeq, self.seenHosts, self.bufferedData))
        rts.evalTimestamp += '#AddNewDataToBuffer${0:6f}${1}${2}${3}'.format(time.time(), self.jobId, self.flowId, sequence)
        if self.lastSeq is None:
            self.lastSeq = sequence
        if sequence > self.lastSeq:
            self.releaseData()
            self.lastSeq = sequence
            self.seenHosts.append(hostId)
            self.bufferedData.append(data)
        elif sequence == self.lastSeq:
            if hostId not in self.seenHosts:
                self.seenHosts.append(hostId)
                self.bufferedData.append(data)
            if len(self.bufferedData) >= expectedNum:
                self.releaseData()

    def releaseData(self):
        #EvalLog('{0:6f},46,release data seq {1} to jobId {2} flowId {3}'.format(time.time(), self.lastSeq, self.jobId, self.flowId))
        #EvalLog('{0:6f},123,{1}'.format(time.time(), self.evalTime))
        rts.evalTimestamp += '#ReleaseBuffer${0:6f}${1}${2}${3}'.format(time.time(), self.jobId, self.flowId, self.lastSeq)
        dataToRelease = self.bufferedData[:]
        self.lastSeq = None
        del self.seenHosts[:]
        del self.bufferedData[:]
        go = jobGoFun[self.jobId][self.flowId]
        goThread = Thread(target=runGo,args=(go, dataToRelease, self.jobId, self.flowId))
        goThread.daemon = True
        goThread.start()

def runGo(goFunc, data, jobId, flowId):
    try:
        rts.evalTimestamp += '#StartRunGo${0:6f}${1}${2}'.format(time.time(), jobId, flowId)
        goFunc(data)
    except Exception, msg:
        logging.warning('Go thread caught exception. jobID {0}. flowID {1}'.format(jobId, flowId))
        print 'go thread caught exception'
        print msg
        traceback.print_exc()
    finally:
        rts.evalTimestamp += '#DoneRunGo${0:6f}${1}${2}'.format(time.time(), jobId, flowId)
        LogUtil.EvalLog('ControllerExecution', rts.evalTimestamp)
        rts.evalTimestamp = 'Begin'
                            
def buildExePlan(jobId, progName, controllerExePlan):
    if jobEvent.has_key(jobId):
        return
    #EvalLog('{0:6f},43,start to build exe plan for job {1}'.format(time.time(), jobId))
    #debugLog('exeMod', 'receive exe plan from rts', jobId)
    jobEvent[jobId] = {}
    jobGoFun[jobId] = {}
    #for flowExePlan in controllerExePlan:
        #debugLog('exeMod', 'flowExePlan. flowId:', flowExePlan.flowId, \
        #         'exePlan:', flowExePlan.exePlan)
    if not sys.modules.has_key(progName):
        __import__(progName)
    for flowExePlan in reversed(controllerExePlan):
        e = hone_exeLib.Subscribe(jobId, flowExePlan.flowId)
        for operator in flowExePlan.exePlan:
            ef = processOp(operator, progName, jobId, e)
            LogUtil.DebugLog('exeMod', 'process one operator: {0}. flowId: {1}. ef type: {2}'.format(
                operator, flowExePlan.flowId, ef.__class__.__name__))
            if (ef.__class__.__name__ == 'FEvent'):
                e = ef
            else:
                e = e >> ef
        jobEvent[jobId][flowExePlan.flowId] = e
    LogUtil.DebugLog('exeMod', 'check jobEvent and jobGoFun: \n{0}\n{1}'.format(jobEvent, jobGoFun))
    #EvalLog('{0:6f},44,done building exe plan for job {1}'.format(time.time(), jobId))

def processOp(operator, progName, jobID, event):
    if not operator:
        return None
    opType = operator[0]
    if not opType in dir(hone_exeLib):
        mgmtModule = sys.modules[progName]
        mgmtFunc = getattr(mgmtModule, opType)
        #LogUtil.DebugLog('exeMod', 'mgmtFunc: {0}'.format(opType))
        return mgmtFunc
    elif opType == 'MergeStreams':
        opFunc = getattr(hone_exeLib, opType)
        flowEvents = [event]
        for subFlowId in operator[1:]:
            flowEvents.append(jobEvent[jobID][subFlowId])
        #LogUtil.DebugLog('exeMod', 'MergeStreams: {0}'.format(str(operator[1:])))
        return opFunc(flowEvents)
    elif (opType == 'ReduceStream') or (opType == 'ReduceList'):
        opFunc = getattr(hone_exeLib, opType)
        init = operator[1]
        f = processOp(operator[2:], progName, jobID, event)
        #debugLog('exeMod', 'Reduce: ', opType, str(operator[2:]))
        return opFunc(f,init)
    else:
        opFunc = getattr(hone_exeLib,opType)
        f = processOp(operator[1:], progName, jobID, event)
        #debugLog('exeMod', 'general op: ', opType, str(operator[1:]))
        return opFunc(f)   
                      
def handleStatsIn(message, expectedNum):
    #debugLog('exeMod', 'statsBuffer:', statsBuffer)
    #rts.evalTimestamp += '#GetNewStats${0:6f}${1}${2}${3}'.format(time.time(), message.jobId, message.flowId, message.sequence)
    hostId = message.hostId
    jobId = message.jobId
    flowId = message.flowId
    sequence = message.sequence
    data = message.content
    if jobId not in statsBuffer:
        statsBuffer[jobId] = {}
    if flowId not in statsBuffer[jobId]:
        statsBuffer[jobId][flowId] = MergedData(jobId, flowId)
    statsBuffer[jobId][flowId].addNewData(hostId, sequence, data, expectedNum)

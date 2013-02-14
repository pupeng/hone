'''
Peng Sun
hone_exeModule.py
Execution module on the controller
'''

from hone_util import *
import hone_exeLib
import hone_rts
from threading import Thread
import sys, os, traceback
#import logging
import time

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
        #debugLog('exeMod', 'addNewData for merge. from hostId', hostId, \
        #         'sequence:', sequence, \
        #         'data:', data, \
        #         'expectedNum:', expectedNum)
        #debugLog('exeMod', 'MergedData class self check', \
        #         'lastSeq:', self.lastSeq, \
        #         'seenHosts', self.seenHosts, \
        #         'bufferedData', self.bufferedData)
        #EvalLog('{0:6f},45,add new data to jobId {1} flowId {2} sequence {3}'.format(time.time(), self.jobId, self.flowId, sequence))
        hone_rts.evalTimestamp += '#{0}${1}${3}${2:6f}$Buffer'.format(self.jobId, self.flowId, time.time(), sequence)
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
        #EvalLog('{0:6f},46,release data seq {1} to jobId {2} flowId {3}'.format(time.time(), self.lastSeq, self.jobId, self.flowId))
        #EvalLog('{0:6f},123,{1}'.format(time.time(), self.evalTime))
        hone_rts.evalTimestamp += '#{0}${1}${3}${2:6f}$Release'.format(self.jobId, self.flowId, time.time(), self.lastSeq)
        dataToRelease = self.bufferedData[:]
        self.lastSeq = None
        del self.seenHosts[:]
        del self.bufferedData[:]
        #debugLog('exeMod', 'jobId:', self.jobId, 'flowId:', self.flowId, 'releaseData: ', dataToRelease)
        go = jobGoFun[self.jobId][self.flowId]
        goThread = Thread(target=runGo,args=(go, dataToRelease, self.jobId, self.flowId))
        goThread.daemon = True
        goThread.start()

def runGo(goFunc, data, jobId, flowId):
    try:
        #EvalLog('{0:6f},47,start go func for jobId {1} flowId {2}'.format(time.time(), jobId, flowId))
        hone_rts.evalTimestamp += '#{0}${1}$0${2:6f}$Start'.format(jobId, flowId, time.time())
        goFunc(data)
        hone_rts.evalTimestamp += '#{0}${1}$0${2:6f}$End'.format(jobId, flowId, time.time())
    except Exception, msg:
        print 'go thread caught exception'
        print msg
        traceback.print_exc()
    finally:
        #EvalLog('{0:6f},48,done go func for jobId {1} flowId {2}'.format(time.time(), jobId, flowId))
        EvalLog('{0:6f},126,{1}'.format(time.time(), hone_rts.evalTimestamp))
        hone_rts.evalTimestamp = ''
                            
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
            #debugLog('exeMod', 'process one operator:', operator, \
            #         'flowId:', flowExePlan.flowId, \
            #         'ef type is:', ef.__class__.__name__)
            if (ef.__class__.__name__ == 'FEvent'):
                e = ef
            else:
                e = e >> ef
        jobEvent[jobId][flowExePlan.flowId] = e
    #debugLog('exeMod', 'check jobEvent and jobGoFun', \
    #         jobEvent, jobGoFun)
    #EvalLog('{0:6f},44,done building exe plan for job {1}'.format(time.time(), jobId))

def processOp(operator, progName, jobID, event):
    if not operator:
        return None
    opType = operator[0]
    if not opType in dir(hone_exeLib):
        mgmtModule = sys.modules[progName]
        mgmtFunc = getattr(mgmtModule, opType)
        #debugLog('exeMod', 'mgmtFunc:', opType)
        return mgmtFunc
    elif (opType == 'MergeStreams'):
        opFunc = getattr(hone_exeLib, opType)
        flowEvents = [event]
        for subFlowId in operator[1:]:
            flowEvents.append(jobEvent[jobID][subFlowId])
        #debugLog('exeMod', 'MergeStreams:', str(operator[1:]))
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


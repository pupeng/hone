# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# agentManager.py
# Brain of hone host agent

import logging
import time
from multiprocessing import Manager, Queue
from threading import Timer, Thread

from agentUtil import LogUtil
from agentTypes import *
from agentSndModule import *
from agentRcvModule import *
from agentDirService import *
import agentConnMeasure
import agentProcMeasure

sndToCtrl = None
sourceJobTable = None
socketTable = None
sourceJobSkList = None
minRunInterval = 5.0 # seconds
stopSchedule = False
eventAndGoFunc = {}
jobFlowQueue = JobFlowMinQueue()

jobNotReady = {}
jobQueueForFile = {}
jobQueueForMerge = {}

evalTimestamp = ''
measureLatency = ''

CtrlAddress = None

def agentManagerRun(ctrlAddress, ctrlPort):
    LogUtil.DebugLog('manager', 'start to run agent manager')
    #EvalLog('{0:6f},50,agentManager starts in pid {1}'.format(time.time(), os.getpid()))
    global CtrlAddress
    CtrlAddress = ctrlAddress
    global minRunInterval
    sourceJobQueue = Queue()
    socketCriteriaQueue = Queue()
    global sourceJobTable
    sourceJobTable = {}
    global socketTable
    socketTable = {}
    global sourceJobSkList
    sourceJobSkList = {}
    rcvModuleProcess = RcvModuleProcess(sourceJobQueue, socketCriteriaQueue)
    dirServiceProcess = DirServiceProcess(socketCriteriaQueue, sourceJobQueue)
    try:
        #EvalLog('{0:6f},51,start rcvModule and dirService processes'.format(time.time()))
        rcvModuleProcess.start()
        dirServiceProcess.start()
        # wait for rcv module to boot
        #EvalLog('{0:6f},52,wait {1} seconds for rcvModule to boot'.format(time.time(), minRunInterval))
        time.sleep(minRunInterval)
        global sndToCtrl
        sndToCtrl = HostAgentSndSocket(ctrlAddress, ctrlPort)
        scheduleLoop = Timer(minRunInterval, scheduleLoopRun)
        scheduleLoop.start()
        time.sleep(minRunInterval / 2.0)
        while True:
            #EvalLog('{0:6f},53,enter main loop to process new source job'.format(time.time()))
            currentTime = time.time()
            while not sourceJobQueue.empty():
                #EvalLog('{0:6f},54,got one new item from IPC queue'.format(time.time()))
                (itemType, itemContent) = sourceJobQueue.get_nowait()
                if itemType == IPCType['NewSourceJob']:
                    newSourceJob = itemContent
                    #debugLog('job', 'new source job', newSourceJob.debug())
                    registerComputePart(newSourceJob)
                    key = composeKey(newSourceJob.jobId, newSourceJob.flowId)
                    if minRunInterval > (newSourceJob.period / 1000.0):
                        minRunInterval = newSourceJob.period / 1000.0
                    alreadyPassedPeriods = int(float(currentTime - newSourceJob.createTime) / float(newSourceJob.period / 1000.0))
                    newSourceJob.deadline = newSourceJob.createTime + newSourceJob.period / 1000.0 * (alreadyPassedPeriods + 1)
                    newSourceJob.lastSequence = alreadyPassedPeriods
                    jobFlowQueue.push(newSourceJob.deadline, key)
                    sourceJobTable[key] = newSourceJob
                    #EvalLog('{0:6f},55,process one new source job done'.format(time.time()))
                elif itemType == IPCType['UpdateSourceJob']:
                    (jobId, middleAddress) = itemContent
                    for key in sourceJobTable.iterkeys():
                        if keyContainJobId(key, jobId):
                            sourceJobTable[key].middleAddress = middleAddress
                            break
                    #EvalLog('{0:6f},112,update middle address {1} for source job {2}'.format(time.time(), middleAddress, jobId))
                elif itemType == IPCType['NewSocket']:
                    (sockfd, theSock) = itemContent
                    socketTable[sockfd] = theSock
                elif itemType == IPCType['DeleteSocket']:
                    if itemContent in socketTable:
                        del socketTable[itemContent]
                elif itemType == IPCType['AddSkToJobFlow']:
                    (jobFlow, sockfd) = itemContent
                    if jobFlow not in sourceJobSkList:
                        sourceJobSkList[jobFlow] = []
                    if sockfd not in sourceJobSkList[jobFlow]:
                        sourceJobSkList[jobFlow].append(sockfd)
                elif itemType == IPCType['RemoveSkFromJobFlow']:
                    (jobFlow, sockfd) = itemContent
                    if (jobFlow in sourceJobSkList) and (sockfd in sourceJobSkList[jobFlow]):
                        sourceJobSkList[jobFlow].remove(sockfd)
            #debugLog('manager', 'one round of main loop')
            #for id in sourceJobTable.keys():
                #debugLog('manager', 'jobId@flowId: ', id, 'sourceJob: ', sourceJobTable[id].debug())
            #debugLog('manager', 'socketTable', socketTable)
            #debugLog('manager', 'sourceJobSkList', sourceJobSkList)
            #debugLog('manager', 'time: ', time.time())
            #EvalLog('{0:6f},56,done one round of main loop. start to sleep {1} seconds'.format(time.time(), minRunInterval))
            time.sleep(minRunInterval)
    except KeyboardInterrupt:
        logging.info('Catch KeyboardInterrupt in agent manager main loop')
        print 'Catch KeyboardInterrupt in agent manager main loop'
        #EvalLog('{0:6f},57,catch keyboard interrupt. start to clean up'.format(time.time()))
    finally:
        #EvalLog('{0:6f},58,ready to exit agent manager. wait {1} seconds for other processes to end'.format(time.time(), minRunInterval))
        global stopSchedule
        stopSchedule = True
        agentProcMeasure.cleanup()
        dirServiceProcess.terminate()
        dirServiceProcess.join()
        rcvModuleProcess.terminate()
        rcvModuleProcess.join()
        logging.info('Agent manager cleans everything up, and is ready to stop.')
        print 'Exit from agent manager'

def scheduleLoopRun():
    #debugLog('schedule', 'schedule loop', time.time())
    #EvalLog('{0:6f},59,start one round of schedule loop'.format(time.time()))
    global evalTimestamp
    global measureLatency
    LogUtil.EvalLog('JobExecutionLoop', evalTimestamp)
    LogUtil.EvalLog('MeasureLatency', measureLatency)
    evalTimestamp = 'Begin${0:6f}'.format(time.time())
    measureLatency = 'Begin${0:6f}'.format(time.time())
    if stopSchedule:
        #debugLog('schedule', 'catch stop signal')
        #EvalLog('{0:6f},60,schedule loop catches stop signal and exit'.format(time.time()))
        logging.info('schedule loop should stop now.')
        return
    # next loop
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
    # schedule jobs to run
    jobFlowToRun = []
    connMeasureJobFlow = []
    procMeasureJobFlow = []
    machineMeasureJobFlow = []
    #debugLog('schedule', 'current time:', currentTime, 'jobFlowQueue:', jobFlowQueue.debug())
    while (not jobFlowQueue.isEmpty()) and (jobFlowQueue.minDeadline() < currentTime):
        jobFlowKey = jobFlowQueue.pop()
        jobFlowToRun.append(jobFlowKey)
        measureType = sourceJobTable[jobFlowKey].measureType
        if measureType == 'conn':
            connMeasureJobFlow.append(jobFlowKey)
        elif measureType == 'proc':
            procMeasureJobFlow.append(jobFlowKey)
        elif measureType == 'machine':
            machineMeasureJobFlow.append(jobFlowKey)
        sourceJobTable[jobFlowKey].updateDeadline()
        jobFlowQueue.push(sourceJobTable[jobFlowKey].deadline, jobFlowKey)
    #EvalLog('{0:6f},61,done preparing jobFlows to run: {1}'.format(time.time(), jobFlowToRun))
    #debugLog('schedule', 'job flow to run: ', jobFlowToRun, \
    #                     'connMeasureJobFlow:', connMeasureJobFlow, \
    #                     'procMeasureJobFlow:', procMeasureJobFlow, \
    #                     'machineMeasureJobFlow:', machineMeasureJobFlow)
    evalTimestamp += '#ScheduleDone${0:6f}'.format(time.time())
    if IsLazyTableEnabled():
        if connMeasureJobFlow:
            connThread = Thread(target=agentConnMeasure.connMeasureRun, args=(connMeasureJobFlow, None))
            connThread.daemon = True
            connThread.start()
        if procMeasureJobFlow:
            procThread = Thread(target=agentProcMeasure.procMeasureRun, args=(procMeasureJobFlow, None))
            procThread.daemon = True
            procThread.start()
        if machineMeasureJobFlow:
            machineThread = Thread(target=agentProcMeasure.machineMeasureRun, args=(machineMeasureJobFlow, None))
            machineThread.daemon = True
            machineThread.start()
    else:
        #EvalLog('{0:6f},109,start no-lazy-m operation'.format(time.time()))
        connThread = Thread(target=agentConnMeasure.connMeasureRun, args=(connMeasureJobFlow, None))
        connThread.daemon = True
        procThread = Thread(target=agentProcMeasure.procMeasureRunAll, args=(procMeasureJobFlow, None))
        procThread.daemon = True
        machineThread = Thread(target=agentProcMeasure.machineMeasureRun, args=(machineMeasureJobFlow, None))
        machineThread.daemon = True
        connThread.start()
        procThread.start()
        machineThread.start()
        connThread.join()
        procThread.join()
        machineThread.join()
        #EvalLog('{0:6f},108,No lazy materialization. done one round of schedule loop.'.format(time.time()))

def registerComputePart(sourceJob):
    #debugLog('job', sourceJob.jobId, sourceJob.flowId, sourceJob.computePart)
    #EvalLog('{0:6f},63,register compute part of source jobId {1} flowId {2}'.format(time.time(), sourceJob.jobId, sourceJob.flowId))
    agentLib.Subscribe(sourceJob)
    #debugLog('job', 'source job table: ', sourceJobTable,\
    #    'job not ready: ', jobNotReady,\
    #    'job queue for file: ', jobQueueForFile,\
    #    'job queue for merge: ', jobQueueForMerge)
    _registerComputePart(sourceJob, sourceJob.computePart)
    #EvalLog('{0:6f},64,done registering compute part of source jobId {1} flowId {2}'.format(time.time(), sourceJob.jobId, sourceJob.flowId))

def _registerComputePart(sourceJob, computePart):
    try:
        if not sys.modules.has_key(sourceJob.progName):
            __import__(sourceJob.progName)
    except ImportError, msg:
        # user-defined function not ready yet
        #debugLog('job', 'Error loading user module: ', sourceJob.progName)
        logging.warning('hone application file is not shipped yet. program name {0}'.format(sourceJob.progName))
        if not jobQueueForFile.has_key(sourceJob.jobId):
            jobQueueForFile[sourceJob.jobId] = []
        jobQueueForFile[sourceJob.jobId].append((sourceJob.flowId, sourceJob.computePart))
        return
    (e, go) = eventAndGoFunc[sourceJob.jobId][sourceJob.flowId]
    for i in range(len(computePart)):
        operator = computePart[i]
        #debugLog('job', 'the operator:', operator)
        (ef, complete) = _processOp(operator, sourceJob.jobId, sourceJob.flowId, sourceJob.progName, e)
        #debugLog('job', 'process operator: ', operator, 'complete? ', complete, 'ef', ef)
        if complete:
            if ef.__class__.__name__=='FEvent':
                e = ef
            else:
                e = e >> ef
        else:
            if not jobQueueForMerge.has_key(sourceJob.jobId):
                jobQueueForMerge[sourceJob.jobId] = {}
            if not jobQueueForMerge[sourceJob.jobId].has_key(ef):
                jobQueueForMerge[sourceJob.jobId][ef] = []
            jobQueueForMerge[sourceJob.jobId][ef].append((sourceJob.flowId, computePart[i:]))
            jobNotReady[sourceJob.jobId][sourceJob.flowId] = (e, go)
            return
    eventAndGoFunc[sourceJob.jobId][sourceJob.flowId] = (e, go)
    #debugLog('job', 'source job: ', sourceJob.debug())
    #debugLog('job', 'source job table:', sourceJobTable)
    del jobNotReady[sourceJob.jobId][sourceJob.flowId]
    if not jobNotReady[sourceJob.jobId]:
        del jobNotReady[sourceJob.jobId]
    if jobQueueForMerge.has_key(sourceJob.jobId) and jobQueueForMerge[sourceJob.jobId].has_key(sourceJob.flowId):
        for (subFlowId, subExePlan) in jobQueueForMerge[sourceJob.jobId][sourceJob.flowId]:
            _registerComputePart(sourceJobTable[composeKey(sourceJob.jobId, subFlowId)], subExePlan)
        del jobQueueForMerge[sourceJob.jobId][sourceJob.flowId]
        if not jobQueueForMerge[sourceJob.jobId]:
            del jobQueueForMerge[sourceJob.jobId]

def _processOp(operator, jobId, flowId, progName, event):
    #debugLog('job', 'operator: ', operator,\
    #    'jobId: ', jobId,\
    #    'flowId: ', flowId,\
    #    'event: ', event)
    if not operator:
        print 'return None'
        return (None, True)
    opType = operator[0]
    if not opType in dir(agentLib):
        mgmtModule = sys.modules[progName]
        mgmtFunc = getattr(mgmtModule, opType)
        #debugLog('job', 'mgmtFunc: ', opType)
        return (mgmtFunc, True)
    elif opType == 'WC':
        opFunc = getattr(agentLib, opType)
        attr = operator[1]
        op = operator[2]
        value = operator[3]
        #debugLog('job', 'where complex: ', attr, op, value)
        return (opFunc(attr,op,value), True)
    elif opType == 'GB':
        opFunc = getattr(agentLib, opType)
        attr = operator[1:]
        #debugLog('lib', 'groupby: ', attr)
        return (opFunc(attr), True)
    elif opType == 'AGG':
        opFunc = getattr(agentLib, opType)
        attr = operator[1:]
        #debugLog('lib', 'AGG', attr)
        return (opFunc(attr), True)
    elif opType == 'ToCtrl':
        opFunc = getattr(agentLib, opType)
        #debugLog('lib', 'ToCtrl', )
        return (opFunc(jobId, flowId), True)
    elif opType == 'ToMiddle':
        opFunc = getattr(agentLib, opType)
        #debugLog('lib', 'ToMiddle')
        return (opFunc(jobId, flowId), True)
    elif opType == 'MergeStreamsForSet':
        opFunc = getattr(agentLib, opType)
        subFlowId = operator[1]
        if (jobId in eventAndGoFunc) and (subFlowId in eventAndGoFunc[jobId]):
            (subEvent, _) = eventAndGoFunc[jobId][subFlowId]
            return (opFunc(event, subEvent), True)
        else:
            return (subFlowId, False)
    elif (opType == 'ReduceStreamSet') or (opType == 'ReduceList'):
        opFunc = getattr(agentLib, opType)
        init = operator[1]
        (f, subcomplete) = _processOp(operator[2:], jobId, flowId, progName, event)
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
        (f, subcomplete) = _processOp(operator[1:], jobId, flowId, progName, event)
        if subcomplete:
            #debugLog('lib', 'other: ', opType, operator[1:])
            return (opFunc(f), True)
        else:
            return (f, False)

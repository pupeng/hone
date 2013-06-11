# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# agentProcMeasure.py
# CPU memory measurement

import traceback
import psutil
import logging
import time
from subprocess import check_output
from multiprocessing import Pool
from threading import Thread
from uuid import getnode as get_mac

import agentManager
from agentUtil import *

def cpuWorker(pid):
    cpu = 0
    try:
        proc = psutil.Process(pid)
        proc.get_cpu_percent(interval=None)
        cpu = proc.get_cpu_percent(interval=0.05)
    except Exception, msg:
        #debugLog('proc', 'psutil exception', 'pid: ', pid)
        logging.warning('psutil cpu measure exception: pid {0}. msg {1}'.format(pid, msg))
    return (pid, cpu)

def memWorker(pid):
    mem = 0
    try:
        proc = psutil.Process(pid)
        mem = proc.get_memory_percent()
    except Exception, msg:
        #debugLog('proc', 'psutil exception', 'pid: ', pid)
        logging.warning('psutil memory measure exception: pid {0}. msg {1}'.format(pid, msg))
    return (pid, mem)

cpuWorkerPool = Pool(10)
memWorkerPool = Pool(10)

def procMeasureRun(jobFlowToM, nothing):
    #debugLog('proc', 'procMeasureRun with jobFlows: ', jobFlowToM)
    #EvalLog('{0:6f},97,start procMeasure for jobFlows {1}'.format(time.time(), jobFlowToM))
    #evalTime = '{0:6f}'.format(time.time())
    pidForApp = {}
    pidsForCpuM = {}
    pidsForMemM = {}
    jobFlowPids = {}
    for jobFlow in jobFlowToM:
        sourceJob = agentManager.sourceJobTable[jobFlow]
        pidAndAppNames = None
        if sourceJob.measureCriteria and ('app' in sourceJob.measureCriteria):
            appName = sourceJob.measureCriteria['app']
            try:
                pidAndAppNames = check_output('ps -A | grep ' + appName + '| cut -b 1-5,24-', shell=True, executable='/bin/bash')
            except Exception:
                #debugLog('proc', 'check_output exception')
                logging.warning('exception in getting the application''s pid. app name: {0}'.format(appName))
                traceback.print_exc()
        else:
            try:
                pidAndAppNames = check_output('ps -A | cut -b 1-5,24-', shell=True, executable='/bin/bash')
            except Exception:
                #debugLog('proc', 'check_output exception')
                logging.warning('exception in getting the whole application list')
                traceback.print_exc()
        if pidAndAppNames:
            pidAndAppNames = map((lambda x: x.lstrip(' ').rstrip(' ').split(' ')), pidAndAppNames.rstrip('\n').split('\n'))
            if pidAndAppNames[0][0] == 'PID':
                pidAndAppNames.pop(0)
            pidAndAppNames = map((lambda x: [int(x[0]), x[1]]), pidAndAppNames)
            #debugLog('proc', 'pidAndAppNames: ', pidAndAppNames)
            for (pid, appName) in pidAndAppNames:
                if jobFlow not in jobFlowPids:
                    jobFlowPids[jobFlow] = []
                jobFlowPids[jobFlow].append(pid)
                pidForApp[pid] = appName
                if 'cpu' in sourceJob.measureStats:
                    pidsForCpuM[pid] = None
                if 'memory' in sourceJob.measureStats:
                    pidsForMemM[pid] = None
    #EvalLog('{0:6f},98,done finding pids for measurement'.format(time.time()))
    #evalTime += '#{0:6f}'.format(time.time())
    pidsForCpuM = pidsForCpuM.keys()
    pidsForMemM = pidsForMemM.keys()
    #debugLog('proc', 'pidForApp: ', pidForApp, \
    #                 'pidsForCpuM: ', pidsForCpuM, \
    #                 'pidsForMemM: ', pidsForMemM)
    cpuRDict = {}
    if pidsForCpuM:
        #EvalLog('{0:6f},99,start cpu measurement pids {1}'.format(time.time(), pidsForCpuM))
        global cpuWorkerPool
        cpuResults = cpuWorkerPool.map(cpuWorker, pidsForCpuM)
        for result in cpuResults:
            cpuRDict[result[0]] = result[1]
        #EvalLog('{0:6f},100,done cpu measurement pids {1}'.format(time.time(), pidsForCpuM))
    memRDict = {}
    if pidsForMemM:
        #EvalLog('{0:6f},101,start memory measurement pids {1}'.format(time.time(), pidsForMemM))
        global memWorkerPool
        memResults = memWorkerPool.map(memWorker, pidsForMemM)
        for result in memResults:
            memRDict[result[0]] = result[1]
        #EvalLog('{0:6f},102,done memory measurement pids {1}'.format(time.time(), pidsForMemM))
    #debugLog('proc', 'app results: ', pidForApp, \
    #                 'cpu measure results: ', cpuRDict, \
    #                 'mem measure results: ', memRDict)
    #evalTime += '#{0:6f}'.format(time.time())
    for jobFlow in jobFlowToM:
        measureResults = []
        sourceJob = agentManager.sourceJobTable[jobFlow]
        if jobFlow in jobFlowPids:
            pids = jobFlowPids[jobFlow]
            for pid in pids:
                result = []
                for name in sourceJob.measureStats:
                    if name == 'app':
                        result.append(pidForApp[pid])
                    elif name == 'cpu':
                        result.append(cpuRDict[pid])
                    elif name == 'memory':
                        result.append(memRDict[pid])
                    elif name == 'hostId':
                        result.append(str(get_mac()))
                measureResults.append(result)
        if measureResults:
            #debugLog('proc', 'measureResults:', measureResults, \
            #                 'jobFlow:', jobFlow)
            (jobId, flowId) = decomposeKey(jobFlow)
            (_, goFunc) = agentManager.eventAndGoFunc[jobId][flowId]
            goThread = Thread(target=runGo, args=(goFunc, measureResults, jobId, flowId))
            goThread.daemon = True
            goThread.start()
    #EvalLog('{0:6f},103,done one round of procMeasure for jobFlows {1}'.format(time.time(), jobFlowToM))
    #evalTime += '#{0:6f}'.format(time.time())
    #EvalLog('{0:6f},119,{1}'.format(time.time(), evalTime))
    #WriteLogs()
    agentManager.measureLatency += '#DoneOneRoundProcMeasure${0:6f}'.format(time.time())

def procMeasureRunAll(jobFlowToM, nothing):
    #EvalLog('{0:6f},110,no lazy m: start procMeasure for jobFlows {1}'.format(time.time(), jobFlowToM))
    pidForApp = {}
    pidsForCpuM = {}
    pidsForMemM = {}
    jobFlowPids = {}
    pidAndAppNames = None
    try:
        pidAndAppNames = check_output('ps -A | cut -b 1-5,24-', shell=True, executable='/bin/bash')
    except Exception:
        #debugLog('proc', 'check_output exception')
        traceback.print_exc()
    if pidAndAppNames:
        pidAndAppNames = map((lambda x: x.lstrip(' ').rstrip(' ').split(' ')), pidAndAppNames.rstrip('\n').split('\n'))
        if pidAndAppNames[0][0] == 'PID':
            pidAndAppNames.pop(0)
        pidAndAppNames = map((lambda x: [int(x[0]), x[1]]), pidAndAppNames)
        for (pid, appName) in pidAndAppNames:
            pidForApp[pid] = appName
            pidsForCpuM[pid] = None
            pidsForMemM[pid] = None
    pidsForCpuM = pidsForCpuM.keys()
    pidsForMemM = pidsForMemM.keys()
    cpuRDict = {}
    global cpuWorkerPool
    cpuResults = cpuWorkerPool.map(cpuWorker, pidsForCpuM)
    for result in cpuResults:
        cpuRDict[result[0]] = result[1]
    memRDict = {}
    global memWorkerPool
    memResults = memWorkerPool.map(memWorker, pidsForMemM)
    for result in memResults:
        memRDict[result[0]] = result[1]
    for jobFlow in jobFlowToM:
        measureResults = []
        sourceJob = agentManager.sourceJobTable[jobFlow]
        for pid in pidForApp.keys():
            result = []
            for name in sourceJob.measureStats:
                if name == 'app':
                    result.append(pidForApp[pid])
                elif name == 'cpu':
                    result.append(cpuRDict[pid])
                elif name == 'memory':
                    result.append(memRDict[pid])
                elif name == 'hostId':
                    result.append(str(get_mac()))
            measureResults.append(result)
        if measureResults:
            (jobId, flowId) = decomposeKey(jobFlow)
            (_, goFunc) = agentManager.eventAndGoFunc[jobId][flowId]
            goThread = Thread(target=runGo, args=(goFunc, measureResults, jobId, flowId))
            goThread.daemon = True
            goThread.start()
    #EvalLog('{0:6f},111,no lazy m: done one round of procMeasureAll for jobFlows {1}. Number of pids: {2} {3}'.format(time.time(), jobFlowToM, len(pidsForCpuM), len(pidsForMemM)))
    agentManager.measureLatency += '#DoneOneRoundProcMeasureAll${0:6f}'.format(time.time())

def machineMeasureRun(jobFlowToM, nothing):
    #debugLog('proc', 'machineMeasureRun. jobFlowToM:', jobFlowToM)
    #EvalLog('{0:6f},104,start machineMeasure for jobFlows {1}'.format(time.time(), jobFlowToM))
    totalCpu = psutil.cpu_percent(interval=0.05)
    totalMemory = psutil.virtual_memory().total
    hostId = str(get_mac())
    for jobFlow in jobFlowToM:
        measureResults = []
        sourceJob = agentManager.sourceJobTable[jobFlow]
        for name in sourceJob.measureStats:
            if name == 'hostId':
                measureResults.append(hostId)
            elif name == 'totalCPU':
                measureResults.append(totalCpu)
            elif name == 'totalMemory':
                measureResults.append(totalMemory)
            elif name == 'IP':
                measureResults.append(SelfIP.GetSelfIP())
        if measureResults:
            #debugLog('proc', 'measureResults:', measureResults)
            (jobId, flowId) = decomposeKey(jobFlow)
            (_, goFunc) = agentManager.eventAndGoFunc[jobId][flowId]
            goThread = Thread(target=runGo, args=(goFunc, measureResults, jobId, flowId))
            goThread.daemon = True
            goThread.start()
    #EvalLog('{0:6f},105,done one round of machineMeasure for jobFlows {1}'.format(time.time(), jobFlowToM))
    agentManager.measureLatency += '#DoneOneRoundMachineMeasure${0:6f}'.format(time.time())

def runGo(goFunc, data, jobId, flowId):
    #evalTime = '{0}#{1}#{2:6f}'.format(jobId, flowId, time.time())
    try:
        #EvalLog('{0:6f},106,run go function for jobId {1} flowId {2}'.format(time.time(), jobId, flowId))
        goFunc(data)
        #evalTime += '#{0:6f}'.format(time.time())
    except Exception, msg:
        logging.warning('go thread caught exception {0}'.format(msg))
        print 'go thread caught exception'
        print msg
        traceback.print_exc()
    finally:
        #EvalLog('{0:6f},107,done go function for jobID {1} flowId {2}'.format(time.time(), jobId, flowId))
        #evalTime += '#{0:6f}'.format(time.time())
        #EvalLog('{0:6f},120,{1}'.format(time.time(), evalTime))
        #WriteLogs()
        pass

def cleanup():
    global cpuWorkerPool
    global memWorkerPool
    try:
        cpuWorkerPool.terminate()
        memWorkerPool.terminate()
    except Exception:
        logging.warning('exception when cleaning up cpuWorkerPool and memWorkerPool')

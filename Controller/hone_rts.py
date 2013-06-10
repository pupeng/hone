# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_rts.py
# The runtime system of the controller
# Core functionality
# It boots recvModule, exeModule, netModule
# It maintains the data structure of topology, hosts, jobs, etc.

''' hone_rts is Singleton, implemented as Python module '''
import socket
import logging
from multiprocessing import Queue

import hone_recvModule as recvModule
import hone_exeModule as exeModule
import hone_netModule as netModule
from hone_partition import *
from hone_util import LogUtil
from hone_hostEntry import *
from hone_job import *

evalTimestamp = 'Begin'

''' supported statistics '''
HoneTableTypes = {'HostConnection': ['app', 'srcHost', 'BytesWritten',
                                     'CurrentTime',
                                     'ThruOctetsReceived',
                                     'OtherReductionsCM',
                                     'OtherReductionsCV',
                                     'MaxRTT',
                                     'SoftErrorReason',
                                     'SndLimTimeSnd',
                                     'MaxRTO',
                                     'Rcvbuf',
                                     'RetranThresh',
                                     'SegmentsOut',
                                     'NonRecovDAEpisodes',
                                     'IpTtl',
                                     'CongSignals',
                                     'BytesSentOut',
                                     'DataSegsOut',
                                     'srcIP',
                                     'LimRwin',
                                     'dstPort',
                                     'MSSRcvd',
                                     'WinScaleSent',
                                     'ZeroRwinRcvd',
                                     'StartTimeSecs',
                                     'AbruptTimeouts',
                                     'NonRecovDA',
                                     'WinScaleRcvd',
                                     'Sndbuf',
                                     'SpuriousFrDetected',
                                     'IpTosOut',
                                     'State',
                                     'Nagle',
                                     'CurReasmQueue',
                                     'ECESent',
                                     'DupAcksOut',
                                     'SndLimTimeCwnd',
                                     'MaxReasmQueue',
                                     'CurSsthresh',
                                     'MSSSent',
                                     'SpuriousRtoDetected',
                                     'CurAppRQueue',
                                     'RTT',
                                     'DupAcksIn',
                                     'MaxMSS',
                                     'LimCwnd',
                                     'TimeStamps',
                                     'MinSsthresh',
                                     'RcvRTT',
                                     'BytesReceived',
                                     'CurAppWQueue',
                                     'SendStall',
                                     'SACKsRcvd',
                                     'SndLimTimeRwin',
                                     'SegsIn',
                                     'RTTVar',
                                     'SndLimTransCwnd',
                                     'MaxRwinSent',
                                     'IpTosIn',
                                     'SndInitial',
                                     'MaxPipeSize',
                                     'ECNsignals',
                                     'PreCongSumCwnd',
                                     'InRecovery',
                                     'SndLimTransRwin',
                                     'SubsequentTimeouts',
                                     'MaxAppRQueue',
                                     'ElapsedMicroSecs',
                                     'LocalAddressType',
                                     'DSACKDups',
                                     'BytesRetran',
                                     'MinRTO',
                                     'MinMSS',
                                     'WillSendSACK',
                                     'ECN',
                                     'MaxSsthresh',
                                     'PipeSize',
                                     'SumOctetsReordered',
                                     'MinRTT',
                                     'MaxCaCwnd',
                                     'SumRTT',
                                     'PostCongSumRTT',
                                     'RecInitial',
                                     'DupAckEpisodes',
                                     'SACKBlocksRcvd',
                                     'WillUseSACK',
                                     'ThruOctetsAcked',
                                     'OtherReductions',
                                     'MaxRwinRcvd',
                                     'SlowStart',
                                     'MaxSsCwnd',
                                     'SegsRetrans',
                                     'CongOverCount',
                                     'LimMSS',
                                     'CurRTO',
                                     'CERcvd',
                                     'CountRTT',
                                     'CurRetxQueue',
                                     'dstIP',
                                     'PreCongSumRTT',
                                     'SoftErrors',
                                     'srcPort',
                                     'SndLimTransSnd',
                                     'ElapsedSecs',
                                     'PostCongCountRTT',
                                     'ActiveOpen',
                                     'StartTimeMicroSecs',
                                     'SmoothedRTT',
                                     'Rwnd',
                                     'CongAvoid',
                                     'ZeroRwinSent',
                                     'Timeouts',
                                     'SndMax',
                                     'SndUna',
                                     'MaxRetxQueue',
                                     'CurRwinSent',
                                     'FastRetran',
                                     'LimSsthresh',
                                     'SndNxt',
                                     'RemAddressType',
                                     'Cwnd',
                                     'CurTimeoutCount',
                                     'MaxAppWQueue',
                                     'DataSegsIn',
                                     'RcvNxt',
                                     'CurMSS'],
                  'LinkStatus': ['BeginDevice', 'BeginPort', 'EndDevice', 'EndPort'],
                  'SwitchStatus': ['switchId', 'portNumber', 'collisions', 'receiveBytes', 'receiveCRCErrors', 'receiveDropped', 'receiveErrors',
                                   'receiveFrameErrors', 'receiveOverrunErrors', 'receivePackets', 'transmitBytes', 'transmitDropped',
                                   'transmitErrors', 'transmitPackets', 'capacity'],
                  'HostRoute' : ['HostA', 'HostB', 'Path'],
                  'AppStatus': ['hostId', 'app', 'cpu', 'memory'],
                  'HostStatus': ['hostId', 'totalCPU', 'totalMemory']}

''' job ID '''
MaxJobId = 100

''' hone_hostinfo system application '''
HoneHostInfoJobId = MaxJobId + 1
HoneHostInfoJob = 'hone_hostinfo'

''' job ID '''
HoneJobIdStartPoint = 60 #random.randint(0, MaxJobId)

''' generate next available HONE job ID '''
def _nextHoneJobId():
    global HoneJobIdStartPoint
    HoneJobIdStartPoint = (HoneJobIdStartPoint + 1) % MaxJobId
    return HoneJobIdStartPoint

''' find the controller's IP address '''
def GetControllerLocalIP():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('google.com', 0))
    return s.getsockname()[0]

# key: hostId, value: HostEntry
HostRecord = {'controller': HostEntry('controller', GetControllerLocalIP()),
              'network'   : HostEntry('network', '127.0.0.1')}

# key: jobId, value: HoneJob
_jobExecution = {}

# key: str(criterion), value: jobId
_controlJobIds = {}

# queue to network module
NetworkModuleQueue = None

''' function to start rts '''
def RtsRun(mgmtProg):
    print 'HONE runtime system starts.'
    logging.info('hone rts starts')
    LogUtil.EvalLog('rts', 'runtime system starts to run')
    mgmtDataflow = map(_executeMgmtMain, mgmtProg)
    LogUtil.DebugLog('global', 'mgmtDataFlow')
    global NetworkModuleQueue
    NetworkModuleQueue = Queue()
    netModuleProcess = netModule.NetworkModuleProcess(NetworkModuleQueue)
    netModuleProcess.start()
    for (eachProgName, eachFlow) in mgmtDataflow:
        LogUtil.DebugLog('global', eachFlow.printDataFlow())
        if eachProgName == HoneHostInfoJob:
            newJobId = HoneHostInfoJobId
        else:
            newJobId = _nextHoneJobId()
            LogUtil.DebugLog('rts', 'prog name: {0}. jobId: {1}.'.format(eachProgName, newJobId))
        partitionedFlow = HonePartitionedFlow(eachProgName, eachFlow)
        newJob = HoneJob(newJobId, partitionedFlow)
        _jobExecution[newJobId] = newJob
    try:
        LogUtil.EvalLog('StartRecvModule', 'start recvModule')
        recvModule.recvModuleRun()
    except KeyboardInterrupt:
        LogUtil.DebugLog('global', 'catch keyboard keyboard interrupt')
    finally:
        netModuleProcess.stop()
        LogUtil.OutputEvalLog()
        print 'Exit from hone_rts'

''' process each mgmt prog '''
def _executeMgmtMain(progName):
    try:
        mgmtModule = __import__(progName)
        mgmtMain = getattr(mgmtModule, 'main')
    except ImportError:
        raise Exception('Fail to import ' + progName)
    except AttributeError, msg:
        raise Exception('Fail to find main function in {0} with msg: {1}'.format(progName, msg))
    finally:
        return (progName, mgmtMain())

def handleHostJoin(hostId, hostAddress):
    LogUtil.DebugLog('rts', 'new host joins with id {0} and address {1}'.format(hostId, hostAddress))
    logging.info('new host joins with id {0} and address {1}'.format(hostId, hostAddress))
    entry = HostEntry(hostId, hostAddress)
    HostRecord[hostId] = entry
    for (jobId, job) in _jobExecution.iteritems():
        # LogUtil.DebugLog('exeGen', 'host {0} eligible for job {1}? {2}'.format(entry.hostId, jobId, job.isHostEligible(entry)))
        if job.isHostEligible(entry):
            entry.addJob(jobId)
            job.addHost(entry)

def handleHostLeave(hostId):
    LogUtil.DebugLog('rts', 'host {0} leaves'.format(hostId))
    logging.info('host {0} leaves'.format(hostId))
    if hostId in HostRecord:
        hostEntry = HostRecord[hostId]
        for jobId in hostEntry.jobs:
            _jobExecution[jobId].removeHost(hostEntry)
        del HostRecord[hostId]

def handleHostInfoUpdate(message):
    #LogUtil.EvalLog('HostInfoUpdate', 'handle host info update hostId {0}'.format(message.hostId))
    hostId = message.hostId
    appCL = message.content
    hostEntry = HostRecord[hostId]
    # LogUtil.DebugLog('rts', 'host info update. hostId {0}. appList {1}. add {2}. delete {3}'.format(
    #     hostId, hostEntry.appList, appCL.add, appCL.delete))
    hostEntry.appList = list((set(hostEntry.appList) - set(appCL.delete)) | set(appCL.add))
    for (jobId, job) in _jobExecution.iteritems():
        eligible = job.isHostEligible(hostEntry)
        if (jobId in hostEntry.jobs) and (not eligible):
            hostEntry.removeJob(jobId)
            job.removeHost(hostEntry)
        elif (jobId not in hostEntry.jobs) and eligible:
            hostEntry.addJob(jobId)
            job.addHost(hostEntry)
            #LogUtil.EvalLog('DoneHostInfoUpdate', 'done update host info of {0}'.format(hostId))

def handleStatsIn(message):
    # LogUtil.DebugLog('rts', 'new stats come in for job {0} flow {1} sequence {2} content {3}'.format(
    #     message.jobId, message.flowId, message.sequence, message.content))
    expectedNum = _jobExecution[message.jobId].GetExpectedNumOfHosts(message.flowId)
    exeModule.handleStatsIn(message, expectedNum)

def handleControlJob(dataflow):
    # LogUtil.DebugLog('rts', 'control job', dataflow.printDataFlow(), dataflow.flow)
    criterion = str(dataflow.getFlowCriterion())
    progName = 'hone_control'
    if criterion in _controlJobIds:
        jobId = _controlJobIds[criterion]
        job = _jobExecution[jobId]
        job.updateControlAction(dataflow.flow[1:])
    else:
        jobId = _nextHoneJobId()
        _controlJobIds[criterion] = jobId
        partitionedFlow = HonePartitionedFlow(progName, dataflow)
        job = HoneJob(jobId, partitionedFlow)
        _jobExecution[jobId] = job
        for hostId, hostEntry in HostRecord.iteritems():
            if job.isHostEligible(hostEntry):
                hostEntry.addJob(jobId)
                job.addHost(hostEntry)
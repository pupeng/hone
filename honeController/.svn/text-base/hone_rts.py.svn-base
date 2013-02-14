'''
Peng Sun
hone_rts.py
The runtime system of the controller
Core functionality
It boots recvModule, exeModule
It maintains the data structure of topology, hosts, jobs, etc. 
'''
    
import hone_recvModule
from hone_partition import *
from hone_util import *
import hone_exeModule as exeModule
from hone_message import *
from hone_sndModule import *

import random
import time
import math
import socket
#import logging

# branching factor
BranchFactor = 4

evalTimestamp = ''

''' supported statistics '''
honeTableTypes = { 'HostConnection': ['app', 'srcHost', 'BytesWritten', \
                                      'CurrentTime', \
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
                   'LinkStatus'    : ['BeginDevice', 'BeginPort', 'EndDevice', \
                                      'EndPort'],
                   'SwitchStatus'  : ['switchId'],
                   'AppStatus'     : ['hostId', 'app', 'cpu', 'memory'],
                   'HostStatus'    : ['hostId', 'totalCPU', 'totalMemory'] }

''' job ID '''
MaxJobId = 100

''' hone_hostinfo system application '''
HoneHostInfoJobId = MaxJobId + 1
HoneHostInfoJob = 'hone_hostinfo'

''' job ID '''
honeJobIdStartPoint = 60 #random.randint(0, MaxJobId)
def _nextHoneJobId():
    global honeJobIdStartPoint
    honeJobIdStartPoint = (honeJobIdStartPoint + 1) % MaxJobId
    return honeJobIdStartPoint

def GetControllerLocalIP():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('google.com', 0))
    return s.getsockname()[0]

''' entry about host's information '''
class HostEntry:
    def __init__(self, hostId, hostAddress, appList=None, jobs=None):
        self.hostId = hostId
        self.hostAddress = hostAddress
        if (appList is None):
            self.appList = []
        else:
            self.appList = appList
        if (jobs is None):
            self.jobs = []
        else:
            self.jobs = jobs

    def addJob(self, jobId):
        if (jobId not in self.jobs):
            self.jobs.append(jobId)
        #debugLog('exeGen', 'hostEntry addJob', \
        #         'hostId:', self.hostId, \
        #         'jobId:', jobId, \
        #         'jobs:', self.jobs)

    def removeJob(self, jobId):
        if (jobId in self.jobs):
            self.jobs.remove(jobId)
        #debugLog('exeGen', 'hostEntry removeJob', \
        #         'hostId:', self.hostId, \
        #         'jobId:', jobId, \
        #         'jobs:', self.jobs)

# key: hostId, value: hostEntry
_hostInfo = {'controller': HostEntry('controller', GetControllerLocalIP())}

# key: jobId, value: honeJob
_jobExecution = {}

''' execution states of a job '''
class HoneJob:
    def __init__(self, jobId, partitionedFlow):
        if not isinstance(partitionedFlow, HonePartitionedFlow):
            raise Exception('improper argument passed into HoneJob')
        self.jobId = jobId
        self.exeFlow = partitionedFlow
        self.createTime = time.time()
        #self.createTime = int(time.time() / float(self.exeFlow.minQueryPeriod) * 1000.0) * (float(self.exeFlow.minQueryPeriod) / 1000.0)
        self.hosts = []
        # number of expected hosts when merging by sequence
        # key: flowId, value: number
        self.expectedNumOfHosts = {}
        for flowId in self.exeFlow.flowToCtrl:
            self.expectedNumOfHosts[flowId] = 0
        for flowId in self.exeFlow.flowToMiddle:
            self.expectedNumOfHosts[flowId] = 0
        # key: hostId, value: parent
        self.hostParent = {}
        # list: 0 is the leaves i.e., the host source, from 1 and up
        self.aggTree = [{}, {'controller': []}]
        # debug
        self.middleJobExpectNum = {}
        # source job installation
        self.installedHosts = {}

    def printAggTree(self):
        name = 1
        hostName = {'controller':'controller'}
        for host in self.aggTree[0].iterkeys():
            hostName[host] = 'host{0}'.format(name)
            name += 1
        print 'current agg tree:'
        for i in reversed(range(len(self.aggTree))):
            line = ''
            for host in self.aggTree[i].iterkeys():
                line += '{0} '.format(_hostInfo[host].hostAddress)
            print line
        print '\n\n'

    def addHostSource(self, hostEntry):
        #EvalLog('{0:6f},25,job {1} add host source {2}'.format(time.time(), self.jobId, hostEntry.hostId))
        if (hostEntry.hostId not in self.hosts):
            self.hosts.append(hostEntry.hostId)
            self.transferExeFile(hostEntry.hostId)
        #debugLog('exeGen', 'honeJob addHostSource', \
        #         'jobId:', self.jobId, \
        #         'hostId:', hostEntry.hostId, \
        #         'hostAddress:', hostEntry.hostAddress, \
        #         'hosts:', self.hosts)
        m = len(self.aggTree[0])
        n = len(self.aggTree)
        if m == 0:
            exeModule.buildExePlan(self.jobId, \
                self.exeFlow.progName, \
                self.exeFlow.controllerExePlan)
        if not self.exeFlow.hostMiddleExePlan:
            self.aggTree[1]['controller'].append(hostEntry.hostId)
            for flowId in self.exeFlow.flowToCtrl:
                self.expectedNumOfHosts[flowId] += 1
            self.updateSourceExe(hostEntry.hostId, 'controller')
        else:
            if m < math.pow(BranchFactor, n-1):
                if n == 2:
                    self.aggTree[1]['controller'].append(hostEntry.hostId)
                    for flowId in self.exeFlow.flowToMiddle:
                        self.expectedNumOfHosts[flowId] += 1
                    self.updateSourceExe(hostEntry.hostId, 'controller')
                else:
                    for searchLevel in range(1,n):
                        for (hostInLevel, hostChildren) in self.aggTree[searchLevel].iteritems():
                            if len(hostChildren) < BranchFactor:
                                self.updateMiddleChild_add(searchLevel, hostInLevel, hostEntry.hostId)
                                self.updateMiddleExe(searchLevel-1, hostEntry.hostId, hostInLevel)
                                if searchLevel == 1:
                                    self.updateSourceExe(hostEntry.hostId, hostInLevel)
                                return
                        self.updateMiddleChild_add(searchLevel, hostEntry.hostId, hostEntry.hostId)
                        self.updateMiddleExe(searchLevel-1, hostEntry.hostId, hostEntry.hostId)
                        if searchLevel == 1:
                            self.updateSourceExe(hostEntry.hostId, hostEntry.hostId)
            else:
                newParentHost = self.aggTree[n-2].keys()[0]
                for childHost in self.aggTree[n-2].keys():
                    self.updateMiddleChild_add(n-1, newParentHost, childHost)
                    self.updateMiddleExe(n-2, childHost, newParentHost)
                    if n == 2:
                        self.updateSourceExe(childHost, newParentHost)
                del self.aggTree[n-1]['controller']
                self.aggTree.append({'controller' : [newParentHost]})
                for flowId in self.exeFlow.flowToMiddle:
                    self.expectedNumOfHosts[flowId] = 1
                self.updateMiddleExe(n-1, newParentHost, 'controller')
                if n == 1:
                    self.updateSourceExe(newParentHost, 'controller')
                for level in range(1,n):
                    self.updateMiddleChild_add(level, hostEntry.hostId, hostEntry.hostId)
                    self.updateMiddleExe(level-1, hostEntry.hostId, hostEntry.hostId)
                    if level == 1:
                        self.updateSourceExe(hostEntry.hostId, hostEntry.hostId)
                self.updateMiddleChild_add(n, 'controller', hostEntry.hostId)
                self.updateMiddleExe(n-1, hostEntry.hostId, 'controller')
                if n == 1:
                    self.updateSourceExe(hostEntry.hostId, 'controller')
        self.printAggTree()
    
    def removeHostSource(self, hostEntry):
        #EvalLog('{0:6f},27,remove host source {1} from job {2}'.format(time.time(), hostEntry.hostId, self.jobId))
        if (hostEntry.hostId in self.hosts):
            self.hosts.remove(hostEntry.hostId)
        #debugLog('exeGen', 'honeJob removeHostSource', \
        #         'jobId:', self.jobId, \
        #         'hostId:', hostEntry.hostId, \
        #         'hostAddress:', hostEntry.hostAddress, \
        #         'hosts:', self.hosts)
#        if (hostEntry.hostId in self.hostMiddle):
#            if (len(self.hostMiddle[hostEntry.hostId]) > 1):
#                newMiddleHost = self.hostMiddle[hostEntry.hostId][1]
#                newMiddleChildren = self.hostMiddle[hostEntry.hostId][1:]
#                self.removeMiddleExe(hostEntry.hostId)
#                self.updateMiddleExe(newMiddleHost, newMiddleChildren)
#                for host in newMiddleChildren:
#                    self.updateSourceExe(host, newMiddleHost)
#            else:
#                self.removeMiddleExe(hostEntry.hostId)
#        else:
#            middleHost = self.hostSource[hostEntry.hostId]
#            self.updateMiddleExe_removeChild(middleHost, hostEntry.hostId)
#        self.removeSourceExe(hostEntry.hostId)
        #debugLog('exeMod', 'removeHostSource in jobId: ' + str(self.jobId), \
        #         'host source len:' + str(len(self.hostSource)), \
        #         self.hostSource, \
        #         'host middle len:' + str(len(self.hostMiddle)), \
        #         self.hostMiddle)
        #EvalLog('{0:6f},28,done remove host source {1} from job {2}'.format(time.time(), hostEntry.hostId, self.jobId))

    def isHostEligible(self, hostEntry):
        #EvalLog('{0:6f},29,check host({1}) is eligible for jobId({2})'.format(time.time(), hostEntry.hostId, self.jobId))
        ret = self.exeFlow.isHostEligible(hostEntry)
        #EvalLog('{0:6f},30,done check eligibility for jobId {1}'.format(time.time(), self.jobId))
        return ret
    
    def transferExeFile(self, hostId):
        #EvalLog('{0:6f},31,transfer file of job ({1}) to host {2}'.format(time.time(), self.jobId, hostId))
        address = _hostInfo[hostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_SendFile
        message.jobId = self.jobId
        HoneHostSndModule().sendFile(address, message, self.exeFlow.progName)
        #EvalLog('{0:6f},32,transfer file done.'.format(time.time()))

    def updateSourceExe(self, hostId, middleHostId):
        if (len(self.exeFlow.hostSourceExePlan) == 0):
            return
        #EvalLog('{0:6f},33,update source job {1} exe on host {2}'.format(time.time(), self.jobId, hostId))
        address = _hostInfo[hostId].hostAddress
        middleAddress = _hostInfo[middleHostId].hostAddress
        message = HoneMessage()
        if hostId in self.installedHosts:
            message.messageType = HoneMessageType_UpdateSourceJob
            message.jobId = self.jobId
            message.content = middleAddress
        else:
            message.messageType = HoneMessageType_InstallSourceJob
            message.jobId = self.jobId
            message.content = (middleAddress, \
                               self.createTime, \
                               self.exeFlow.progName, \
                               self.exeFlow.hostSourceExePlan)
            self.installedHosts[hostId] = None
        if hostId not in self.hostParent:
            self.hostParent[hostId] = {}
        self.hostParent[hostId][0] = middleHostId
        self.aggTree[0][hostId] = []
        #debugLog('rts', 'sendmessage', message.messageType)
        HoneHostSndModule().sendMessage(address, message)

#    def removeSourceExe(self, hostId):
#        #EvalLog('{0:6f},35,remove source job {1} from host {2}'.format(time.time(), self.jobId, hostId))
#        if (hostId in self.hostSource):
#            del self.hostSource[hostId]
#            for flowId in self.exeFlow.flowToCtrl:
#                self.expectedNumOfHosts[flowId] -= 1
        #debugLog('exeMod', 'removeSourceExe. expected num of hosts', \
        #         self.expectedNumOfHosts)
        #EvalLog('{0:6f},36,done remove source job {1} from host {2}'.format(time.time(), self.jobId, hostId))

    def updateMiddleExe(self, level, childHost, parentHost):
        if not self.exeFlow.hostMiddleExePlan:
            return
        address = _hostInfo[childHost].hostAddress
        parentAddress = _hostInfo[parentHost].hostAddress
        message = HoneMessage()
        if (childHost in self.hostParent) and (level in self.hostParent[childHost]):
            if parentAddress != self.hostParent[childHost][level]:
                message.messageType = HoneMessageType_UpdateMiddleJob
                message.jobId = self.jobId
                message.level = level
                if childHost not in self.middleJobExpectNum:
                    self.middleJobExpectNum[childHost] = {}
                if level not in self.middleJobExpectNum[childHost]:
                    self.middleJobExpectNum[childHost][level] = 0
                self.middleJobExpectNum[childHost][level] += 0
                message.content = (self.middleJobExpectNum[childHost][level], parentAddress)
            else:
                message = None
        else:
            if childHost not in self.hostParent:
                self.hostParent[childHost] = {}
            if childHost in self.aggTree[level]:
                numOfChild = len(self.aggTree[level][childHost])
            else:
                numOfChild = 0
            message.messageType = HoneMessageType_InstallMiddleJob
            message.jobId = self.jobId
            message.level = level
            if childHost not in self.middleJobExpectNum:
                self.middleJobExpectNum[childHost] = {}
            if level not in self.middleJobExpectNum[childHost]:
                self.middleJobExpectNum[childHost][level] = 0
            self.middleJobExpectNum[childHost][level] = numOfChild
            message.content = (numOfChild,\
                               parentAddress, \
                               self.exeFlow.progName,\
                               self.exeFlow.hostMiddleExePlan)
        self.hostParent[childHost][level] = parentHost
        if childHost not in self.aggTree[level]:
            self.aggTree[level][childHost] = []
        if message:
            #debugLog('rts', 'sendmessage', message.messageType)
            HoneHostSndModule().sendMessage(address, message)
        #debugLog('exeMod', 'updateMiddleExe. expected num of hosts', \
        #         self.expectedNumOfHosts)
        #EvalLog('{0:6f},38,done update job {1} middle exe on host {2}'.format(time.time(), self.jobId, middleHostId))

    def updateMiddleChild_add(self, level, parentHost, childHost):
        if (len(self.exeFlow.hostMiddleExePlan) == 0):
            return
        if parentHost == 'controller':
            for flowId in self.exeFlow.flowToMiddle:
                self.expectedNumOfHosts[flowId] += 1
            self.aggTree[level][parentHost].append(childHost)
            if childHost not in self.hostParent:
                self.hostParent[childHost] = {}
            self.hostParent[childHost][level-1] = parentHost
            return
        if (parentHost in self.hostParent) and (level in self.hostParent[parentHost]):
            parentParentAddress = _hostInfo[self.hostParent[parentHost][level]].hostAddress
        else:
            parentParentAddress = _hostInfo['controller'].hostAddress
        message = HoneMessage()
        if parentHost not in self.aggTree[level]:
            self.aggTree[level][parentHost] = []
            message.messageType = HoneMessageType_InstallMiddleJob
            message.jobId = self.jobId
            message.level = level
            message.content = (1, \
                               parentParentAddress, \
                               self.exeFlow.progName, \
                               self.exeFlow.hostMiddleExePlan)
            if parentHost not in self.middleJobExpectNum:
                self.middleJobExpectNum[parentHost] = {}
            if level not in self.middleJobExpectNum[parentHost]:
                self.middleJobExpectNum[parentHost][level] = 0
            self.middleJobExpectNum[parentHost][level] = 1
        else:
            message.messageType = HoneMessageType_UpdateMiddleJob
            message.jobId = self.jobId
            message.level = level
            if parentHost not in self.middleJobExpectNum:
                self.middleJobExpectNum[parentHost] = {}
            if level not in self.middleJobExpectNum[parentHost]:
                self.middleJobExpectNum[parentHost][level] = 0
            self.middleJobExpectNum[parentHost][level] += 1
            message.content = (self.middleJobExpectNum[parentHost][level], parentParentAddress)
        self.aggTree[level][parentHost].append(childHost)
        if childHost not in self.aggTree[level-1]:
            self.aggTree[level-1][childHost] = []
        if childHost not in self.hostParent:
            self.hostParent[childHost] = {}
        self.hostParent[childHost][level-1] = parentHost
        address = _hostInfo[parentHost].hostAddress
        #debugLog('rts', 'sendmessage', message.messageType)
        HoneHostSndModule().sendMessage(address, message)

#    def updateMiddleExe_removeChild(self, middleHostId, childHostId):
#        if (len(self.exeFlow.hostMiddleExePlan) == 0):
#            return
#        #EvalLog('{0:6f},41,update middle job {1}: remove child'.format(time.time(), self.jobId))
#        self.hostMiddle[middleHostId].remove(childHostId)
#        address = _hostInfo[middleHostId].hostAddress
#        message = HoneMessage()
#        message.messageType = HoneMessageType_UpdateMiddleJob
#        message.jobId = self.jobId
#        message.content = -1
#        HoneHostSndModule().sendMessage(address, message)
#        #debugLog('exeMod', 'updateMiddleExe_removeChild. expectedNumOfHosts', \
#        #         self.expectedNumOfHosts)
#        #EvalLog('{0:6f},42,done update middle job {1}: remove child'.format(time.time(), self.jobId))
#
#    def removeMiddleExe(self, middleHostId):
#        if (middleHostId in self.hostMiddle):
#            del self.hostMiddle[middleHostId]
#            for flowId in self.exeFlow.flowToMiddle:
#                self.expectedNumOfHosts[flowId] -= 1
#        #debugLog('exeMod', 'removeMiddleExe. expectedNumOfHosts', \
#        #         self.expectedNumOfHosts)

''' function to start rts '''
def RtsRun(mgmtProg):
    print 'hone runtime system starts to run'
    EvalLog('{0:6f},3,runtime system starts to run'.format(time.time()))
    mgmtDataflow = map(_executeMgmtMain, mgmtProg)
    #EvalLog('{0:6f},4,get management programs'.format(time.time()))
    #debugLog('global', 'mgmtDataFlow')
    for (eachProgName, eachFlow) in mgmtDataflow:
        #debugLog('global', eachFlow.printDataFlow())
        if (eachProgName == HoneHostInfoJob):
            newJobId = HoneHostInfoJobId
        else:
            newJobId = _nextHoneJobId()
        #debugLog('rts', eachProgName, newJobId)
        #EvalLog('{0:6f},5,start to partition program flow {1}'.format(time.time(), eachProgName))
        partitionedFlow = HonePartitionedFlow(eachProgName, eachFlow)
        #EvalLog('{0:6f},6,done partitioning program flow {1}'.format(time.time(), eachProgName))
        newJob = HoneJob(newJobId, partitionedFlow)
        _jobExecution[newJobId] = newJob
    try:
        #EvalLog('{0:6f},7,start recvModule'.format(time.time()))
        hone_recvModule.recvModuleRun()
    except KeyboardInterrupt:
        #debugLog('global', 'catch keyboard interrupt')
        pass
    finally:
        #EvalLog('{0:6f},8,exit hone runtime system'.format(time.time()))
        WriteLogs()
        print 'Exit from hone_rts'
        
''' process each mgmt prog '''
def _executeMgmtMain(progName):
    try:
        mgmtModule = __import__(progName)
        mgmtMain = getattr(mgmtModule, 'main')
    except ImportError:
        raise Exception('Fail to import ' + progName)
    except AttributeError, msg:
        raise Exception('Fail to find main function in ' + progName + msg)
    finally:
        return (progName, mgmtMain())

def handleHostJoin(hostId, hostAddress):
    #debugLog('rts', 'new host joins', hostId, hostAddress)
    #EvalLog('{0:6f},17,rts handle host join Id {1} address {2}'.format(time.time(), hostId, hostAddress))
    entry = HostEntry(hostId, hostAddress)
    _hostInfo[hostId] = entry
    #debugLog('rts', 'host info', _hostInfo)
    for (jobId, job) in _jobExecution.iteritems():
        #debugLog('exeGen', 'job.isHostEligible', jobId, entry.hostId, \
        #         job.isHostEligible(entry))
        if job.isHostEligible(entry):
            entry.addJob(jobId)
            job.addHostSource(entry)
    #EvalLog('{0:6f},18,rts done handle host join'.format(time.time()))

def handleHostLeave(hostId):
    #debugLog('rts', 'host leaves', hostId, _hostInfo)
    #EvalLog('{0:6f},19,rts handle host leave Id {1}'.format(time.time(), hostId))
    if (hostId in _hostInfo):
        hostEntry = _hostInfo[hostId]
        for jobId in hostEntry.jobs:
            _jobExecution[jobId].removeHostSource(hostEntry)
        del _hostInfo[hostId]
    #EvalLog('{0:6f},20,rts done handle host leave Id {1}'.format(time.time(), hostId))
    
def handleHostInfoUpdate(message):
    #EvalLog('{0:6f},21,handle host info update hostId {1}'.format(time.time(), message.hostId))
    hostId = message.hostId
    appCL = message.content
    hostEntry = _hostInfo[hostId]
    #debugLog('rts', 'host info update', hostId, hostEntry.appList, appCL.add, appCL.delete)
    hostEntry.appList = list((set(hostEntry.appList) - set(appCL.delete)) \
                             | set(appCL.add))
    for (jobId, job) in _jobExecution.iteritems():
        eligible = job.isHostEligible(hostEntry)
        if (jobId in hostEntry.jobs) and (not eligible):
            hostEntry.removeJob(jobId)
            job.removeHostSource(hostEntry)
        elif (jobId not in hostEntry.jobs) and eligible:
            hostEntry.addJob(jobId)
            job.addHostSource(hostEntry)
    #EvalLog('{0:6f},22,done handle host info update hostId {1}'.format(time.time(), message.hostId))

def handleStatsIn(message):
    #EvalLog('{0:6f},23,handle stats in. jobId {1}. flowId {2}. sequence {3}'.format(time.time(), message.jobId, message.flowId, message.sequence))
    #debugLog('rts', 'new stats come in', message.jobId, \
    #         message.flowId, message.sequence, message.content)
    expectedNum = _jobExecution[message.jobId].expectedNumOfHosts[message.flowId]
    exeModule.handleStatsIn(message, expectedNum)
    #EvalLog('{0:6f},24,done handle new stats jobId {1} flowId {2}'.format(time.time(), message.jobId, message.flowId))

#def addControlJob(content):
#    ''' 
#    if _GLOBAL_DEBUG_:
#        rtsLoggingLock.acquire()
#        print 'in addControlJob'
#        print content
#        rtsLoggingLock.release() '''
#    dataflow = content[1]
#    criteria = dataflow.myFlow[2]
#    cr = []
#    for i in range(1,len(criteria)):
#        cr.append(criteria[i][2])
#    cr = tuple(cr)
#    '''
#    if _CONTROL_DEBUG_:
#        rtsLoggingLock.acquire()
#        print 'in addControlJob'
#        print repr(cr)
#        print 'controlJobs:'
#        print controlJobs
#        rtsLoggingLock.release() '''
#    if controlJobs.has_key(cr):
#        jobId = controlJobs[cr]
#        newRate = dataflow.myFlow[5][1]
#        for item in jobExePlanHost[jobId].iterkeys():
#            if isinstance(item, int):
#                flowIndex = item
#                break
#        '''
#        if _GLOBAL_DEBUG_:
#            rtsLoggingLock.acquire()
#            print 'newRate:'+str(newRate)
#            print 'jobExePlanHost of '+jobId+' '+str(flowIndex)
#            print jobExePlanHost[jobId][flowIndex]
#            rtsLoggingLock.release() '''
#        try:
#            if jobExePlanHost[jobId][flowIndex][1][1] != newRate:
#                # update job on hosts
#                jobExePlanHost[jobId][flowIndex][1][1] = newRate
#                for hostId in hostsOfJob[jobId]:
#                    updateJobOnHost(jobId,hostId)
#        except:
#            rtsLoggingLock.acquire()
#            print 'exception point A'
#            print jobId
#            print flowIndex
#            print jobExePlanHost[jobId]
#            rtsLoggingLock.release()
#            sys.exit(0)
#    else:
#        jobId = _nextHoneJobId()
#        _processQueryPart(dataflow)
#        jobEligibleBar[jobId] = generateJobEligibleBar(dataflow)
#        jobCreateTime = time.time()
#        jobExePlanHost[jobId] = {}
#        jobExePlanHost[jobId]['progName'] = 'controldummy'
#        jobExePlanHost[jobId]['jobCreateTime'] = jobCreateTime
#        jobExePlanNet[jobId] = {}
#        jobExePlanNet[jobId]['progName'] = 'controldummy'
#        jobExePlanCtrl[jobId] = {}
#        jobExePlanCtrl[jobId]['progName'] = 'controldummy'
#        _addExePlans(dataflow, jobId)
#        controlJobs[cr] = jobId
#        for hostId in hosts.iterkeys():
#            hostAddr = hosts[hostId][0]
#            if hostMatchJob(hostId,hostAddr,jobId):
#                installNewJobOnHost(jobId, hostId)
#                jobsOnHost[hostId].append(jobId)
#                if not (jobId in hostsOfJob):
#                    hostsOfJob[jobId] = []
#                hostsOfJob[jobId].append(hostId)  
#    '''
#    if _GLOBAL_DEBUG_:
#        rtsLoggingLock.acquire()
#        print 'Done addControlJob'
#        print jobExePlanHost[jobId]
#        rtsLoggingLock.release() '''

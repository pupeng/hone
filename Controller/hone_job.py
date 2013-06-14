# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_job.py
# Define class HoneJob to contain job-related operations

import hone_rts as rts
import hone_exeModule as exeModule
from hone_message import *
from hone_sndModule import *
from hone_partition import *
from hone_aggTreeFormation import *

''' execution states of a job '''
class HoneJob:
    # public methods
    def __init__(self, jobId, partitionedFlow):
        if not isinstance(partitionedFlow, HonePartitionedFlow):
            raise Exception('improper argument passed into HoneJob')
        # construct necessary data members
        self.jobId = jobId
        self.exeFlow = partitionedFlow
        self.createTime = time.time()
        self.hosts = {}
        # record the links in the aggregation tree
        # start from 0 for the leave-to-immediate-parent link.
        # the last element is the links to the controller
        # aggStructRecord[n] = {parentHostId : list of children host IDs}
        self.aggStructRecord = []
        # get instance of aggTreeFormation class
        self.treeFormatter = TreeFormatterFactory.GetNewFormatter(self)
        # register controller-side and network-side execution
        exeModule.buildExePlan(self.jobId, self.exeFlow.progName, self.exeFlow.controllerExePlan)
        if self.exeFlow.networkExePlan:
            item = ('NewNetworkJob', [self.jobId, self.createTime, self.exeFlow.progName, self.exeFlow.networkExePlan])
            rts.NetworkModuleQueue.put(item)

    def addHost(self, hostEntry):
        logging.info('job {0} add host {1}'.format(self.jobId, hostEntry.hostId))
        if hostEntry.hostId not in self.hosts:
            self.hosts[hostEntry.hostId] = None
            self._transferExeFile(hostEntry.hostId)
            #self.addAggLink(0, hostEntry.hostId, 'controller')
            self.treeFormatter.addLeaf(hostEntry)
            self.DisplayAggTree()

    def removeHost(self, hostEntry):
        logging.info('Remove host {0} from job {1}'.format(hostEntry.hostId, self.jobId))
        if hostEntry.hostId in self.hosts:
            del self.hosts[hostEntry.hostId]
            self.treeFormatter.removeLeaf(hostEntry)

    def isHostEligible(self, hostEntry):
        return self.exeFlow.isHostEligible(hostEntry)

    def addAggLink(self, level, childHostId, parentHostId):
        logging.info('job {0} add a level-{3} link from host {1} to host {2}'.format(self.jobId, childHostId, parentHostId, level))
        while len(self.aggStructRecord) <= level:
            self.aggStructRecord.append({})
        if parentHostId not in self.aggStructRecord[level]:
            self.aggStructRecord[level][parentHostId] = []
            self._installMiddleExe(parentHostId, level)
        if childHostId not in self.aggStructRecord[level][parentHostId]:
            self.aggStructRecord[level][parentHostId].append(childHostId)
            self._updateMiddleExe_NumOfChildren(parentHostId, level)
        if level == 0:
            if (childHostId in self.hosts) and (self.hosts[childHostId] is not None):
                self._updateSourceExe(childHostId, parentHostId)
            else:
                self._installSourceExe(childHostId, parentHostId)
                self.hosts[childHostId] = True
        else:
            self._updateMiddleExe_Parent(childHostId, level - 1, parentHostId)
        # LogUtil.DebugLog('tree', 'addAggLink', 'level {0}'.format(level), 'child {0}'.format(rts.HostRecord[childHostId].hostAddress),
        #                  'parent {0}'.format(rts.HostRecord[parentHostId].hostAddress), 'aggStructRecord: ', self.aggStructRecord)

    def removeAggLink(self, level, childHostId, parentHostId):
        logging.info('job {0} remove a level-{1} link from host {2} to host {3}'.format(
                     self.jobId, level, childHostId, parentHostId))
        if childHostId in self.aggStructRecord[level][parentHostId]:
            self.aggStructRecord[level][parentHostId].remove(childHostId)
            self._updateMiddleExe_NumOfChildren(parentHostId, level)
            if len(self.aggStructRecord[level][parentHostId]) == 0:
                del self.aggStructRecord[level][parentHostId]
        # LogUtil.DebugLog('tree', 'removeAggLink', 'level {0}'.format(level), 'child {0}'.format(rts.HostRecord[childHostId].hostAddress),
        #                  'parent {0}'.format(rts.HostRecord[parentHostId].hostAddress), 'aggStructRecord:', self.aggStructRecord)

    def GetExpectedNumOfHosts(self, flowId):
        if flowId in self.exeFlow.flowToCtrl:
            return len(self.hosts.keys())
        elif flowId in self.exeFlow.flowFromNet:
            return 1
        else:
            return len(self.aggStructRecord[len(self.aggStructRecord) - 1]['controller'])

    def DisplayAggTree(self):
        LogUtil.DebugLog('tree', self.treeFormatter.displayTree())

    def updateControlAction(self, newAction):
        # LogUtil.DebugLog('control', self.exeFlow.hostSourceExePlan)
        oldAction = self.exeFlow.hostSourceExePlan[1:]
        if str(newAction) != str(oldAction):
            for hostId in self.hosts.iterkeys():
                address = rts.HostRecord[hostId].hostAddress
                message = HoneMessage()
                message.messageType = HoneMessageType_UpdateControlJob
                message.jobId = self.jobId
                message.content = newAction
                HoneHostSndModule().sendMessage(address, message)

    # private methods
    def _transferExeFile(self, hostId):
        address = rts.HostRecord[hostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_SendFile
        message.jobId = self.jobId
        HoneHostSndModule().sendFile(address, message, self.exeFlow.progName)

    def _installSourceExe(self, hostId, parentHostId):
        logging.info('job {0} install source exe on host {1} to parent {2}'.format(self.jobId, hostId, parentHostId))
        if not self.exeFlow.hostSourceExePlan:
            return
        address = rts.HostRecord[hostId].hostAddress
        parentAddress = rts.HostRecord[parentHostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_InstallSourceJob
        message.jobId = self.jobId
        message.content = (parentAddress,
                           self.createTime,
                           self.exeFlow.progName,
                           self.exeFlow.hostSourceExePlan)
        HoneHostSndModule().sendMessage(address, message)

    def _updateSourceExe(self, hostId, parentHostId):
        logging.info('job {0} update source exe of host {1} to parent {2}'.format(self.jobId, hostId, parentHostId))
        if not self.exeFlow.hostSourceExePlan:
            return
        address = rts.HostRecord[hostId].hostAddress
        parentAddress = rts.HostRecord[parentHostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_UpdateSourceJob
        message.jobId = self.jobId
        message.content = parentAddress
        HoneHostSndModule().sendMessage(address, message)

    def _installMiddleExe(self, hostId, level):
        logging.info('job {0} install level-{1} middle exe on host {2}'.format(self.jobId, level, hostId))
        if (not self.exeFlow.hostMiddleExePlan) or (hostId == 'controller'):
            return
        address = rts.HostRecord[hostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_InstallMiddleJob
        message.jobId = self.jobId
        message.level = level
        message.content = (len(self.aggStructRecord[level][hostId]),
                           self.exeFlow.progName,
                           self.exeFlow.hostMiddleExePlan)
        HoneHostSndModule().sendMessage(address, message)

    def _updateMiddleExe_NumOfChildren(self, hostId, level):
        logging.info('job {0} update level-{1} middle exe on host {2} to update number of children to {3}'.format(
                     self.jobId, level, hostId, len(self.aggStructRecord[level][hostId])))
        if (not self.exeFlow.hostMiddleExePlan) or (hostId == 'controller'):
            return
        address = rts.HostRecord[hostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_UpdateMiddleJob
        message.jobId = self.jobId
        message.level = level
        message.content = (len(self.aggStructRecord[level][hostId]), None)
        HoneHostSndModule().sendMessage(address, message)

    def _updateMiddleExe_Parent(self, hostId, level, parentHostId):
        logging.info('job {0} update level-{1} middle exe on host {2} to update parent address to {3}'.format(
                     self.jobId, level, hostId, parentHostId))
        if (not self.exeFlow.hostMiddleExePlan) or (hostId == 'controller'):
            return
        address = rts.HostRecord[hostId].hostAddress
        parentAddress = rts.HostRecord[parentHostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_UpdateMiddleJob
        message.jobId = self.jobId
        message.level = level
        message.content = (None, parentAddress)
        HoneHostSndModule().sendMessage(address, message)

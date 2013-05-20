"""
Author: Peng Sun
hone_job.py
Define class HoneJob to contain job-related operations
"""

import hone_rts as rts
import hone_exeModule as exeModule
from hone_message import *
from hone_sndModule import *
from hone_partition import *

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
        # TODO self.aggTreeFormatter = ....
        # register controller-side and network-side execution
        exeModule.buildExePlan(self.jobId, self.exeFlow.progName, self.exeFlow.controllerExePlan)
        # TODO netModule.build(...)


    def addHost(self, hostEntry):
        logging.info('job {0} add host {1}', self.jobId, hostEntry.hostId)
        if hostEntry.hostId not in self.hosts:
            self.hosts[hostEntry.hostId] = None
            self._transferExeFile(hostEntry.hostId)
            self.addAggLink(0, hostEntry.hostId, 'controller')


        # TODO delete and move to tree class
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
            if m < math.pow(BranchFactor, n - 1):
                if n == 2:
                    self.aggTree[1]['controller'].append(hostEntry.hostId)
                    for flowId in self.exeFlow.flowToMiddle:
                        self.expectedNumOfHosts[flowId] += 1
                    self.updateSourceExe(hostEntry.hostId, 'controller')
                else:
                    for searchLevel in range(1, n):
                        for (hostInLevel, hostChildren) in self.aggTree[searchLevel].iteritems():
                            if len(hostChildren) < BranchFactor:
                                self.updateMiddleChild_add(searchLevel, hostInLevel, hostEntry.hostId)
                                self.updateMiddleExe(searchLevel - 1, hostEntry.hostId, hostInLevel)
                                if searchLevel == 1:
                                    self.updateSourceExe(hostEntry.hostId, hostInLevel)
                                return
                        self.updateMiddleChild_add(searchLevel, hostEntry.hostId, hostEntry.hostId)
                        self.updateMiddleExe(searchLevel - 1, hostEntry.hostId, hostEntry.hostId)
                        if searchLevel == 1:
                            self.updateSourceExe(hostEntry.hostId, hostEntry.hostId)
            else:
                newParentHost = self.aggTree[n - 2].keys()[0]
                for childHost in self.aggTree[n - 2].keys():
                    self.updateMiddleChild_add(n - 1, newParentHost, childHost)
                    self.updateMiddleExe(n - 2, childHost, newParentHost)
                    if n == 2:
                        self.updateSourceExe(childHost, newParentHost)
                del self.aggTree[n - 1]['controller']
                self.aggTree.append({'controller': [newParentHost]})
                for flowId in self.exeFlow.flowToMiddle:
                    self.expectedNumOfHosts[flowId] = 1
                self.updateMiddleExe(n - 1, newParentHost, 'controller')
                if n == 1:
                    self.updateSourceExe(newParentHost, 'controller')
                for level in range(1, n):
                    self.updateMiddleChild_add(level, hostEntry.hostId, hostEntry.hostId)
                    self.updateMiddleExe(level - 1, hostEntry.hostId, hostEntry.hostId)
                    if level == 1:
                        self.updateSourceExe(hostEntry.hostId, hostEntry.hostId)
                self.updateMiddleChild_add(n, 'controller', hostEntry.hostId)
                self.updateMiddleExe(n - 1, hostEntry.hostId, 'controller')
                if n == 1:
                    self.updateSourceExe(hostEntry.hostId, 'controller')
                    #self.printAggTree()

    def removeHost(self, hostEntry):
        logging.info('Remove host {0} from job {1}', hostEntry.hostId, self.jobId)
        if hostEntry.hostId in self.hosts:
            del self.hosts[hostEntry.hostId]


        # TODO add calls to tree class
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
        return self.exeFlow.isHostEligible(hostEntry)

    def addAggLink(self, level, childHostId, parentHostId):
        logging.info('job {0} add a level-{3} link from host {1} to host {2}',
                     self.jobId, childHostId, parentHostId, level)
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

    def removeAggLink(self, level, childHostId, parentHostId):
        logging.info('job {0} remove a level-{1} link from host {2} to host {3}',
                     self.jobId, level, childHostId, parentHostId)
        if childHostId in self.aggStructRecord[level][parentHostId]:
            self.aggStructRecord[level][parentHostId].remove(childHostId)
            self._updateMiddleExe_NumOfChildren(parentHostId, level)
            if len(self.aggStructRecord[level][parentHostId]) == 0:
                del self.aggStructRecord[level][parentHostId]

    def GetExpectedNumOfHosts(self, flowId):
        if flowId in self.exeFlow.flowToCtrl:
            return len(self.hosts.keys())
        else:
            return len(self.aggStructRecord[len(self.aggStructRecord) - 1]['controller'])

    # TODO delete
    def printAggTree(self):
        name = 1
        hostName = {'controller': 'controller'}
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

    # private methods
    def _transferExeFile(self, hostId):
        address = rts._hostInfo[hostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_SendFile
        message.jobId = self.jobId
        HoneHostSndModule().sendFile(address, message, self.exeFlow.progName)

    def _installSourceExe(self, hostId, parentHostId):
        logging.info('job {0} install source exe on host {1} to parent {2}', self.jobId, hostId, parentHostId)
        if not self.exeFlow.hostSourceExePlan:
            return
        address = rts._hostInfo[hostId].hostAddress
        parentAddress = rts._hostInfo[parentHostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_InstallSourceJob
        message.jobId = self.jobId
        message.content = (parentAddress,
                           self.createTime,
                           self.exeFlow.progName,
                           self.exeFlow.hostSourceExePlan)
        HoneHostSndModule().sendMessage(address, message)

    def _updateSourceExe(self, hostId, parentHostId):
        logging.info('job {0} update source exe of host {1} to parent {2}', self.jobId, hostId, parentHostId)
        if not self.exeFlow.hostSourceExePlan:
            return
        address = rts._hostInfo[hostId].hostAddress
        parentAddress = rts._hostInfo[parentHostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_UpdateSourceJob
        message.jobId = self.jobId
        message.content = parentAddress
        HoneHostSndModule().sendMessage(address, message)

    def _installMiddleExe(self, hostId, level):
        logging.info('job {0} install level-{1} middle exe on host {2}', self.jobId, level, hostId)
        if (not self.exeFlow.hostMiddleExePlan) or (hostId == 'controller'):
            return
        address = rts._hostInfo[hostId].hostAddress
        message = HoneMessage()
        message.messageType = HoneMessageType_InstallMiddleJob
        message.jobId = self.jobId
        message.level = level
        message.content = (len(self.aggStructRecord[level][address]),
                           self.exeFlow.progName,
                           self.exeFlow.hostMiddleExePlan)
        HoneHostSndModule().sendMessage(address, message)

    def _updateMiddleExe_NumOfChildren(self, hostId, level):
        logging.info('job {0} update level-{1} middle exe on host {2} to update number of children to {3}',
                     self.jobId, level, hostId, len(self.aggStructRecord[level][hostId]))
        if (not self.exeFlow.hostMiddleExePlan) or (hostId == 'controller'):
            return
        address = rts._hostInfo[hostId].hostAddress
        message.messageType = HoneMessageType_UpdateMiddleJob
        message.jobId = self.jobId
        message.level = level
        message.content = (len(self.aggStructRecord[level][hostId]), None)
        HoneHostSndModule().sendMessage(address, message)

    def _updateMiddleExe_Parent(self, hostId, level, parentHostId):
        logging.info('job {0} update level-{1} middle exe on host {2} to update parent address to {3}',
                     self.jobId, level, hostId, parentHostId)
        if (not self.exeFlow.hostMiddleExePlan) or (hostId == 'controller'):
            return
        address = rts._hostInfo[hostId].hostAddress
        parentAddress = rts._hostInfo[parentHostId].hostAddress
        message.messageType = HoneMessageType_UpdateMiddleJob
        message.jobId = self.jobId
        message.level = level
        message.content = (None, parentAddress)
        HoneHostSndModule().sendMessage(address, message)
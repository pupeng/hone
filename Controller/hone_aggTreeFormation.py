"""
Author: Peng Sun
hone_aggTreeFormation.py
Define factory and class to compose the aggregation tree
"""

import hone_rts as rts
from hone_hostEntry import *

class TreeFormatterBase:
    ''' base class of tree formatter '''
    def addLeaf(self, hostEntry):
        raise NotImplementedError('Derived class should implement this method')

    def removeLeaf(self, hostEntry):
        raise NotImplementedError('Derived class should implement this method')

class SimpleTreeFormatter(TreeFormatterBase):
    ''' basic implementation of formatting aggregation tree by the sequence of hosts' entering the system '''
    def __init__(self):
        self.aggTree =

    def addLeaf(self, hostEntry):
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

    def removeLeaf(self, hostEntry):
        print 'removenode'

class TreeFormatterFactory:
    ''' factory method for creating concrete TreeFormatter '''
    @staticmethod
    def GetNewFormatter(description='simple'):
        if description == 'simple':
            return SimpleTreeFormatter()
        else:
            return SimpleTreeFormatter()


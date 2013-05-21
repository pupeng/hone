"""
Author: Peng Sun
hone_aggTreeFormation.py
Define factory and class to compose the aggregation tree
"""

import math

from hone_job import *
from hone_hostEntry import *

DefaultBranchFactor = 2

class TreeFormatterBase:
    ''' base class of tree formatter '''
    def addLeaf(self, hostEntry):
        raise NotImplementedError('Derived class should implement this method')

    def removeLeaf(self, hostEntry):
        raise NotImplementedError('Derived class should implement this method')

class SimpleNode:
    def __init__(self, hostId, nodeLevel):
        self.hostId = hostId
        self.nodeLevel = nodeLevel
        self.parent = None
        self.children = []

    def getHostId(self):
        return self.hostId

    def setParent(self, node):
        assert isinstance(node, SimpleNode)
        self.parent = node

    def getParent(self):
        return self.parent

    def addChild(self, node):
        assert isinstance(node, SimpleNode)
        if node not in self.children:
            self.children.append(node)

    def removeChild(self, node):
        assert isinstance(node, SimpleNode)
        if node in self.children:
            self.children.remove(node)

    def getChildren(self):
        return self.children

    def setNodeLevel(self, nodeLevel):
        self.nodeLevel = nodeLevel

    def getNodeLevel(self):
        return self.nodeLevel

class SimpleTreeFormatter(TreeFormatterBase):
    ''' basic implementation of formatting aggregation tree by the sequence of hosts' entering the system '''
    def __init__(self, job, branchFactor=DefaultBranchFactor):
        self.controllerNode = SimpleNode('controller', 1)
        self.aggTree = [[], [self.controllerNode]]
        self.branchFactor = branchFactor
        self.job = job

    def addLeaf(self, hostEntry):
        n = len(self.aggTree)
        node = SimpleNode(hostEntry.hostId, 0)
        foundSpot = False
        searchNodeLevel = 1
        while (searchNodeLevel < n) and (not foundSpot):
            for nodeToCheck in self.aggTree[searchNodeLevel]:
                if len(nodeToCheck.getChildren) < self.branchFactor:
                    nodeToCheck.addChild(node)
                    node.setParent(nodeToCheck)
                    self.aggTree[searchNodeLevel - 1].append(node)
                    self.job.addAggLink(searchNodeLevel - 1, node.getHostId(), nodeToCheck.getHostId())
                    foundSpot = True
                    break
            if not foundSpot:
                selfPromoteNode = SimpleNode(node.getHostId(), node.getNodeLevel() + 1)
                selfPromoteNode.addChild(node)
                node.setParent(selfPromoteNode)
                self.aggTree[searchNodeLevel - 1].append(node)
                self.job.addAggLink(searchNodeLevel - 1, node.getHostId(), selfPromoteNode.getHostId())
                node = selfPromoteNode
            searchNodeLevel += 1
        if not foundSpot:



        m = len(self.aggTree[0])
        n = len(self.aggTree)

        if m < math.pow(self.branchFactor, n - 1):


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
    def GetNewFormatter(job, description='simple'):
        if description == 'simple':
            return SimpleTreeFormatter(job)
        else:
            return SimpleTreeFormatter(job)


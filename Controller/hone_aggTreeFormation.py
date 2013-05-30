# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_aggTreeFormation.py
# Define factory and class to compose the aggregation tree

import logging

from hone_job import *
from hone_hostEntry import *

DefaultBranchFactor = 2

class TreeFormatterBase:
    ''' base class of tree formatter '''
    def addLeaf(self, hostEntry):
        raise NotImplementedError('Derived class should implement the method')

    def removeLeaf(self, hostEntry):
        raise NotImplementedError('Derived class should implement the method')

    def displayTree(self):
        raise NotImplementedError('Derived class should implement the method')

class SimpleNode:
    def __init__(self, hostId, hostAddress, nodeLevel):
        self.hostId = hostId
        self.hostAddress = hostAddress
        self.nodeLevel = nodeLevel
        self.parent = None
        self.children = []

    def getHostId(self):
        return self.hostId

    def getHostAddress(self):
        return self.hostAddress

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

    def resetChildren(self):
        self.children = []

    def setNodeLevel(self, nodeLevel):
        self.nodeLevel = nodeLevel

    def getNodeLevel(self):
        return self.nodeLevel

class SimpleTreeFormatter(TreeFormatterBase):
    ''' basic implementation of formatting aggregation tree by the sequence of hosts' entering the system '''
    def __init__(self, job, branchFactor=DefaultBranchFactor):
        self.controllerNode = SimpleNode('controller', 'controller IP', 1)
        self.aggTree = [[], [self.controllerNode]]
        self.branchFactor = branchFactor
        self.job = job

    def addLeaf(self, hostEntry):
        n = len(self.aggTree)
        node = SimpleNode(hostEntry.hostId, hostEntry.hostAddress, 0)
        foundSpot = False
        searchNodeLevel = 1
        while (searchNodeLevel < n) and (not foundSpot):
            for nodeToCheck in self.aggTree[searchNodeLevel]:
                if len(nodeToCheck.getChildren()) < self.branchFactor:
                    nodeToCheck.addChild(node)
                    node.setParent(nodeToCheck)
                    self.aggTree[searchNodeLevel - 1].append(node)
                    self.job.addAggLink(searchNodeLevel - 1, node.getHostId(), nodeToCheck.getHostId())
                    foundSpot = True
                    break
            if not foundSpot:
                selfPromoteNode = SimpleNode(node.getHostId(), node.getHostAddress(), node.getNodeLevel() + 1)
                selfPromoteNode.addChild(node)
                node.setParent(selfPromoteNode)
                self.aggTree[searchNodeLevel - 1].append(node)
                self.job.addAggLink(searchNodeLevel - 1, node.getHostId(), selfPromoteNode.getHostId())
                node = selfPromoteNode
            searchNodeLevel += 1
        if not foundSpot:
            self.controllerNode.setNodeLevel(self.controllerNode.getNodeLevel() + 1)
            self.aggTree[n - 1].remove(self.controllerNode)
            self.aggTree.append([self.controllerNode])
            nodeToPromote = self.controllerNode.getChildren()[0]
            newNode = SimpleNode(nodeToPromote.getHostId(), nodeToPromote.getHostAddress(), nodeToPromote.getNodeLevel() + 1)
            for child in self.controllerNode.getChildren():
                newNode.addChild(child)
                self.job.removeAggLink(n - 2, child.getHostId(), self.controllerNode.getHostId())
                self.job.addAggLink(n - 2, child.getHostId(), newNode.getHostId())
            self.controllerNode.resetChildren()
            newNode.setParent(self.controllerNode)
            self.controllerNode.addChild(newNode)
            self.aggTree[n - 1].append(newNode)
            self.job.addAggLink(n - 1, newNode.getHostId(), self.controllerNode.getHostId())
            node.setParent(self.controllerNode)
            self.controllerNode.addChild(node)
            self.aggTree[n - 1].append(node)
            self.job.addAggLink(n - 1, node.getHostId(), self.controllerNode.getHostId())
            self.job.addAggLink(n - 1, node.getHostId(), self.controllerNode.getHostId())

    def removeLeaf(self, hostEntry):
        # TODO add handler for deletion of nodes
        logging.warning('Node deletion in aggregation tree is not fully implemented yet!')
        for node in self.aggTree[0]:
            if node.getHostId() == hostEntry.hostId:
                node.getParent().removeChild(node)
                self.aggTree[0].remove(node)
                self.job.removeAggLink(0, node.getHostId(), node.getParent().getHostId())

    def displayTree(self):
        message = 'jobID: {0}\n'.format(self.job.jobId)
        for i in reversed(range(1, len(self.aggTree))):
            for node in self.aggTree[i]:
                message += 'Level {0:2} node. ID: {1}. IP: {2}.\n'.format(i, node.getHostId(), node.getHostAddress())
                message += '            Children nodes: \n'
                for child in node.getChildren():
                    message += '            Child ID: {0}. IP: {1}.\n'.format(child.getHostId(), child.getHostAddress())
                message += '\n'
        return message

class TreeFormatterFactory:
    ''' factory method for creating concrete TreeFormatter '''
    @staticmethod
    def GetNewFormatter(job, description='simple'):
        if description == 'simple':
            return SimpleTreeFormatter(job)
        else:
            return SimpleTreeFormatter(job)

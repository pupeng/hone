# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# agentLib.py
# library for executing jobs

import time
import os
from uuid import getnode as get_mac

import agentFreLib as freLib
import agentControlModule
import agentManager
from agentUtil import *
from agentSndModule import *
from hone_message import *

HostId = get_mac()
StartingTime = 1334262600

HostRelayPort = 8877
ControllerPort = 8866

########################
# Foundation Operators #
########################
# subscribe. 
def Subscribe(sourceJob):
    e,go = freLib.RawEvent()
    if not agentManager.eventAndGoFunc.has_key(sourceJob.jobId):
        agentManager.eventAndGoFunc[sourceJob.jobId] = {}
    agentManager.eventAndGoFunc[sourceJob.jobId][sourceJob.flowId] = (e, go)
    if not agentManager.jobNotReady.has_key(sourceJob.jobId):
        agentManager.jobNotReady[sourceJob.jobId] = {}
    agentManager.jobNotReady[sourceJob.jobId][sourceJob.flowId] = sourceJob

# MapStreamSet: ((h,a)->(h,b)) -> (Stream (h,a) -> Stream (h,b))
def MapStreamSet(f):
    return freLib.Lift(f)

# MapList: (a->b) -> (List a -> List b)
def MapList(f):
    def newFunc(x):
        #debugLog('lib', 'newFunc of MapList', x)
        return map(f, x)
    return newFunc

# FilterStreamSet: (a->bool) -> (Stream a -> Stream a)
def FilterStreamSet(f):
    return freLib.Filter(f)

# FilterList: (a->bool) -> (List a -> List a)
def FilterList(f):
    def newFunc(x):
        #debugLog('lib', 'newFunc of Filterlist', x)
        return filter(f, x)
    return newFunc

# ReduceStreamSet: ((a->b->a)->a) -> (Stream b -> Stream a)
def ReduceStreamSet(f,init, init_type=None):
    def newFunc((next,last)):
        newout = f(next,last)
        #debugLog('lib', 'newFunc of ReduceStreamSet', (next, last), newout)
        return (newout,newout)
    return freLib.LoopPre(init, freLib.Lift(newFunc, type_fun=lambda x:init_type), c_type=init_type)

# ReduceList: ((a->b->a)->a) -> (List b -> a)
def ReduceList(f, init):
    def newFunc(x):
        #debugLog('lib', 'newFunc of ReduceList', x)
        return reduce(f, x, init)
    return newFunc

# MergeSet: (Set of Streams a, Set of Streams b) -> Set of Streams (a,b)
# TODO long-term issue on changing how Merge() works
def MergeStreamsForSet(a, b):
    return freLib.Merge(a, b)
    
# WhereComplex
def WC(attr, op, value):
    value = float(value)
    def newFunc(x):
        #debugLog('lib', 'new func of WC', x)
        if op=='=':
            return float(x[attr])==value
        elif op=='!=':
            return float(x[attr])!=value
        elif op=='>':
            return float(x[attr])>value
        elif op=='<':
            return float(x[attr])<value
        elif op=='>=':
            return float(x[attr])>=value
        else:
            return float(x[attr])<=value
    return MapStreamSet(FilterList(newFunc))

# groupby in query
def GB(attr):
    def newFunc(x):
        tables = {}
        for item in x:
            criteria = []
            for pos in attr:
                criteria.append(item[pos])
            criteria = tuple(criteria)
            if not tables.has_key(criteria):
                tables[criteria] = []
            tables[criteria].append(item)
        #debugLog('lib', 'newFunc of GB', x, tables.values())
        return tables.values()
    return MapStreamSet(newFunc)

# aggregation in query
def AGG(attr):
    def sumOp(last,new,number):
        last += new
        return last
    def avgOp(last,new,number):
        last = float(last*number+new)/float(number+1)
        return last
    def maxOp(last,new,number):
        print 'reach maxOp'
        if new>last:
            return new
        else:
            return last
    def minOp(last,new,number):
        if new<last:
            return new
        else:
            return last
    aggOp = {'sum':sumOp,
             'avg':avgOp,
             'max':maxOp,
             'min':minOp}
    def handleSingleTable(table):
        resultRow = table[0]
        for i in range(1,len(table)):
            row = table[i]
            for (pos, op) in attr:
                resultRow[pos] = aggOp[op](resultRow[pos],row[pos],i)
                #debugLog('lib', 'attribute: ', attr, \
                #                'row: ', row, \
                #                'op: ', op, \
                #                'result row: ', resultRow)
        return resultRow
    def newFunc(x):
        #debugLog('lib', 'newFunc of AGG', x)
        if x:
            if type(x[0])==type(list()):
                for i in range(len(x)):
                    x[i] = handleSingleTable(x[i])
            else: # x is a single table
                x = handleSingleTable(x)
        #debugLog('lib', 'AGG result', x)
        return x
    return MapStreamSet(newFunc)

# send to controller
def ToCtrl(jobID, flowId):
    def push(x):
        LogUtil.DebugLog('lib', 'in ToCtrl', jobID, flowId, x)
        if x or isinstance(x, (int,long,float,complex)):
            key = composeKey(jobID, flowId)
            sequence = agentManager.sourceJobTable[key].lastSequence
            message = HoneMessage()
            message.messageType = HoneMessageType_StatsIn
            message.hostId = HostId
            message.jobId = jobID
            message.flowId = flowId
            message.sequence = sequence
            message.content = x
            #debugLog('lib', 'send message to controller')
            agentManager.evalTimestamp += '#StartToCtrl${0:6f}${1}${2}${3}'.format(time.time(), jobID, flowId, sequence)
            agentManager.sndToCtrl.sendMessage(message)
            agentManager.evalTimestamp += '#DoneToCtrl${0:6f}${1}${2}${3}'.format(time.time(), jobID, flowId, sequence)
    return freLib.FListener(push=push)

def ToMiddle(jobId, flowId):
    def push(x):
        LogUtil.DebugLog('lib', 'in ToMiddle', jobId, flowId, x)
        if x or isinstance(x, (int,long,float,complex)):
            key = composeKey(jobId, flowId)
            if key in agentManager.sourceJobTable:
                middleAddress = agentManager.sourceJobTable[key].middleAddress
                sequence = agentManager.sourceJobTable[key].lastSequence
                message = HoneMessage()
                message.messageType = HoneMessageType_RelayStatsIn
                message.hostId = HostId
                message.jobId = jobId
                message.flowId = flowId
                message.level = 1
                message.sequence = sequence
                message.content = x
                sndTimestamp = 'Begin${0:6f}${1}${2}${3}'.format(time.time(), jobId, flowId, sequence)
                if middleAddress == agentManager.CtrlAddress:
                    sndSocket = agentManager.sndToCtrl
                else:
                    port = HostRelayPort
                    sndSocket = HostAgentRelaySndSocket(middleAddress, port)
                agentManager.evalTimestamp += '#StartToMiddle${0:6f}${1}${2}${3}${4}'.format(time.time(), jobId, flowId, sequence, middleAddress)
                sndSocket.sendMessage(message)
                agentManager.evalTimestamp += '#DoneToMiddle${0:6f}${1}${2}${3}'.format(time.time(), jobId, flowId, sequence)
                sndTimestamp += 'End${0:6f}'.format(time.time())
                LogUtil.EvalLog('ToMiddleOneRound', sndTimestamp)
    return freLib.FListener(push=push)

# rate limit
def RateLimit(jobID):
    def push(x):
        #debugLog('control', 'push ratelimit rule', jobID, x, agentControlModule.jobAndQueueRate)
        (queueID, rate) = agentControlModule.jobAndQueueRate[jobID]
        for conn in x:
            criteria = {'app':conn[0],
                        'srcIP':conn[3],
                        'dstIP':conn[5],
                        'srcPort':conn[4],
                        'dstPort':conn[6]}
            agentControlModule.trafficControl(criteria, queueID, rate)
    return freLib.FListener(push=push)

def TreeMerge(f):
    return MapStreamSet(f)

# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_frplib
# provides library for controller execution module

import sys

import hone_lib as lib
import hone_freLib as freLib
import hone_exeModule as exeModule

########################
# Foundation Operators #
########################
# subscribe. to support merging flows, I need to modify here
def Subscribe(jobId, flowId):
    e,go = freLib.RawEvent()
    if jobId not in exeModule.jobGoFun:
        exeModule.jobGoFun[jobId] = {}
    exeModule.jobGoFun[jobId][flowId] = go
    return e

# MapStream: (a->b) -> (Stream a -> Stream b)
def MapStream(f):
    return freLib.Lift(f)

# MapList: (a->b) -> (List a -> List b)
def MapList(f):
    def newFunc(x):
        return map(f,x)
    return newFunc

# FilterStream: (a->bool) -> (Stream a -> Stream a)
def FilterStream(f):
    return freLib.Filter(f)

# FilterList: (a->bool) -> (List a -> List a)
def FilterList(f):
    def newFunc(x):
        return filter(f,x)
    return newFunc

# ReduceStream: ((a->b->a)->a) -> (Stream b -> Stream a)
def ReduceStream(f,init, init_type=None):
    def newFunc((next,last)):
        newout = f(next,last)
        return (newout,newout)
    return freLib.LoopPre(init, freLib.Lift(newFunc, type_fun=lambda x:init_type), c_type=init_type)

# ReduceList: ((a->b->a)->a) -> (List b -> a)
def ReduceList(f,init):
    def newFunc(x):
        return reduce(f,x,init)
    return newFunc

# MergeStreams: (Stream a, Stream b) -> Stream (a,b)
def MergeStreams(streams):
    if (len(streams) == 2):
        return freLib.Merge(streams[0], streams[1])
    else:
        return freLib.Merge(streams[0], MergeStreams(streams[1:]))
    
# Print_listener : string -> output channel -> L string
def Print(g=None,s=sys.stdout):    
    def push(x):
        if g is None:
            print >>s, x
        else:
            g(x)
    return freLib.FListener(push=push)

# Install_listener : Policy
queueToRts = None
def RegisterPolicy(f=None):
    def push(rs):
        for rule in rs:
            rate = rule[1]['rateLimit']
            cr = []
            for key in sorted(rule[0].iterkeys()):
                value = rule[0][key]
                cr.append((key,'==',value))
            plan = (lib.Select(['app','srcHost','dstHost','srcIP','srcPort','dstIP','dstPort']) *
                    lib.From('HostConnection') *
                    lib.Where(cr) *
                    lib.Every(1)) >> lib.RateLimit(rate)
            queueToRts.put([['ControlJob'],plan])
        #print 'control policies'
        #print rs
    return freLib.FListener(push=push)

def MergeHosts(f=None):
    def coreFunc(newData,lastData):
        (isComplete, dataList, dataBuffer) = lastData
        if isComplete:
            dataList = [dataBuffer]
            isComplete = False
        newSeq = newData[1]
        if dataList:
            lastSeq = dataList[0][1]
        else:
            lastSeq = newSeq
        if newSeq == lastSeq:
            dataList.append(newData)
        elif newSeq > lastSeq:
            isComplete = True
            dataBuffer = newData
        return (isComplete, dataList, dataBuffer)
    def helperFunc1(lastData):
        return lastData[0]
    def helperFunc2(lastData):
        return lastData[1]
    init = (False, [], None)
    return ReduceStream(coreFunc,init)>>FilterStream(helperFunc1)>>MapStream(helperFunc2)

def TreeMerge(f):
    return freLib.Lift(f)

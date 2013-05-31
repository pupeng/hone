# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# agentLib.py
# library for executing jobs

import time
from uuid import getnode as get_mac

import agentRcvModule
import agentManager
import agentFreLib as freLib
from agentUtil import *
from agentSndModule import *
from hone_message import *

HostId = get_mac()

ControllerPort = 8866
HostRelayPort = 8877

def ToUpperLevel(jobId, flowId, level):
    def push(x):
        if x or isinstance(x, (int,long,float,complex)):
            key = composeMiddleJobKey(jobId, flowId, level)
            if key in agentRcvModule.middleJobTable:
                parentAddress = agentRcvModule.middleJobTable[key].parentAddress
                sequence = agentRcvModule.middleJobTable[key].lastSeq
                message = HoneMessage()
                message.messageType = HoneMessageType_RelayStatsIn
                message.hostId = HostId
                message.jobId = jobId
                message.flowId = flowId
                message.level = level + 1
                message.sequence = sequence
                message.content = x
                if parentAddress == agentManager.CtrlAddress:
                    port = ControllerPort
                else:
                    port = HostRelayPort
                if parentAddress:
                    sndSocket = HostAgentRelaySndSocket(parentAddress, port)
                    sndSocket.sendMessage(message)
                agentRcvModule.middleEvalTimestamp += '#DoneToUpperLevel${0:6f}${1}${2}${3}${4}${5}'.format(time.time(), jobId, flowId, message.level, message.sequence, parentAddress)
                # LogUtil.DebugLog('lib', 'in ToUpperLevel', jobId, flowId, level, sequence)
                if level == agentRcvModule.highestMiddleJobLevel:
                    LogUtil.EvalLog('MiddleExecution', agentRcvModule.middleEvalTimestamp)
                    agentRcvModule.middleEvalTimestamp = 'Begin'
    return freLib.FListener(push=push)
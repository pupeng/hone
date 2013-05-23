'''
Peng Sun
agentLib.py
library for executing jobs
'''

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
        #debugLog('lib', 'in ToUpperLevel', jobId, flowId, level)
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
                #debugLog('lib', 'send message to middle {0}'.format(parentAddress), 'level {0}'.format(message.level), 'port', port)
                sndSocket = HostAgentRelaySndSocket(parentAddress, port)
                sndSocket.sendMessage(message)
                LogUtil.EvalLog('ToUpperLevel', 'jobId {0} flowId {1} level {2} parent address {3}'.format(jobId, flowId, level, parentAddress))
    return freLib.FListener(push=push)
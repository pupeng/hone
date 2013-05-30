# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_recvModule.py
# The receiver module on the controller
# listening to the messages from the hosts

import hone_rts as rts
from hone_message import *
from hone_util import *

from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
import cPickle as pickle
from cStringIO import StringIO
import logging

_honeCtrlListeningPort = 8866

class HostConnectionTypes:
    AgentDirect, Relay = range(2)

class HoneCommProtocol(LineReceiver):
    ''' the protocol between controller and host agent '''
    def __init__(self):
        self.hostName = None
        self.connectionType = None
        self.typeActions = {
            HoneMessageType_HostJoin : self.handleHostJoin,
            HoneMessageType_StatsIn  : self.handleStatsIn,
            HoneMessageType_RelayStatsIn: self.handleRelayStatsIn}
    
    ''' receive a new line from agent. Data in the line is HoneMessage '''
    def lineReceived(self, line):
        dst = StringIO(line)
        message = pickle.load(dst)
        dst.close()
        if self.hostName == None:
            self.hostName = message.hostId
        self.typeActions.get(message.messageType, self.handleUnknownType)(message)

    def connectionLost(self, reason):
        if self.hostName and (self.connectionType == HostConnectionTypes.AgentDirect):
            rts.handleHostLeave(self.hostName)

    def handleHostJoin(self, message):
        self.connectionType = HostConnectionTypes.AgentDirect
        hostAddress = self.transport.getPeer().host
        rts.handleHostJoin(message.hostId, hostAddress)
        
    def handleStatsIn(self, message):
        if (message.jobId == rts.HoneHostInfoJobId):
            rts.handleHostInfoUpdate(message)
        else:
            rts.evalTimestamp += '#NewStatsIn${0:6f}${1}${2}${3}'.format(time.time(), message.jobId, message.flowId, message.sequence)
            rts.handleStatsIn(message)

    def handleRelayStatsIn(self, message):
        self.connectionType = HostConnectionTypes.Relay
        rts.evalTimestamp += '#NewStatsIn${0:6f}${1}${2}${3}'.format(time.time(), message.jobId, message.flowId, message.sequence)
        rts.handleStatsIn(message)

    def handleUnknownType(self, message):
        logging.warning('Got unknown message type {0}'.format(message.messageType))

class HoneCommFactory(Factory):
    def buildProtocol(self, addr):
        return HoneCommProtocol()

def recvModuleRun():
    try:
        logging.info('hone recvModule starts')
        print 'HONE recvModule starts on port {0}.'.format(_honeCtrlListeningPort)
        reactor.listenTCP(_honeCtrlListeningPort, HoneCommFactory())
        reactor.run()
    except KeyboardInterrupt:
        LogUtil.DebugLog('rts', 'catch keyboard interrupt')
    except Exception, msg:
        logging.error('Exception {0}'.format(msg))
        print 'Exception: ', msg
    finally:
        logging.info('Exit from hone_recvModule')
        print 'Exit from hone_recvModule'
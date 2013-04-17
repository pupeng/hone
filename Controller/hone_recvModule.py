'''
Peng Sun
hone_recvModule.py
The receiver module on the controller
listening to the messages from the hosts
'''

import hone_rts
from hone_message import *
from hone_util import *

from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
import cPickle as pickle
from cStringIO import StringIO
import time

_honeCtrlListeningPort = 8866

class HoneCommProtocol(LineReceiver):
    ''' the protocol between controller and host agent '''
    def __init__(self):
        self.hostName = None
        self.typeActions = {
            HoneMessageType_HostJoin : self.handleHostJoin,
            HoneMessageType_StatsIn  : self.handleStatsIn,
            HoneMessageType_RelayStatsIn: self.handleStatsIn}
    
    ''' receive a new line from agent. Data in the line is HoneMessage '''
    def lineReceived(self, line):
        #debugLog('rts', 'in lineReceived', repr(line))
        #EvalLog('{0:6f},10,receive a new message'.format(time.time()))
        dst = StringIO(line)
        message = pickle.load(dst)
        dst.close()
        if (self.hostName == None):
            self.hostName = message.hostId
        self.typeActions.get(message.messageType, \
                             self.handleUnknownType) \
                             (message)

    def connectionLost(self, reason):
        #EvalLog('{0:6f},11,host leaves {1}'.format(time.time(), self.hostName))
        #hone_rts.handleHostLeave(self.hostName)
        #EvalLog('{0:6f},14,done handle host leave {1}'.format(time.time(), self.hostName))
        # TODO
        pass

    def handleHostJoin(self, message):
        #EvalLog('{0:6f},12,new host joins: {1}'.format(time.time(), message.hostId))
        hostAddress = self.transport.getPeer().host
        hone_rts.handleHostJoin(message.hostId, hostAddress)
        #EvalLog('{0:6f},15,done handle host join {1}'.format(time.time(), message.hostId))
        
    def handleStatsIn(self, message):
        if (message.jobId == hone_rts.HoneHostInfoJobId):
            hone_rts.handleHostInfoUpdate(message)
        else:
            hone_rts.evalTimestamp += '#{0}${2}${3}${1:6f}$StatsIn'.format(message.jobId, time.time(), message.flowId, message.sequence)
            hone_rts.handleStatsIn(message)
        #EvalLog('{0:6f},16,done handle new stats for jobId {1}'.format(time.time(), message.jobId))

    def handleUnknownType(self, message):
        print 'Got unknown message type'
        print message.messageType
        
class HoneCommFactory(Factory):
    def buildProtocol(self, addr):
        return HoneCommProtocol()

def recvModuleRun():
    try:
        print 'hone rts recvModule starts on port ' + str(_honeCtrlListeningPort)
        reactor.listenTCP(_honeCtrlListeningPort, HoneCommFactory())
        #debugLog('rts', 'recvModules starts to run on port', \
        #         _honeCtrlListeningPort)
        reactor.run()
    except KeyboardInterrupt:
        debugLog('rts', 'catch keyboard interrupt')
    except Exception, msg:
        #debugLog('rts', 'catch unknown exception', msg)
        #logging.error('Exception: {0}'.format(msg))
        print 'Exception: ', msg
    finally:
        #EvalLog('{0:6f},9,stop recvModule'.format(time.time()))
        print 'Exit from hone_recvModule'


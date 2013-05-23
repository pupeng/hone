'''
Peng Sun
agentSndModule

Host agent send module
send stats, hostJoin, etc to the controller
'''

import socket
import sys
import logging
import cPickle as pickle
from uuid import getnode as get_mac
from cStringIO import StringIO

from agentUtil import LogUtil
from hone_message import *

ctrlCommPort = 8866

class HostAgentSndSocket:
    def __init__(self, controllerAddress = 'localhost', controllerPort = ctrlCommPort):
        try:
            self.hostSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.hostSock.connect((controllerAddress, controllerPort))
            message = HoneMessage()
            message.messageType = HoneMessageType_HostJoin
            message.hostId = get_mac()
            #message.hostId = str(random.randint(0, 1000000))
            self.sendMessage(message)
        except socket.error, msg:
            logging.error('connection to controller error: {0}'.format(msg))
            print 'connect error '
            print msg
            if self.hostSock:
                self.hostSock.close()
            self.hostSock = None
        except Exception:
            if self.hostSock:
                self.hostSock.close()
            self.hostSock = None
        if self.hostSock is None:
            logging.error('Connection to controller error in HostAgentSndSocket. Agent will stop.')
            print 'Connection to controller error in HostAgentSndSocket. Agent will stop.'
            sys.exit()
    
    def sendMessage(self, message):
        if self.hostSock:
            src = StringIO()
            pickle.dump(message, src, pickle.HIGHEST_PROTOCOL)
            data = src.getvalue() + '\r\n'
            src.close()
            self.hostSock.sendall(data)
            #debugLog('sndModule', 'send message. messageType:', \
            #         message.messageType, 'jobId', message.jobId, \
            #         'flowId:', message.flowId, 'sequence:', \
            #         message.sequence, 'content:', message.content)
    
    def closeSocket(self):
        self.hostSock.close()
        
    def recvMessage(self):
        return self.hostSock.recv(1024)

class HostAgentRelaySndSocket:
    def __init__(self, middleAddress, port):
        try:
            self.hostSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.hostSock.connect((middleAddress, port))
        except socket.error, msg:
            logging.warning('Relay connection to middle error: {0}'.format(msg))
            print 'connect error '
            print msg
            if self.hostSock:
                self.hostSock.close()
            self.hostSock = None
        except Exception:
            self.hostSock = None
        if self.hostSock is None:
            logging.warning('socket error in HostAgentRelaySndSocket')
            print 'socket error in HostAgentRelaySndSocket'

    def sendMessage(self, message):
        if self.hostSock:
            src = StringIO()
            pickle.dump(message, src, pickle.HIGHEST_PROTOCOL)
            data = src.getvalue() + '\r\n'
            src.close()
            self.hostSock.sendall(data)
            #debugLog('sndModule', 'send message. messageType:',\
            #    message.messageType, 'jobId', message.jobId,\
            #    'flowId:', message.flowId, 'sequence:',\
            #    message.sequence, 'content:', message.content)
'''
Peng Sun
hone_sndModule.py
module for sending commands and files to host agent
'''
import socket
from hone_util import *
from cStringIO import StringIO
import cPickle as pickle

hostCommPort = 8877
networkCommPort = 6633

class HoneHostSndModule:
    def __init__(self):
        self.hostSock = None
        
    def send(self, address, port, data):
        try:
            self.hostSock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            self.hostSock.connect((address, port))
            self.hostSock.sendall(data)
        except socket.error, msg:
            print 'Socket connect error'
            print msg
        finally:
            self.hostSock.close()
            self.hostSock = None

    def sendMessage(self, hostAddress, message):
        src = StringIO()
        pickle.dump(message, src, pickle.HIGHEST_PROTOCOL)
        data = src.getvalue() + '\r\n'
        src.close()
        #debugLog('snd', 'sendMessage. Messagey type:', \
        #         message.messageType, \
        #         'jobId:', message.jobId, \
        #         'content', message.content,\
        #         'hostAddress', hostAddress)
        self.send(hostAddress, hostCommPort, data)
    
    def sendFile(self, hostAddress, message, fileName):
        f = open(fileName+'.py', 'r')
        fileContent = f.read()
        f.close()
        message.content = (fileName, fileContent)
        self.sendMessage(hostAddress, message)

if __name__ == '__main__':
    from hone_message import *
    address = '127.0.0.1'
    hostSnd = HoneHostSndModule()
    message = HoneMessage()
    message.jobId = 77
    message.messageType = HoneMessageType_SendFile
    hostSnd.sendFile(address, message, 'exp_queryDebug')
    message.messageType = HoneMessageType_InstallSourceJob
    message.content = (address, 1234567, [['query'], ['MapStreamSet']])
    hostSnd.sendMessage(address, message)
    message.messageType = HoneMessageType_UpdateSourceJob
    message.content = '10.0.0.1'
    hostSnd.sendMessage(address, message)


'''
Peng Sun
agentRun.py
Main entry of host agent
python agentRun.py controllerIPaddress controllerPort
'''

import sys
import struct
import socket
#import logging
#import time
import datetime
import agentManager
import agentUtil

#LogLevel = logging.INFO

def main():
    if (len(sys.argv) < 3):
        print 'Provide controller IP address and port'
        print 'Usage: python agentRun.py controllerIP controllerPort'
        sys.exit()
    logFileName = str(datetime.datetime.now()).translate(None, ' :-.')
    logFileName = 'logs/' + logFileName + '.log'
    agentUtil.SetLogFileName(logFileName)
    #logging.basicConfig(filename=logFileName, level=LogLevel,\
    #    format='%(asctime)s.%(msecs).03d,%(module)17s,%(funcName)19s,%(lineno)3d,%(message)s',\
    #    datefmt='%m/%d/%Y %H:%M:%S')
    ctrlAddress = sys.argv[1]
    ctrlPort = sys.argv[2]
    if (ctrlAddress != 'localhost'):
        try:
            struct.unpack('I', socket.inet_pton(socket.AF_INET, ctrlAddress))
        except socket.error, msg:
            print 'Controller IP Address Invalid: '+ctrlAddress
            sys.exit()   
    if not str.isdigit(ctrlPort):
        print 'Controller Port Invalid: '+ctrlPort
        sys.exit()
    agentManager.agentManagerRun(ctrlAddress, int(ctrlPort))
    
if __name__=='__main__':
    main()

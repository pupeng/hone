# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# agentRun.py
# Main entry of host agent
# python agentRun.py controllerIPaddress controllerPort

import sys
import struct
import socket
import logging

import agentManager
from agentUtil import LogUtil

def main():
    if len(sys.argv) < 3:
        print 'Provide controller IP address and port'
        print 'Usage: python agentRun.py controllerIP controllerPort'
        sys.exit()
    LogUtil.InitLogging()
    ctrlAddress = sys.argv[1]
    ctrlPort = sys.argv[2]
    if ctrlAddress != 'localhost':
        try:
            struct.unpack('I', socket.inet_pton(socket.AF_INET, ctrlAddress))
        except socket.error, msg:
            print 'Controller IP Address Invalid: ' + ctrlAddress
            sys.exit()   
    if not str.isdigit(ctrlPort):
        print 'Controller Port Invalid: ' + ctrlPort
        sys.exit()
    logging.info('hone agent starts for controller at {0} on {1}'.format(ctrlAddress, ctrlPort))
    print 'HONE agent starts.'
    agentManager.agentManagerRun(ctrlAddress, int(ctrlPort))
    
if __name__=='__main__':
    main()

# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from datetime import datetime
import sys
from threading import Timer
import time

numberOfUsers = 0
lastCheck = None
port = None

class testProtocol(LineReceiver):
    def connectionMade(self):
        global numberOfUsers
        numberOfUsers += 1

    def lineReceived(self, line):
        global numberOfUsers
        global lastCheck
        global port
        if not lastCheck:
            lastCheck = time.time()
        if ((time.time() - lastCheck) > 10):
            print 'Port: {2}. Number of Connections: {0}. time: {1}'.format(numberOfUsers, time.time(), port)
            lastCheck = time.time()

    def connectionLost(self, reason):
        global numberOfUsers
        numberOfUsers -= 1
        
class testFactory(Factory):
    def buildProtocol(self, addr):
        return testProtocol()

if __name__=='__main__':
    if (len(sys.argv) < 2):
        print "Please give port"
        sys.exit()
    print 'Listening on port {0}'.format(sys.argv[1])
    port = int(sys.argv[1])
    reactor.listenTCP(port, testFactory())
    reactor.run()
    print 'Exit stressTestServer'

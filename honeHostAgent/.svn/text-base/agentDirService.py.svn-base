'''
Peng Sun
agentDirService.py

Directory service
wrap the C implementation of directory service, and communicate with agentConnMeasure
see the C implementation in agent_dir_service.c
'''

import time
import multiprocessing
import fnmatch
import ipaddr
import traceback
#import logging
from agentUtil import *
from agentTypes import *
from agent_dir_service import agent_dir_service_run, agent_dir_service_recv, agent_dir_service_cleanup

class DirServiceProcess(StoppableProcess):
    #def __init__(self, passedSocketCriteriaQueue, passedSocketTable, passedSourceJobSkList):
    def __init__(self, passedSocketCriteriaQueue, passedSocketQueue):
        super(DirServiceProcess, self).__init__()
        self.socketCriteriaQueue = passedSocketCriteriaQueue
        self.socketCriteria = {}
        self.socketQueue = passedSocketQueue
        self.socketTable = {}
        self.sourceJobSkList = {}
         
    def run(self):
        print 'agentDirService starts'
        #EvalLog('{0:6f},88,dirService starts'.format(time.time()))
        try:
            agent_dir_service_run()
            while not self.shouldStop():
                #debugLog('dir', 'socketCriteria: ', self.socketCriteria)
                message = agent_dir_service_recv()
                if message:
                    message = message.split('\n')[0]
                    message = message.split('#')
                    #debugLog('dir', 'get new message from kernel:', message)
                    itemType = message[0]
                    appName = message[1]
                    sockfd = message[2]
                    if itemType == 'connect':
                        if self.passSocketCriteria(message):
                            newSock = SocketStruct(sockfd=sockfd, app=appName, srcAddress=message[3], srcPort=message[4], dstAddress=message[5], dstPort=message[6])
                            self.socketTable[sockfd] = newSock
                            item = (IPCType['NewSocket'], (sockfd, newSock))
                            self.socketQueue.put(item)
                    elif itemType == 'send':
                        if sockfd in self.socketTable:
                            self.socketTable[sockfd].updateBytesWritten(int(message[7]))
                        else:
                            if self.passSocketCriteria(message):
                                newSock = SocketStruct(sockfd=sockfd, app=appName, srcAddress=message[3], srcPort=message[4], dstAddress=message[5], dstPort=message[6])
                                newSock.updateBytesWritten(int(message[7]))
                                self.socketTable[sockfd] = newSock
                                item = (IPCType['NewSocket'], (sockfd, newSock))
                                self.socketQueue.put(item)
                    elif itemType == 'close':
                        if sockfd in self.socketTable:
                            item = (IPCType['DeleteSocket'], sockfd)
                            self.socketQueue.put(item)
                            del self.socketTable[sockfd]
                            for jobFlow in self.sourceJobSkList.keys():
                                if sockfd in self.sourceJobSkList[jobFlow]:
                                    self.sourceJobSkList[jobFlow].remove(sockfd)
                                    item = (IPCType['RemoveSkFromJobFlow'], (jobFlow, sockfd))
                                    self.socketQueue.put(item)
                    else:
                        print 'unknown kernel message: {0}'.format(message)
                time.sleep(0.001)
        except Exception as e:
            print 'Got exception in dir service process: {0}'.format(e)
            traceback.print_exc()
        finally:
            #EvalLog('{0:6f},89,dirService exits'.format(time.time()))
            agent_dir_service_cleanup()
            #EvalLog('{0:6f},90,done kernel communication cleanup'.format(time.time()))
            WriteLogs()
            print 'Exit from agentDirService'

    def passSocketCriteria(self, message):
        appName = message[1]
        sockfd = message[2]
        srcAddress = message[3]
        srcPort = message[4]
        dstAddress = message[5]
        dstPort = message[6]
        ret = False
        while not self.socketCriteriaQueue.empty():
            (itemType, itemContent) = self.socketCriteriaQueue.get_nowait()
            if itemType == IPCType['InstallSocketCriteria']:
                (key, measureCriteria) = itemContent
                self.socketCriteria[key] = measureCriteria
            elif itemType == IPCType['DeleteSocketCriteria']:
                if itemContent in self.socketCriteria:
                    del self.socketCriteria[itemContent]
        for (jobFlow, criteria) in self.socketCriteria.iteritems():
            result = True
            for (cr, value) in criteria.iteritems():
                if cr == 'app':
                    if fnmatch.fnmatch(appName, value):
                        result = result and True
                    else:
                        result = False
                elif cr == 'srcIP':
                    if ipaddr.IPAddress(srcAddress) in ipaddr.IPNetwork(value):
                        result = result and True
                    else:
                        result = False
                elif cr == 'srcPort':
                    if srcPort == value:
                        result = result and True
                    else:
                        result = False
                elif cr == 'dstIP':
                    if ipaddr.IPAddress(dstAddress) in ipaddr.IPNetwork(value):
                        result = result and True
                    else:
                        result = False
                elif cr == 'dstPort':
                    if dstPort == value:
                        result = result and True
                    else:
                        result = False
                else:
                    print 'unknown tuple match {0}:{1}'.format(cr, value)
            if result:
                if jobFlow not in self.sourceJobSkList:
                    self.sourceJobSkList[jobFlow] = []
                if sockfd not in self.sourceJobSkList[jobFlow]:
                    self.sourceJobSkList[jobFlow].append(sockfd)
                    item = (IPCType['AddSkToJobFlow'], (jobFlow, sockfd))
                    self.socketQueue.put(item)
            ret = ret or result
        return ret

if __name__ == '__main__':
    dirServiceProcess = DirServiceProcess({}, {}, {})
    try:
        dirServiceProcess.start()
        dirServiceProcess.join()
    except KeyboardInterrupt:
        #debugLog('dir', 'catch keyboardinterrupt')
        dirServiceProcess.stop()
        dirServiceProcess.join()
    finally:
        print 'Exit from main'

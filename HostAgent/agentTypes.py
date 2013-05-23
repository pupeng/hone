'''
Peng Sun
agentTypes

Multiple helper classes
'''

import multiprocessing
import heapq

class StoppableProcess(multiprocessing.Process):
    def __init__(self):
        super(StoppableProcess, self).__init__()
        self._stopEvent = multiprocessing.Event()

    def stop(self):
        self._stopEvent.set()

    def shouldStop(self):
        return self._stopEvent.is_set()

class SourceJob:
    def __init__(self, jobId, flowId):
        self.jobId = jobId
        self.flowId = flowId
        self.middleAddress = None
        self.progName = None
        self.createTime = None
        self.period = None
        self.measureType = None # 'conn' or 'proc' or 'machine'
        self.measureStats = []
        self.measureCriteria = {}
        self.computePart = None
        self.lastSequence = None
        self.deadline = None

    def updateDeadline(self):
        self.deadline += self.period / 1000
        self.lastSequence += 1
    
    def debug(self):
        info = 'jobId: {0}. '.format(self.jobId)
        info += 'flowId: {0}. '.format(self.flowId)
        info += 'middleAddress: {0}. '.format(self.middleAddress)
        info += 'progName: {0}. '.format(self.progName)
        info += 'createTime: {0}. '.format(self.createTime)
        info += 'period: {0}. '.format(self.period)
        info += 'measureType: {0}. '.format(self.measureType)
        info += 'measureStats: {0}. '.format(self.measureStats)
        info += 'measureCriteria: {0}. '.format(self.measureCriteria)
        info += 'computePart: {0} '.format(self.computePart)
        info += 'lastSequence: {0}. '.format(self.lastSequence)
        return info

class MiddleJob:
    def __init__(self, jobId, flowId, level):
        self.jobId = jobId
        self.flowId = flowId
        self.level = level
        self.progName = None
        self.event = None
        self.goFunc = None
        self.expectedNumOfChild = None
        self.parentAddress = None
        self.lastSeq = None

    def debug(self):
        return 'jobId {0} flowId {1} level {2} progname {3} expect {4} parent {5} lastSeq {6}'.format(self.jobId, self.flowId, self.level, self.progName, self.expectedNumOfChild, self.parentAddress, self.lastSeq)

class SocketStruct:
    def __init__(self,sockfd,app=None,srcAddress=None,srcPort=None,dstAddress=None,dstPort=None):
        self.sockfd = sockfd
        self.app = app
        self.srcAddress = srcAddress
        self.srcPort = srcPort
        self.dstAddress = dstAddress
        self.dstPort = dstPort
        self.cid = None
        self.bytesWritten = None

    def setCid(self, cid):
        self.cid = cid

    def updateBytesWritten(self, bytesWritten):
        if self.bytesWritten:
            self.bytesWritten += bytesWritten
        else:
            self.bytesWritten = bytesWritten

    def GetTuple(self):
        return str(self.srcAddress) + ':' + str(self.srcPort) + ':' + str(self.dstAddress) + ':' + str(self.dstPort)

class JobFlowMinQueue:
    def __init__(self):
        self.queue = []

    def push(self, deadline, jobFlowKey):
        heapq.heappush(self.queue, (deadline, jobFlowKey))

    def pop(self):
        (_, jobFlowKey) = heapq.heappop(self.queue)
        return jobFlowKey

    def isEmpty(self):
        if self.queue:
            return False
        return True

    def minDeadline(self):
        if self.isEmpty():
            return None
        return self.queue[0][0]

    def debug(self):
        return self.queue
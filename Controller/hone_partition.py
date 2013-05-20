'''
Peng Sun
hone_partition.py
partition the instance of honeDataFlow into honePartitionedFlow
return to rts
'''

from hone_lib import *
from hone_message import *
from hone_util import *
import ipaddr

class HonePartitionedFlow:
    def __init__(self, progName, honeDataFlow):
        if not isinstance(honeDataFlow, HoneDataFlow):
            raise Exception('improper argument passed into partitionDataFlow')
        self.progName = progName
        self.hostSourceExePlan = []
        self.hostMiddleExePlan = []
        self.networkExePlan = []
        self.controllerExePlan = []
        self.criterion = {'app'  : [],
                          'srcIP': [],
                          'dstIP': []}
        self.flowToCtrl= []
        self.flowToMiddle = []
        self.minQueryPeriod= None
        self.addExePlan(honeDataFlow)
        self.debug()

    def isHostEligible(self, hostEntry):
        ret = True
        if (len(self.criterion['app']) == 0) \
           or (len(set(hostEntry.appList) - set(self.criterion['app'])) \
               < len(hostEntry.appList)):
            ret = ret and True
        else:
            ret = False
        for ipnet in self.criterion['srcIP']:
            if ipaddr.IPAddress(hostEntry.hostAddress) in \
               ipaddr.IPNetwork(ipnet):
                ret = ret and True
            else:
                ret = False
        for ipnet in self.criterion['dstIP']:
            if ipaddr.IPAddress(hostEntry.hostAddress) in \
               ipaddr.IPNetwork(ipnet):
                ret = ret and True
            else:
                ret = False
        return ret

    def addExePlan(self, dataFlow):
        (hostSource, hostMiddle, network, controller) = \
            self.partition(dataFlow)
        if (len(hostSource) > 0):
            self.hostSourceExePlan.append(FlowExePlan(dataFlow.flowId, hostSource))
        if (len(hostMiddle) > 0):
            self.hostMiddleExePlan.append(FlowExePlan(dataFlow.flowId, hostMiddle))
        if (len(network) > 0):
            self.networkExePlan.append(FlowExePlan(dataFlow.flowId, network))
        self.controllerExePlan.append(FlowExePlan(dataFlow.flowId, controller))
        if (dataFlow.flow[0].wh is not None):
            for criteria in dataFlow.flow[0].wh:
                if (criteria[0] in self.criterion):
                    if (criteria[1] != '=='):
                        raise Exception('Must give == to app or srcIP or dstIP \
                                         in Where clause')
                    self.criterion[criteria[0]].append(criteria[2])
        for subFlow in dataFlow.subFlows:
            self.addExePlan(subFlow)

    def partition(self, dataFlow):
        flowId = dataFlow.flowId
        flow = dataFlow.flow
        hostSource = []
        hostMiddle = []
        network = []
        controller = []
        tableName = flow[0].ft
        period = flow[0].ev
        if (self.minQueryPeriod is None) or (period < self.minQueryPeriod):
            self.minQueryPeriod = period
        if (tableName == 'HostConnection') or (tableName == 'AppStatus') or \
            (tableName == 'HostStatus'):
            numOp = 1
            while (numOp < len(flow)):
                if (flow[numOp][0] == 'MergeHosts') or \
                   (flow[numOp][0] == 'TreeMerge'):
                   break
                numOp += 1
            if (numOp == len(flow)):
                hostSource = flow
            else:
                hostSource = flow[0 : numOp]
                if (flow[numOp][0] == 'MergeHosts'):
                    self.flowToCtrl.append(flowId)
                    hostSource.append(['ToCtrl'])
                    controller = flow[(numOp + 1) : ]
                else:
                    self.flowToMiddle.append(flowId)
                    hostSource.append(['ToMiddle'])
                    hostMiddle = [flow[numOp], ['ToUpperLevel']]
                    controller = flow[numOp : ]
        else:
            network.append(flow[0])
            controller = flow[1 : ]
        return (hostSource, hostMiddle, network, controller)

    def debug(self):
        pass
        #debugLog('part', 'host source')
        #for flowExePlan in self.hostSourceExePlan:
            #debugLog('part', 'flowId:', flowExePlan.flowId, flowExePlan.exePlan)
        #debugLog('part', 'host middle')
        #for flowExePlan in self.hostMiddleExePlan:
            #debugLog('part', 'flowId:', flowExePlan.flowId, flowExePlan.exePlan)
        #debugLog('part', 'network')
        #for flowExePlan in self.networkExePlan:
            #debugLog('part', 'flowId:', flowExePlan.flowId, flowExePlan.exePlan)
        #debugLog('part', 'controller')
        #for flowExePlan in self.controllerExePlan:
            #debugLog('part', 'flowId:', flowExePlan.flowId, flowExePlan.exePlan)
        #debugLog('part', 'flowToCtrl', self.flowToCtrl)
        #debugLog('part', 'flowToMiddle', self.flowToMiddle)


'''
Peng Sun
hone_lib.py
provide library for mgmt program to create dataflow
'''

import hone_rts
from hone_util import *
from hone_message import *
import inspect
from cStringIO import StringIO

globalFlowId = 0

''' class for data flow '''
def getNextFlowId():
    global globalFlowId
    globalFlowId += 1
    return globalFlowId

class HoneDataFlow:
    def __init__(self, q, operator):
        self.flow = []
        self.subFlows = [] #list of class HoneDataFlow items. Merged flows
        self.flowId = getNextFlowId()
        if (q != None):
            self.flow.append(q)
        if (operator != None):
            self.flow.append(operator)
        #debugLog('lib', 'new HoneDataFlow', self.flow)
                   
    def __rshift__(self, other):
        #debugLog('lib', 'In rshift of HoneDataFlow', 'self', self.flow, 'other', \
        #         other.flow)
        self.flow = self.flow + other.flow
        return self
    
    def addSubFlow(self, x):
        self.subFlows.append(x)

    def printDataFlow(self):
        buf = StringIO()
        print >>buf, 'flow id: ',self.flowId
        if (isinstance(self.flow[0], HoneQuerySerialized)):
            print >>buf, 'Select:',self.flow[0].se
            print >>buf, 'From:',self.flow[0].ft
            print >>buf, 'Where:',self.flow[0].wh
            print >>buf, 'Groupby:',self.flow[0].gp
            print >>buf, 'Every:',self.flow[0].ev
            print >>buf, 'Aggregate:',self.flow[0].agg
            print >>buf, self.flow[1:]
        else:
            print >>buf, self.flow
        print >>buf, '\n'
        ret = buf.getvalue()
        buf.close()
        for subFlow in self.subFlows:
            ret += subFlow.printDataFlow()
        return ret

''' query part '''
class HoneQuery:
    def __init__(self,var,ft,wh,gp,every,agg,compose):
        self.complete = False
        self.var = var
        self.ft = ft
        self.wh = wh
        self.gp = gp
        self.every = every
        self.agg = agg
        self.compose = compose
    
    def __rshift__(self, other):
        HoneQuerySyntaxCheck(self)
        #debugLog('lib', 'new HoneQuery instance created', self.printQuery())
        return self.convertToHoneDataFlow() >> other
    
    def __mul__(self, other):
        otherName = other.__class__.__name__
        if otherName=='HoneQuery':
            return other.compose(self)
        else:
            raise Exception('HoneQuery cannot compose with %s' % otherName)

    def printQuery(self):
        ret = StringIO()
        print >>ret, 'HoneQuery Select:',self.var
        print >>ret, 'HoneQuery From:',self.ft
        print >>ret, 'HoneQuery Where:',self.wh
        print >>ret, 'HoneQuery Groupby:',self.gp
        print >>ret, 'HoneQuery Every:',self.every
        print >>ret, 'HoneQuery Aggregate:',self.agg
        return ret.getvalue()

    def convertToHoneDataFlow(self):
        query = HoneQuerySerialized()
        query.se = self.var
        query.ft = self.ft
        query.wh = self.wh
        query.gp = self.gp
        query.ev = self.every
        query.agg = self.agg
        return HoneDataFlow(query, None)

def Select(x):
    def compose(q):
        if q.var == None:
            q.var = []
        q.var = q.var+x
        return q
    agg = None
    for i in range(0,len(x)):
        if (type(x[i]) == type(tuple())):
            if (agg == None):
                agg = []
            agg.append(x[i])
            x[i] = x[i][0]
    return HoneQuery(x,None,None,None,1000,agg,compose)

def From(ft):
    def compose(q):
        q.ft = ft
        return q
    return HoneQuery(None,ft,None,None,None,None,compose)

def Where(wh):
    def compose(q):
        if q.wh == None:
            q.wh = []
        q.wh = q.wh + wh
        return q
    return HoneQuery(None,None,wh,None,None,None,compose)

def Groupby(gp):
    def compose(q):
        if q.gp == None:
            q.gp = []
        q.gp = q.gp + gp
        return q
    return HoneQuery(None,None,None,gp,None,None,compose)

def Every(every):
    def compose(q):
        q.every = every
        return q
    return HoneQuery(None,None,None,None,every,None,compose)

def HoneQuerySyntaxCheck(q):
    #debugLog('lib', 'syntax check of query', q.printQuery())
    varOnlySupportEqualInWhere = ['app', 'srcIP', 'dstIP', 'srcPort', 'dstPort']
    if q.var is None:
        raise Exception('HoneQuery must at least have a Select')
    if q.ft is None:
        raise Exception('HoneQuery must have a From table')
    if not hone_rts.honeTableTypes.has_key(q.ft):
        raise Exception('HoneQuery: No such From Table {}'.format(q.ft))
    varName = []
    for typ in q.var:
        varName.append(typ)
    if not (q.wh is None):
        for (typ, op, value) in q.wh:
            if not typ in varName:
                raise Exception('HoneQuery: Where of not-Selected columns')
            if (typ in varOnlySupportEqualInWhere) and (not (op == '==')):
                raise Exception('Var {} only support == in Where clause'.format(typ))
    if not (q.gp is None):
        for typ in q.gp:
            if not typ in varName:
                raise Exception('HoneQuery: Groupby of not-Selected columns')
    for typ in varName:
        if not (typ in hone_rts.honeTableTypes[q.ft]):
            raise Exception('HoneQuery No type {} in Table {}'.format(typ, q.ft))
    if q.agg is not None:
        for (typ, op) in q.agg:
            if not op in ['max', 'min', 'sum', 'avg']:
                raise Exception('Only max, min, sum, avg are supported in Select {}'.format(typ))
    if (q.ft == 'AppStatus'):
        if 'app' not in varName:
            #debugLog('lib', 'syntax check', q.printQuery())
            raise Exception('Must Select \'app\' in AppStatus table')

''' operator part '''

def MapStreamSet(f):
    if (isinstance(f,HoneDataFlow)):
        return HoneDataFlow(None,['MapStreamSet'] + f.flow[0])
    else:
        return HoneDataFlow(None,['MapStreamSet', f.__name__])

def MapStream(f):
    if (isinstance(f,HoneDataFlow)):
        return HoneDataFlow(None,['MapStream'] + f.flow[0])
    else:
        return HoneDataFlow(None,['MapStream', f.__name__])

def MapList(f):
    if isinstance(f,HoneDataFlow):
        return HoneDataFlow(None,['MapList'] + f.flow[0])
    else:
        return HoneDataFlow(None,['MapList', f.__name__])
    
def FilterStreamSet(f):
    if isinstance(f,HoneDataFlow):
        return HoneDataFlow(None,['FilterStreamSet'] + f.flow[0])
    else:
        return HoneDataFlow(None,['FilterStreamSet', f.__name__])
    
def FilterStream(f):
    if isinstance(f,HoneDataFlow):
        return HoneDataFlow(None,['FilterStream'] + f.flow[0])
    else:
        return HoneDataFlow(None,['FilterStream', f.__name__])
    
def FilterList(f):
    if isinstance(f,HoneDataFlow):
        return HoneDataFlow(None,['FilterList'] + f.flow[0])
    else:
        return HoneDataFlow(None,['FilterList', f.__name__])
    
def ReduceStreamSet(f, init):
    if isinstance(f,HoneDataFlow):
        return HoneDataFlow(None,['ReduceStreamSet', init] + f.flow[0])
    else:
        return HoneDataFlow(None,['ReduceStreamSet', init, f.__name__])

def ReduceStream(f, init):
    if isinstance(f,HoneDataFlow):
        return HoneDataFlow(None,['ReduceStream', init] + f.flow[0])
    else:
        return HoneDataFlow(None,['ReduceStream', init, f.__name__])

def ReduceList(f, init):
    if isinstance(f,HoneDataFlow):
        return HoneDataFlow(None,['ReduceList', init] + f.flow[0])
    else:
        return HoneDataFlow(None,['ReduceList', init, f.__name__])

def MergeHosts():
    return HoneDataFlow(None,['MergeHosts'])

def MergeStreams(stream1, stream2):
    if isinstance(stream1, HoneQuery):
        stream1 = stream1.convertToHoneDataFlow()
    if isinstance(stream2, HoneQuery):
        stream2 = stream2.convertToHoneDataFlow()
    operator = ['MergeStreams']
    stream1.addSubFlow(stream2)
    operator.append(stream2.flowId)
    stream1.flow.append(operator)
    return stream1

def MergeStreamsForSet(stream1, stream2):
    if isinstance(stream1, HoneQuery):
        stream1 = stream1.convertToHoneDataFlow()
    if isinstance(stream2, HoneQuery):
        stream2 = stream2.convertToHoneDataFlow()
    operator = ['MergeStreamsForSet']
    stream1.addSubFlow(stream2)
    operator.append(stream2.flowId)
    stream1.flow.append(operator)
    return stream1

def Print(f=None):
    if f:
        return HoneDataFlow(None, ['Print', f.__name__])
    else:
        return HoneDataFlow(None, ['Print'])
    
def RegisterPolicy(f=None):
    return HoneDataFlow(None, ['RegisterPolicy'])

def RateLimit(rate):
    return HoneDataFlow(None, ['RateLimit', rate])

def TreeMerge(f):
    return HoneDataFlow(None, ['TreeMerge', f.__name__])

'''
Peng Sun
example.py
An example management program for debugging HONE development
'''

from hone_lib import *
import inspect
import sys

def myQuery1():
    q = (Select(['app','srcIP','srcPort','dstIP','dstPort','Cwnd', ('BytesSentOut', 'sum'), ('RTT','avg'), ('BytesRetran', 'max')]) *
         From('HostConnection') *
         Where([('srcIP', '==', '127.0.0.0/0')]) *
         #Where([('app', '==', 'xc'), ('dstPort','==','5000'), ('Cwnd','>','100'), ('RTT','<','100')]) *
         Groupby(['srcIP', 'dstIP']) *
         Every(1000))
    return q

def myQuery2():
    q = (Select(['app','cpu','memory']) *
         From('AppStatus') *
         Every(1000))
    return q

def myQuery3():
    q = (Select(['BeginDevice','BeginPort']) *
         From('LinkStatus') *
         Every(3000))
    return q

def myQuery4():
    q = (Select(['hostId', 'totalCPU']) *
         From('HostStatus') *
         Every(4000))
    return q

def myPrint(x):
    print x
    print 'Just myPrint. Nothing special.'
    
def myRowFunc(x):
    print 'I am myRowFunc'
    print x
    return x

def myAggregation(x, y):
    print 'I am myAggregation'
    return x

def myTreeMerge(x):
    print 'I am myTreeMerge'
    return x

def main():
    stream1 = myQuery1() >> MapStreamSet(MapList(myRowFunc)) >> \
              MapStreamSet(ReduceList(myAggregation,[])) >> TreeMerge(myTreeMerge)
    stream2 = myQuery2() >> MergeHosts()
    stream3 = myQuery3()
    stream4 = myQuery4() >> MergeHosts()
    return (MergeStreams(MergeStreams(MergeStreams(stream1, stream2), stream3), stream4) >>
            MapStream(MapList(myRowFunc)) >> 
            MapStream(ReduceList(myAggregation,[])) >> 
            Print(myPrint))

if __name__ == '__main__':
    dataflow = main()
    print dataflow.flow
    for eachSubFlow in dataflow.subFlows:
        print eachSubFlow.flow

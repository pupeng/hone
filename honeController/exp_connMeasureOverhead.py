'''
HONE application
Author: Peng Sun
Purpose:
Run experiment to see connMeasure overhead
'''

from hone_lib import *

def Query():
    q = (Select(['app', 'srcIP', 'srcPort', 'dstIP', 'dstPort', 'BytesSentOut', 'Cwnd']) *
         From('HostConnection') *
         Every(1000))
    return q

def NoOp(x):
    print len(x)
    return x

def main():
    return (Query()>>MapStreamSet(NoOp))

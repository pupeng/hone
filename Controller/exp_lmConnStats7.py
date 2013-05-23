'''
HONE application
Author: Peng Sun
Purpose:
Test lazy materialization for conn measurement
match test in exp2: connStats7
'''

from hone_lib import *

def query():
    q = (Select(['app', 'srcIP', 'srcPort', 'dstIP', 'dstPort', 'BytesWritten', 'BytesSentOut']) *
         From('HostConnection') *
         #Where([('app', '==', 'test_prog')]) *
         Every(1000))
    return q

def noOp(x):
    print len(x)
    if x:
        print x[0]
    return x

def main():
    return (query()>>MapStreamSet(noOp))

'''
Peng Sun
eval2
'''

from hone_lib import *
from math import *
import time, sys

_DRL_DEBUG_ = True

K = 0.2

def query():
    q = (Select(['app','BytesSentOut','BytesWritten','SegmentsOut']) *
        From('HostConnection') *
        Where([('app','==','test_prog')]) *
        Every(3))
    return q

def checkPoint(x):
    for conn in x:
        print 'BS:'+str(conn[1])+' BW:'+str(conn[2])+' SO:'+str(conn[3])
    return None

def main():
    return (query()>>MapSet(checkPoint))



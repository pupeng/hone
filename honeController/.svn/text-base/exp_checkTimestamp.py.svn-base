'''
Peng Sun
checktimestamp
'''

from hone_lib import *
from math import *
import time, sys

_CTS_DEBUG_ = True

def query():
    q = (Select(['app','timestamp']) *
        From('HostConnection') *
        Where([('app','==','test_prog')]) *
        Every(1))
    return q

def processData(x):
    if x:
        return [len(x), x[0][1]]
    else:
        return None

def myPrint(x):
    (hostID, seq, data) = x
    [number, ts] = data
    print 'hostID:'+hostID+' seq:'+str(seq)+' number:'+str(number)+' timestamp:'+str(ts)

def main():
    return (query()>>MapSet(processData)>>MergeHosts()>>Print(myPrint))
            





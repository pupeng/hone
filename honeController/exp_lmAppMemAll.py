'''
HONE application
Author: Peng Sun
Purpose:
Test lazy materialization for conn measurement
match test in exp2: appCpuAll
'''

from hone_lib import *

def query():
    q = (Select(['app', 'memory']) *
         From('AppStatus') *
         Every(1000))
    return q

def noOp(x):
    print len(x)
    return x

def main():
    return (query()>>MapStreamSet(noOp))

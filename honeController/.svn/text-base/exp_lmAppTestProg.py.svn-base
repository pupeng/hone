'''
HONE application
Author: Peng Sun
Purpose:
Test lazy materialization for conn measurement
match test in exp2: appTestProg
'''

from hone_lib import *

def query():
    q = (Select(['app', 'cpu', 'memory']) *
         From('AppStatus') *
         Where([('app', '==', 'test_prog')]) *
         Every(1000))
    return q

def noOp(x):
    print x
    return x

def main():
    return (query()>>MapStreamSet(noOp))

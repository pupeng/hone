# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Test lazy materialization for conn measurement
# match test in exp2: appTestProg

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

# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Run experiment to see connMeasure overhead

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

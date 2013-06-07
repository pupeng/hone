# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Debug: check the correctness of timestamp

from hone_lib import *

def query():
    q = (Select(['app','StartTimeSecs','ElapsedSecs','StartTimeMicroSecs','ElapsedMicroSecs']) *
        From('HostConnection') *
        Where([('app','==','test_prog')]) *
        Every(1000))
    return q

def ProcessData(table):
    if table:
        (app, startSecs, elapsedSecs, startMicrosecs, elapsedMicrosecs) = table[0]
        timestamp = startSecs + elapsedSecs+startMicrosecs / 1000000.0 + elapsedMicrosecs / 1000000.0
        return [len(table), timestamp]

def DebugPrint(x):
    [numberOfConns, timestamp] = x
    print 'number of conns:{0} timestamp:{1}'.format(numberOfConns, timestamp)

def main():
    return (query()>>
            MapStreamSet(ProcessData) >>
            Print(DebugPrint))

            





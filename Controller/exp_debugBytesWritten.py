# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Debug the measurement of number of bytes written

from hone_lib import *


def query():
    q = (Select(['app','BytesSentOut','BytesWritten','SegmentsOut']) *
        From('HostConnection') *
        Where([('app','==','test_prog')]) *
        Every(3000))
    return q

def CheckPoint(x):
    for conn in x:
        print 'Application {0} BytesSent: {1} BytesWritten: {2} SegmentsOut: {3}'.format(conn[0], conn[1], conn[2], conn[3])

def main():
    return (query()>>
            MapStreamSet(CheckPoint))



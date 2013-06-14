# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Elephant flow detection and scheduling

from hone_lib import *

def LinkQuery():
    return (Select(['BeginDevice', 'BeginPort', 'EndDevice', 'EndPort']) *
            From('LinkStatus') *
            Every(2000))

def SwitchQuery():
    return (Select(['switchId', 'portNumber']) *
            From('SwitchStatus') *
            Every(2000))

def RouteQuery():
    return (Select(['HostAId', 'HostBId', 'Path']) *
            From('Route') *
            Every(2000))

def main():
    # stream = MergeStreams(LinkQuery(), SwitchQuery()) >> Print()
    stream = RouteQuery() >> Print()
    return stream

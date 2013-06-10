# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# find the network topology

import time
from hone_lib import *

def LinkQuery():
    return (Select(['BeginDevice', 'BeginPort', 'EndDevice', 'EndPort']) *
            From('LinkStatus') *
            Every(3000))

def FindRoutesForHostPair(links):
    # remove the out-most list structure
    links = links[0]
    for link in links:
        pass
    hosts = filter(lambda  x: x[1] is None, links)


def PrintHelper(x):
    print time.time()
    print x

def SwitchQuery():
    return (Select(['switchId', 'portNumber', 'capacity']) *
            From('SwitchStatus') *
            Every(3000))

def main():
    return (SwitchQuery() >>
    # return (LinkQuery() >>
    #         MapStream(FindRoutesForHostPair) >>
            Print(PrintHelper))
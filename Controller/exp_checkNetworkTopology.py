# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# find the network topology

from hone_lib import *

def LinkQuery():
    return (Select(['BeginDevice','EndDevice']) *
            From('LinkStatus') *
            Every(3000))

def main():
    return (LinkQuery() >>
            Print())

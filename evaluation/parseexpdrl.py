# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# parse drl result for plotting

import sys
import math
import re
import os
import json

def main():
    logFile = open('drldata/drl_throughput.txt', 'r')
    logs = logFile.read().split('\n')
    logFile.close()
    logs.pop(0)
    logs = map(lambda x : x.split(' '), logs)
    results = {}
    for log in logs:
        if log[0] == 'host':
            (_, hostId, timestamp, rate) = log
            if hostId not in results:
                results[hostId] = []
            results[hostId].append([timestamp, rate])
        elif log[0] == 'aggregate':
            (_, timestamp, rate) = log
            if 'agg' not in results:
                results['agg'] = []
            results['agg'].append([timestamp, rate])
    print results

if __name__ == '__main__':
    main()
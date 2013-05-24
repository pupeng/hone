# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_hostinfo.py
# A system application to collect the hosts' inforamtion
# Collect the applications running on hosts

from hone_lib import *

class ChangeList(object):
    def __init__(self):
        self.hostId = None
        self.add = []
        self.delete = []

def infoQuery():
    q = (Select(['hostId', 'app']) *
         From('AppStatus') *
         Every(1000))
    return q

def appDiff(newTable, state):
    (existedList, changeList) = state
    changeList.hostId = newTable[0][0]
    newList = []
    for [hostId, appName] in newTable:
        newList.append(appName)
    changeList.add = list(set(newList) - set(existedList))
    changeList.delete = list(set(existedList) - set(newList))
    existedList = newList
    return (existedList, changeList)

def getChangeList(state):
    return state[1]

def noOp(x):
    return x

def main():
    return (infoQuery() >> 
            ReduceStreamSet(appDiff, ([], ChangeList())) >>
            MapStreamSet(getChangeList) >>
            MergeHosts() >>
            MapStream(noOp))
    
if __name__ == '__main__':
    from cStringIO import StringIO
    import cPickle as pickle
    from hone_message import *
    import sys
    dataFlow = main()
    dataFlow.printDataFlow()
    changeList = ChangeList()
    changeList.hostId = 12345
    changeList.add = [1, 2, 3]
    changeList.delete = [4, 5]
    message = HoneMessage()
    message.messageType = HoneMessageType_StatsIn
    message.hostId = changeList.hostId
    message.jobId = 6789
    message.flowId = 1
    message.sequence = 0
    message.content = changeList
    buf = StringIO()
    pickle.dump(message, buf, pickle.HIGHEST_PROTOCOL)
    wire = buf.getvalue()
    buf.close()
    print 'old', message
    print 'wire', len(wire), sys.getsizeof(wire)
    buf2 = StringIO(wire)
    message2 = pickle.load(buf2)
    buf2.close()
    print 'new', message2
    print message2.content.add


    

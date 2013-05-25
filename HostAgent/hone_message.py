# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_message.py
# define the message between agent and controller
# define all classes for serialization

HoneMessageType_HostJoin         = 0
HoneMessageType_StatsIn          = 1
HoneMessageType_InstallSourceJob = 2
HoneMessageType_UpdateSourceJob  = 3
HoneMessageType_InstallMiddleJob = 4
HoneMessageType_UpdateMiddleJob  = 5
HoneMessageType_SendFile         = 6
HoneMessageType_RelayStatsIn     = 7

class HoneMessage(object):
    def __init__(self):
        self.messageType = None
        self.hostId = None
        self.jobId = None
        self.flowId = None
        self.sequence = None
        self.content = None
        self.level = None

# for serialization of HoneQuery
class HoneQuerySerialized(object):
    def __init__(self):
        self.se = None
        self.ft = None
        self.wh = None
        self.gp = None
        self.ev = None
        self.agg = None

class FlowExePlan(object):
    def __init__(self, flowId, exePlan):
        self.flowId = flowId
        self.exePlan = exePlan


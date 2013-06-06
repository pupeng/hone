# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_hostEntry.py
# Define class to contain host-related information

from hone_util import LogUtil

''' entry about host's information '''
class HostEntry:
    def __init__(self, hostId, hostAddress, appList=None, jobs=None):
        self.hostId = hostId
        self.hostAddress = hostAddress
        if appList is None:
            self.appList = []
        else:
            self.appList = appList
        if jobs is None:
            self.jobs = []
        else:
            self.jobs = jobs

    def addJob(self, jobId):
        if jobId not in self.jobs:
            self.jobs.append(jobId)
            # LogUtil.DebugLog('exeGen', 'HostEntry addJob. hostId: {0}. jobId: {1}.'.format(self.hostId, jobId))

    def removeJob(self, jobId):
        if jobId in self.jobs:
            self.jobs.remove(jobId)
            # LogUtil.DebugLog('exeGen', 'HostEntry removeJob. hostId: {0}. jobId: {1}'.format(self.hostId, jobId))

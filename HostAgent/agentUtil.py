# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# agentUtil.py
# utility

import inspect
import multiprocessing
import logging
import os
import datetime
from uuid import getnode as get_mac

_LAZY_M_ENABLED_ = True

IPCType = {'NewSourceJob' : 1,
           'UpdateSourceJob': 2,
           'InstallSocketCriteria': 3,
           'DeleteSocketCriteria': 4,
           'NewSocket': 5,
           'DeleteSocket': 6,
           'AddSkToJobFlow': 7,
           'RemoveSkFromJobFlow': 8}

def IsLazyTableEnabled():
    return _LAZY_M_ENABLED_

def composeKey(jobId, flowId):
    return '{0}@{1}'.format(jobId, flowId)

def decomposeKey(key):
    [jobId, flowId] = key.split('@')
    return (int(jobId), int(flowId))

def keyContainJobId(key, jobId):
    [keyJobId, keyFlowId] = key.split('@')
    return (keyJobId == str(jobId))

def middleJobKeyContainJobIdAndLevel(key, jobId, level):
    [keyJobId, _, keyLevel] = key.split('@')
    return (keyJobId == str(jobId)) and (keyLevel == str(level))

def composeMiddleJobKey(jobId, flowId, level):
    return '{0}@{1}@{2}'.format(jobId, flowId, level)


class LogUtil:
    _LogLevel_  = logging.DEBUG
    _MAN_DEBUG_ = False
    _SND_DEBUG_ = False
    _RCV_DEBUG_ = True
    _CONTROL_DEBUG_ = True
    _DIRSERVICE_DEBUG_ = False
    _LIB_DEBUG_ = True
    _JOB_BUILD_DEBUG_ = False
    _SCH_DEBUG_ = False
    _CONN_DEBUG_ = False
    _PROC_DEBUG_ = False

    LoggingLock = multiprocessing.Lock()

    LogFileName = ''

    DebugFlags = {'manager' : _MAN_DEBUG_,
                   'sndMod'  : _SND_DEBUG_,
                   'rcvMod'  : _RCV_DEBUG_,
                   'control' : _CONTROL_DEBUG_,
                   'dir'     : _DIRSERVICE_DEBUG_,
                   'lib'     : _LIB_DEBUG_,
                   'job'     : _JOB_BUILD_DEBUG_,
                   'schedule': _SCH_DEBUG_,
                   'conn'    : _CONN_DEBUG_,
                   'proc'    : _PROC_DEBUG_}

    EvalData = []

    @staticmethod
    def InitLogging():
        if not os.path.exists('logs'):
            os.makedirs('logs')
        hostId = get_mac()
        LogUtil.LogFileName = 'logs/agent_{0}_{1}.log'.format(hostId, str(datetime.datetime.now()).translate(None, ' :-.'))
        logging.basicConfig(filename=LogUtil.LogFileName, level=LogUtil._LogLevel_,
                            format='%(levelname)8s,%(asctime)s.%(msecs).3d,%(module)17s,%(funcName)21s,%(lineno)3d,%(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')

    @staticmethod
    def DebugLog(section, *args):
        flag = LogUtil.DebugFlags.get(section, False)
        if flag:
            LogUtil.LoggingLock.acquire()
            _, fileName, lineNumber, _, _, _ = inspect.stack()[1]
            tmp = fileName.split('/')
            fileName = tmp[len(tmp) - 1]
            print '\nDEBUG ' + fileName + ', L' + str(lineNumber) + ': '
            for i in range(0, len(args)):
                print args[i]
            print '\n'
            LogUtil.LoggingLock.release()

    @staticmethod
    def EvalLog(eventId, msg):
        #logging.debug('{0},{1}'.format(eventId, msg))
        LogUtil.EvalData.append('{0},{1}'.format(eventId, msg))

    @staticmethod
    def OutputEvalLog():
        output = open(LogUtil.LogFileName + '.eval', 'a')
        for data in LogUtil.EvalData:
            print >>output, data
        output.close()
        LogUtil.EvalData = []

# _loggingLock = multiprocessing.Lock()
#
# _logFileName = None
# _logs = []
#
# def debugLog(module, *args):
#     flag = _debugFlags.get(module, False)
#     if flag:
#         _loggingLock.acquire()
#         _, fileName, lineNumber, _, _, _ = inspect.stack()[1]
#         tmp = fileName.split('/')
#         fileName = tmp[len(tmp) - 1]
#         print '\nDEBUG ' + fileName + ', L' + str(lineNumber) + ': '
#         for i in range(0, len(args)):
#             print args[i]
#         print '\n'
#         _loggingLock.release()
#
# def SetLogFileName(logFileName):
#     global _logFileName
#     _logFileName = logFileName
#
# def EvalLog(info):
#     #print 'EvalLog: {0}'.format(info)
#     global _logs
#     _logs.append(info)
#
# def WriteLogs():
#     global _logFileName
#     global _logs
#     if _logFileName and _logs:
#         output = open(_logFileName, 'a')
#         for log in _logs:
#             print >>output, log
#         output.close()
#         del _logs[:]

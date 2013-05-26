# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# hone_util.py
# util functions for HONE

import datetime
import logging
import os
import time
import multiprocessing
import inspect

class LogUtil:
    _LogLevel_      = logging.DEBUG
    _GLOBAL_DEBUG_  = False
    _LIB_DEBUG_     = False
    _RTS_DEBUG_     = False
    _EXEGEN_DEBUG_  = False
    _PART_DEBUG_    = False
    _EXEMOD_DEBUG_  = False
    _SND_DEBUG_     = False
    _CONTROL_DEBUG_ = False
    _EVALUATION_    = False

    LoggingLock = multiprocessing.Lock()

    LogFileName = ''
    
    DebugFlags = {'global' : _GLOBAL_DEBUG_,
                  'lib'    : _LIB_DEBUG_,
                  'rts'    : _RTS_DEBUG_, 
                  'part'   : _PART_DEBUG_,
                  'exeGen' : _EXEGEN_DEBUG_,
                  'exeMod' : _EXEMOD_DEBUG_,
                  'snd'    : _SND_DEBUG_,
                  'evaluation' : _EVALUATION_ }

    EvalData = []
    
    @staticmethod
    def InitLogging():
        if not os.path.exists('logs'):
            os.makedirs('logs')
        LogUtil.LogFileName = 'logs/controller_{0}.log'.format(str(datetime.datetime.now()).translate(None, ' :-.'))
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
'''
Peng Sun
hone_util.py
util functions for HONE
'''

import datetime
import logging
import os
import time

class LogUtil:
    _LogLevel_      = logging.INFO
    _GLOBAL_DEBUG_  = False
    _LIB_DEBUG_     = False
    _RTS_DEBUG_     = False
    _EXEGEN_DEBUG_  = False
    _PART_DEBUG_    = False
    _EXEMOD_DEBUG_  = False
    _SND_DEBUG_     = False
    _CONTROL_DEBUG_ = False
    _EVALUATION_    = False
    
    DebugFlags = {'global' : _GLOBAL_DEBUG_,
                  'lib'    : _LIB_DEBUG_,
                  'rts'    : _RTS_DEBUG_, 
                  'part'   : _PART_DEBUG_,
                  'exeGen' : _EXEGEN_DEBUG_,
                  'exeMod' : _EXEMOD_DEBUG_,
                  'snd'    : _SND_DEBUG_,
                  'evaluation' : _EVALUATION_ }
    
    @staticmethod
    def InitLogging():
        logFileName = str(datetime.datetime.now()).translate(None, ' :-.')
        logFileName = 'logs/' + logFileName + '.log'
        d = os.path.dirname(logFileName)
        if not os.path.expanduser(d):
            os.makedirs(d)
        logging.basicConfig(filename=logFileName, level=LogUtil._LogLevel_,
                            format='%(asctime)s.%(msecs).3d,%(module)17s,%(funcName)21s,%(lineno)3d,%(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')
        
    @staticmethod
    def DebugLog(section, msg):
        flag = LogUtil.DebugFlags.get(section, False)
        if flag:
            print msg

    @staticmethod
    def EvalLog(eventId, msg):
        logging.debug('{0:6f},{1},{2}'.format(time.time(), eventId, msg))
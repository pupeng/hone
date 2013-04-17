'''
Peng Sun
hone_util.py
util functions for HONE
'''
import inspect
import multiprocessing

_GLOBAL_DEBUG_  = False
_LIB_DEBUG_     = False
_RTS_DEBUG_     = False
_EXEGEN_DEBUG_  = False
_PART_DEBUG_    = False
_EXEMOD_DEBUG_  = False
_SND_DEBUG_     = False

_CONTROL_DEBUG_ = False

_loggingLock = multiprocessing.Lock()

_debugFlags = {'global' : _GLOBAL_DEBUG_,
               'lib'    : _LIB_DEBUG_,
               'rts'    : _RTS_DEBUG_, 
               'part'   : _PART_DEBUG_,
               'exeGen' : _EXEGEN_DEBUG_,
               'exeMod' : _EXEMOD_DEBUG_,
               'snd'    : _SND_DEBUG_ }

_logFileName = None
_logs = []

def debugLog(module, *args):
    flag = _debugFlags.get(module, False)
    if flag:
        _loggingLock.acquire()
        _, fileName, lineNumber, _, _, _ = inspect.stack()[1]
        tmp = fileName.split('/')
        fileName = tmp[len(tmp) - 1]
        print '\nDEBUG ' + fileName + ', L' + str(lineNumber) + ': '
        for i in range(0, len(args)):
            print args[i]
        print '\n'
        _loggingLock.release()

def SetLogFileName(logFileName):
    global _logFileName
    _logFileName = logFileName

def EvalLog(info):
    global _logs
    _logs.append(info)

def WriteLogs():
    global _logFileName
    global _logs
    if _logFileName and _logs:
        output = open(_logFileName, 'a')
        for log in _logs:
            print >>output, log
        output.close()
        del _logs[:]

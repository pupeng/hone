'''
Peng Sun
hone_run.py
Run hone system
usage: python hone_run.py mgmtprog.py
'''

import sys
#import logging
import time
import datetime
import hone_rts
from hone_util import *

#LogLevel = logging.INFO

def main():
    if (len(sys.argv)<2):
        print "Please provide management program"
        sys.exit()
    logFileName = str(datetime.datetime.now()).translate(None, ' :-.')
    logFileName = 'logs/' + logFileName + '.log'
    SetLogFileName(logFileName)
    #logging.basicConfig(filename=logFileName, level=LogLevel, \
    #                    format='%(asctime)s.%(msecs).3d,%(module)17s,%(funcName)21s,%(lineno)3d,%(message)s', \
    #                    datefmt='%m/%d/%Y %H:%M:%S')
    # organize the management programs
    mgmtProg = [hone_rts.HoneHostInfoJob] + sys.argv[1:]
    #debugLog('global', 'mgmt programs', mgmtProg)
    try:
        print 'Hone controller starts'
        #EvalLog('{0:6f},1,controller starts'.format(time.time()))
        hone_rts.RtsRun(mgmtProg)
    except KeyboardInterrupt:
        debugLog('global', 'catch keyboard interrupt')
    finally:
        #EvalLog('{0:6f},2,controller stops'.format(time.time()))
        WriteLogs()
        print 'Hone controller stopped'

if __name__ == '__main__':
    main()

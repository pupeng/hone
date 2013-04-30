'''
Peng Sun
hone_run.py
Run hone system
usage: python hone_run.py mgmtprog.py
'''

import logging
import sys

import hone_rts
from hone_util import LogUtil

def main():
    if (len(sys.argv)<2):
        print "Please provide management program"
        sys.exit()
    # initialize logging
    LogUtil.initLogging()
    # organize the management programs
    mgmtProg = [hone_rts.HoneHostInfoJob] + sys.argv[1:]
    logging.info('Controller takes the following programs: %s', mgmtProg)
    LogUtil.debugLog('global', 'mgmt programs', mgmtProg)
    try:
        print 'Hone controller starts'
        LogUtil.evalLog(1, 'controller starts')
        hone_rts.RtsRun(mgmtProg)
    except KeyboardInterrupt:
        LogUtil.debugLog('global', 'catch keyboard interrupt')
    finally:
        LogUtil.evalLog(2, 'controller stops')
        logging.info('Controller stops.')
        print 'Hone controller stops'

if __name__ == '__main__':
    main()

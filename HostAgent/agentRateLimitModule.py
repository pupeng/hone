# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# agentRateLimitModule
# Rate limit

import socket
from subprocess import call, check_output

from agentUtil import LogUtil

# public
JobAndQueueRate = {}

# private
_tcQueueId = 10
_isInitialized = False
_interface = None
_connsInJob = {}

# public methods
def AddRateLimitJob(jobId, rate):
    if not _isInitialized:
        _initTrafficControl()
    if jobId not in JobAndQueueRate:
        queueId = _nextQueueId()
        JobAndQueueRate[jobId] = (queueId, rate)
        cmd_add_queue(_interface, queueId, rate)

def UpdateRateLimitJob(jobId, rate):
    if jobId not in JobAndQueueRate:
        return
    (queueId, oldRate) = JobAndQueueRate[jobId]
    if rate != oldRate:
        cmd_modify_queue(_interface, queueId, rate)
        JobAndQueueRate[jobId] = (queueId, rate)

def AddConnToJob(connCriterion, jobId):
    app = None
    saddr = None
    sport = None
    daddr = None
    dport = None
    for (key, value) in connCriterion.iteritems():
        if key == 'app':
            app = value
        elif key == 'srcIP':
            saddr = value
        elif key == 'srcPort':
            sport = value
        elif key == 'dstIP':
            daddr = value
        elif key == 'dstPort':
            dport = value
    connKey = str((app, saddr, sport, daddr, dport))
    if connKey not in _connsInJob:
        _connsInJob[connKey] = jobId
        (queueId, _) = JobAndQueueRate[jobId]
        iptablesRule = 'OUTPUT '
        if saddr:
            iptablesRule += '-s {0} '.format(saddr)
        if daddr:
            iptablesRule += '-d {0} '.format(daddr)
        iptablesRule += '-p tcp '
        if sport:
            iptablesRule += '--sport {0} '.format(sport)
        if dport:
            iptablesRule += '--dport {0} '.format(dport)
        #if app:
            #pid = check_output('ps aux | grep '+app+' | head -n 1 | cut -b 10-14', shell=True, executable='/bin/bash')
            #pid = pid.lstrip().rstrip('\n')
            #iptablesRule.append('-m owner --pid-owner '+pid)
        iptablesRule += '-j MARK --set-mark {0}'.format(queueId)
        LogUtil.DebugLog('control', 'new iptables rule', iptablesRule)
        cmd_add_rule(iptablesRule)

# private methods
def _nextQueueId():
    global _tcQueueId
    _tcQueueId += 1
    return _tcQueueId

def _getDataPlaneIp():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('google.com', 0))
    ip = s.getsockname()[0]
    s.close()
    return ip

def _initTrafficControl():
    dataPlaneIp = _getDataPlaneIp()
    global _interface
    _interface = check_output("ifconfig | grep -B1 {0} | head -n1 | awk \'{print $1}\'".format(dataPlaneIp), shell=True, executable='/bin/bash')
    _interface = _interface.split('\n')[0]
    LogUtil.DebugLog('control', 'the data plane interface', _interface)
    cmd_clear_rule(_interface)
    cmd_add_qdisc(_interface)
    cmd_add_root(_interface)
    global _isInitialized
    _isInitialized = True

def cmd_clear_rule(interface):
    call("tc qdisc del dev {0} root &> /dev/null".format(interface), shell=True, executable='/bin/bash')
    call("iptables --flush &> /dev/null", shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_clear_rule')

def cmd_add_qdisc(interface):
    command = "tc qdisc add dev {0} root handle 1: htb".format(interface)
    call(command, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_add_gdisc', command)

def cmd_add_root(interface):
    command = "tc class add dev {0} parent 1: classid 1:fffe htb rate 1000mbps".format(interface)
    call(command, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_add_root', command)

def cmd_add_queue(interface, queueId, rate):
    if rate < 100: # if < 100kbps, just let it go with 100kbps
        rate = 100
    command1 = "tc class add dev {0} parent 1:fffe classid 1:{1} htb rate {2}kbps".format(interface, queueId, rate)
    command2 = "tc filter add dev {0} protocol ip parent 1: prio 0 handle {1} fw flowid 1:{1}".format(interface, queueId)
    call(command1, shell=True, executable='/bin/bash')
    call(command2, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_add_queue', command1, command2)

def cmd_modify_queue(interface, queueId, rate):
    if rate < 100:
        rate = 100
    command = "tc class change dev {0} parent 1:fffe classid 1:{1} htb rate {2}kbps".format(interface, queueId, rate)
    call(command, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_modify_queue', command)

def cmd_add_rule(rule):
    command = "iptables -A {0}".format(rule)
    call(command, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_add_rule', command)

def cmd_del_queue(interface, queueId):
    command = "tc class del dev {0} parent 1:fffe classid 1:{1} &> /dev/null".format(interface, queueId)
    call(command, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_del_queue', command)

def cmd_show_class(interface, className):
    command = "tc -s class show dev {0} | grep {1}".format(interface, className)
    call(command, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_show_class', command)

#########################################################
# Old version. Just for reference
# interface = None
# tcLock = None
# controlPolicy = {}
# iptablesRules = {}
# tcQueueID = 10
# jobAndQueueRate = {}
#
# def controlModuleRun(loggingLock, newControlJobQueue):
#     global tcLock
#     tcLock = loggingLock
#     initTrafficControl()
#     try:
#         while(1):
#             (controlAction, content) = newControlJobQueue.get()
#             if controlAction=='rateLimit':
#                 criteria = content[0]
#                 rate = content[1]
#                 trafficControl(criteria, rate)
#             elif controlAction=='cpuLimit':
#                 print 'Not Implemented Yet '+controlAction
#             elif controlAction=='memoryLimit':
#                 print 'Not Implemented Yet '+controlAction
#             else:
#                 print 'Not Implemented Yet '+controlAction
#     except KeyboardInterrupt:
#         cmd_clear_rule(interface)
#         print 'Exit from agentControlModule'
#
# def trafficControl(criteria, queueID, rate):
#     LogUtil.DebugLog('control', criteria, queueID, rate)
#     global controlPolicy
#     global iptablesRules
#     app = None
#     saddr = None
#     sport = None
#     daddr = None
#     dport = None
#     queueID = str(queueID)
#     rate = str(int(rate))
#     for (key, value) in criteria.iteritems():
#         if key=='app':
#             app = value
#         elif key=='srcIP':
#             saddr = value
#         elif key=='srcPort':
#             sport = str(value)
#         elif key=='dstIP':
#             daddr = value
#         elif key=='dstPort':
#             dport = str(value)
#     policyMatch = (app,saddr,sport,daddr,dport)
#     LogUtil.DebugLog('control', 'policyMatch', policyMatch)
#     if controlPolicy.has_key(policyMatch):
#         #print 'point A'
#         (queueID, oldRate) = controlPolicy[policyMatch]
#         #print 'point B'
#         cmd_modify_queue(interface, queueID, rate)
#         #print 'point C'
#         controlPolicy[policyMatch] = (queueID, rate)
#         #print 'point D'
#     else:
#         #print 'point E'
#         controlPolicy[policyMatch] = (queueID, rate)
#         #print 'point F'
#         #print repr(interface)
#         #print repr(queueID)
#         #print repr(rate)
#         cmd_add_queue(interface, queueID, rate)
#         #print 'point G'
#         iptablesRule = ['OUTPUT']
#         #print 'point H'
#         if saddr:
#             iptablesRule.append('-s '+saddr)
#         if daddr:
#             iptablesRule.append('-d '+daddr)
#         iptablesRule.append('-p tcp')
#         if sport:
#             iptablesRule.append('--sport '+sport)
#         if dport:
#             iptablesRule.append('--dport '+dport)
#         #if app:
#             #pid = check_output('ps aux | grep '+app+' | head -n 1 | cut -b 10-14', shell=True, executable='/bin/bash')
#             #pid = pid.lstrip().rstrip('\n')
#             #iptablesRule.append('-m owner --pid-owner '+pid)
#         iptablesRule.append('-j MARK --set-mark '+queueID)
#         iptablesRule = ' '.join(iptablesRule)
#         LogUtil.DebugLog('control', iptablesRule)
#         iptablesRules[policyMatch] = iptablesRule
#         cmd_add_rule(iptablesRule)
#
# if __name__=='__main__':
#     initTrafficControl()
#     action = 'rateLimit'
#     criteria = {'dport': '5000',
#                 'app': 'sshd'}
#     rate = 100
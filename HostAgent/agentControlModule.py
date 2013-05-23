'''
Peng Sun
agentControlModule
Traffic control mechanism
'''

import fnmatch
from subprocess import call, check_output
from multiprocessing import Queue, Lock

from agentUtil import LogUtil

interface = None
tcLock = None
controlPolicy = {}
iptablesRules = {}
tcQueueID = 10
jobAndQueueRate = {}

def addNewControlJob(jobID,rate):
    queueID = nextQueueID()
    jobAndQueueRate[jobID] = (queueID, rate)

def nextQueueID():
    global tcQueueID
    tcQueueID += 1
    return tcQueueID

def initTrafficControl():
    global interface
    interface = check_output("ifconfig | grep -B1 192.168.17 | head -n1 | awk \'{print $1}\'",shell=True,executable='/bin/bash')
    interface = interface.split('\n')[0]
    LogUtil.DebugLog('control', 'the interface', repr(interface))
    cmd_clear_rule(interface)
    cmd_add_qdisc(interface)
    cmd_add_root(interface)
    
def cmd_add_qdisc(interface):
    call("/sbin/tc qdisc add dev "+interface+" root handle 1: htb", shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_add_gdisc')

def cmd_add_root(interface):
    call("/sbin/tc class add dev "+interface+" parent 1: classid 1:fffe htb rate 1000mbps", shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_add_root')

def cmd_add_queue(interface, queueID, rate):
    #print "/sbin/tc class add dev "+interface+" parent 1:fffe classid 1:"+queueID+" htb rate "+rate+"kbps"
    rate = int(rate)
    if rate<1000:
        rate = 1000
    call("/sbin/tc class add dev "+interface+" parent 1:fffe classid 1:"+queueID+" htb rate "+str(rate)+"kbps", shell=True, executable='/bin/bash')
    call("/sbin/tc filter add dev "+interface+" protocol ip parent 1: prio 0 handle "+queueID+" fw flowid 1:"+queueID, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_add_queue')

def cmd_add_rule(rule):
    call("iptables -A "+rule, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_add_rule')

def cmd_modify_queue(interface, queueID, rate):
    rate = int(rate)
    if rate<1000:
        rate = 1000
    call("/sbin/tc class change dev "+interface+" parent 1:fffe classid 1:"+str(queueID)+" htb rate "+str(rate)+"kbps", shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_modify_queue')

def cmd_del_queue(interface, queueID):
    call("/sbin/tc class del dev "+interface+" parent 1:fffe classid 1:"+str(queueID)+" > /dev/null", shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_del_queue')
    
def cmd_show_class(interface, className):
    call("/sbin/tc -s class show dev "+interface+" | grep "+className, shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_show_class')
    
def cmd_clear_rule(interface):
    call("/sbin/tc qdisc del dev "+interface+" root &> /dev/null", shell=True, executable='/bin/bash')
    call("/sbin/iptables --flush &> /dev/null", shell=True, executable='/bin/bash')
    LogUtil.DebugLog('control', 'done cmd_clear_rule')

def controlModuleRun(loggingLock, newControlJobQueue):
    global tcLock
    tcLock = loggingLock
    initTrafficControl()
    try:
        while(1):
            (controlAction, content) = newControlJobQueue.get()
            if controlAction=='rateLimit':
                criteria = content[0]
                rate = content[1]
                trafficControl(criteria, rate)
            elif controlAction=='cpuLimit':
                print 'Not Implemented Yet '+controlAction
            elif controlAction=='memoryLimit':
                print 'Not Implemented Yet '+controlAction
            else:
                print 'Not Implemented Yet '+controlAction
    except KeyboardInterrupt:
        cmd_clear_rule(interface)
        print 'Exit from agentControlModule'
            
def trafficControl(criteria, queueID, rate):
    LogUtil.DebugLog('control', criteria, queueID, rate)
    global controlPolicy
    global iptablesRules
    app = None
    saddr = None
    sport = None
    daddr = None
    dport = None
    queueID = str(queueID)
    rate = str(int(rate))
    for (key, value) in criteria.iteritems():
        if key=='app':
            app = value
        elif key=='srcIP':
            saddr = value
        elif key=='srcPort':
            sport = str(value)
        elif key=='dstIP':
            daddr = value
        elif key=='dstPort':
            dport = str(value)
    policyMatch = (app,saddr,sport,daddr,dport)
    LogUtil.DebugLog('control', 'policyMatch', policyMatch)
    if controlPolicy.has_key(policyMatch):
        #print 'point A'
        (queueID, oldRate) = controlPolicy[policyMatch]
        #print 'point B'
        cmd_modify_queue(interface, queueID, rate)
        #print 'point C'
        controlPolicy[policyMatch] = (queueID, rate)
        #print 'point D'
    else:
        #print 'point E'
        controlPolicy[policyMatch] = (queueID, rate)
        #print 'point F'
        #print repr(interface)
        #print repr(queueID)
        #print repr(rate)
        cmd_add_queue(interface, queueID, rate)
        #print 'point G'
        iptablesRule = ['OUTPUT']
        #print 'point H'
        if saddr:
            iptablesRule.append('-s '+saddr)
        if daddr:
            iptablesRule.append('-d '+daddr)
        iptablesRule.append('-p tcp')
        if sport:
            iptablesRule.append('--sport '+sport)
        if dport:
            iptablesRule.append('--dport '+dport)
        #if app:
            #pid = check_output('ps aux | grep '+app+' | head -n 1 | cut -b 10-14', shell=True, executable='/bin/bash')
            #pid = pid.lstrip().rstrip('\n')
            #iptablesRule.append('-m owner --pid-owner '+pid)
        iptablesRule.append('-j MARK --set-mark '+queueID)
        iptablesRule = ' '.join(iptablesRule)
        LogUtil.DebugLog('control', iptablesRule)
        iptablesRules[policyMatch] = iptablesRule
        cmd_add_rule(iptablesRule)  
        
if __name__=='__main__':
    initTrafficControl()
    action = 'rateLimit'
    criteria = {'dport': '5000',
                'app': 'sshd'}
    rate = 100
    
    
    
    
    
    
    
    
    
    
    

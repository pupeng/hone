# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# agentConnMeasure.py
# Measure connection-level statistics (TCP, via web10G)

import time
import traceback
from threading import Thread
from uuid import getnode as get_mac

import agentManager
import agent_web10g_measure as web10g
from agentUtil import *

web10g_types_dict = {
'ThruOctetsReceived' : 'ThruOctetsReceived',
'Rwnd' : 'CurRwinRcvd',
'OtherReductionsCM' : 'OtherReductionsCM',
'RTT' : 'SampleRTT',
'OtherReductionsCV' : 'OtherReductionsCV',
'BytesRetran' : 'OctetsRetrans',
'MaxRTT' : 'MaxRTT',
'SoftErrorReason' : 'SoftErrorReason',
'SndLimTimeSnd' : 'SndLimTimeSnd',
'MaxRTO' : 'MaxRTO',
'Rcvbuf' : 'Rcvbuf',
'SndLimTransRwin' : 'SndLimTransRwin',
'NonRecovDAEpisodes' : 'NonRecovDAEpisodes',
'IpTtl' : 'IpTtl',
'CongSignals' : 'CongSignals',
'DataSegsOut' : 'DataSegsOut',
'LimRwin' : 'LimRwin',
'MSSRcvd' : 'MSSRcvd',
'WinScaleSent' : 'WinScaleSent',
'ZeroRwinRcvd' : 'ZeroRwinRcvd',
'StartTimeSecs' : 'StartTimeSecs',
'AbruptTimeouts' : 'AbruptTimeouts',
'NonRecovDA' : 'NonRecovDA',
'ECNsignals' : 'ECNsignals',
'Sndbuf' : 'Sndbuf',
'SpuriousFrDetected' : 'SpuriousFrDetected',
'IpTosOut' : 'IpTosOut',
'State' : 'State',
'Nagle' : 'Nagle',
'CurReasmQueue' : 'CurReasmQueue',
'ECESent' : 'ECESent',
'DupAcksOut' : 'DupAcksOut',
'SndLimTimeCwnd' : 'SndLimTimeCwnd',
'srcPort' : 'LocalPort',
'CurSsthresh' : 'CurSsthresh',
'MSSSent' : 'MSSSent',
'SpuriousRtoDetected' : 'SpuriousRtoDetected',
'CurAppRQueue' : 'CurAppRQueue',
'DupAcksIn' : 'DupAcksIn',
'LimCwnd' : 'LimCwnd',
'TimeStamps' : 'TimeStamps',
'MinSsthresh' : 'MinSsthresh',
'RcvRTT' : 'RcvRTT',
'SACKsRcvd' : 'SACKsRcvd',
'SendStall' : 'SendStall',
'MaxMSS' : 'MaxMSS',
'SndLimTimeRwin' : 'SndLimTimeRwin',
'SegsIn' : 'SegsIn',
'RTTVar' : 'RTTVar',
'SndLimTransCwnd' : 'SndLimTransCwnd',
'CurAppWQueue' : 'CurAppWQueue',
'OtherReductions' : 'OtherReductions',
'IpTosIn' : 'IpTosIn',
'SndInitial' : 'SndInitial',
'MaxPipeSize' : 'MaxPipeSize',
'WinScaleRcvd' : 'WinScaleRcvd',
'PreCongSumCwnd' : 'PreCongSumCwnd',
'InRecovery' : 'InRecovery',
'RetranThresh' : 'RetranThresh',
'SubsequentTimeouts' : 'SubsequentTimeouts',
'PreCongSumRTT' : 'PreCongSumRTT',
'ElapsedMicroSecs' : 'ElapsedMicroSecs',
'LocalAddressType' : 'LocalAddressType',
'DSACKDups' : 'DSACKDups',
'MinRTO' : 'MinRTO',
'MinMSS' : 'MinMSS',
'WillSendSACK' : 'WillSendSACK',
'ECN' : 'ECN',
'MaxSsthresh' : 'MaxSsthresh',
'PipeSize' : 'PipeSize',
'SumOctetsReordered' : 'SumOctetsReordered',
'MinRTT' : 'MinRTT',
'MaxCaCwnd' : 'MaxCaCwnd',
'SumRTT' : 'SumRTT',
'PostCongSumRTT' : 'PostCongSumRTT',
'RecInitial' : 'RecInitial',
'DupAckEpisodes' : 'DupAckEpisodes',
'SACKBlocksRcvd' : 'SACKBlocksRcvd',
'WillUseSACK' : 'WillUseSACK',
'srcIP' : 'LocalAddress',
'ThruOctetsAcked' : 'ThruOctetsAcked',
'MaxRwinSent' : 'MaxRwinSent',
'MaxRwinRcvd' : 'MaxRwinRcvd',
'SlowStart' : 'SlowStart',
'MaxSsCwnd' : 'MaxSsCwnd',
'SegsRetrans' : 'SegsRetrans',
'CongOverCount' : 'CongOverCount',
'LimMSS' : 'LimMSS',
'CurRTO' : 'CurRTO',
'CERcvd' : 'CERcvd',
'ElapsedSecs' : 'ElapsedSecs',
'CurRetxQueue' : 'CurRetxQueue',
'MaxAppRQueue' : 'MaxAppRQueue',
'Cwnd' : 'CurCwnd',
'SoftErrors' : 'SoftErrors',
'SndLimTransSnd' : 'SndLimTransSnd',
'CountRTT' : 'CountRTT',
'PostCongCountRTT' : 'PostCongCountRTT',
'BytesSentOut' : 'DataOctetsOut',
'StartTimeMicroSecs' : 'StartTimeMicroSecs',
'SmoothedRTT' : 'SmoothedRTT',
'RcvNxt' : 'RcvNxt',
'dstPort' : 'RemPort',
'CongAvoid' : 'CongAvoid',
'ZeroRwinSent' : 'ZeroRwinSent',
'Timeouts' : 'Timeouts',
'dstIP' : 'RemAddress',
'SndMax' : 'SndMax',
'SegmentsOut' : 'SegsOut',
'SndUna' : 'SndUna',
'MaxRetxQueue' : 'MaxRetxQueue',
'CurRwinSent' : 'CurRwinSent',
'FastRetran' : 'FastRetran',
'BytesReceived' : 'DataOctetsIn',
'LimSsthresh' : 'LimSsthresh',
'SndNxt' : 'SndNxt',
'RemAddressType' : 'RemAddressType',
'ActiveOpen' : 'ActiveOpen',
'CurTimeoutCount' : 'CurTimeoutCount',
'MaxAppWQueue' : 'MaxAppWQueue',
'MaxReasmQueue' : 'MaxReasmQueue',
'DataSegsIn' : 'DataSegsIn',
'CurMSS' : 'CurMSS'
}

web10g_var_location = {
    'LocalAddressType' : 0,
    'LocalAddress' : 1,
    'LocalPort' : 2,
    'RemAddressType' : 3,
    'RemAddress' : 4,
    'RemPort' : 5,
    'SegsOut' : 6,
    'DataSegsOut' : 7,
    'DataOctetsOut' : 8,
    'SegsRetrans' : 9,
    'OctetsRetrans' : 10,
    'SegsIn' : 11,
    'DataSegsIn' : 12,
    'DataOctetsIn' : 13,
    'ElapsedSecs' : 14,
    'ElapsedMicroSecs' : 15,
    'CurMSS' : 16,
    'PipeSize' : 17,
    'MaxPipeSize' : 18,
    'SmoothedRTT' : 19,
    'CurRTO' : 20,
    'CongSignals' : 21,
    'CurCwnd' : 22,
    'CurSsthresh' : 23,
    'Timeouts' : 24,
    'CurRwinSent' : 25,
    'MaxRwinSent' : 26,
    'ZeroRwinSent' : 27,
    'CurRwinRcvd' : 28,
    'MaxRwinRcvd' : 29,
    'ZeroRwinRcvd' : 30,
    'SndLimTransRwin' : 31,
    'SndLimTransCwnd' : 32,
    'SndLimTransSnd' : 33,
    'SndLimTimeRwin' : 34,
    'SndLimTimeCwnd' : 35,
    'SndLimTimeSnd' : 36,
    'SendStall' : 37,
    'RetranThresh' : 38,
    'NonRecovDAEpisodes' : 39,
    'SumOctetsReordered' : 40,
    'NonRecovDA' : 41,
    'SampleRTT' : 42,
    'RTTVar' : 43,
    'MaxRTT' : 44,
    'MinRTT' : 45,
    'SumRTT' : 46,
    'CountRTT' : 47,
    'MaxRTO' : 48,
    'MinRTO' : 49,
    'IpTtl' : 50,
    'IpTosIn' : 51,
    'IpTosOut' : 52,
    'PreCongSumCwnd' : 53,
    'PreCongSumRTT' : 54,
    'PostCongSumRTT' : 55,
    'PostCongCountRTT' : 56,
    'ECNsignals' : 57,
    'DupAckEpisodes' : 58,
    'RcvRTT' : 59,
    'DupAcksOut' : 60,
    'CERcvd' : 61,
    'ECESent' : 62,
    'ActiveOpen' : 63,
    'MSSSent' : 64,
    'MSSRcvd' : 65,
    'WinScaleSent' : 66,
    'WinScaleRcvd' : 67,
    'TimeStamps' : 68,
    'ECN' : 69,
    'WillSendSACK' : 70,
    'WillUseSACK' : 71,
    'State' : 72,
    'Nagle' : 73,
    'MaxSsCwnd' : 74,
    'MaxCaCwnd' : 75,
    'MaxSsthresh' : 76,
    'MinSsthresh' : 77,
    'InRecovery' : 78,
    'DupAcksIn' : 79,
    'SpuriousFrDetected' : 80,
    'SpuriousRtoDetected' : 81,
    'SoftErrors' : 82,
    'SoftErrorReason' : 83,
    'SlowStart' : 84,
    'CongAvoid' : 85,
    'OtherReductions' : 86,
    'CongOverCount' : 87,
    'FastRetran' : 88,
    'SubsequentTimeouts' : 89,
    'CurTimeoutCount' : 90,
    'AbruptTimeouts' : 91,
    'SACKsRcvd' : 92,
    'SACKBlocksRcvd' : 93,
    'DSACKDups' : 94,
    'MaxMSS' : 95,
    'MinMSS' : 96,
    'SndInitial' : 97,
    'RecInitial' : 98,
    'CurRetxQueue' : 99,
    'MaxRetxQueue' : 100,
    'CurReasmQueue' : 101,
    'MaxReasmQueue' : 102,
    'SndUna' : 103,
    'SndNxt' : 104,
    'SndMax' : 105,
    'ThruOctetsAcked' : 106,
    'RcvNxt' : 107,
    'ThruOctetsReceived' : 108,
    'CurAppWQueue' : 109,
    'MaxAppWQueue' : 110,
    'CurAppRQueue' : 111,
    'MaxAppRQueue' : 112,
    'LimCwnd' : 113,
    'LimSsthresh' : 114,
    'LimRwin' : 115,
    'LimMSS' : 116,
    'OtherReductionsCV' : 117,
    'OtherReductionsCM' : 118,
    'StartTimeSecs' : 119,
    'StartTimeMicroSecs' : 120,
    'Sndbuf' : 121,
    'Rcvbuf' : 122
}

web10g_string_type_var = [1, 4]

def connMeasureRun(jobFlowToM, nothing):
    #debugLog('conn', 'job flow to measure in conn:', jobFlowToM)
    connMeasureTimestamp = 'Begin${0:6f}'.format(time.time())
    #EvalLog('{0:6f},91,start connMeasure of jobFlows: {1}'.format(time.time(), jobFlowToM))
    skToMByCid = web10g.IntStringDict()
    skToMByTuple = web10g.StringStringDict()
    skWithJobFlow = {}
    statsToM = web10g.IntList()
    statsToMPy = {}
    for jobFlow in jobFlowToM:
        #debugLog('conn', 'jobFlow: ', jobFlow, 'sk list:', agentManager.sourceJobSkList[jobFlow])
        if jobFlow in agentManager.sourceJobSkList:
            for sockfd in agentManager.sourceJobSkList[jobFlow]:
                sk = agentManager.socketTable[sockfd]
                if sk.cid:
                    skToMByCid[sk.cid] = sk.sockfd
                else:
                    theTuple = sk.GetTuple()
                    skToMByTuple[theTuple] = sk.sockfd
                if sockfd not in skWithJobFlow:
                    skWithJobFlow[sockfd] = []
                skWithJobFlow[sockfd].append(jobFlow)
            sourceJob = agentManager.sourceJobTable[jobFlow]
            for name in sourceJob.measureStats:
                if name in web10g_types_dict:
                    statsToM.append(web10g_var_location[web10g_types_dict[name]])
                    statsToMPy[web10g_var_location[web10g_types_dict[name]]] = None
    #debugLog('conn', 'skToMByCid: ', skToMByCid, 'skToMByTuple:', skToMByTuple)
    # take snapshot via web10G
    statsToMPy = sorted(statsToMPy.keys())
    connMeasureTimestamp += '#DoneFindSk${0:6f}${1}'.format(time.time(), (skToMByCid.size() + skToMByTuple.size()))
    agentManager.evalTimestamp += '#DoneFindSk${0:6f}${1}'.format(time.time(), (skToMByCid.size() + skToMByTuple.size()))
    if IsLazyTableEnabled():
        if skToMByCid.size() or skToMByTuple.size():
            skSnapshot = web10g.measure(skToMByCid, skToMByTuple, statsToM)
        else:
            skSnapshot = {}
    else:
        skSnapshot = web10g.measure(skToMByCid, skToMByTuple, statsToM)
        #EvalLog('{0:6f},109,no lazy m: number of sockets for measurement: {1}'.format(time.time(), len(skSnapshot.keys())))
    connMeasureTimestamp += '#DoneWeb10GMeasure${0:6f}${1}'.format(time.time(), (skToMByCid.size() + skToMByTuple.size()))
    agentManager.evalTimestamp += '#DoneWeb10GMeasure${0:6f}${1}'.format(time.time(), (skToMByCid.size() + skToMByTuple.size()))
    # generate measure results for runJobs
    sockStats = {}
    for jobFlow in jobFlowToM:
        #EvalLog('{0:6f},115,start job data {1}'.format(time.time(), jobFlow))
        measureResults = []
        sourceJob = agentManager.sourceJobTable[jobFlow]
        for sockfd in agentManager.sourceJobSkList[jobFlow]:
            if (sockfd in skSnapshot):
                if sockfd not in sockStats:
                    sockStats[sockfd] = {}
                    data = skSnapshot[sockfd].split('#')
                    agentManager.socketTable[sockfd].setCid(int(data[0]))
                    for i in range(len(statsToMPy)):
                        if statsToMPy[i] in web10g_string_type_var:
                            sockStats[sockfd][statsToMPy[i]] = str(data[i+1])
                        else:
                            sockStats[sockfd][statsToMPy[i]] = int(data[i+1])
                #debugLog('conn', 'got snapshot: ', sockfd, \
                #                 'current time:', time.time(), \
                #                 'snapshot time:', (snapshot['StartTimeSecs']+snapshot['ElapsedSecs']+float(snapshot['StartTimeMicroSecs'])/1000000.0+float(snapshot['ElapsedMicroSecs'])/1000000.0))
                result = []
                for name in sourceJob.measureStats:
                    if name == 'BytesWritten':
                        result.append(agentManager.socketTable[sockfd].bytesWritten)
                    elif name == 'app':
                        result.append(agentManager.socketTable[sockfd].app)
                    elif name == 'srcHost':
                        result.append(str(get_mac()))
                    elif name == 'CurrentTime':
                        result.append(time.time())
                    #elif name == 'all':
                    #    for value in snapshot.itervalues():
                    #        result.append(value)
                    else:
                        result.append(sockStats[sockfd][web10g_var_location[web10g_types_dict[name]]])
                measureResults.append(result)
        #EvalLog('{0:6f},116,done job data {1}'.format(time.time(), jobFlow))
        if measureResults:
            (jobId, flowId) = decomposeKey(jobFlow)
            (_, goFunc) = agentManager.eventAndGoFunc[jobId][flowId]
            goThread = Thread(target=runGo, args=(goFunc, measureResults, jobId, flowId))
            goThread.daemon = True
            goThread.start()
    #evalTime += '#{0:6f}'.format(time.time())
    #EvalLog('{0:6f},96,done one round of conn measurement for jobFlows {1}'.format(time.time(), jobFlowToM))
    connMeasureTimestamp += '#DoneOneRoundConnMeasure${0:6f}'.format(time.time())
    agentManager.measureLatency += '#DoneOneRoundConnMeasure${0:6f}'.format(time.time())
    LogUtil.EvalLog('OneRoundOfConnMeasure', connMeasureTimestamp)

def runGo(goFunc, data, jobId, flowId):
    agentManager.evalTimestamp += '#StartRunGoOfJobFlow${0:6f}${1}${2}'.format(time.time(), jobId, flowId)
    try:
        #EvalLog('{0:6f},94,start go function for jobId {1} flowId {2}'.format(time.time(), jobId, flowId))
        goFunc(data)
        #evalTime += '#{0:6f}'.format(time.time())
    except Exception, msg:
        logging.warning('go thread of jobId {0} flowId {1} caught exception: {2}'.format(jobId, flowId, msg))
        print 'go thread caught exception'
        print msg
        traceback.print_exc()
    finally:
        #EvalLog('{0:6f},95,done go function for jobId {1} flowId {2}'.format(time.time(), jobId, flowId))
        #evalTime += '#{0:6f}'.format(time.time())
        #EvalLog('{0:6f},118,{1}'.format(time.time(), evalTime))
        #WriteLogs()
        LogUtil.OutputEvalLog()

if __name__ == '__main__':
    for key in web10g_types_dict.iterkeys():
        print '\'{0}\','.format(key)

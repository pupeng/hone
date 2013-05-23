'''
HONE application
Author: Peng Sun
Purpose:
debug
'''

from hone_lib import *

def query1():
    q = (Select(['hostId','app','cpu','memory'])*
         From('AppStatus')*
         Every(1000))
    return q

def query2():
    q = (Select(['app', 
                 'ThruOctetsReceived',
                 'OtherReductionsCM',
                 'OtherReductionsCV',
                 'MaxRTT',
                 'SoftErrorReason',
                 'SndLimTimeSnd',
                 'MaxRTO',
                 'Rcvbuf',
                 'RetranThresh',
                 'SegmentsOut',
                 'NonRecovDAEpisodes',
                 'IpTtl',
                 'CongSignals',
                 'BytesSentOut',
                 'DataSegsOut',
                 'srcIP',
                 'LimRwin',
                 'dstPort',
                 'MSSRcvd',
                 'WinScaleSent',
                 'ZeroRwinRcvd',
                 'StartTimeSecs',
                 'AbruptTimeouts',
                 'NonRecovDA',
                 'WinScaleRcvd',
                 'Sndbuf',
                 'SpuriousFrDetected',
                 'IpTosOut',
                 'State',
                 'Nagle',
                 'CurReasmQueue',
                 'ECESent',
                 'DupAcksOut',
                 'SndLimTimeCwnd',
                 'MaxReasmQueue',
                 'CurSsthresh',
                 'MSSSent',
                 'SpuriousRtoDetected',
                 'CurAppRQueue',
                 'RTT',
                 'DupAcksIn',
                 'MaxMSS',
                 'LimCwnd',
                 'TimeStamps',
                 'MinSsthresh',
                 'RcvRTT',
                 'BytesReceived',
                 'CurAppWQueue',
                 'SendStall',
                 'SACKsRcvd',
                 'SndLimTimeRwin',
                 'SegsIn',
                 'RTTVar',
                 'SndLimTransCwnd',
                 'MaxRwinSent',
                 'IpTosIn',
                 'SndInitial',
                 'MaxPipeSize',
                 'ECNsignals',
                 'PreCongSumCwnd',
                 'InRecovery',
                 'SndLimTransRwin',
                 'SubsequentTimeouts',
                 'MaxAppRQueue',
                 'ElapsedMicroSecs',
                 'LocalAddressType',
                 'DSACKDups',
                 'BytesRetran',
                 'MinRTO',
                 'MinMSS',
                 'WillSendSACK',
                 'ECN',
                 'MaxSsthresh',
                 'PipeSize',
                 'SumOctetsReordered',
                 'MinRTT',
                 'MaxCaCwnd',
                 'SumRTT',
                 'PostCongSumRTT',
                 'RecInitial',
                 'DupAckEpisodes',
                 'SACKBlocksRcvd',
                 'WillUseSACK',
                 'ThruOctetsAcked',
                 'OtherReductions',
                 'MaxRwinRcvd',
                 'SlowStart',
                 'MaxSsCwnd',
                 'SegsRetrans',
                 'CongOverCount',
                 'LimMSS',
                 'CurRTO',
                 'CERcvd',
                 'CountRTT',
                 'CurRetxQueue',
                 'dstIP',
                 'PreCongSumRTT',
                 'SoftErrors',
                 'srcPort',
                 'SndLimTransSnd',
                 'ElapsedSecs',
                 'PostCongCountRTT',
                 'ActiveOpen',
                 'StartTimeMicroSecs',
                 'SmoothedRTT',
                 'Rwnd',
                 'CongAvoid',
                 'ZeroRwinSent',
                 'Timeouts',
                 'SndMax',
                 'SndUna',
                 'MaxRetxQueue',
                 'CurRwinSent',
                 'FastRetran',
                 'LimSsthresh',
                 'SndNxt',
                 'RemAddressType',
                 'Cwnd',
                 'CurTimeoutCount',
                 'MaxAppWQueue',
                 'DataSegsIn',
                 'RcvNxt',
                 'CurMSS']) *
         From('HostConnection') *
         Every(1000))
    return q

def query3():
    q = (Select(['hostId','totalCPU','totalMemory']) *
         From('HostStatus') *
         Every(1000))
    return q


def GetLen(x):
    print 'Data length: {0}'.format(len(x))
    if x:
        print x[0]
    return x

def main():
    stream1 = query1() >> MapStreamSet(GetLen)
    stream2 = query2() >> MapStreamSet(GetLen)
    stream3 = query3() >> MapStreamSet(GetLen)
    return MergeStreamsForSet(MergeStreamsForSet(stream1, stream2), stream3)


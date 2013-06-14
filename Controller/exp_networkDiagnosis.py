# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# find flows with low congestion window size, and find the shared network links among those flows as the bottleneck links

from hone_lib import *

def ConnQuery():
    return (Select(['srcIP', 'dstIP', 'Cwnd']) *
            From('HostConnection') *
            # Where([('Cwnd', '<', '100')]) *
            Every(5000))

def RouteQuery():
    return (Select(['HostAId', 'HostBId', 'Path']) *
            From('Route') *
            Every(5000))

def HostInfoQuery():
    return (Select(['hostId', 'IP']) *
            From('HostStatus') *
            Every(5000))

def FormatHostInfoToDict(x):
    result = {}
    for (hostId, ip) in x:
        result[hostId] = ip
    return result

def JoinTables(x):
    (routes, hostInfo) = x
    routes = routes[0]
    result = {}
    for (hostAId, hostBId, path) in routes:
        hostAIP = hostInfo[hostAId]
        hostBIP = hostInfo[hostBId]
        result[(hostAIP, hostBIP)] = path
        # path is a list of dictionaries, in which key 'switch' is the switch ID, and key 'port' is the interface number
    return result

def FormatRouteToAString(path):
    result = ''
    for switchInfo in path:
        result += '{0},{1}#'.format(switchInfo['switch'], switchInfo['port'])
    return result

def FindLongestCommonSubstring(data):
    substr = ''
    if len(data) > 1 and len(data[0]) > 0:
        for i in range(len(data[0])):
            for j in range(len(data[0]) - i + 1):
                if j > len(substr) and all(data[0][i:i+j] in x for x in data):
                    substr = data[0][i:i+j]
    return substr

def IdentifySharedLinks(x):
    (ipPairRoutes, faultyFlowsFromHosts) = x
    problemLinks = []
    for flowOnEachHost in faultyFlowsFromHosts:
        for (src, dst, _) in flowOnEachHost:
            problemLinks.append(ipPairRoutes[(src, dst)])
    problemLinks = map(FormatRouteToAString, problemLinks)
    bottleneckLinks = FindLongestCommonSubstring(problemLinks)
    return bottleneckLinks

def main():
    hostInfoStream = HostInfoQuery() >> MergeHosts() >> MapStream(FormatHostInfoToDict)
    ipPairRouteStream = MergeStreams(RouteQuery(), hostInfoStream) >> MapStream(JoinTables)
    faultyFlowsStream = ConnQuery() >> MergeHosts()
    stream = MergeStreams(ipPairRouteStream, faultyFlowsStream) >> MapStream(IdentifySharedLinks) >> Print()
    return stream




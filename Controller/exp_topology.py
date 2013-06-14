# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# HONE application
# Discover link up and down in the network, and build the current topology as a graph structure

from hone_lib import *

class ChangeList(object):
    def __init__(self):
        self.add = []
        self.delete = []

class Node(object):
    def __init__(self, id):
        self.deviceId = id
        self.links = {}

    def addLink(self, nodeId, port):
        self.links[nodeId] = port

    def removeLink(self, nodeId):
        if nodeId in self.links:
            del self.links[nodeId]

allNodes = {}

def LinkQuery():
    return (Select(['BeginDevice', 'BeginPort', 'EndDevice', 'EndPort']) *
            From('LinkStatus') *
            Every(2000))

def DiscoverLinkChanges(newListOfLinks, state):
    newListOfLinks = newListOfLinks[0]
    newListOfLinks = map(lambda x : '{0}#{1}#{2}#{3}'.format(x[0], x[1], x[2], x[3]), newListOfLinks)
    (changeList, oldListOfLinks) = state
    changeList.add = list(set(newListOfLinks) - set(oldListOfLinks))
    changeList.delete = list(set(oldListOfLinks) - set(newListOfLinks))
    return (changeList, newListOfLinks)

def GetChanges(state):
    return state[0]

def BuildTopology(linkChanges):
    newLinks = map(lambda x : x.split('#'), linkChanges.add)
    for newLink in newLinks:
        (deviceA, portA, deviceB, portB) = newLink
        if deviceA not in allNodes:
            allNodes[deviceA] = Node(deviceA)
        if deviceB not in allNodes:
            allNodes[deviceB] = Node(deviceB)
        allNodes[deviceA].addLink(deviceB, portA)
        allNodes[deviceB].addLink(deviceA, portB)
    deleteLinks = map(lambda x : x.split('#'), linkChanges.delete)
    for deleteLink in deleteLinks:
        (deviceA, portA, deviceB, portB) = deleteLink
        if deviceA in allNodes:
            allNodes[deviceA].removeLink(deviceB)
        if deviceB in allNodes:
            allNodes[deviceB].removeLink(deviceA)
    return allNodes

def main():
    stream = LinkQuery() >> ReduceStream(DiscoverLinkChanges, (ChangeList(), [])) >> MapStream(GetChanges)
    stream = stream >> MapStream(BuildTopology)
    stream = stream >> Print()
    return stream

# class StaticFlowPusher(object):
#
#     def __init__(self, server):
#         self.server = server
#
#     def get(self, data):
#         ret = self.rest_call({}, 'GET')
#         return json.loads(ret[2])
#
#     def set(self, data):
#         ret = self.rest_call(data, 'POST')
#         return ret[0] == 200
#
#     def remove(self, objtype, data):
#         ret = self.rest_call(data, 'DELETE')
#         return ret[0] == 200
#
#     def rest_call(self, data, action):
#         path = '/wm/staticflowentrypusher/json'
#         headers = {
#             'Content-type': 'application/json',
#             'Accept': 'application/json',
#             }
#         body = json.dumps(data)
#         conn = httplib.HTTPConnection(self.server, 8080)
#         conn.request(action, path, body, headers)
#         response = conn.getresponse()
#         ret = (response.status, response.reason, response.read())
#         print ret
#         conn.close()
#         return ret
#
# pusher = StaticFlowPusher('localhost')
#
# def SetStaticFlows():
#     flow1 = {
#     'switch':"00:00:00:10:18:56:ab:6a",
#     "name":"flow-mod-1",
#     "cookie":"0",
#     "priority":"32768",
#     "ingress-port":"1",
#     "active":"true",
#     "actions":"output=flood"
#     }
#     flow2 =	{
#     'switch':"00:00:00:10:18:56:ab:6a",
#     "name":"flow-mod-2",
#     "cookie":"0",
#     "priority":"32768",
#     "ingress-port":"2",
#     "active":"true",
#     "actions":"output=flood"
#     }
#     flow3 =	{
#     'switch':"00:00:00:10:18:56:ab:6a",
#     "name":"flow-mod-3",
#     "cookie":"0",
#     "priority":"32768",
#     "ingress-port":"3",
#     "active":"true",
#     "actions":"output=flood"
#     }
#     flow4 =	{
#     'switch':"00:00:00:10:18:56:90:b4",
#     "name":"flow-mod-4",
#     "cookie":"0",
#     "priority":"32768",
#     "ingress-port":"1",
#     "active":"true",
#     "actions":"output=flood"
#     }
#     flow5 =	{
#     'switch':"00:00:00:10:18:56:90:b4",
#     "name":"flow-mod-5",
#     "cookie":"0",
#     "priority":"32768",
#     "ingress-port":"2",
#     "active":"true",
#     "actions":"output=flood"
#     }
#     flow6 =	{
#     'switch':"00:00:00:10:18:56:90:b4",
#     "name":"flow-mod-6",
#     "cookie":"0",
#     "priority":"32768",
#     "ingress-port":"3",
#     "active":"true",
#     "actions":"output=flood"
#     }
#     flow7 =	{
#     'switch':"00:00:00:10:18:56:ab:54",
#     "name":"flow-mod-7",
#     "cookie":"0",
#     "priority":"32768",
#     "ingress-port":"1",
#     "active":"true",
#     "actions":"output=flood"
#     }
#     flow8 =	{
#     'switch':"00:00:00:10:18:56:ab:54",
#     "name":"flow-mod-8",
#     "cookie":"0",
#     "priority":"32768",
#     "ingress-port":"2",
#     "active":"true",
#     "actions":"output=flood"
#     }
#     pusher.set(flow1)
#     pusher.set(flow2)
#     pusher.set(flow3)
#     pusher.set(flow4)
#     pusher.set(flow5)
#     pusher.set(flow6)
#     pusher.set(flow7)
#     pusher.set(flow8)


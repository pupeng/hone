from subprocess import check_output
import json
import os

def GetLinks():
    links = []
    result = check_output('curl -s http://localhost:8080/wm/topology/links/json', \
                         shell=True, executable='/bin/bash')
    switchLinks = json.loads(result)
    for link in switchLinks:
        sswitch = str(link['src-switch'])
        sport = link['src-port']
        dswitch = str(link['dst-switch'])
        dport = link['dst-port']
        links.append([sswitch, sport, dswitch, dport])
    result = check_output('curl -s http://localhost:8080/wm/device/', \
                         shell=True, executable='/bin/bash')
    hostLinks = json.loads(result)
    for link in hostLinks:
        attachDevices = link['attachmentPoint']
        if attachDevices:
            mac = str(link['mac'][0]).translate(None, ':')
            for device in attachDevices:
                deviceId = str(device['switchDPID'])
                devicePort = device['port']
                links.append([mac, None, deviceId, devicePort])
    return links

def GetSwitchStats(switchId, statsType):
    command = 'curl -s http://localhost:8080/wm/core/switch/{0}/{1}/json'.format(\
              switchId, statsType)
    result = check_output(command, shell=True, executable='/bin/bash')
    stats = json.loads(result)
    return stats[switchId]

def GetRoute(switchIdA, portA, switchIdB, portB):
    result = check_output('curl -s http://localhost:8080/wm/topology/route/{0}/{1}/{2}/{3}/json'.format(switchIdA, portA, switchIdB, portB), shell=True, executable='/bin/bash')
    routes = json.loads(result)
    return routes

def GetSwitchProperties():
    result = check_output('curl -s http://localhost:8080/wm/core/controller/switches/json', \
                         shell=True, executable='/bin/bash')
    switches = json.loads(result)
    switchProperties = {}
    for switch in switches:
        switchId = str(switch['dpid'])
        switchProperties[switchId] = switch
    return switchProperties

if __name__ == '__main__':
    links = GetLinks()
    hosts = filter(lambda x: x[1] is None, links)
    print 'Links:'
    for link in links:
        print link
    routes = GetRoute(hosts[0][2], hosts[0][3], hosts[2][2], hosts[2][3])
    print 'Routes between {0} and {1}'.format(hosts[0][0], hosts[2][0])
    print routes
    print 'switch stats:'
    print GetSwitchStats(links[1][0], 'flow')


import agent_web10g_measure as web10g
import time

if __name__ == '__main__':
    a = web10g.IntStringDict()
    a[1193] = '345'
    a[1194] = '189'
    b = web10g.StringStringDict()
    b['127.0.0.1:9000:127.0.0.1:35888'] = '847'
    statsToM = web10g.IntList()
    statsToM.append(1)
    statsToM.append(4)
    statsToM.append(1)
    statsToM.append(2)
    statsToM.append(5)
    statsToM.append(8)
    point1 = time.time()
    result = web10g.measure(a, b, statsToM)
    point2 = time.time()
    for sockfd in result.keys():
        print 'python: sockfd {0} data {1}'.format(sockfd, result[sockfd])
    print '{0:3f}'.format((point2 - point1) * 1000)

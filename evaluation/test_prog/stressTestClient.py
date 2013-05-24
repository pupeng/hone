'''
Created on Apr 25, 2012

@author: pengsun
'''

import socket, time, sys
from multiprocessing import Process

serverPort = 9000
sleepTime = 10

def oneClient(serverAddr):
    try:
        mysock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        mysock.connect((serverAddr, serverPort))
        message = "test hone\r\n"
        while(1):
            mysock.sendall(message)
            time.sleep(sleepTime)
    except:
        mysock.close()
        
if __name__=='__main__':
    if len(sys.argv)<3:
        print 'give server address and number of clients'
        print 'syntax: python stressTestClient address number'
        sys.exit()
    serverAddr = sys.argv[1]
    number = int(sys.argv[2])
    clientProcess = []
    try:
        for i in range(number):
            cp = Process(target=oneClient, args=(serverAddr,))
            clientProcess.append(cp)
            cp.start()
        for cp in clientProcess:
            cp.join()
    except:
        for cp in clientProcess:
            if cp.is_alive():
                cp.terminate()
        print 'Exit from stressTestClient'
from subprocess import check_output, call

destinations = ['10.1.1.21', '10.1.1.22', '10.1.1.23', '10.1.1.24']

def main():
    selfIP = check_output('ifconfig | grep -b1 eth0 | grep inet | cut -b24-34', shell=True, executable='/bin/bash').rstrip()
    for dst in destinations:
        if dst != selfIP:
            command = 'screen -S client{0} -d -m ./runTrafclient {0}'.format(dst)
            call(command, shell=True, executable='/bin/bash')

if __name__ == '__main__':
    main()

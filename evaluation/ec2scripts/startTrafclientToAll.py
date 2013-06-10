from subprocess import check_output, call

destinations = [
    '10.166.0.183',
    '10.166.7.91',
    '10.154.149.174',
    '10.147.159.54']

def main():
    selfIP = check_output('ifconfig | grep -b1 eth0 | grep inet | cut -b24-34', shell=True, executable='/bin/bash').rstrip()
    for dst in destinations:
        if dst != selfIP:
            command = 'screen -S client{0} -d -m ./runTrafclient {0}'.format(dst)
            call(command, shell=True, executable='/bin/bash')

if __name__ == '__main__':
    main()

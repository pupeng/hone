## HONE Project

Programmable Host-Network Traffic Management from Princeton CS. 

Project website: [http://hone.cs.princeton.edu/][hone]

Author: Peng Sun

Contributors:  
Zhihong Xu, Lavanya Jose, Minlan Yu, Jennifer Rexford, Michael J. Freedman,
David Walker

Public Amazon EC2 AMI:  
[ami-bd0a7dd4][ami] (HONE-enabled Ubuntu Cloud Image 11.04)

Public VirtualBox VM image:  
[HONE VM image][vm]

Dependency:
* psutil 0.7.0
* web10G kernel patch 3.2
* estats userland library (included with web10G kernel patch 3.2)
* Python 2.7+
* ipaddr-py 2.1.10
* Python Twisted 13.0.0

Initial Setup:

1.  Insert HONE kernel module on host machine. 
    The current kernel module is for HONE-enabled image only, since we have
    modified kernel NETLINK to make it work.
        cd ~/hone/HostAgent/kpsimple
        sudo make up
2.  Compile c++ implementation into python modules
        cd ~/hone/HostAgent
        ./swig_modules

Notes:
1. Start the HONE Controller:
    enter Controller directory
    run: python hone_run.py mgmtProgram

2. Start the HONE host agent:
    remember to go through 'Initial Setup'
    enter HostAgent
    python agentRun.py controllerIP controllerPort

3. If running on EC2
    ec2scripts/startHoneAgent finds the EC2 internal IP of the
    instance named hone-controller, and start HONE agent against it. 

4. Log level:
    change log level in Controller/hone_run.py and HostAgent/agentRun.py
    All evaluation logs are info level

5. Network module:
    first start floodlight controller to control the switches
    Controller/hone_netModule.py contains functions to communicate with FL

[hone]: http://hone.cs.princeton.edu/
[ami]: https://console.aws.amazon.com/ec2/home?region=us-east-1#launchAmi=ami-bd0a7dd4
[vm]: http://hone.cs.princeton.edu/files/hone-vm.ova

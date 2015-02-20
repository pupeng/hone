## HONE Project

Programmable Host-Network Traffic Management from Princeton CS. 

Project website: [http://hone.cs.princeton.edu/][hone]  
I highly encourage you to visit our website. You can get an overview of the
system, and find various management applications we have built. Those examples
can give you a quick sense of what HONE can do, and how you will program on
HONE. 

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

Setup steps:

1.  Insert HONE kernel module on host machine. 
    The current kernel module is for HONE-enabled image only, since we have
    modified kernel NETLINK to make it work.

        cd ~/hone/HostAgent/kpsimple
        sudo make up

2.  Compile C++ implementation into python modules. 

        cd ~/hone/HostAgent
        ./swig_modules

3.  Start network module (if necessary for your scenario). 
    HONE interacts with the network devices via Floodlight. Please check out how
    to install and start Floodlight on its [Getting-started page][floodlight]. 

4.  Start the HONE controller.

        cd ~/hone/Controller
        python hone_run.py mgmtProgramName

5.  Start the HONE host agent.

        cd ~/hone/HostAgent
        python agentRun.py controllerIP controllerPort # default port is 8866


[hone]: http://hone.cs.princeton.edu/
[ami]: https://console.aws.amazon.com/ec2/home?region=us-east-1#launchAmi=ami-bd0a7dd4
[vm]: http://hone.cs.princeton.edu/files/hone-vm.ova
[floodlight]: http://www.projectfloodlight.org/getting-started/

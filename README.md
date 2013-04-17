hone
====

HONE Project
Programmable Host-Network Traffic Management
from Princeton CS. 

Author:
Peng Sun

Contributors:
Zhihong Xu, Lavanya Jose, Minlan Yu

Supervisors:
Jennifer Rexford, Michael J. Freedman, David Walker

Public Amazon EC2 AMI:
ami-d92ebab0                   hone-enabled Ubuntu Cloud Image 11.04

Public VirtualBox VM image:
(publish soon)

Dependency:
psutil                         0.7.0
web10g kernel patch            3.2
estats userland library        included with kernel patch 3.2
Python                         2.7+
ipaddr-py                      2.1.10

Initial Setup:
1. Insert HONE kernel probe on host machine. 
   The current kernel probe is for hone-enabled image only,
   since we have modified kernel NETLINK to make it work. 
     enter HostAgent/HoneKernelProbe
     sudo make up
2. Compile c++ implementation to python modules
    enter HostAgent
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

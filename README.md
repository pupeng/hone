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

Initial Setup:
1. insert kernel module kpsimple on host machine
    enter honeHostAgent/kpsimple
    sudo make up
2. compile c++ implementation to python modules
    enter honeHostAgent
    ./swig_modules

Notes:
1. start honeController:
    enter honeController directory
    run: python hone_run.py mgmtProgram

2. start honeHostAgent
    remember to go through 'Initial Setup'
    enter honeHostAgent
    python agentRun.py controllerIP controllerPort

3. If running on EC2
    ec2scripts/startHoneAgent finds the EC2 internal IP of hone-controller
    instance, and start Hone agent against it. 

4. log level:
    change log level in honeController/hone_run.py and honeHostAgent/agentRun.py
    All evaluation logs are info level

5. network module:
    first start floodlight controller to control the switches
    honeController/hone_netModule.py contains functions to communicate with FL

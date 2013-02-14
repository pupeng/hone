hone
====

A research project from Princeton CS on joint host/network traffic 
management solution for data centers.

HONE Project
Programmable Host-Network Traffic Management

Author:
Peng Sun

Contributors:
Zhihong Xu, Lavanya Jose, Minlan Yu

Supervisors:
Jennifer Rexford, Michael J. Freedman, David Walker

Dependency:
psutil  0.6.1
estats_userland 2.0.1
web10g  kernel-2.6.38

Notes:
1. start honeController:
    enter honeController directory
    run: python hone_run.py mgmtProgram

2. start honeHostAgent
    remember to insert kernel module
    enter honeHostAgent/kpsimple
    sudo make up
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


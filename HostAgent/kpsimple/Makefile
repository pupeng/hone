# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# makefile for kpsimple kernel module

obj-m += kpsimple.o

PWD := $(shell pwd)

up: kpsimple.ko
	insmod kpsimple.ko

down: 
	rmmod kpsimple

clear: down clean

kpsimple.ko: kpsimple.c kpsimple.h
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules	

clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
	

obj-m += honekernelprobe.o

PWD := $(shell pwd)

up: honekernelprobe.ko
	insmod honekernelprobe.ko

down: 
	rmmod honekernelprobe

clear: down clean

honekernelprobe.ko: honekernelprobe.c honekernelprobe.h
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules	

clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
	

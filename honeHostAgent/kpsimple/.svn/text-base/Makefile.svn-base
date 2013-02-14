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
	

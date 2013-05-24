#! /bin/bash

# python twisted library
sudo apt-get install python-twisted

# psutil library
tar -xvf psutil-0.7.0.tar.gz
cd psutil-0.7.0
sudo python setup.py install
cd ..
rm -rf psutil-0.7.0

# estats library
tar -xvf estats_userland-2.0.1.tar.gz
cd estats_userland-2.0.1/
./configure --enable-python
make
sudo make install
cd ..
sudo rm -rf estats_userland-2.0.1

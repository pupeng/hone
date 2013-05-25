# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

from subprocess import call
import sys, time

def main():
    try:
        for i in range(5, 600, 5):
            #number = raw_input('Type a number -> ')
            #raw_input('Enter to continue --> ')
            number = 5
            call('./test_prog 192.168.17.51 '+str(number)+' &> /dev/null &', shell=True, executable='/bin/bash')
            print 'current number of connection:'+str(i)
            time.sleep(10)
    except KeyboardInterrupt:
        print 'Exit now'
        sys.exit()

if __name__=='__main__':
    main()

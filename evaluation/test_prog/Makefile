# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

all: test_prog

test_prog: test_prog.c
	gcc -lpthread -o test_prog test_prog.c

clean:
	rm -f *.o test_prog

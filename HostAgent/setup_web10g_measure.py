# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# use swig to convert the C implementation of web10g measure to python module

from distutils.core import setup, Extension

agent_web10g_measure_module = Extension('_agent_web10g_measure',\
                              sources=['web10g_measure.cpp','web10g_measure_wrap.cpp'],\
                              library_dirs=['/usr/local/lib'],\
                              libraries=['estats'],)

setup(name='agent_web10g_measure',ext_modules=[agent_web10g_measure_module],py_modules=['agent_web10g_measure'],)



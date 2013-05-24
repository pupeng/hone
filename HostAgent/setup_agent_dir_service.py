# Copyright (c) 2011-2013 Peng Sun. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the COPYRIGHT file.

# use swig to convert the C implementation of dir service to python module

from distutils.core import setup, Extension

agent_dir_service_module = Extension('_agent_dir_service', sources=['agent_dir_service_wrap.c','agent_dir_service.c'],)

setup(name='agent_dir_service',ext_modules=[agent_dir_service_module],py_modules=['agent_dir_service'],)



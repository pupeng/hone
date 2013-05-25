/* Copyright (c) 2011-2013 Peng Sun. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the COPYRIGHT file.
 *
 * swig interface for directory service
 */

%module agent_dir_service
%{
#include "agent_dir_service.h"
%}

extern void agent_dir_service_cleanup(void);
extern void agent_dir_service_run(void);
extern char* agent_dir_service_recv(void);

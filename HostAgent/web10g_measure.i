/* Copyright (c) 2011-2013 Peng Sun. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the COPYRIGHT file.
 *
 * swig interface for web10g measurement
 */

%module agent_web10g_measure

%{
#include "web10g_measure.h"
%}

%include "std_map.i"
%include "std_vector.i"
%include "std_string.i"
%include "web10g_measure.h"

namespace std
{
    %template(IntStringDict) map<int, string>;
    %template(StringStringDict) map<string, string>;
    %template(IntList) vector<int>;
}

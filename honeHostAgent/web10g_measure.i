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


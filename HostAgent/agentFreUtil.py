################################################################################
# The Frenetic Project                                                         #
# frenetic@frenetic-lang.org                                                   #
################################################################################
# Licensed to the Frenetic Project by one or more contributors. See the        #
# NOTICE file distributed with this work for additional information            #
# regarding copyright and ownership. The Frenetic Project licenses this        #
# file to you under the following license.                                     #
#                                                                              #
# Redistribution and use in source and binary forms, with or without           #
# modification, are permitted provided the following conditions are met:       #
# - Redistributions of source code must retain the above copyright             #
#   notice, this list of conditions and the following disclaimer.              #
# - Redistributions in binary form must reproduce the above copyright          #
#   notice, this list of conditions and the following disclaimer in            #
#   the documentation or other materials provided with the distribution.       #
# - The names of the copyright holds and contributors may not be used to       #
#   endorse or promote products derived from this work without specific        #
#   prior written permission.                                                  #
#                                                                              #
# Unless required by applicable law or agreed to in writing, software          #
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT    #
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the     #
# LICENSE file distributed with this work for specific language governing      #
# permissions and limitations under the License.                               #
################################################################################
# /src/frenetic_util.py                                                        #
# Utility functions                                                            #
# $Id$ #
################################################################################

from time import time

class IllegalArgument(Exception):
    def __init__(self,value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def current_time():
    return int(time())

# Reference cells
class FRef:
    def __init__(self):
        self.x = None
    def get(self):
        return self.x
    def set(self,x):
        self.x = x

# Exceptions
class FError(Exception):
    def __init__(self,s):
        self.s = s
    def __str__(self):
        return self.s

# Infix operators
class Infix:
    def __init__(self, function):
        self.function = function
    def __ror__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __or__(self, other):
        return self.function(other)
    def __rlshift__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __rshift__(self, other):
        return self.function(other)
    def __call__(self, value1, value2):
        return self.function(value1, value2)


def add_pair(p1,p2):
    (x1,y1) = p1 
    (x2,y2) = p2
    return (x1 + x2, y1 + y2)

def sub_pair(p1,p2):
    (x1,y1) = p1 
    (x2,y2) = p2
    return (x1 - x2, y1 - y2)

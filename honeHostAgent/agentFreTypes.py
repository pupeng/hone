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
# /src/frenetic_types.py                                                       #
# Type representations                                                         #
# $Id$ #
################################################################################

class FType:
    def __init__(self, type):
        self.type = type

    def __repr__(self):
        return "Type: %s" % self.__str__()

    def __str__(self):
        return "<%s>" % str(self.type)

    def __eq__(self, other):
        try:
            return self.type == other.type
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

class FEventType(FType):
    def __str__(self):
        return "Event<%s>" % str(self.type)

class FBehaviorType(FType):
    def __str__(self):
        return "Behavior<%s>" % str(self.type)

class FTypeFun:
    def __init__(self, return_type = None, fun = None):
        self.return_type = return_type
        if fun is None:
            fun = lambda x : return_type
        self.fun = fun

class FTypeException(Exception):
    """Frenetic type exception"""
    pass

def is_of_type(a, b):
    if a is None or b is None:
        return True
    return a == b

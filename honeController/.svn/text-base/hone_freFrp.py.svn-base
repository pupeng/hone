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
# /src/frenetic_frp.py                                                         #
# Frenetic FRP representation                                                  #
# $Id$ #
################################################################################

import threading

import hone_freUtil as util

def mk_add_listener(self):
    def f(l):
        x = self.next()
        self.listeners[x] = l
        def d():
            del self.listeners[x]
        return d
    return f

def mk_prepare(self):
    def f():
        for l in self.listeners.itervalues():
            l.prepare()
    return f

def mk_push(self):
    def f(x):
        for l in self.listeners.itervalues():
            l.push(x)
    return f

def mk_finish(self):
    def f():
        for l in self.listeners.itervalues():
            l.finish()
    return f

def mk_terminate(self):
    def f():
        for l in self.listeners.itervalues():
            l.terminate()
    return f
    
def mk_next(self):
    def f():
        self.lid = self.lid + 1
        return self.lid
    return f

# Composition operators 
def ComposeEFEF(ef1,ef2):
    ef1.add_listener(ef2)
    ef = FEventFun(ef2.add_listener,ef1.prepare,ef1.push,ef1.finish,ef1.terminate)
    return ef

def ComposeEEF(e1,ef2):
    e = None
    def prepare():
        e.fresh = False
        ef2.prepare()
    def push(x):
        ef2.push(x)
    def finish():
        ef2.finish()
        e.fresh = True
    e1.add_listener(FListener(prepare,push,finish,ef2.terminate))
    e = FEvent(ef2.add_listener)
    return e

def ComposeEL(e,l):
    e.add_listener(l)

# Events
class FEvent:
    def __init__(self,add_listener=None,type=None):
        self.fresh = True
        self.lid = 0
        self.listeners = {}
        self.next = mk_next(self)
        self.add_listener = mk_add_listener(self) if add_listener is None else add_listener
        self.type = type
    def __rshift__(self,other):
        other_name = other.__class__.__name__ 
        if other_name == "FEventFun":
            return ComposeEEF(self,other)
        elif other_name == "FListener":
            return ComposeEL(self,other)
        else:
            raise util.IllegalArgument("Cannot compose FEvent and %s" % other_name)

# Event functions
class FEventFun:
    def __init__(self,add_listener=None,prepare=None,push=None,finish=None,terminate=None,type_fun=None):
        self.listeners = {}
        self.lid = 0
        self.next = mk_next(self)
        self.add_listener = mk_add_listener(self) if add_listener is None else add_listener
        self.prepare = mk_prepare(self) if prepare is None else prepare
        self.push = mk_push(self) if push is None else push
        self.finish = mk_finish(self) if finish is None else finish
        self.terminate = mk_terminate(self) if terminate is None else terminate
        self.type_fun = (lambda x : None) if type_fun is None else type_fun
    def __rshift__(self,other):
        other_name = other.__class__.__name__ 
        if other_name == "FEventFun":
            return ComposeEFEF(self,other)
        else:
            raise util.IllegalArgument("Cannot compose FEvent and %s" % other_name)

# Listeners
class FListener:
    def __init__(self,prepare=(lambda:()),push=lambda x:(),finish=lambda:(),terminate=(lambda:())):
        self.prepare = prepare
        self.push = push
        self.finish = finish
        self.terminate = terminate

# Behaviors
class FBehavior:    
    def __init__(self,pull,type=None):
        self.pull = pull
        self.type = type

# event_lock: a global lock ensuring atomic propogation of events.
event_lock = threading.Lock()

# RawEvent: generates an event stream and a "go" function to propagate a value
def RawEvent():
    e = FEvent()
    def go(x):
        event_lock.acquire()
        e.fresh = False
        for l in e.listeners.itervalues():
            l.prepare()
        for l in e.listeners.itervalues():
            l.push(x)
        for l in e.listeners.itervalues():
            l.finish()
        event_lock.release()
        e.fresh = True
    return (e,go)

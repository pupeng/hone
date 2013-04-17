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
# /src/frenetic_lib.py                                                         #
# Frenetic standard library                                                    #
# $Id$ #
################################################################################

import inspect
import sys
import threading
import agentFreUtil as util
import agentFreTypes as types

from agentFreFrp import FEvent, FEventFun, FListener, FBehavior, RawEvent


# JNF: these flags should really be set from the command line
#      turning them on for now
RUN_TESTS = False
DEBUG = True

##########
# EVENTS #
##########

# switchJoin : E switch
(switchJoin_E,switchJoin_go) = RawEvent()
def SwitchJoin():
    return switchJoin_E

# switchLeave : E switch
(switchLeave_E,switchLeave_go) = RawEvent()
def SwitchLeave():
    return switchLeave_E

# seconds : E int 
#(seconds_E,seconds_go) = RawEvent()

#seconds_going = False
#def seconds_callback():    
#  seconds_go(util.current_time())
#  net.inst.post_callback(1, seconds_callback)
#  return True

#def Seconds():
#  global seconds_going
#  if not seconds_going:
#    seconds_going = True
#    seconds_callback()
#  return seconds_E

# PortEvents() : E (PortEvent)
#(portEvent_E, portEvent_go) = RawEvent()
#def PortEvents():
#    return portEvent_E

# Input : string -> E string
def Input(prompt):
    (e,go) = RawEvent()
    def run():
        try:
            while True:
                x = raw_input(prompt)
                go(x)
        except EOFError:
            for l in e.listeners.itervalues():
                l.terminate()
            exit(0)
    t = threading.Thread(target=run)
    t.start()
    return e

# Merge : E a * E b -> E (a option * b option)
def Merge(e1,e2):    
    e = None
    def prepare():
        e.fresh = False
        for l in e.listeners.itervalues():
            l.prepare()
    def push1(x1):
        if e2.fresh or e2.cell.get() != None:
            x2 = e2.cell.get()
            for l in e.listeners.itervalues():
                l.push((x1,x2))
            e2.cell.set(None)
        else:
            e1.cell.set(x1)
    def push2(x2):
        if e1.fresh or e1.cell.get() != None:
            x1 = e1.cell.get()
            for l in e.listeners.itervalues():
                l.push((x1,x2))
            e1.cell.set(None)
        else:
            e2.cell.set(x2)
    def finish():
        x1 = e1.cell.get()
        x2 = e2.cell.get()
        e1.cell.set(None)
        e2.cell.set(None)
        if x2 != None:
            for l in e.listeners.itervalues():
                l.push((None,x2))
        if x1 != None:
            for l in e.listeners.itervalues():
                l.push((x1,None))
            for l in e.listeners.itervalues():
                l.finish()
        e.fresh = True
    def terminate1():
        e.term1 = True
        if e.term2:
            for l in e.listeners.itervalues():
                l.terminate()
    def terminate2():
        e.term2 = True
        if e.term1:
            for l in e.listeners.itervalues():
                l.terminate()
    e1.add_listener(FListener(prepare,push1,finish,terminate1))
    e2.add_listener(FListener(prepare,push2,finish,terminate2))
    
    if e1.type is None or e2.type is None:
        out_type = None
    else:
        # TODO(astory): option
        out_type = types.FType((e1.type.type, e2.type.type))
    e = FEvent(type=out_type)
    e.term1 = False
    e.term2 = False
    e1.cell = util.FRef()
    e2.cell = util.FRef()
    return e

# Split E (a * b) -> E a * E b
def Split(e):
    e1 = None
    e2 = None
    def prepare():
        e1.fresh = False
        e2.fresh = False
        for l in e1.listeners.itervalues():
            l.prepare()
        for l in e2.listeners.itervalues():
            l.prepare()
    def push((x1,x2)):
        for l in e1.listeners.itervalues():
            l.push(x1)
        for l in e2.listeners.itervalues():
            l.push(x2)
    def finish():
        for l in e1.listeners.itervalues():
            l.finish()
        for l in e2.listeners.itervalues():
            l.finish()
        e1.fresh = True
        e2.fresh = True
    def terminate():
        for l in e1.listeners.itervalues():
            l.terminate()
        for l in e2.listeners.itervalues():
            l.terminate()
    t1 = t2 = None
    if e.type is not None:
        try:
            (r1, r2) = e.type.type
            (t1, t2) = (types.FType(r1), types.FType(r2))
        except (TypeError, ValueError):
            raise types.FTypeException("%s not of type %s" % (e.type, "a * b"))

    e1 = FEvent(type=t1)
    e2 = FEvent(type=t2)
    e.add_listener(FListener(prepare,push,finish,terminate))
    return (e1,e2)

# Apply : E a * EF a b -> E b
def Apply(e1,ef2):
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
    t = None
    if ef2.type_fun is not None:
        t = ef2.type_fun(e1.type)
    e = FEvent(ef2.add_listener, type=t)
    return e

############################
# UNIT TESTS AND DEBUGGING #
############################

def unit_test(i, ef, o):
    if RUN_TESTS:
        def location():
            (filename,lineno) = inspect.stack()[2][1:3]
            return "File \"" + filename + "\", line " + str(lineno)
        (ei,go) = RawEvent()
        e = Apply(ei,ef)
        def f(x):
            y = o.pop(0)
            if x != y:
                print "%s: Unit test error\nexpected %s\nbut found %s" % (location(),str(y),str(x))
        e.add_listener(FListener(push=f))
        for x in i:
            go(x)
        for l in ei.listeners.itervalues():
            l.terminate()
        if len(o) != 0:
            print "%s: Unit test error\nextra outputs %s" % (location(),str(o))
    return

def unit_test_merge(i,ef,o):
    if RUN_TESTS:
        def location():
            (filename,lineno) = inspect.stack()[2][1:3]
            return "File \"" + filename + "\", line " + str(lineno)
        (eia,goa) = RawEvent()
        (eib,gob) = RawEvent()
        e = ef(eia,eib)
        def f((p,q)):
            (x,y) = (None,None)
            if len(o) > 0:
                (x,y) = o.pop(0)
            if (x,y) != (p,q):
                print sys.stderr, "%s: Unit test error\nexpected %s\nbut found %s" % (location(),str((x,y)), str((p,q)))
        e.add_listener(FListener(push=f))
        for (a,b) in i:
            if not(a is None) and not(b is None):
                # This isn't truly a parallel call
                (goa(a),gob(b))
            elif not(a is None):
                goa(a)
            elif not(b is None):
                gob(b)
        for l in eia.listeners.itervalues():
            l.terminate()
        for l in eib.listeners.itervalues():
            l.terminate()
        if len(o) != 0:
            print sys.stderr, "%s: Unit test error\nextra outputs %s" % (location(),str(o))
    return

def debug_test(i,ef):
    if DEBUG:
        def location():
            (filename,lineno) = inspect.stack()[2][1:3]
            return "File \"" + filename + "\", line " + str(lineno)
        (ei,go) = RawEvent()
        e = Apply(ei,ef)
        o = []
        def f(x):
            o.append(x)
        e.add_listener(FListener(push=f))
        for x in i:
            go(x)
        ei.terminate()
        print "%s: Test result:\n%s" % (location(),str(o))
    return

###################
# EVENT FUNCTIONS #
###################

# Lift: (a -> b) -> EF a b
def Lift(g, in_type=None, out_type=None, type_fun=None):
    """Lift function g into an EF.  Optional type annotations {in,out}_type, or
    you can give a type conversion function, which overrides static types.
    
    Note that in_type and out_type MUST be wrapped in types.FType,or something
    with its interface, and type_fun should expect its input and output wrapped
    in the same class.
    
    A None type object acts as a signal to ignore type checking this
    type.  It should not throw type exceptions."""
    ef = None
    def push(x):
        y = g(x)
        for l in ef.listeners.itervalues():
            l.push(y)
    if type_fun is None:
        def type_fun(test_in_type):
            if in_type is None or types.is_of_type(test_in_type, in_type):
                return out_type
            else:
                raise types.FTypeException(
                    "%s not of type %s" % (test_in_type, in_type))
    ef = FEventFun(push=push, type_fun=type_fun)
    return ef

# Compose: EF a b * EF b c -> EF a c
def Compose(ef1,ef2):
    ef1.add_listener(ef2)
    def type_fun(t):
        if t is None:
            return None
        out1 = ef1.type_fun(t)
        if out1 is None:
            return None
        out2 = ef2.type_fun(out1)
        return out2
    ef = FEventFun(
            ef2.add_listener,
            ef1.prepare,
            ef1.push,
            ef1.finish,
            ef1.terminate,
            type_fun)
    return ef

# First : EF a b -> EF (a * c) (b * c)
def First(ef1):
    ef = None
    def prepare():
        ef1.prepare()
        for l in ef.listeners.itervalues():
            l.prepare()
    def push((x1,x2)):
        ef.cell.set(x2)
        ef1.push(x1)
    def push2(y1):
        y2 = ef.cell.get()
        for l in ef.listeners.itervalues():
            l.push((y1,y2))
    def finish():
        ef1.finish()
        for l in ef.listeners.itervalues():
            l.finish()
    def terminate():
        ef1.terminate()
        for l in ef.listeners.itervalues():
            l.terminate()
    def type_fun(in_type):
        if in_type is None:
            return None
        try:
            (a, c) = in_type.type
        except ValueError:
            raise types.FTypeException("%s not of type (a * c)" % in_type)
        return types.FType((ef1.type_fun(types.FType(a)).type, c))
    ef = FEventFun(
        prepare=prepare,
        push=push,
        finish=finish,
        terminate=terminate,
        type_fun=type_fun)
    ef.cell = util.FRef()
    ef1.add_listener(FListener(push=push2))
    return ef

# LoopPre : c -> EF (a,c) (b,c) -> EF a b
def LoopPre(c, ef1, c_type=None):
    ef = None
    def prepare():
        ef1.prepare()
        for l in ef.listeners.itervalues():
            l.prepare()
    def push(x):
        ef1.push((x,ef.cell.get()))
    def push2((y1,y2)):
        ef.cell.set(y2)
        for l in ef.listeners.itervalues():
            l.push(y1)
    def finish():
        ef1.finish()
        for l in ef.listeners.itervalues():
            l.finish()
    def terminate():
        ef1.terminate()
        for l in ef.listeners.itervalues():
            l.terminate()
    def type_fun(a):
        if a is None or c_type is None:
            return None
        eftype = ef1.type_fun(types.FType((a.type, c_type.type)))
        if eftype is None:
            return None
        (b, c) = eftype.type
        if not types.is_of_type(c_type, types.FType(c)):
            raise types.FTypeException("%s is not of type %s" % (c, c_type))
        return types.FType(b)
    ef = FEventFun(
            prepare=prepare,
            push=push,
            finish=finish,
            terminate=terminate,
            type_fun = type_fun)
    ef.cell = util.FRef()
    ef.cell.set(c)
    ef1.add_listener(FListener(push=push2))
    return ef

# bind: (a -> E b) -> EF a b 

# Filter : (a -> bool) -> EF a a
def Filter(g):
    ef = None
    def push(x): 
        if g(x):
            for l in ef.listeners.itervalues():
                l.push(x)
    ef = FEventFun(push=push, type_fun = lambda a : a)
    return ef

# Group : (a -> b) -> EF a (b * E a)
def Group(g, b_type=None):
    ef = None
    def prepare():
        for l in ef.listeners.itervalues():
            l.prepare()
        for e in ef.table.values():
            e.fresh = False
            for l in e.listeners.itervalues():
                l.prepare()
    def push(x):
        y = g(x)
        e = None
        if not (ef.table.has_key(y)):
            e = FEvent()
            ef.table[y] = e
            for l in ef.listeners.itervalues():
                l.push((y,e))
        else:
            e = ef.table[y]
        for l in e.listeners.itervalues():
            l.push(x)
    def finish():
        for e in ef.table.values():
            for l in e.listeners.itervalues():
                l.finish()
            e.fresh = True
        for l in ef.listeners.itervalues():
            l.finish()
    def terminate():
        for e in ef.table.values():
            for l in e.listeners.itervalues():
                l.terminate()            
        for l in ef.listeners.itervalues():
            l.terminate()
    def type_fun(a):
        if a is not None and b_type is not None:
            return types.FType((b_type.type, types.FEventType(a.type)))
        else:
            return None
    ef = FEventFun(prepare=prepare,
        push=push,
        finish=finish,
        terminate=terminate,
        type_fun=type_fun)
    ef.table = {}
    return ef

# Regroup : ((a * a) -> Bool) -> EF (b * E a) (b * E a)
def Regroup(feq):
    # helper function to split a nested subevent
    def mk_subevent(e,outer_prepare,outer_push):
        sube_cell = util.FRef()
        def mk():
            sube = FEvent()
            sube.last_cell = util.FRef()
            return sube
        def subprepare():
            sube = sube_cell.get()
            sube.fresh = False
            for l in sube.listeners.itervalues():
                l.prepare()
        def subpush(x):
            sube = sube_cell.get()
            last = sube.last_cell.get()
            if not (last is None) and not (feq(last,x)):
                # terminate / create new subevent
                sube_old = sube
                sube = mk()
                subterminate()
                sube_cell.set(sube)
                subprepare()
                outer_prepare()
                outer_push(sube)
            for l in sube.listeners.itervalues():
                l.push(x)
            sube.last_cell.set(x)
        def subfinish():
            sube = sube_cell.get()
            for l in sube.listeners.itervalues():
                l.finish()
            sube.fresh = True
        def subterminate():
            sube = sube_cell.get()
            for l in sube.listeners.itervalues():
                l.terminate()
        sube = mk()
        sube_cell.set(sube)
        e.add_listener(FListener(subprepare,subpush,subfinish,subterminate))
        return sube
    ef = None    
    def prepare():
        for l in ef.listeners.itervalues():
            l.prepare()
    def push((x,e)):
        outer_push = lambda e: push((x,e))
        sube = mk_subevent(e,prepare,outer_push)
        for l in ef.listeners.itervalues():
            l.push((x,sube))
    # TODO(astory): consider checking for correctness
    ef = FEventFun(push=push, type_fun = lambda a : a)
    return ef

# Ungroup : int option * (b * a -> b) -> b -> EF (c * E a) (c * b)
def Ungroup(n,f,init, init_type=None):
    ef = None
    def go(x,y):
        for l in ef.listeners.itervalues():
            l.push((x,y))
    def mk_lpush(e):
        def g(z):            
            (x,i,b,y) = ef.table[e]
            if not b:
                y = f(y,z)
                if not (n is None) and i == n - 1:
                    b = True
                    go(x,y)
                ef.table[e] = (x,i+1,b,y)
        return g
    def mk_lterm(e):
        def g():
            (x,i,b,y) = ef.table[e]
            if not b:
                go(x,y)
        return g
    def push((x,e)):
        ef.table[e] = (x,0,False,init)
        e.add_listener(FListener(push=mk_lpush(e),terminate=mk_lterm(e)))
    def type_fun(t):
        if t is None or init_type is None:
            return None
        try:
            (c, a) = t.type
        except (TypeError, ValueError):
            raise types.FTypeException("%s not an instance of (c * E a)" % a)
        f_out_type = f.type_fun(types.FType((init_type.type, a.type)))
        if not types.is_of_type(f_out_type, init_type):
            raise types.FTypeException(
                "%s not of expected type: f generates %s instead of %s"
                % (t, f_out_type, init_type))
        return types.FType((c, init_type.type))
    ef = FEventFun(push=push,type_fun=type_fun)
    ef.table = {}
    return ef

#############
# BEHAVIORS #
#############

# Hold : a -> E a -> B a
def Hold(a,e1):
    b = None
    def pull():
        return b.cell.get()
    def push(a):
        b.cell.set(a)
    b = FBehavior(pull, types.FBehaviorType(e1.type))
    b.cell = util.FRef()
    b.cell.set(a)
    e1.add_listener(FListener(push=push))
    return b

# Snapshot : B a -> E b -> E (a,b)
def Snapshot(b1,e2):
    e = None
    def prepare():
        e.fresh = False
        for l in e.listeners.itervalues():
            l.prepare()
    def push(b):
        a = b1.pull()
        for l in e.listeners.itervalues():
            l.push((a,b))
    def finish():
        for l in e.listeners.itervalues():
            l.finish()
        e.fresh = True
    def terminate():
        for l in e.listeners.itervalues():
            l.terminate()
    e2.add_listener(FListener(prepare,push,finish,terminate))
    e = FEvent(type=types.FEventType((b1.type, e2.type)))
    return e

############################
# LISTENERS and REGISTRARS #
############################

#def Attach(e,l):
#    e.add_listener(l)
#
## Print_listener : string -> output channel -> L string
#def Print(g=None,s=sys.stdout):    
#    def push(x):
#        if g is None:
#            print >> s, str(x)
#        else:
#            g(x)
#    return FListener(push=push)
#
## Install_listener : Policy
#def Control():
#    def push(rs):
#        #exeModule something for policies
#        print 'control policies'
#    return FListener(push=push)

# register_static : (rule set) -> unit
#def register_static_rules(rs):
#    rts.set_rules(rs)


# Nox_packet_listener L (switch * nox_packet * port)
#def NOXSend():
#  def push((dpid,packet,port)):
#    a = [[openflow.OFPAT_OUTPUT,[0,port]]]
#    net.inst.send_openflow_packet(dpid, packet.tostring(), a)
#  return FListener(push=push)

#def NOXSendPkt():
#  def push(pkt):
#    a = [[openflow.OFPAT_OUTPUT,[0,net.inport(net.header(pkt))]]]
#    net.inst.send_openflow_packet(net.switch(net.header(pkt)), pkt.payload.tostring(), a)
#  return FListener(push=push)


############
## LIBRARY #
############
## TODO(astory): type variables
#
## Identity : EF a a 
#def Identity():
#    return Lift(lambda x: x, type_fun=lambda x: x)
#
## Probe : string -> EF a a
#def Probe(y, s=sys.stdout):
#    def f(x):
#        print >> s, (str(y) + str(x))
#        return x
#    return Lift(f, type_fun=lambda x:x)
#
## Dup : EF a (a * a)
#def Dup():
#    def type_fun(t):
#        if t is None:
#            return None
#        return types.FType((t.type, t.type))
#    return Lift(lambda x: (x,x), type_fun=type_fun)
#
## Smash : a * b * E a * E b -> E (a * b)
#def Smash(a,b,e1,e2):
#  def m((a2,b2),(a1,b1)):
#    if a2 is None:
#      return (a1,b2)
#    elif b2 is None:
#      return (a2,b1)
#    else:
#      return (a2,b2)
#  t = None
#  t1 = e1.type
#  t2 = e2.type
#  if t1 is not None and t2 is not None:
#    t = FType((t1,t2))
#  e = Merge(e1,e2) >> \
#      Accum((a,b), m, init_type=t)
#  return e
#      
## Fst : EF (a * b) a
#def Fst():
#    def typef(t):
#        if t is None:
#            return None
#        try:
#            (x,y) = t.type
#            return types.FType(x)
#        except (TypeError, ValueError):
#            raise types.FTypeException("%s not of type a * b" % t)
#    return Lift(lambda (x,y): x, type_fun=typef)
#
## Snd : Ef (a * b) b
#def Snd():
#    def typef(t):
#        if t is None:
#            return None
#        try:
#            (x,y) = t.type
#            return types.FType(y)
#        except (TypeError, ValueError):
#            raise types.FTypeException("%s not of type a * b" % t)
#    return Lift(lambda (x,y): y, type_fun=typef)
#
## Swap : EF (a * b) (a * b)
#def Swap():
#    # TODO(astory): I think this is supposed to be a,b -> b,a
#    def typef(t):
#        if t is None:
#            return None
#        try:
#            (x,y) = t.type
#            return types.FType((y,x))
#        except (TypeError, ValueError):
#            raise types.FTypeException("%s not of type a * b" % t)
#    return Lift(lambda (x,y): (y,x), type_fun=typef)
#
## Second : EF c d -> EF (a * c) (a * d)
#def Second(ef2):
#    return Compose(Swap(),Compose(First(ef2),Swap()))
#
## Pair : EF a b * EF c d -> EF (a * c) -> EF (b * d)
#def Pair(ef1,ef2):
#    return Compose(First(ef1),Second(ef2))
#
## Ticks : EF a int
#def Ticks():
#    def type_fun(t):
#        try:
#            (a, i) = t.type
#        except (TypeError, ValueError):
#            raise types.FTypeException("%s not of type a * integer" % t)
#        if i != "integer":
#            raise FTypeException("%s not of type %s" % (t, "a * integer"))
#        return types.FType(("integer", "integer"))
#    return LoopPre(0, Lift(
#            lambda (x,y): (y,y+1),
#            type_fun=type_fun), c_type=types.FType("integer"))
#    
## Reverse : EF string string
#def Reverse():
#    return Lift(lambda x: ''.join([y for y in reversed(x)]),
#            in_type=types.FType("string"),
#            out_type=types.FType("string"))
#
## Split2 : EF string (string * string)
#def Split2(s):    
#    def f(x):
#        try: 
#            i = x.index(s)
#            x1 = x[:i]
#            x2 = x[i+1:len(x)]
#            return (x1,x2)
#        except ValueError:
#            return (x,"")        
#    return Lift(f, types.FType("string"), types.FType(("string", "string")))
#
## Tabulate : EF string ((string, string list) dictionary)
#def Tabulate():
#    def f(((k,v),d)):
#        if not d.has_key(k):
#            d[k] = []        
#        d[k].append(v)
#        return (d,d)
#    # TODO(astory): list, dictionary types
#    return LoopPre({},Lift(f,
#            in_type=types.FType("string"),
#            out_type=types.FType((("string", "string list"), "dictionary"))),
#           c_type="dictionary")
#
## TopK : int -> EF ((string,string list) dictionary) (string list)
#def TopK(n):
#    def f(d):
#        l = []
#        for (k,v) in d.items():
#            i = reduce(lambda x,w: x + len(w),v,0)
#            l.append((k,i))
#        l.sort(cmp=lambda (k1,i1),(k2,i2): cmp(i1,i2))
#        return map(lambda (k,i):k, l[-n:])
#    return Lift(f,
#            in_type=types.FType((("string", "string list"), "dictionary")),
#            out_type=types.FType("string list"))
#
## Calm : EF a a
#def Calm():
#    def calmfilter((x,(isMatched,y))): 
#        return isMatched
#    def calmupdate((x,(isLastMatched,y))):
#        if isLastMatched:
#            return (x, (x != y, x))
#        else:
#            return (x, (True, x))
#    def type_fun(tup):
#        (x,(boolean,y)) = tup.type
#        if boolean != "boolean":
#            raise types.FTypeException(
#                "%s is not of type %s" % (tup, "a * (boolean * a)"))
#        return tup
#    return LoopPre(
#        (False,None),
#        Compose(
#            Lift(calmupdate, type_fun=type_fun),
#            Filter(calmfilter)),
#        c_type=types.FType(("boolean", None)))
#
## Beacon : E int
#def Beacon(n):
#    return\
#        Seconds() >>\
#        (Lift(lambda x:(x // n)*n,types.FType("integer"), types.FType("integer")) >> Calm())
#
## Divide : ((a * a) -> Bool) -> EF a (E a)
#def Divide(feq):
#    return (Group(lambda x:None, b_type=types.FType(None)) >>
#            Regroup(feq) >>
#            Snd())
#
## GroupByTime : ???
#def GroupByTime(n):
#    # types intentionally left none
#    return (Lift(lambda (o,ps): (o,Merge(ps,Beacon(n)))) >>
#            Regroup(net.beacon_sp()))
#
## ReGroupByTime : ???
#def ReGroupByTime(n):
#    # types intentionally left none
#    def add_beacon((x, s)):
#        return (x, Merge(s, Beacon(n)))
#    def append_packets(l, (p,b)):
#        if (p is None):
#            return l[:]
#        else:
#            return list_append(l, p)
#    sf = (Lift(add_beacon) >> 
#          Regroup(net.beacon_sp()) >>
#          Ungroup(None, append_packets, []))
#    return sf
#
## SplitByTime : ???
#def SplitByTime(n):
#    # types intentionally left none
#    sf = (Group(constant_gp(None)) >>
#          ReGroupByTime(n) >>
#          Snd())
#    return sf
#    
## Accum : a * (a -> b -> a) -> EF b a
#def Accum(init, f, init_type=None):
#    def body((next,last)):
#        newout = f(next,last)
#        return (newout,newout)
#    return LoopPre(init, Lift(body,type_fun=lambda x:init_type), c_type=init_type) 
# 
## Tally : EF a int
#def Tally():
#  return Lift(lambda x: 1, type_fun=lambda a : types.FType("integer"))
#
## Sum : EF int int
#def Sum():
#  return Accum(0,(lambda x, y: x + y), init_type=types.FType("integer"))
#
## LiftF : (unit -> EF a b) -> EF (E a) (E b)
#def LiftF(eff):
#    # TODO(astory): figure out typing
#    return Lift(lambda ea: Apply(ea, eff ()))
#
## StickyMerge : E (a option) * E (b option) -> E (a * b)
#def StickyMerge(e1,e2):
#    # TODO(astory): figure out typing
#    def f(((x,y),(xl,yl))):
#        retx = xl if (x is None) else x
#        rety = yl if (y is None) else y
#        return ((retx,rety),(retx,rety))
#    return (Apply(Merge(e1,e2),LoopPre((None,None),Lift(f))))
#
###############
## UNIT TESTS #
###############
#
#unit_test([1,2,3,4,5],Dup(),[(1,1),(2,2),(3,3),(4,4),(5,5)])
#unit_test([1,2,3,4,5,6,7,8,9,10], Filter(lambda x: x == 5), [5])
#unit_test([1,2,3,4,5,6,7,8,9,10], Filter(lambda x: x > 5), [6,7,8,9,10])
#unit_test([1,10,100,1000,10000],Tally(),[1,1,1,1,1])
#unit_test([1,10,100,1000,10000],Compose(Tally(),Sum()),[1,2,3,4,5])
#unit_test_merge([('a',None),(None,1),('b',None),(None,2),(None,3),('c',None)],
#                StickyMerge,
#                [('a',None),('a',1),('b',1),('b',2),('b',3),('c',3)])
#
## functional list append
#def list_append(l,x):
#    l_copy = l[:]
#    l_copy.append(x)
#    return l_copy
#
#unit_test([1,2,3,4,5,6,7,8,9,10],
#           Group(lambda x: x / 5) >>
#           Regroup(lambda x,y:True) >>
#           Ungroup(None,list_append,[]),
#          [(0,[1,2,3,4]), (1,[5,6,7,8,9]), (2,[10])])
#
#unit_test([1,2,3,4,5,6,7,8,9,10],
#           Group(lambda x: x / 5) >>
#           Regroup(lambda x,y:False) >>
#           Ungroup(None,lambda acc,x:x,None),
#           [(0,1),(0,2),(0,3),(1,5),(1,6),(1,7),(1,8),(0,4),(1,9),(2,10)])
#
#unit_test([1,2,3,4,5,6,7,8,9,10],
#           Group(lambda x: x / 5) >>
#           Regroup(lambda x,y:False) >>
#           Ungroup(1,lambda acc,x:x,None),
#           [(0,1),(0,2),(0,3),(0,4),(1,5),(1,6),(1,7),(1,8),(1,9),(2,10)])
#
#unit_test([1,2,3,4,5,6,7,8,9,10],
#           Divide(lambda x1,x2: x1 / 5 == x2 / 5) >>
#           Lift(lambda x: (None,x)) >>
#           Ungroup(None,list_append,[]) >>
#           Snd(),
#           [[1,2,3,4],[5,6,7,8,9],[10]])

###########
# Queries #
###########

# Queries 
#class FQuery:
#  def __init__(self,typ,num,fp,gp,sp,win,compose):
#    self.complete = False
#    self.typ = typ
#    self.num = num
#    self.fp = fp
#    self.gp = gp
#    self.sp = sp
#    self.win = win
#    self.compose = compose
#  def __rshift__(self,other):
#    other_name = other.__class__.__name__ 
#    if other_name == "FEventFun":
#      return Subscribe(self) >> other
#    elif other_name == "FListener":
#      return Subscribe(self) >> other
#    else:
#      raise util.IllegalArgument("Cannot compose FQuery and %s" % other_name)
#  def __mul__(self,other):
#    other_name = other.__class__.__name__ 
#    if other_name == "FQuery":
#      return other.compose(self)
#    else:
#      raise util.IllegalArgument("Cannot compose FQuery and %s" % other_name)
#
#def Select(x):
#  def compose(q):
#    q.typ = x
#    q.complete = True
#    return q
#  q = FQuery(x,None,net.true_fp(),[],[],None,compose)
#  q.complete = True
#  return q
#
#def Where(fp):
#  def compose(q):
#    q.fp = net.and_fp([q.fp,fp])
#    return q
#  return FQuery('packets',None,fp,[],[],None,compose)
#
#def GroupBy(gp):
#  def compose(q):
#    for f in gp:
#      q.gp.append(f)
#    return q
#  return FQuery('packets',None,net.true_fp(),gp,[],None,compose)
#
#def SplitWhen(sp):
#  def compose(q):
#    for f in sp:
#      q.sp.append(f)
#    return q
#  return FQuery('packets',None,net.true_fp(),[],sp,None,compose)
#
#def Limit(num):
#  def compose(q):
#    q.num = num
#    return q
#  return FQuery('packets',num,net.true_fp(),[],[],None,compose)
#
#def Every(win):
#  def compose(q):
#    q.win = win
#    return q
#  return FQuery('packets',None,net.true_fp(),[],[],win,compose)
#
## subscribe
#def Subscribe(q):
#  if not q.complete:
#    raise util.IllegalArgument("FQuery must at least have a Select")
#  return rts.add_query(q)
#
##packets
#def Packets():
#  return Subscribe(Select('packets'))

import simpy
import random
import os
import binascii
from copy import deepcopy, copy


from collections import defaultdict

class Block(object):
    """ Represent a block sent by a node """

    def __init__(self, nid, xround, xid, payload=()):
        self.id = (nid, xround, xid)
        self.payload = tuple(payload)

    def prev_block(self):
        _, xround, _ = self.id
        if xround == 0:
            return None

        return self.payload[0]     



class BlockStore(object):
    """ Stores blocks received and delivers them when they become valid. """

    def __init__(self):
        """ Initialize a block store that tracks past dependencies."""
        self.store = {}
        self.active = {}
        self.dep = defaultdict(list)
        self.listener = None


    def insert_block(self, block):
        """ Insert a block into the store, and callback a listener for blocks ready to process."""
        ID = block.id

        # Insert the block in the store
        if ID not in self.store:
            self.store[ID] = block

            # Update dependencies
            for item in block.payload:
                if type(item) == Block: # Its a block
                    self.dep[tuple(item.id)] += [ block ]

            self._try_activate(block)


    def _try_activate(self, block):
        assert block.id in self.store

        # Update dependencies
        ready_to_activate = True
        for item in block.payload:
            if type(item) == Block:
                if item.id not in self.active:
                    ready_to_activate = False

        if ready_to_activate:
            self.active[block.id] = True

            # Call the listener
            if self.listener:
                self.listener(block)

            # Try to activate all dependencies
            for item in self.dep[block.id]:
                self._try_activate(self.store[item.id])

    def register_listener(self, listen):
        """ Register a callback listener to process ready blocks."""
        self.listener = listen


def test_dep():
    bs = BlockStore()
    T0 = Block(0, 0, "XXX", [])
    T1 = Block(2, 1, "YYY", [T0])
    T2 = Block(3, 1, "ZZZ", [T0])
    T3 = Block(4, 1, "AAA", [T2, T1])
    T4 = Block(5, 1, "AAA", [T3])

    bs.insert_block(T1)
    assert len(bs.active) == 0

    bs.insert_block(T2)
    assert len(bs.active) == 0

    bs.insert_block(T0)
    assert len(bs.active) == 3

    bs.insert_block(T4)
    assert len(bs.active) == 3

    bs.insert_block(T3)
    assert len(bs.active) == 5

    print(bs.active)


class StateAnnotator(object):

    def __init__(self, node_ids):
        """ Define the set of nodes over which to get consensus. """
        self.node_ids = node_ids
        self.block_states = {}
        self.final = {}

        self.row = {}
        self.max_row = 0

        self.trace = set([(3, 102)])


    def process_block(self, block):
        prev = block.prev_block()
        if prev is not None:
            state = deepcopy(self.block_states[prev.id])
        else:
            state = defaultdict(list)
            state["final"] = {}
            state["delay"] = {}
            state["timeouts"] = defaultdict(list) # map int -> struct

        self.block_states[block.id] = state

        # Make prepropose for received block
        (nid, xround, xid) = block.id
        pp = ("pp", nid, xround, 0, xid) # pre-propose
        
        messages = [ pp ] # Add the prepropose to messages.
        out = [ pp ] # also output a preprepare message

        # Estimate the timeout of this block:
        for item in block.payload:
            if type(item) == Block:
                other_nid, other_round, _ = item.id
                state["delay"][other_nid] = abs(xround - other_round)

        # TODO: make byzantine by picking the 2/3rd largest value
        TIMEOUT = max([1] + list(state["delay"].values())) * 10 # Magic: 10
        state["TIMEOUT"] = max(1, TIMEOUT)

        for other_nid in self.node_ids:
            state["timeouts"][xround + TIMEOUT] += [( other_nid, xround, 0 )]

            if (other_nid, xround) in self.trace:
                CSTART = '\33[92m'
                CEND   = '\33[0m'
                print(CSTART + "%s: (%s, %s) SET TIMEOUT v=0" % (xround, other_nid, xround) + CEND)


        # Detect all timeouts
        if xround in state["timeouts"]:
            for (to_nid, to_round, to_view) in state["timeouts"][xround]:
                if (to_nid, to_round, "final") not in state:

                    # Get the current view_n for this block
                    current_v = 0
                    if (to_nid, to_round, "v") in state:
                        current_v = state[(to_nid, to_round, "v")]
                    state[(to_nid, to_round, "v")] = current_v

                    if current_v > to_view:
                        continue # We have already moved view

                    if (to_nid, to_round) in self.trace:
                        CSTART = '\33[92m'
                        CEND   = '\33[0m'
                        print(CSTART + "%s: (%s, %s) TIMEOUT: new_v=%s" % (xround, to_nid, to_round, to_view) + CEND)

                    prepared_xid = None
                    if (to_nid, to_round, to_view, "prepared") in state:
                        prepared_xid = state[(to_nid, to_round, to_view, "prepared")]
                        
                    # Emit a view change                    
                    vc = ("vc", to_nid, to_round, to_view + 1, prepared_xid, nid)
                    state[(to_nid, to_round, "v")] += 1
                    messages += [ vc ]
                    out += [ vc ]

        # Process messages from this block.
        out += self._process_messages(state, nid, nid, block.id,  messages)

        # Process messages from other blocks.
        for item in block.payload:
            if type(item) == Block:
                other_nid, _, _ = item.id
                block_state = self.block_states[item.id]
                messages = block_state[ "messages" ]

                out += self._process_messages(state, other_nid, nid, block.id,  messages)

        state[ "messages" ] = out

    def _process_messages(self, state, other_nid, nid, bid, messages):
        out = []
        messages = copy(messages)
        while len(messages) > 0:
            msg = messages.pop(0)
            new = self.process_msg(state, msg, other_nid, nid, bid)
            messages += new
            out += new
        return out        


    def get_v(self, state, msg):
        """ Get the view number for a state."""
        _, nid, xround = msg[:3]
        if (nid, xround, "v") in state:
            v = state[(nid, xround, "v")]
        else:
            state[(nid, xround, "v")] = 0
            v = 0
        return v


    def get_vcs(self, state, msg, v = None):
        # TODO: check that we should store view changes by view number?
        if v is None:
            v = self.get_v(state, msg)

        _, nid, xround = msg[:3]
        if (nid, xround, v, "vcs") not in state:
            state[(nid, xround, v, "vcs")] = {}
        return state[(nid, xround, v, "vcs")]


    def augment_pr_pp(self, state, nid, xround, xv, xid):
        """ Create a store for preprepares and prepares. """
        if (nid, xround, xv, xid) not in state:
            state[(nid, xround, xv, xid)] = (set(), set())
        return  state[(nid, xround, xv, xid)]



    def process_msg(self, state, msg, sender, receiver, orig_block):
        xtype = msg[0]
        out = []

        orig_nid, orig_round, orig_xid = orig_block

        # If a decision was made on this block -- shortcut any further messages.
        nid, xround = msg[1:3]
        if (nid, xround) in state["final"]:
            return out

        v = self.get_v(state, msg)

        if xtype == "pp":
            # pre-propose mesage
            _, nid, xround, xv, xid = msg
            if xv == v: # TODO: and valid view!
                if (nid, xround, xv, "pp") not in state:
                    assert xv == 0 or (nid, xround, xv, "HNV") in state

                    # We have not prepared anything.
                    prs, cms = self.augment_pr_pp(state, nid, xround, xv, xid)

                    p = ("pr", nid, xround, xv, xid, receiver)
                    out += [ p ]
                    state[(nid, xround, xv, "pp")] = msg

                    prs.add(sender)
                    prs.add(receiver)
            else:
                #print("Received in view=%s for view=%s" % (v, xv))
                #print("LOST: %s" % str(msg))
                pass

        elif xtype == "pr":
            # propose message
            _, nid, xround, xv, xid, xfrom = msg
            if xv == v:
                assert xv == 0 or (nid, xround, xv, "HNV") in state
                prs, cms = self.augment_pr_pp(state, nid, xround, xv, xid)
                
                if xfrom not in prs:
                    prs.add(xfrom)

                    if len(prs) == 3: # TODO: 2f+1
                        assert receiver not in cms
                        
                        # Send commit
                        c = ("cm", nid, xround, xv, xid, receiver)
                        cms.add(receiver)
                        out += [ c ]

                        # Update marker
                        if (nid, xround, xv, "prepared") not in state:
                            state[(nid, xround, xv, "prepared")] = xid
                        assert state[(nid, xround, xv, "prepared")] == xid

        elif xtype == "cm":
            # commit message
            _, nid, xround, xv, xid, xfrom = msg
            if xv <= v:
                prs, cms = self.augment_pr_pp(state, nid, xround, xv, xid)
                
                if xfrom not in cms:
                    cms.add(xfrom)
                    
                    if len(cms) == 3: # TODO: 2f+1
                        
                        # Deliver
                        if (nid, xround) not in state["final"]: 
                            # Deliver only once
                            state["final"][(nid, xround)] = xid

                            # TODO: clean up all state related to this (nid, xround)
                            clean_up = 0
                            for K in list(state):
                                if K[:2] in state["final"]:
                                    del state[K]
                                    # print(K)
                                    clean_up += 1
                            #print("Cleaned: ", clean_up)

                            self.deliver(nid, xround, xid, receiver, orig_block, v, state)


                        if (nid, xround) not in self.final:
                            self.final[(nid, xround)] = xid
                        else:
                            # Check consensus
                            assert self.final[(nid, xround)] == xid
            else:
                #print("Received in view=%s for view=%s" % (v, xv))
                #print("LOST", msg)
                assert False

        elif xtype == "vc":

            # view change message
            _, nid, xround, xv, xid, xfrom = msg
            vcs = self.get_vcs(state, msg, xv)

            if xfrom not in vcs:
                vcs[xfrom] = xid

                if xv >= v and len(vcs) == 3:
                        
                    # Increase the view number
                    state[(nid, xround, "v")] = xv

                    # Which xid to go for:
                    all_xids = set(vcs.values())
                    if len(all_xids) == 1:
                        assert all_xids == set([ None ])
                        new_xid = None
                    else:
                        assert len(all_xids) == 2
                        all_xids.remove(None)
                        new_xid = list(all_xids)[0]
                    
                    nv = ("nv", nid, xround, v, new_xid, receiver)
                    out += [ nv ]

        elif xtype == "nv":
            _, nid, xround, xv, new_xid, xfrom = msg
            if xv >= v:
                if (nid, xround, xv, "HNV") not in state:
                    # Increment if needed the view_n
                    state[(nid, xround, "v")] = xv
                    
                    # Set a new timeout
                    (_, orig_round, _) = orig_block
                    state["timeouts"][orig_round + state["TIMEOUT"] * 2**xv] += [( nid, xround, xv )]
                    state[(nid, xround, xv, "HNV")] = True

                    if (nid, xround) in self.trace:
                        CSTART = '\33[92m'
                        CEND   = '\33[0m'
                        print(CSTART + "%s: (%s, %s) NV: %s SET TIMEOUT v=%s" % (orig_round, nid, xround, new_xid, xv) + CEND)


                    # Inject a preprepare
                    pp = ("pp", nid, xround, xv, new_xid)
                    out += [ pp ]
            
        else:
            assert False

        nid, xround = msg[1:3]
        if (nid, xround) in self.trace:
            CSTART = '\33[93m'
            CEND   = '\33[0m'

            print(CSTART + "%s: (%s, %s) --- %s" % (orig_round, nid, xround, str(msg)) + CEND)
            CSTART = '\33[92m'
            for K in state:
                if K[:2] == (nid, xround):
                    print(CSTART + "%s: (%s, %s) %s = %s" % (orig_round, nid, xround, str(K), str(state[K])) + CEND)

        return out


    def deliver(self, nid, xround, final_xid, receiver, orig_block, viewn, state):
        if final_xid:
            final_xid = final_xid[:6]

        # Compute size stats:
        c = len(state)

        if final_xid != None:
            CSTART = '\033[94m' # green
        else:
            CSTART = '\033[91m' # red
        CEND = '\033[0m'

        if (nid, xround) not in self.final:


            if xround not in self.row:
                self.row[xround] = set()
            self.row[xround].add( nid )

            if self.max_row not in self.row:
                self.row[self.max_row] = set()                

            while len(self.row[self.max_row]) == 4:
                self.max_row += 1
                if self.max_row not in self.row:
                    self.row[self.max_row] = set()                

            # print(self.max_row, self.row[self.max_row])
            print("%s(%s) Deliver final: (%s, %s): %s (at block: %s view: %s mem: %s full:%s)%s" % (CSTART, receiver, nid, xround, final_xid, orig_block[1], viewn, c, self.max_row, CEND))


class Node(object):
    def __init__(self, env, nid):
        self.env = env
        self.nid = nid
        self.block = 0
        self.action = env.process(self.run())

        self.received = []

        self.blockstore = BlockStore()
        self.blockstore.register_listener(self.validBlock)
        
        self.sa = StateAnnotator([0, 1, 2, 3])
        self.faulty = False

    def run(self):
        while True:
            # print("(%s) Seal block %s" % (self.nid, self.block))

            # Seal a new block
            idx = binascii.hexlify(os.urandom(16))
            block = Block(self.nid, self.block, idx, self.received)
            net.broadcast(self, block)
            self.block += 1

            # Always include a link to the previous block
            self.received = [ ]
            self.getBlock( block )

            # Wait until sealing the next block
            delay = random.expovariate(1.0 / BLOCK_JITTER)
            yield self.env.timeout(BLOCK_INTERVAL + delay)


    def getTransaction(self, T):
        # print("(%s) Got transaction %s" % (self.nid, T))
        self.received += [ T ]


    def getBlock(self, block):
        self.blockstore.insert_block(block)


    def validBlock(self, block):
        # print("(%s) Got block %s" % (self.nid, block.id))
        self.received += [ block ]
        if self.nid == 0:
            self.sa.process_block( block )


class Client(object):
    def __init__(self, env, nodes):
        self.env = env
        self.nodes = nodes
        self.action = env.process(self.run())
        self.transaction = 0


    def run(self):
        while True:
            delay = random.expovariate(1.0/0.2)
            yield self.env.timeout(delay)
            N = random.choice(self.nodes)
            N.getTransaction((self.transaction,))
            self.transaction += 1


class Network(object):
    def __init__(self, env, nodes):
        self.env = env
        self.nodes = nodes


    def send(self, n, block, delay=None):
        if delay is None:
            delay = random.expovariate(1.0/NETWORK_DELAY)
        yield self.env.timeout(delay)
        n.getBlock(block)


    def broadcast(self, from_n, block):
        for nx in self.nodes:
            if from_n.faulty:
                continue
            if nx.nid != from_n.nid:
                env.process(self.send(nx, block))
            else:
                env.process(self.send(nx, block, 0.0))


if __name__ == "__main__":
    random.seed(12)
    BLOCK_JITTER = 0.001 # 0.2
    NETWORK_DELAY = 5.0
    BLOCK_INTERVAL = 2.0

    env = simpy.Environment()
    nodes = [Node(env, nid) for nid in range(4)]
    nodes[3].faulty = True
    sender = Client(env, nodes)
    net = Network(env, nodes)

    env.run(until=500)
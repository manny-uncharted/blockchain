import logging
from socket import timeout
import time
import yaml
import argparse
from random import randint, random
from collections import Counter
import json
import sys

import asyncio
import aiohttp
from aiohttp import web
import hashlib

VIEW_SET_INTERVAL = 10

class View:
    def __init__(self, view_num, num_nodes):
        self._view_num = view_num
        self._num_nodes = num_nodes
        self._leader = view_num % num_nodes
        # Minimum interval to set the view number
        self._min_set_interval = VIEW_SET_INTERVAL
        self._last_set_time = time.time()

    # encoding the data to json
    def get_view(self):
        return self._view_num

    def set_view(self, view):
        """Returns True if it successfully updates view number
        Returns False otherwise"""
        if time.time - self._last_set_time < self._min_set_interval:
            return False
        self._last_set_time = time.time()
        self._view_num = view
        self._leader = view % self._num_nodes

    def get_leader(self):
        return self._leader

class Status:
    """Records the state of every slot"""
    PREPARE = "prepare"
    COMMIT = "commit"
    REPLY = "reply"

    def __init__(self, f):
        self.f = f
        self.request = 0
        self.prepare_msg = {}
        self.prepare_certificate = None # proposal
        self.commit_msg = {}
        """This sets a condition in which tthe node can only receive more than 2f +1 commit messages, but cannot commit if there are any bubbles previously."""
        self.commit_certificate = None # proposal

        # Set it to True only after commit
        self.is_committed = False

    class Certificate:
        def __init__(self, view, proposal=0):
            """
            input: 
                view: object of class View
                proposal: proposal in json_data(dict)
            """
            self._view = view
            self._proposal = proposal
        
        def to_dict(self):
            """
            Converting the Certificate to dictionary
            """
            return {
                'view': self._view.get_view(),
                'proposal' : self._proposal
            }

        def dumps_from_dict(self, dictionary):
            """
            Update the view from the form after self.to_dict
            Accepts input:
                dictionary = {
                    'view' : self._view.get_view(),
                    'proposal': self._proposal
                }
            """
            self._view.set_view(dictionary['view'])
            self._proposal = dictionary['proposal']

        def get_proposal(self):
            return self._proposal

    
    class SequenceElement:
        def __init__(self, proposal):
            self.proposal = proposal
            self.from_nodes = set([])

    def _update_sequence(self, msg_type, view, proposal, from_node):
        """
        Updates the record in the status by message type
        Accepts Input:
            msg_type: Status.PREPARE or Status.COMMIT
            view: View object of self._follow_view
            proposal: proposal in json_data
            from_node: The node sending the given message.
        """
        """The keys need to include the hash(proposal) function to account for situations where we get different proposals from BFT nodes. 
        We need to sort key in json.dumps to make sure that we are getting the same string everytime we call json.dumps. 
        We would use hashlib md5 so we can get the same hash each time."""
        hash_object = hashlib.md5(json.dumps(proposal, sort_keys=true).encode('utf-8'))
        key = (view.get_view(), hash_object.digest())
        if msg_type == Status.PREPARE:
            if key not in self.prepare_msg:
                self.prepare_msg[key] = self.SequenceElement(proposal)
            self.prepare_msg[key].from_nodes.add(from_node)
        elif msg_type == Status.COMMIT:
            if key not in self.commit_msg:
                self.commit_msg[key] = self.SequenceElement(proposal)
            self.commit_msg[key].from_nodes.add(from_node)
    

    def _check_majority(self, msg_type):
        """
        Checks if the node recieves more than 2f + 1 given type message in the same view.
        Accepted Argument:
            msg_type: self.PREPARE or self.COMMIT
        """
        if msg_type == Status.PREPARE:
            if self.prepare_certificate:
                return True
            for key in self.prepare_msg:
                if len(self.prepare_msg[key].from_nodes)>= 2 * self.f + 1:
                    return True
            return False
        
        if msg_type == self.COMMIT:
            if self.commit_certificate:
                return True
            for key in self.commit_msg:
                if len(self.commit_msg[key].from_nodes) >= 2 * self.f + 1:
                    return True
            return False

class CheckPoint:
    """
    Records all the status of the checkpoint for the given PBFTHandler
    """
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote' # receives checkpoint vote
    def __init__(self, checkpoint_interval, nodes, f, node_index, lose_rate = 0, network_timeout = 10):
        self._checkpoint_interval = checkpoint_interval
        self._nodes = nodes
        self._f = f
        self._node_index = node_index
        self._loss_rate = lose_rate
        self._log = logging.getLogger(__name__)
        """Defining the next slot of the given globally accepted checkpoint."""
        self.next_slot = 0
        # Globally accepted checkpoint
        self.checkpoint = []
        
        """Using the hash of the checkpoint to record votes for the given checkpoint"""
        self._received_votes_by_ckpt = {}
        self._session = None
        self._network_timeout = network_timeout

        self._log.info("---> %d: Create Checkpoint.", self._node_index)

    # class to record the status of received checkpoints
    class ReceiveVotes:
        def __init__(self, ckpt, next_slot):
            self.from_nodes = set([])
            self.checkpoint = ckpt
            self.next_slot = next_slot

        def get_commit_upperbound(self):
            """This is to return the upperbound that could commit (return upperbound = true upperbound +1)"""
            return self.next_slot + 2 * self._checkpoint_interval

        def _hash_ckpt(self, ckpt):
            """
            Accepted Arguments:
                ckpt: the checkpoint to be hashed
            output:
                hash_object: the hash of the checkpoint in the format of a binary string
            """
            hash_object = hashlib.md5(json.dumps(ckpt, sort_keys=true).encode('utf-8'))
            return hash_object.digest()

        async def receive_vote(self, ckpt_vote):
            """
            Trigger when pBFTHandler receives checkpoint votes.
            First, we update the checkpoint status. Second, update the checkpoint if more than 2f + 1 nodes agree with the given checkpoint
            Input:
                ckpt_vote = {
                    'node_index': self._node_index
                    'next_slot': self._next_slot + self._checkpoint_interval
                    'ckpt': json.dumps(ckpt)
                    'type': 'vote'
                }
            """
            self._log.debug("---> %d: Received Checkpoint votes", self._node_index)
            ckpt = json.loads(ckpt_vote['ckpt'])
            next_slot = ckpt_vote['next_slot']
            from_node = ckpt_vote['node_index']


            hash_ckpt = self._hash_ckpt(ckpt)
            if hash_ckpt not in self._received_votes_by_ckpt:
                self._received_votes_by_ckpt[hash_ckpt] = (
                    CheckPoint.ReceiveVotes(ckpt, next_slot)
                )
            status = self._received_votes_by_ckpt[hash_ckpt]
            status.from_nodes.add(from_node)
            for hash_ckpt in self._received_votes_by_ckpt:
                if (self._received_votes_by_ckpt[hash_ckpt].next_slot > self.next_slot and len(self.received_votes_by_ckpt[hash_ckpt].from_nodes) >= 2* self._f +1):
                    self._log,info("---> %d: Update Checkpoint by receiving votes", self._node_index)
                    self.next_slot = self._received_votes_by_ckpt[hash_ckpt].next_slot
                    self.checkpoint = self._received_votes_by_ckpt[hash_ckpt].checkpoint

        async def propose_vote(self, commit_decisions):
            """When the node slot of committed messages exceed self._next_slot + self._checkpoint_interval, propose new checkpoint to broadcast every node
            Accepted Argument:
                commit_decision: list of tuple[((client_index, client_sequence), data), ....]
            Output: 
                next_slot for the new update and garbage collection of the Status object.
            """
            proposed_checkpoint = self.checkpoint + commit_decisions
            await self._broadcast_checkpoint(proposed_checkpoint,
                'vote', CheckPoint.RECEIVE_CKPT_VOTE)

        async def _post(self, nodes, command, json_data):
            """
            Broadcats json_data to all nodes in network with given command.
            Accepted Argument: 
                nodes: list of nodes
                command: action
                json_data: data in json format.
            """
            if not self._session:
                timeout = aiohttp.ClientTimeout(self._network_timeout)
                self.session = aiohttp.ClientSession(timeout=timeout)
            for i, node in enumerate(nodes):
                if random() > self._loss_rate:
                    self._log.debug("make request to %d, %s", i, command)
                    try:
                        await self._seesion.post(
                            self.make_url(node, command), json=json_data
                        )
                    except Exception as e:
                        self._log.error(e)
                        pass
        
        @staticmethod
        def make_url(node, command):
            """
            Accepted Argument:
                node: dictionary with key of host(url) and port number
                command: action
            Output: 
                The url to send with the given node and action.
            """
            return "http://{}:{}/{}".format(node['host'], node['port'], command)

        async def _broadcast_checkpoint(self, ckpt, msg_type, command):
            json_data = {
                'node_index': self._node_index,
                'next_slot': self.next_slot + self._checkpoint_interval,
                'ckpt': json.dumps(ckpt),
                'type': msg_type
            }
            await self._post(self._nodes, command, json_data)

        def get_ckpt_info(self):
            """
            Get the checkpoint serialized information called by synchronize function to get the checkpoint information.
            """
            json_data = {
                'next_slot': self.next_slot,
                'ckpt': json_dumps(self.checkpoint)
            }
            return json_data

        
        def update_checkpoint(self, json_data):
            """updates the checkpoint when input checkpoint cover more slots than the currently available slots
            Accepts Arguments:
                json_data = {
                    'next_slot': self._next_slot,
                    'ckpt': json.dumps(ckpt)
                }
            """
            self._log.deug("update_checkpoint: next_slot: %d; update_slot: %d", self.next_slot, json_data['next_slot'])
            if json_data['next_slot'] > self.next_slot:
                self._log.info("---> %d: Update checkpoint by synchronization.", self._node_index)
                self.next_slot = json_data['next_slot']
                self.checkpoint = json.loads(json_data['ckpt'])
        

        async def receive_sync(sync_ckpt):
            """Triggers when it receives checkpoint synchronization maessages
            input: 
                sync_ckpt = {
                    'node_index': self._node_index
                    'next_slot': self._next_slot + self._checkpoint_interval
                    'ckpt': json.dumps(ckpt)
                    'type': 'sync'
                }
            """
            self._log.debug("receive_sync in checkpoint: current next_slot:" "%d; update to: %d", self.next_slot, json_data['next_slot'])

            if sync_ckpt['next_slot'] > self._next_slot:
                self.next_slot = sync_ckpt['next_slot']
                self.checkpoint = json.loads(sync_ckpt['ckpt'])

        
        async def garbage_collection(self):
            """Cleans those ReceieveCKPT objects whose next_slot is smaller than or equal to the current."""

            deletes = []
            for hash_ckpt in self._received_votes_by_ckpt:
                if self._received_votes_by_ckpt[hash_ckpt].next-slot <= next_slot:
                    deletes.append(hash_ckpt)
            for hash_ckpt in deletes:
                del self._received_votes_by_ckpt['hash_ckpt']

class ViewChangeVotes:
    """
    Record  which nodes vote for the proposed view change.
    In addition, store all the information including:
    1. Checkpoints who has the largest information(largest next_slot)
    2. Prepare certificate with largest for each slot sent from voted nodes.
    """
    def __init__(self, node_index, num_total_nodes):
        # Current node index
        self._node_index = node_index
        # Total number of node in the system.
        self._num_total_nodes = num_total_nodes
        # Number of faults tolerant
        self._f = (self._num_total_nodes -1) // 3
        # Record the nodes for current view
        self.from_nodes = set()
        # The prepare_certificate with highest view for each slot
        self.prepare_certificate_by_slot = {}
        self.latest_checkpoint = None
        self._log = logging.getLogger(__name__)
    
    def receive_vote(self, json_data):
        """
        Receive the vote message and make the update:
        1. Updates the information in given vote storage - prepares certificate.
        2. Updates the node in from_nodes.
        input: 
            json_data: the json_data received by view change vote broadcast:
                {
                    "node_index": self._index,
                    "view_number": self._follow_view.get_view(),
                    "checkpoint": self._ckpt.get_ckpt_info(),
                    "prepared_certificates": self.get_prepare_certificates(),
                }
        """
        update_view = None

        prepare_certificates = json_data["prepare_certificates"]
        self._log.debug("%d update prepare_certificates for view %d",self._node_index, json_data['view_number'])

        for slot in prepare_certificates:
            prepare_certificate = Status.Certificate(View(0, self._num_total_nodes))
            # keeping the prepare certificate who has the largest view number
            if slot not in self.prepare_certificate_by_slot or (
                self.prepare_certificate_by_slot[slot]._view.get_view() < (prepare_certificate._view.get_view())):
                self.prepare_certificate_by_slot[slot] = prepare_certificate
        self.from_nodes.add(json_data['node_index'])

class PBFTHandler:
    REQUEST = "request"
    PREPREPARE = "preprepare"
    PREPARE = "prepare"
    COMMIT = "commit"
    REPLY = "reply"
    NO_OP = "NOP"
    RECEIVE_SYNC = "receive_sync"
    RECEIVE_CKPT_VOTE = "receive_ckpt_vote"
    VIEW_CHANGE_VOTE = "view_change_vote"
    VIEW_CHANGE_REQUEST = "view_change_request"

    def __init__(self, index, conf):
        self._nodes = conf['nodes']
        self._node_cnt = len(self._nodes) # node count
        self._index = index
        # Number of faults tolerant
        self._f = (self._node_cnt -1) // 3

        # leader
        self._view = View(0, self._node_cnt)
        self._next_propose_slot = 0

        # TODO: Test fixed
        if self._index == 0:
            self._is_leader = True
        else:
            self._is_leader = False

        # Network Simulation
        self._loss_rate = conf['loss%'] / 100

        # Time configuration 
        self._network_timeout = conf['misc']['network_timeout']

        # Checkpoint
        # After finishing committing self._checkpoint, the next_slot will be updated to the next_slot of the checkpoint
        self._checkpoint_interval = conf['ckpt_interval']
        self._ckpt = Checkpoint(self._checkpoint_interval, self._nodes, self._f, self._index, self._loss_rate, self._network_timeout)

        # Commit
        self._last_commit_slot = -1

        # Indicate the current leader
        # TODO: Test fixed
        self._leader = 0


        # The largest view either promised or accepted by the leader
        self._follow_view = View(0, self._node_cnt)
        # Restore the votes number and information for each view number
        self._view_change_votes_by_view_number = {}

        # Record all the status of the given slot
        # To adjust json key, slot is a string
        self._status_by_slot = {}

        self._sync_interval = conf['sync_interval']


        self._session = None
        self._log = logging.getLogger(__name__)

    
    @staticmethod
    def make_url(node, command):
        """
        input:
            node: dictionary with key of host(url) and port
            command: action
        output:
            url: url to be sent with given node and action
        """

        return "http://{}:{}/{}".format(node['host'], node['port'], command)
    
    async def _make_request(self, nodes, command, json_data):
        """
        Sends json data to given nodes.
        input:
            nodes: list of dictionary with key of host(url) and port
            command: command to be sent
            json_data: json data to be sent
        output:
            response: list of response from given nodes(node_index, response)
        """

        resp_list = []
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                if not self._session:
                    timeout = aiohttp.ClientTimeout(total=self._network_timeout)
                    self._session = aiohttp.ClientSession(timeout=timeout)
                self._log.debug("make request to %d, %s", i, command)
                try:
                    resp = await self._session.post(self.make_url(node, command), json=json_data)
                    resp_list.append((i, resp))

                except Exception as e:
                    self._log.error("make request to %d, %s failed: %s", i, command, e)
                    pass
        return resp_list

    async def _make_response(self, resp):
        """
        Drop response by chance, via sleep for sometime.
        """
        if random() < self._loss_rate:
            await asyncio.sleep(self._network_timeout)
        return resp

    async def _post(self, nodes, command, json_data):
        """
        Broadcast json_data to all node in nodes with given command.
        input:
            nodes: list of nodes
            command: action
            json_data: data in json format
        """

        if not self._session:
            timeout = aiohttp.ClientTimeout(total=self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                self._log.debug("post to %d, %s", i, command)
                try:
                    _ = await self._session.post(self.make_url(node, command), json=json_data)
                except Exception as e:
                    self._log.error(e)
                    pass
    def _legal_slot(self, slot):
        """
        The slot is legal only when it's between upperbound and the lowerbound.
        input:
            slot: slot number (string integer direct get from the json_data proposal key.)
        output:
            boolean: to express the result True: the slot is legal
        """
        if int(slot) < self._ckpt.next_slot or int(slot) >= self._ckpt.get_commit_upperbound():
            return False
        else:
            return True

    async def preprepare(self, json_data):
        """
        Prepare: Deal with request from the client and broadcast to other replicas.
        input:
            json_data: json data web request from the client
                {
                    id: (client_id, client_seq),
                    client_url: "url string",
                    timestamp: "time",
                    data: "string"
                }
        """
        this_slot = str(self._next_propose_slot)
        self._next_propose_slot = int(this_slot) + 1

        self._log.info("---> %d: on preprepare, propose at slot: %d", self._index, int(this_slot))

        if this_slot not in self._status_by_slot:
            self_status_by_slot[this_slot] = Status(self._f)
        self._status_by_slot[this_slot].request = json_data

        preprepare_msg = {
            'leader': self._index,
            'view': self._view.get_view(),
            'proposal': {
                'this_slot': json_data
            },
            'type': 'preprepare'
        }

        await self._post(self._nodes, PBFTHandler.PREPARE, preprepare_msg)

    async def get_request(self, request):
        """
        Handle the request from client if leader, otherwise redirect to the leader.
        """
        self._log.info("---> %d: on request", self._index)

        if not self._is_leader:
            if self._leader != None:
                raise web.HTTPTemporaryRedirect(self.make_url(self._nodes[self._leader], PBFTHandler.REQUEST))
            else:
                raise web.HTTPServiceUnavailable()
        else:
            json_data = await request.json()
            await self.preprepare(json_data)
            return web.Response(text='OK')

    async def propose(self, request):
        """
        Once we recieve pre-prepare message from client, broadcasts the prepare message to all replicas
        input:
            request: preprepare message from preprepare
                preprepare_msg = {
                    'leader': self._index,
                    'view': self._view.get_view(),
                    'proposal': {
                        'this_slot': json_data
                    },
                    'type': 'preprepare'
                }
        """
        json_data = await request.json()

        if json_data['view'] < self._follow_view.get_view():
            # when the receive message with view < follow_view, do nothing
            return web.Response(text='OK')
        
        self._log.info("---> %d: receive preprepare msg from %d", self._index, json_data['leader'])

        self._log.info("---> %d: on prepare", self._index)
        for slot in json_data['proposal']:
            if not self._legal_slot(slot):
                continue
            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)

            prepare_msg = {
                'index': self._index,
                'view': json_data['view'],
                'proposal': {
                    'slot': json_data['proposal'][slot]
                },
                'type': Status.PREPARE
            }
            await self._post(self._nodes, PBFTHandler.COMMIT, prepare_msg)
        return web.Response(text='OK')

    async def commit(self, request):
        """
        Once receive more than 2f + 1 prepare message, send the commit message.
        input:
            request: commit message from prepare:
                prepare_msg = {
                    'index': self._index,
                    'view': self._n,
                    'proposal': {
                        'this_slot': json_data
                    },
                    'type': 'prepare'
                }
        """
        json_data = await request.json()
        self._log.info("---> %d: receives prepare msg from %d", self._index, json_data['index'])

        if json_data['view'] < self._follow_view.get_view():
            # when the receive message with view < follow_view, do nothing
            return web.Response(text='OK')

        self._log.info("---> %d: on commit", self._index)

        for slot in json_data['proposal']:
            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
            status = self._status_by_slot[slot]

            view = View(json_data['view'], self._node_cnt)
            status._update_sequence(view, json_data['type'], json_data['proposal'][slot], json_data['index'])

            if status._check_majority(json_data['type']):
                status.prepare_certificate = Status.Certificate(view, json_data['proposal'][slot])
                commit_msg = {
                    'index': self._index,
                    'view': json_data['view'],
                    'proposal': {
                        'slot': json_data['proposal'][slot]
                    },
                    'type': Status.COMMIT
                }
                await self._post(self._nodes, PBFTHandler.COMMIT, commit_msg)
        return web.Response(text='OK')
    
    async def reply(self, request):
        """
        Once receive more than 2f + 1 commit message, append the commit certificate and cannot change anymore. In addition, if there is no bubbles ahead, commit the given slots and update the last_commit_slot.
        input: 
            request: commit message from commit:
                preprepare_msg = {
                    'index': self._index,
                    'n': self._n,
                    'proposal': {
                        this_slot: json_data
                    },
                    'type': 'commit'
                }
        """

        json_data = await request.json()
        self._log.info("---> %d: on reply", self._index)

        if json_data['view'] < self._follow_view.get_view():
            # when receive message with view < follow_view, do nothing
            return web.Response(text='OK')
        
        self._log.info("---> %d: receive commit msg from %d", self._index, json_data['index'])

        for slot in json_data['proposal']:
            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
            status = self._status_by_slot[slot]

            view = View(json_data['view'], self._node_cnt)
            status._update_sequence(json_data['type'], view, json_data['proposal'][slot], json_data['index'])

            """Commit only when no commit certificate and gets more than 2f + 1 commit message."""
            if not status.commit_certificate and status._check_majority(json_data['type']):
                status.commit_certificate = Status.Certificate(view, json_data['proposal'][slot])

                self._log.debug("---> %d Add commit certificate to slot %d", int(slot))

                # Reply only once and only when no bubble ahead
                if self._last_commit_slot == int(slot) - 1 and not status.is_committed:

                    reply_msg = {
                        'index': self._index,
                        'view': json_data['view'],
                        'proposal': json_data['proposal'][slot],
                        'type'; Status.REPLY
                    }
                    status.is_committed = True
                    self._last_commit_slot += 1

                    """When commit messages fill the next checkpoint, propose a new checkpoint."""
                    if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                        await self._ckpt.propose_vote(self.get_commit_decisions())
                        self._log.info("---> %d: Propose checkpoint with last slot: %d." "In addition, current checkpoint's next_slot is: %d", self._index, self._last_commit_slot, self._ckpt.next_slot)
                    
                    # Commit
                    await self._commit_action()
                    try:
                        await self._session.post(json_data['proposal'][slot]['client_url'], json=reply_msg)
                    except:
                        self._log.error("Send message failed to %s", json_data['proposal'][slot]['client_url'])
                        pass
                    else:
                        self._log.info("%d reply to %s successfully!!", self._index, json_data['proposal'][slot]['client_url'])
        return web.Response(text="OK")

    def get_commit_decisions(self):
        """
        Get the commit decision between the next slot of the current ckpt until last commit slot
        output:
            commit_decisions: list of tuple: [((client_index, client_seq), data), ...]
        """
        commit_decisions = []
        for i in range(self._ckpt.next_slot, self._last_commit_slot + 1):
            status = self._status_by_slot[str(i)]
            proposal = status.commit_certificate._proposal
            commit_decisions.append((proposal['id'], proposal['data']))

        return commit_decisions

    async def _commit_action(self):
        """
        Dump the current commit decisions to disk.
        """
        with open("{}.dump".format(self._index), 'w') as f:
            dump_data = self._ckpt.checkpoint + self.get_commit_decisions()
            json.dump(dump_data, f)

    async def receive_ckpt_vote(self, request):
        """
        Receive the message sent from Checkpoint.propose_vote().
        """
        self._log.info("---> %d: receive checkpoint vote.", self._index)
        json_data = await request.json()
        await self._ckpt.receive_vote(json_data)
        return web.Response(text="Ok")

    async def receive_sync(self, request):
        """
        Update the checkpoint and fill the bubble when receive sync messages.
        input:
            request: {
                'checkpoint': json_data = {
                    'next_slot': self._next_slot,
                    'ckpt': json.dumps(ckpt)
                },
            }
        """



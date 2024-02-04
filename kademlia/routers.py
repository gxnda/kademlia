import threading
from abc import abstractmethod
from datetime import datetime
from time import sleep
from typing import Callable, Optional

import my_queues
from kademlia.buckets import KBucket
from kademlia.constants import Constants
from kademlia.contact import Contact
from kademlia.dht import DHT
from kademlia.dictionaries import ContactQueueItem, FindResult, QueryReturn
from kademlia.errors import AllKBucketsAreEmptyError, ValueCannotBeNoneError
from id import ID
from kademlia import TRY_CLOSEST_BUCKET
from main import DEBUG
from node import Node


class BaseRouter:
    def __init__(self, node: Node):
        self.closer_contacts: list[Contact]
        self.further_contacts: list[Contact]
        self.node: Node = node
        self.dht: Optional[DHT] = None  # TODO: Is it optional?
        # self.locker

    def find_closest_nonempty_kbucket(self, key: ID) -> KBucket:
        """
        Helper method.
        Code listing 34.
        """
        # gets all non-empty buckets from bucket list
        non_empty_buckets: list[KBucket] = [
            b for b in self.node.bucket_list.buckets if (len(b.contacts) != 0)
        ]
        if len(non_empty_buckets) == 0:
            raise AllKBucketsAreEmptyError(
                "No non-empty buckets can be found.")

        return sorted(non_empty_buckets,
                      key=(lambda b: b.id.value ^ key.value))[0]

    def rpc_find_nodes(self, key: ID, contact: Contact):
        # what is node??
        new_contacts, timeout_error = contact.protocol.find_node(
            self.node.our_contact, key)

        if self.dht:
            self.dht.handle_error(timeout_error, contact)

        return new_contacts, None, None

    def rpc_find_value(self, key: ID, contact: Contact) -> tuple[list[Contact], Contact, str]:
        nodes: list[Contact] = []
        ret_val: Optional[str] = None
        found_by: Optional[Contact] = None

        other_contacts, val, error = contact.protocol.find_value(self.node.our_contact, key)
        self.dht.handle_error(error, contact)

        if not error.has_error():
            if other_contacts is not None:
                for other_contact in other_contacts:
                    nodes.append(other_contact)
            else:
                if val is None:
                    raise ValueCannotBeNoneError("None values are not expected, nor supported from FIND_VALUE RPC.")
                else:
                    nodes.append(contact)
                    found_by = contact
                    ret_val = val

        return nodes, found_by, ret_val

    def query(self,
              key: ID,
              nodes_to_query: list[Contact],
              rpc_call: Callable,
              closer_contacts: list[Contact],
              further_contacts: list[Contact]) -> QueryReturn:
        found: bool = False
        found_by: Optional[Contact] = None
        val: str = ""

        for n in nodes_to_query:
            found, val, found_by = self.get_closer_nodes(
                key=key,
                node_to_query=n,
                rpc_call=rpc_call,
                closer_contacts=closer_contacts,
                further_contacts=further_contacts
            )
            if found:
                break

        return QueryReturn(
            found=found,
            contacts=closer_contacts,
            found_by=found_by,
            val=val
        )

    @abstractmethod
    def lookup(self, key: ID, rpc_call: Callable, give_me_all=False) -> QueryReturn | None:
        pass

    @staticmethod
    def get_closest_nodes(key: ID, bucket: KBucket) -> list[Contact]:
        """
        Get sorted list of closest contacts to the given key.
        :param key: key to look close to.
        :param bucket: bucket to look in.
        :return: sorted list of contacts by distance (sorted by XOR distance to parameter key)
        """
        return sorted(bucket.contacts, key=lambda c: c.id ^ key)

    def get_closer_nodes(self,
                         key: ID,
                         node_to_query: Contact,
                         rpc_call: Callable[[ID, Contact], tuple[list[Contact], Contact, str]],
                         further_contacts: list[Contact],
                         closer_contacts: list[Contact]
                         ) -> tuple[bool, str, Contact]:
        """
        TODO: Create docstring
        Tells closer nodes to look for key
        :param key:
        :param node_to_query:
        :param rpc_call:
        :param further_contacts:
        :param closer_contacts:
        :return:
        """
        contacts, found_by, val = rpc_call(key, node_to_query)
        peers_nodes: list[Contact] = []
        for contact in contacts:
            if contact.id.value != self.node.our_contact.id.value and node_to_query.id.value != contact.id.value:
                if contact not in closer_contacts and contact not in further_contacts:
                    peers_nodes.append(contact)

        nearest_node_distance = node_to_query.id ^ key

        # lock (locker)
        close_peer_nodes = [p for p in peers_nodes if (p.id ^ node_to_query.id) < nearest_node_distance]
        for p in close_peer_nodes:
            if p.id not in [c.id for c in closer_contacts]:
                closer_contacts.append(p)

        # lock (locker)
        far_peer_nodes = [p for p in peers_nodes if (p.id ^ node_to_query.id) >= nearest_node_distance]
        for p in far_peer_nodes:
            if p.id not in [c.id for c in further_contacts]:
                further_contacts.append(p)

        return val is not None, val, found_by


class Router(BaseRouter):
    """
    TODO: Talk about what this does.
    """

    def __init__(self, node: Node = None) -> None:
        super().__init__(node)
        self.node: Node = node
        self.closer_contacts: list[Contact] = []
        self.further_contacts: list[Contact] = []
        self.dht: Optional[DHT] = None
        # self.lock = WithLock(Lock())

    def lookup(self,
               key: ID,
               rpc_call: Callable,
               give_me_all: bool = False) -> QueryReturn:
        """
        Performs main Kademlia Lookup.
        :param key: Key to be looked up
        :param rpc_call: RPC call to be used.
        :param give_me_all: TODO: Implement.
        :return: returns query result.
        """
        have_work = True
        ret = []
        contacted_nodes = []
        # closer_uncontacted_nodes = []
        # further_uncontacted_nodes = []

        all_nodes = self.node.bucket_list.get_close_contacts(
            key, self.node.our_contact.id)[0:Constants.K]

        nodes_to_query: list[Contact] = all_nodes[0:Constants.A]

        for i in nodes_to_query:
            if i.id.value ^ key.value < self.node.our_contact.id.value ^ key.value:
                self.closer_contacts.append(i)
            else:
                self.further_contacts.append(i)

        # all untested contacts just get dumped here.
        for i in all_nodes[Constants.A + 1:]:
            self.further_contacts.append(i)

        for i in nodes_to_query:
            if i not in contacted_nodes:
                contacted_nodes.append(i)

        # In the spec they then send parallel async find_node RPC commands
        query_result: QueryReturn = (self.query(key, nodes_to_query, rpc_call,
                                                self.closer_contacts,
                                                self.further_contacts))

        if query_result["found"]:  # if a node responded
            return query_result

        # add any new closer contacts
        for i in self.closer_contacts:
            if i.id not in [j.id for j in ret
                            ]:  # if id does not already exist inside list
                ret.append(i)

        while len(ret) < Constants.K and have_work:
            closer_uncontacted_nodes = [
                i for i in self.closer_contacts if i not in contacted_nodes
            ]
            further_uncontacted_nodes = [
                i for i in self.further_contacts if i not in contacted_nodes
            ]

            # If we have uncontacted nodes, we still have work to be done.
            have_closer: bool = len(closer_uncontacted_nodes) > 0
            have_further: bool = len(further_uncontacted_nodes) > 0
            have_work: bool = have_closer or have_further
            """
            Spec: of the k nodes the initiator has heard of closest 
            to the target,
            it picks the 'a' that it has not yet queried and resends 
            the FIND_NODE RPC to them.
            """
            if have_closer:
                new_nodes_to_query = closer_uncontacted_nodes[:Constants.A]
                for i in new_nodes_to_query:
                    if i not in contacted_nodes:
                        contacted_nodes.append(i)

                query_result = (self.query(key, new_nodes_to_query, rpc_call,
                                           self.closer_contacts,
                                           self.further_contacts))

                if query_result["found"]:
                    return query_result

            elif have_further:
                new_nodes_to_query = further_uncontacted_nodes[:Constants.A]
                for i in new_nodes_to_query:
                    if i not in contacted_nodes:
                        contacted_nodes.append(i)

                query_result = (self.query(key, new_nodes_to_query, rpc_call,
                                           self.closer_contacts,
                                           self.further_contacts))

                if query_result["found"]:
                    return query_result

        # return k closer nodes sorted by distance,

        contacts = sorted(ret[:Constants.K], key=(lambda x: x.id ^ key))
        return {
            "found": False,
            "contacts": contacts,
            "val": None,
            "found_by": None
        }



class ParallelRouter(BaseRouter):
    def __init__(self, node: Node = None):
        # TODO: Should these be empty?
        super().__init__(node)
        self.contact_queue = my_queues.InfiniteLinearQueue()  # TODO: Make protected - should it be infinite?
        self.node: Node = node
        self.semaphore = threading.Semaphore()  # TODO: Make protected
        self.now: datetime = datetime.now()  # Should this be now?
        self.stop_work = False
        self.initialise_thread_pool()

    def initialise_thread_pool(self):  # TODO: Make protected.
        threads: list[threading.Thread] = []
        for _ in range(Constants.MAX_THREADS):
            thread = threading.Thread(target=self.rpc_caller)
            # thread.is_background = True
            thread.start()

    def queue_work(self,
                   key: ID,
                   contact: Contact,
                   rpc_call: Callable,
                   closer_contacts: list[Contact],
                   further_contacts: list[Contact],
                   find_result: FindResult) -> None:

        self.contact_queue.enqueue(
            ContactQueueItem(
                key=key,
                contact=contact,
                rpc_call=rpc_call,
                closer_contacts=closer_contacts,
                further_contacts=further_contacts,
                find_result=find_result)
        )

        self.semaphore.release()

    def rpc_caller(self) -> None:
        """
        "when a value is found, it takes a snapshot of the current closer contacts and stores
        all the information about a closer contact in fields belonging to the ParallelLookup class."
        :return:
        """
        flag = True
        while flag:  # I hate this.
            self.semaphore.wait_one()  # don't think this is real
            item: ContactQueueItem = self.contact_queue.dequeue()
            if item:
                found, val, found_by = self.get_closer_nodes(item["key"],
                                            item["contact"],
                                            item["rpc_call"],
                                            item["closer_contacts"],
                                            item["further_contacts"]
                                            )
                if val or found_by:
                    if not self.stop_work:
                        # Possible multiple "found"
                        # lock(locker)
                        item["find_result"]["found"] = True
                        item["find_result"]["found_by"] = found_by
                        item["find_result"]["found_value"] = val
                        item["find_result"]["found_contacts"] = item["closer_contacts"]

    def set_query_time(self) -> None:
        self.now = datetime.now()

    def query_time_expired(self) -> bool:
        """
        Returns true if the query time has expired.
        :return:
        """
        return (datetime.now() - self.now).total_seconds() > Constants.QUERY_TIME

    def dequeue_remaining_work(self):
        dequeue_result = True
        while dequeue_result:
            dequeue_result = self.contact_queue.dequeue()

    def stop_remaining_work(self):
        self.dequeue_remaining_work()
        self.stop_work = True

    def parallel_found(self, find_result: FindResult) -> tuple[
        type(FindResult["found"]),
        bool,
        type(FindResult["found_contacts"]),
        type(FindResult["found_by"]),
        type(FindResult["found_value"])
    ]:
        """
        :param find_result:
        :param found_ret: given as a tuple so that it is used as reference.
        :return:
        """
        # lock(locker)
        if find_result["found"]:
            # lock(find_result["found_contacts"]
            found_ret = (True, find_result["found_contacts"], find_result["found_by"], find_result["found_value"])

        return find_result["found"], True, find_result["found_contacts"], find_result["found_by"], find_result["found_value"]

    def lookup(self, key: ID, rpc_call: Callable, give_me_all: bool = False):  # TODO: Very much incomplete

        if not isinstance(self.node, Node):
            raise TypeError("ParallelRouter must have instance node.")

        stop_work: bool = False
        have_work: bool = True
        find_result: FindResult = FindResult()
        ret: list[Contact] = []
        contacted_nodes: list[Contact] = []
        closer_contacts: list[Contact] = []
        further_contacts: list[Contact] = []
        found: bool = False
        contacts: list[Contact] = []
        found_by: Optional[Contact] = None
        val: str = ""

        # TODO: Why do I do this?
        if TRY_CLOSEST_BUCKET:
            # Spec: The lookup initiator starts by picking a nodes from its closest non-empty k-bucket
            bucket = self.find_closest_nonempty_kbucket(key)

            # Not in spec -- sort by the closest nodes in the closest bucket.
            all_nodes: list[Contact] = self.node.bucket_list.get_close_contacts(
                key, self.node.our_contact.id)[0:Constants.K]

            nodes_to_query: list[Contact] = all_nodes[0:Constants.A]
        else:
            if DEBUG:
                all_nodes: list[Contact] = self.node.bucket_list.get_kbucket(key).contacts[0:Constants.K]
            else:
                # For unit testing, this is a bad way to get a list of close contacts with virtual nodes
                # because we're always going to get the closest nodes right at the get go.
                all_nodes: list[Contact] = self.node.bucket_list.get_close_contacts(key, self.node.our_contact.id)[0:Constants.K]

            nodes_to_query: list[Contact] = all_nodes[0:Constants.A]

            # Also not explicity in specification:
            # any closer node in the alpha list is immediately added to our closer contact list,
            # and any further node in the alpha list is immediately added to our further contact list.
            for c in nodes_to_query:
                if (c.id ^ key) < (self.node.our_contact.id ^ key):
                    closer_contacts.append(c)
                else:
                    further_contacts.append(c)

            # the remaining contacts can be put here.
            for c in all_nodes:
                if c not in nodes_to_query:
                    further_contacts.append(c)

        # we're about to contact these nodes.
        for c in nodes_to_query:
            if c.id not in [i.id for i in contacted_nodes]:
                contacted_nodes.append(c)

        # Spec: the initiator then sends parallel asynchronous FIND_NODE RPCS to the
        # Constants.A nodes it has chosen, Constants.A is a system-wide concurrency parameter,
        # such as 3.

        for c in nodes_to_query:
            self.queue_work(key=key,
                            contact=c,
                            rpc_call=rpc_call,
                            closer_contacts=closer_contacts,
                            further_contacts=further_contacts,
                            find_result=find_result)

        self.set_query_time()

        # add any new closer contacts to the list we're going to return.
        for c in closer_contacts:
            if c.id not in [r.id for r in ret]:
                ret.append(c)

        # The lookup terminates when the initiator has queried and
        # received responses from the k closest nodes it has seen.
        while len(ret) < Constants.K and have_work:
            sleep(Constants.RESPONSE_WAIT_TIME)  # Should this be time.sleep or asyncio.sleep, or threading ?

            found_return = self.parallel_found(find_result)
            if found_return:
                self.stop_remaining_work()
                return found_return

            closer_uncontacted_nodes = [c for c in closer_contacts if c not in contacted_nodes]
            further_uncontacted_nodes = [c for c in further_contacts if c not in contacted_nodes]

            have_closer = len(closer_uncontacted_nodes) > 0
            have_further = len(further_uncontacted_nodes) > 0

            have_work = have_closer or have_further or not self.query_time_expired()

            # for the k nodes the initiator has heard of closest to the target...
            alpha_nodes = None

            if have_closer:
                # we're about to contact these nodes.
                if len(closer_uncontacted_nodes) >= Constants.A:
                    alpha_nodes = closer_uncontacted_nodes[0: Constants.A - 1]
                else:
                    alpha_nodes = closer_uncontacted_nodes

            elif have_further:
                if len(further_uncontacted_nodes) >= Constants.A:
                    alpha_nodes = further_uncontacted_nodes[0: Constants.A - 1]
                else:
                    alpha_nodes = further_uncontacted_nodes

            if alpha_nodes:
                for a in alpha_nodes:
                    if a.id not in [c.id for c in contacted_nodes]:
                        contacted_nodes.append(a)
                    self.queue_work(
                        key=key,
                        contact=a,
                        rpc_call=rpc_call,
                        closer_contacts=closer_contacts,
                        further_contacts=further_contacts,
                        find_result=find_result
                    )

                    self.set_query_time()


import threading
from abc import abstractmethod
from datetime import datetime
from time import sleep
from typing import Callable, Optional

import kademlia_dht.my_queues as my_queues
from kademlia_dht.buckets import KBucket
from kademlia_dht.constants import Constants
from kademlia_dht.contact import Contact
from kademlia_dht.dictionaries import ContactQueueItem, FindResult
from kademlia_dht.errors import AllKBucketsAreEmptyError, ValueCannotBeNoneError
from kademlia_dht.id import ID
from kademlia_dht.node import Node


class BaseRouter:
    def __init__(self, node: Node):
        self.closer_contacts: list[Contact] = []
        self.further_contacts: list[Contact] = []
        self.node: Node = node
        self.dht = None
        # self.locker

    def find_closest_nonempty_kbucket(self, key: ID) -> KBucket:
        """
        Finds the closest non empty Kbucket in our nodes bucket list to a given key.
        :param key:
        :return:
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
        """
        Performs find nodes() on “contact”, where we are the sender, searching for “key”.
        :param key:
        :param contact:
        :return:
        """
        new_contacts, timeout_error = contact.protocol.find_node(
            self.node.our_contact, key)

        if self.dht:
            self.dht.handle_error(timeout_error, contact)

        return new_contacts, None, None

    def rpc_find_value(self, key: ID, contact: Contact) -> tuple[list[Contact], Contact, str]:
        """
        Performs find value() on “contact”, where we are the sender searching for a value
        corresponding to “key”, or K contacts close to “key”, if we find the value, we
        will also receive the contact that contains it.
        :param key:
        :param contact:
        :return:
        """
        nodes: list[Contact] = []
        ret_val: Optional[str] = None
        found_by: Optional[Contact] = None

        other_contacts, val, error = contact.protocol.find_value(self.node.our_contact, key)
        if self.dht:
            self.dht.handle_error(error, contact)
        else:
            print("[Client] Router: No DHT to handle possible error.\nError:", error)

        if not error or not error.has_error():
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

    def _query(self,
               key: ID,
               nodes_to_query: list[Contact],
               rpc_call: Callable,
               closer_contacts: list[Contact],
               further_contacts: list[Contact]) -> FindResult:
        """
        Gets nodes that are closer to “key” than a node we are querying – we query all of nodes_to_query.
        This ends as soon as we find a value. A FindResult object is then returned containing closer_contacts,
        found, found_by, and the value we found.

        Gets nodes by performing rpc-call on the node to query, looking for “key”, if we don’t know it,
        it is added to a list “peers_nodes”. Checking each contact in peers_nodes, we check if it is closer
        to the key than the node_to_query, if it is, we add it to closer_contacts. We do a similar check to
        check if it is further, and then we add it to further_contacts.

        :param key:
        :param nodes_to_query:
        :param rpc_call:
        :param closer_contacts:
        :param further_contacts:
        :return:
        """
        found: bool = False
        found_by: Optional[Contact] = None
        val: str = ""

        for n in nodes_to_query:
            found, val, found_by, closer_contacts, further_contacts = self.get_closer_nodes(
                key=key,
                node_to_query=n,
                rpc_call=rpc_call,
                closer_contacts=closer_contacts,
                further_contacts=further_contacts
            )
            if found:
                break

        return FindResult(
            found=found,
            contacts=closer_contacts,
            found_by=found_by,
            val=val
        )

    @abstractmethod
    def lookup(self, key: ID, rpc_call: Callable, give_me_all=False) -> FindResult | None:
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
                         ) -> tuple[bool, str, Contact, list[Contact], list[Contact]]:
        """
        Gets nodes that are closer to “key” than “node_to_query”.

        Gets nodes by performing rpc-call on the node to query, looking for “key”, if we don’t know it,
        it is added to a list “peers_nodes”. Checking each contact in peers_nodes, we check if it is closer
        to the key than the node_to_query, if it is, we add it to closer_contacts. We do a similar check to
        check if it is further, and then we add it to further_contacts.

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
            if contact.id.value not in [self.node.our_contact.id.value, node_to_query.id.value]:
                if contact not in [closer_contacts, further_contacts]:
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

        return val is not None, val, found_by, closer_contacts, further_contacts


class Router(BaseRouter):
    """
    TODO: Talk about what this does.
    """

    def __init__(self, node: Node = None) -> None:
        super().__init__(node)
        # self.lock = WithLock(Lock())

    def lookup(self,
               key: ID,
               rpc_call: Callable,
               give_me_all: bool = False) -> FindResult:
        """
        This performs the main Kademlia lookup algorithm serially.
        This method initiates a Kademlia lookup operation, searching for nodes closest to the given key.
        starts by getting nodes from the closest non-empty k-bucket and continues querying nodes
        serially until it has gathered responses from the closest k nodes or until we run out of
        nodes to contact.



        :param key: Key to be looked up
        :param rpc_call: RPC call to be used.
        :param give_me_all: If all contacts should be returned or not - for testing purposes mainly.
        :return: returns query result.
        """
        contacted_nodes = []
        closer_uncontacted_nodes = []
        further_uncontacted_nodes = []
        if Constants.TRY_CLOSEST_BUCKET:
            # Spec: The lookup initator starts by picking a nodes from its closest non-empty k-bucket
            bucket: KBucket = self.find_closest_nonempty_kbucket(key)

            # Not in spec: sort by the closest nodes in the closest bucket.
            all_nodes: list[Contact] = self.node.bucket_list.get_close_contacts(
                key, self.node.our_contact.id)[0:Constants.K]
            nodes_to_query: list[Contact] = all_nodes[0:Constants.A]

            for i in all_nodes[Constants.A + 1:]:
                self.further_contacts.append(i)
        else:
            if Constants.DEBUG:
                all_nodes: list[Contact] = self.node.bucket_list.get_kbucket(key).contacts[0:Constants.K]
            else:
                # This is a bad way to get a list of close contacts with virtual nodes because we're always going to
                # get the closest nodes right at the get go.
                all_nodes: list[Contact] = self.node.bucket_list.get_close_contacts(
                    key, self.node.our_contact.id)[0:Constants.K]
            nodes_to_query: list[Contact] = all_nodes[:Constants.A]

            # Also not explicitly in spec:
            # Any closer node in the alpha list is immediately added to our closer contact list
            # and any further node in the alpha list is immediately added to our further contact list.
            for n in nodes_to_query:
                if (n.id ^ key) < (self.node.our_contact.id ^ key):
                    self.closer_contacts.append(n)
                else:
                    self.further_contacts.append(n)

            # The remaining contacts not tested yet can be put here.
            for n in all_nodes[Constants.A + 1:]:
                self.further_contacts.append(n)

        # We're about to contact these nodes.
        for n in nodes_to_query:
            if n.id not in [i.id for i in contacted_nodes]:
                contacted_nodes.append(n)

        # Spec: The initiator then sends parallel, async FIND_NODE RPCs to the "a" nodes it has chosen,
        # "a" is a system-wide parameter, such as 3.
        query_result: FindResult = self._query(key, nodes_to_query, rpc_call, self.closer_contacts,
                                               self.further_contacts)
        if query_result["found"]:
            # For unit testing
            closer_contacts_unittest = self.closer_contacts
            further_contacts_unittest = self.further_contacts
            return query_result

        # Add any new closer contacts to the list we're going to return.
        ret: list[Contact] = []
        for c in self.closer_contacts:
            if c.id not in [i.id for i in ret]:
                ret.append(c)

        # Spec: The lookup terminates when the initator has queried and received responses from the k closest nodes
        # it has seen.
        have_work = True
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

            # Spec: of the k nodes the initiator has heard of closest to the target,
            # it picks the 'a' that it has not yet queried and resends the FIND_NODE RPC to them.
            if have_closer:
                new_nodes_to_query = closer_uncontacted_nodes[:Constants.A]
                for c in new_nodes_to_query:
                    if c.id not in [i.id for i in contacted_nodes]:
                        contacted_nodes.append(c)

                query_result = (self._query(key, new_nodes_to_query, rpc_call,
                                            self.closer_contacts,
                                            self.further_contacts))

                if query_result["found"]:
                    # # For unit testing.
                    # closer_contacts_unittest = self.closer_contacts
                    # further_contacts_unittest = self.further_contacts
                    return query_result

            elif have_further:
                new_nodes_to_query = further_uncontacted_nodes[:Constants.A]
                for c in further_uncontacted_nodes:
                    if c not in [i.id for i in contacted_nodes]:
                        contacted_nodes.append(c)

                query_result = (self._query(key, new_nodes_to_query, rpc_call,
                                            self.closer_contacts,
                                            self.further_contacts))

                if query_result["found"]:
                    # # For unit testing.
                    # closer_contacts_unittest = self.closer_contacts
                    # further_contacts_unittest = self.further_contacts
                    return query_result

        # if DEBUG:  # For unit testing
        #     closer_contacts_unittest = self.closer_contacts
        #     further_contacts_unittest = self.further_contacts

        # return k closer nodes sorted by distance,

        # Spec (sort of): return max(k) closer nodes, sorted by distance.
        # For unit testing give_me_all can be true so that we can match against our alternate way of
        # getting closer contacts.
        # contacts, val, found, found_by
        return FindResult(
            found=False,
            contacts=(ret if give_me_all else sorted(ret, key=lambda c: c.id ^ key)[:Constants.K]),
            found_by=None,
            val=None
        )


class ParallelRouter(BaseRouter):
    def __init__(self, node: Node = None):
        super().__init__(node)
        self.__contact_queue = my_queues.InfiniteLinearQueue()
        self.__semaphore = threading.Semaphore()
        self.__now: datetime = datetime.now()
        self.__stop_work = False
        self.__threads: list[threading.Thread] = []
        self.__initialise_thread_pool()

    def __initialise_thread_pool(self) -> None:
        """
        Creates and starts MAX-THREADS # of threads, which all run self.rpc-caller.
        :return:
        """
        for _ in range(Constants.MAX_THREADS):
            thread = threading.Thread(target=self.__rpc_caller)
            # thread.is_background = True
            self.__threads.append(thread)
            thread.start()

    def queue_work(self,
                   key: ID,
                   contact: Contact,
                   rpc_call: Callable,
                   closer_contacts: list[Contact],
                   further_contacts: list[Contact],
                   find_result: FindResult) -> None:
        """
        Adds new Contact Queue Item to self.__contact-queue, all the
        parameters listed are added. The semaphore is released at the end of
        this function to signal that there is work available in the queue.
        :param key:
        :param contact:
        :param rpc_call:
        :param closer_contacts:
        :param further_contacts:
        :param find_result:
        :return:
        """

        self.__contact_queue.enqueue(
            ContactQueueItem(
                key=key,
                contact=contact,
                rpc_call=rpc_call,
                closer_contacts=closer_contacts,
                further_contacts=further_contacts,
                find_result=find_result)
        )

        self.__semaphore.release()

    def __rpc_caller(self) -> None:
        """
        This is ran on each thread in parallel inside the Router; it is an infinite loop that
        exists for as long as the Router is running. It will check for work from the semaphore
         – if there is no work it will wait until there is. Once there is work it will dequeue
         an item from the queue and get K nodes closer to “key” than “contact”, updating the
         FindResult – this works even though it has been dequeued because python refers to
         lists as references (for example, if you pass a list into a function, the function
         can edit the list, and that will persist outside the function), so we can refer to
         item[“findResult”] because the reference will persist in the “lookup” method.
        :return:
        """
        flag = True
        while flag:  # I hate this.
            self.__semaphore.acquire()
            item: ContactQueueItem = self.__contact_queue.dequeue()
            if item:
                found, val, found_by, item["closer_contacts"], item["further_contacts"] = self.get_closer_nodes(
                        item["key"],
                        item["contact"],
                        item["rpc_call"],
                        item["closer_contacts"],
                        item["further_contacts"]
                    )
                if val or found_by:
                    if not self.__stop_work:
                        # Possible multiple "found"
                        # lock(locker)
                        item["find_result"]["found"] = True
                        item["find_result"]["found_by"] = found_by
                        item["find_result"]["val"] = val
                        item["find_result"]["contacts"] = item["closer_contacts"]

    def set_query_time(self) -> None:
        """
        Sets self.now() to current time.
        :return:
        """
        self.__now = datetime.now()

    def _query_time_expired(self) -> bool:
        """
        Returns true if the query time has expired.

        Returns if the time since query was triggered is longer than Constants REQUEST-TIMEOUT.
        :return:
        """
        return (datetime.now() - self.__now).total_seconds() > Constants.REQUEST_TIMEOUT

    def __dequeue_remaining_work(self):
        """
        Dequeues everything from the contact queue.
        :return:
        """
        dequeue_result = True
        while dequeue_result:
            dequeue_result = self.__contact_queue.dequeue()

    def _stop_remaining_work(self):
        """
        Dequeues everything from the contact queue, then sets “stop work” to True, so no more work will be done.
        :return:
        """
        self.__dequeue_remaining_work()
        self.__stop_work = True

    @classmethod
    def parallel_found(cls, find_result: FindResult, found_ret: FindResult) -> tuple[bool, FindResult]:
        """
        Overlays found-ret with find-result if find-result has a value.
        It then returns if the overlay has been performed, and the new found_ret.
        :param find_result:
        :param found_ret:
        :return:
        """
        # lock(locker)
        if find_result["found"]:
            # lock(find_result["contacts"]
            # lock found ret
            found_ret["found"] = True
            found_ret["contacts"] = find_result["contacts"]
            found_ret["found_by"] = find_result["found_by"]
            found_ret["val"] = find_result["val"]

        return find_result["found"], found_ret

    def lookup(self, key: ID, rpc_call: Callable, give_me_all: bool = False) -> FindResult:
        """
        Performs a similar algorithm to what has been performed in "Router", but this
        time it is performed in parallel. First it makes sure that self.node actually exists
        to prevent any strange errors from happening down the line. It then gets a list
        of K 'all nodes' it intends to query, and then bits of chunks of ALPHA each time and
        queries them individually. It then groups these ALPHA nodes_to_query into closer_contacts
        and further_contacts, depending on if they are closer than or further than our ID to the
        parameter "key" by the XOR metric. All of the closer and further contacts are then placed
        into a ContactQueueItem (and appended to our contact queue) with each member of the nodes
        to query - This will be handled by the Constants.MAX_THREADS running rpc_caller. The
        time since last query is then updated. This process then iterates, biting of chunks of
        ALPHA contacts until there are no closer or further uncontacted nodes, or until one of the threads
        handling the contact queue finds the value we are looking for which matches the key-value
        pair with "key", if that is the case, the FindResult object containing the value will be returned.


        :param key:
        :param rpc_call:
        :param give_me_all:
        :return:
        """

        if not isinstance(self.node, Node):
            raise TypeError("ParallelRouter must have instance node.")
        have_work: bool = True
        find_result: FindResult = FindResult(found=False, found_by=None, val="", contacts=[])
        ret: list[Contact] = []
        contacted_nodes: list[Contact] = []
        closer_contacts: list[Contact] = []
        further_contacts: list[Contact] = []
        found_return = FindResult(found=False, found_by=None, val="", contacts=[])

        if Constants.DEBUG:
            all_nodes: list[Contact] = self.node.bucket_list.get_kbucket(key).contacts[0:Constants.K]
        else:
            # For unit testing, this is a bad way to get a list of close contacts with virtual nodes
            # because we're always going to get the closest nodes right at the get go.
            all_nodes: list[Contact] = self.node.bucket_list.get_close_contacts(key, self.node.our_contact.id)[0:Constants.K]

        nodes_to_query: list[Contact] = all_nodes[0:Constants.A]
        # Also not explicitly in specification:
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

        # Spec: the initiator then sends parallel asynchronous FIND_NODE RPCs to the
        # Constants.A nodes it has chosen.
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
            sleep(Constants.RESPONSE_WAIT_TIME / 1000)

            found, found_return = self.parallel_found(find_result, found_return)
            if found:
                self._stop_remaining_work()
                return found_return

            closer_uncontacted_nodes = [c for c in closer_contacts if c not in contacted_nodes]
            further_uncontacted_nodes = [c for c in further_contacts if c not in contacted_nodes]

            have_closer = len(closer_uncontacted_nodes) > 0
            have_further = len(further_uncontacted_nodes) > 0
            have_work = have_closer or have_further or not self._query_time_expired()

            # for the k nodes the initiator has heard of closest to the target...
            alpha_nodes = None

            if have_closer:
                # we're about to contact these nodes.
                if len(closer_uncontacted_nodes) >= Constants.A:
                    alpha_nodes = closer_uncontacted_nodes[0: Constants.A - 1]
                else:
                    alpha_nodes = closer_uncontacted_nodes

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


        self._stop_remaining_work()
        return FindResult(
            found=False,
            contacts=ret if give_me_all else sorted(ret[0:Constants.K], key=lambda c: c.id ^ key),
            found_by=None,
            val=None
        )


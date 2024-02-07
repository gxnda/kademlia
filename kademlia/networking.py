import threading
from time import sleep
from typing import Optional, TypedDict, Callable
from http.server import BaseHTTPRequestHandler, HTTPServer

import kademlia.main as main
import kademlia.pickler as pickler
from kademlia.dictionaries import PingRequest, StoreRequest, FindNodeRequest, FindValueRequest, ErrorResponse, \
    CommonRequest
from kademlia.id import ID


class Server(HTTPServer):
    def __init__(self, server_address: tuple[str, int], RequestHandlerClass):
        HTTPServer.__init__(
            self,
            server_address=server_address,
            RequestHandlerClass=RequestHandlerClass
        )

        # TODO: Should these be double slashed?
        self.routing_methods: dict[str, type] = {
            "/ping": PingRequest,  # "ping" should refer to type PingRequest
            "/store": StoreRequest,  # "store" should refer to type StoreRequest
            "/find_node": FindNodeRequest,  # "find_node" should refer to type FindNodeRequest
            "/find_value": FindValueRequest  # "find_value" should refer to type FindValueRequest
        }


class HTTPSubnetRequestHandler(BaseHTTPRequestHandler):

    def common_request_handler(self,
                               method_name: str, common_request: CommonRequest, node):  # TODO: Make protected.
        old_self_instance = self  # To prevent other threads overwriting it,
        # lock isn't used because I don't want to make the program wait.

        # Test what happens if a node does not respond
        if main.DEBUG:
            if node.our_contact.protocol.type == "TCPSubnetProtocol":
                if not node.our_contact.protocol.responds:
                    # Exceeds 500ms timeout
                    print("[Server] Does not respond, sleeping for timeout.")
                    sleep(1)

        try:
            method: Callable = getattr(node, method_name)
            # Calls method, eg: server_store.
            response = method(common_request)
            encoded_response = pickler.encode_data(response)
            print("[Server] Sending encoded 200: ", response)
            old_self_instance.send_response(code=200)

            # print("Adding headers... - Is wfile closed:", self.wfile.closed)
            old_self_instance.send_header("Content-Type", "application/octet-stream")
            old_self_instance.end_headers()
            # print("Finished headers - Is wfile closed:", self.wfile.closed)

            # print("Writing 200...", self.wfile.closed)
            old_self_instance.wfile.write(encoded_response)
            print("[Server] Writing response success!")

        except Exception as e:
            print("[Server] Exception sending response.")
            error_response: ErrorResponse = ErrorResponse(
                error_message=str(e),
                random_id=ID.random_id()
            )
            print("[Server] Sending encoded 400:", error_response)
            encoded_response = pickler.encode_data(error_response)

            old_self_instance.send_header("Content-Type", "application/octet-stream")
            old_self_instance.end_headers()
            old_self_instance.send_response(code=400)  # , message=encoded_response.decode("latin1"))

            old_self_instance.wfile.write(encoded_response)

        # old_self_instance.wfile.close()
        # finally:
        #     if not old_self_instance.wfile.closed:
        #         old_self_instance.wfile.close()
        #     else:
        #         print("[Server] Response body was already closed! (What on earth, something's gone wrong!)")

    def do_POST(self):
        print("[Server] POST Received.")
        routing_methods = {
            "/ping": PingRequest,  # "ping" should refer to type PingRequest
            "/store": StoreRequest,  # "store" should refer to type StoreRequest
            "/find_node": FindNodeRequest,  # "find_node" should refer to type FindNodeRequest
            "/find_value": FindValueRequest  # "find_value" should refer to type FindValueRequest
        }

        content_length = int(self.headers['Content-Length'])
        encoded_request: bytes = self.rfile.read(content_length)
        # encoded_request: bytes = self.rfile.read()
        decoded_request: dict = pickler.decode_data(encoded_request)
        # print("[Server] Request received:", decoded_request)
        request_dict = decoded_request
        path: str = self.path
        # Remove "/"
        # Prefix our call with "server_" so that the method name is unambiguous.
        method_name: str = "server_" + path[1:]  # path.substring(2)
        # What type is the request?
        try:
            # path is something like /ping or /find_node
            request_type: Optional[TypedDict] = routing_methods[path]
        except KeyError:
            request_type: Optional[TypedDict] = None

        # if we know what the request wants (if it's a ping/find_node RPC etc.)
        if request_type:
            subnet: int = request_dict["subnet"]
            common_request: CommonRequest = CommonRequest(
                protocol=request_dict.get("protocol"),
                protocol_name=request_dict.get("protocol_name"),
                random_id=request_dict.get("random_id"),
                sender=request_dict.get("sender"),
                key=request_dict.get("key"),
                value=request_dict.get("value"),
                is_cached=request_dict.get("is_cached"),
                expiration_time_sec=request_dict.get("expiration_time_sec")
            )

            # If we know the node on the subnet, this should always happen right?
            # Because this is for testing on the same PC.
            node = self.server.subnets.get(subnet)  # should be valid if inheriting from SubnetServer?
            if node:
                print("[Server] Request called:", node.bucket_list.buckets)
                self.common_request_handler(method_name, common_request, node)
                # print("Starting thread...")
                # new_thread = threading.Thread(
                #     target=self.common_request_handler,
                #     args=(method_name, common_request, node)
                # )
                # new_thread.start()

            else:
                print("[Server] Subnet node not found.")
                encoded_response = pickler.encode_data({"error_message": "Subnet node not found."})
                self.send_header("Content-Type", "application/octet-stream")
                self.end_headers()
                self.send_response(400)
                self.wfile.write(encoded_response)

            # context.close_connection = True


class TCPSubnetServer(HTTPServer):
    def __init__(self, server_address: tuple[str, int]):
        HTTPServer.__init__(
            self,
            server_address=server_address,
            RequestHandlerClass=HTTPSubnetRequestHandler
        )

        self.routing_methods: dict[str, type] = {
            "/ping": PingRequest,  # "ping" should refer to type PingRequest
            "/store": StoreRequest,  # "store" should refer to type StoreRequest
            "/find_node": FindNodeRequest,  # "find_node" should refer to type FindNodeRequest
            "/find_value": FindValueRequest  # "find_value" should refer to type FindValueRequest
        }

        self.subnets: dict = {}

    def start(self) -> None:
        """
        Starts the server.
        Holds up the entire program though, would recommend placing in a thread.
        :return:
        """
        print("[Server] Starting server...")
        self.serve_forever()

    def stop(self):
        """
        Stops the server.
        :return:
        """
        print("[Server] Stopping server...")
        self.shutdown()
        self.server_close()

    def thread_start(self) -> threading.Thread:
        """
        Starts the server on a thread, which is returned
        :return: Thread the server is running on
        """
        thread = threading.Thread(target=self.start)
        thread.start()
        return thread

    def thread_stop(self, thread: threading.Thread) -> None:
        """
        Stops the server on a given thread.
        If the thread is invalid, the server will still shut
        :param thread:
        :return:
        """
        self.shutdown()
        self.server_close()
        thread.join()  # wait for the thread to finish.
        print("[Server] Server stopped.")

    def register_protocol(self, subnet: int, node):
        self.subnets[subnet] = node


class TCPServer(HTTPServer):  # TODO: Create.
    def __init__(self, node):
        server_address: tuple[str, int] = (node.our_contact.protocol.ip, node.our_contact.protocol.port)
        HTTPServer.__init__(
            self,
            server_address=server_address,
            RequestHandlerClass=HTTPSubnetRequestHandler
        )
        self.route_packets = {
            "/Ping": PingRequest,
            "/Store": StoreRequest,
            "/FindNode": FindNodeRequest,
            "/FindValue": FindValueRequest
        }
        self.node = node


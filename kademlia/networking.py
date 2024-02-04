import threading
from time import sleep
from typing import Optional
from http.server import BaseHTTPRequestHandler, HTTPServer

import main
import pickler


class CommonRequestHandler:
    pass


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

    def send_error_response(self):
        self.send_response()


class HTTPSubnetRequestHandler(BaseHTTPRequestHandler):

    def common_request_handler(self,
                               method_name: str,
                               common_request: CommonRequest,
                               node):  # TODO: Make protected.
        if main.DEBUG:
            print(node.our_contact.protocol.type)
            if node.our_contact.protocol.type == "TCPSubnetProtocol":
                if not node.our_contact.protocol.responds:
                    # Exceeds 500ms timeout
                    sleep(secs=1)
        try:
            method = getattr(node, method_name)
            response = method(common_request)
            encoded_response = pickler.encode_data(response)
            self.send_response(200, str(encoded_response))
        except Exception as e:
            error_response: ErrorResponse = ErrorResponse(
                error_message=str(e)
                random_id=ID.random_id()
            )
            self.send_response(400, )



    def do_POST(self):
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
        
        print("Request received:", decoded_request)
        
        request_dict = decoded_request
        path: str = self.path
        # Remove "/"
        # Prefix our call with "server_" so that the method name is unambiguous.
        method_name: str = "server_" + path[1:]  # path.substring(2)

        # What type is the request?
        try:
            # path is something like /ping or /find_node
            request_type: Optional[str] = routing_methods[path]
        except KeyError:
            request_type: Optional[str] = None

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
                new_thread = threading.Thread(
                    target=self.common_request_handler,  # TODO: This does not exist.
                    args=(method_name, common_request, node, self)
                )
                new_thread.start()

            else:
                encoded_response = pickler.encode_data("Subnet node not found.")
                self.send_response(400, "Subnet node not found.")  # TODO: Make encrypted.

            # context.close_connection = True


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


class TCPSubnetServer(HTTPServer):
    def __init__(self, server_address: tuple[str, int]):
        HTTPServer.__init__(
            self,
            server_address=server_address,
            RequestHandlerClass=HTTPSubnetRequestHandler
        )

        # TODO: Should these be double slashed?
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
        print("Starting server...")
        self.serve_forever()

    def stop(self):
        """
        Stops the server.
        :return:
        """
        print("Stopping server...")
        self.shutdown()

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
        thread.join()  # wait for the thread to finish.
        print("Server stopped.")

    def register_protocol(self, subnet: int, node):
        self.subnets[subnet] = node

    def process_a_request(self, context: BaseHTTPRequestHandler):  # TODO: May be obsolete.
        """
        I don't know much about HTTP Servers.
        "The server is a straightforward HttpListener implemented as a C# HttpListenerContext
        object, but note how the subnet ID is used to route the request to the specific node
        associated with the subnet." - Marc Clifton

        I am planning to take in a BaseHTTPRequestHandler object and read the body.
        The body should be a pickled dictionary containing key value pairs of the following values:

            protocol: object
            protocol_name: str
            random_id: int
            sender: int
            key: int
            value: int
            is_cached: bool
            expiration_time_sec: int
            subnet: int
        """
        context.handle_one_request()

        # TODO: Add Encryption !!!!!!!

        encoded_request: bytes = context.rfile.read()
        decoded_request: dict = pickler.decode_data(encoded_request)
        request_dict = decoded_request

        print(context.request, context.command)
        if context.command == "POST":
            path: str = context.path
            # Remove "//"
            # Prefix our call with "Server" so that the method name is unambiguous.
            method_name: str = "Server" + path[2:]  # path.substring(2)

            # What type is the request?
            try:
                # path is something like //ping or //find_node
                request_type: type | None = self.routing_methods[path]
            except KeyError:
                request_type: type | None = None

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
                node = self.subnets.get(subnet)
                if node:
                    # TODO: Make Asynchronous
                    new_thread = threading.Thread(
                        target=CommonRequestHandler,  # TODO: This does not exist.
                        args=(method_name, common_request, node, context)
                    )
                    new_thread.start()

                else:
                    send_error_response(
                        context,
                        ErrorResponse("Subnet node not found.")
                    )

                # context.close_connection = True

import asyncio
import json
import logging
import threading
from asyncio import Lock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import sleep
from typing import Optional, Callable

from .constants import Constants
from .dictionaries import (PingRequest, StoreRequest,
                                       FindNodeRequest,
                                       FindValueRequest, ErrorResponse,
                                       CommonRequest, PingSubnetRequest,
                                       StoreSubnetRequest,
                                       FindNodeSubnetRequest,
                                       FindValueSubnetRequest)
from .errors import IncorrectProtocolError
from .id import ID
from .node import Node
from .protocols import TCPProtocol, decode_protocol

logger = logging.getLogger("__main__")


class BaseServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], request_handler_class):
        logger.info(f"[Server] Server socket address: {server_address}")
        ThreadingHTTPServer.__init__(
            self,
            server_address=server_address,
            RequestHandlerClass=request_handler_class
        )

        self.routing_methods: dict[str, type] = {
            "/ping": PingRequest,  # "ping" should refer to type PingRequest
            "/store": StoreRequest,  # "store" should refer to type StoreRequest
            "/find_node": FindNodeRequest,  # "find_node" should refer to type FindNodeRequest
            "/find_value": FindValueRequest  # "find_value" should refer to type FindValueRequest
        }
        self.subnets = {}
        self.subnet_lock = threading.Lock()

    def start(self) -> None:
        """
        Starts the server.
        :return:
        """
        logger.info("[Server] Starting server...")
        self.serve_forever()

    def stop(self):
        """
        Stops the server.
        :return:
        """
        logger.warning("[Server] Stopping server...")
        self.shutdown()
        self.server_close()

    def thread_start(self) -> threading.Thread:
        """
        Starts the server on a specific thread that is returned –
        this is probably obsolete now that ThreadingHTTPServer is used, instead of HTTPServer.
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
        logger.info("[Server] Server stopped.")


class HTTPRequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        request_type, request_dict, method_name = self.base_post_handling()

        # if we know what the request wants (if it's a ping/find_node RPC etc.)
        if request_type:
            common_request: CommonRequest = CommonRequest(
                protocol=request_dict.get("protocol"),
                random_id=request_dict.get("random_id"),
                sender=request_dict.get("sender"),
                key=request_dict.get("key"),
                value=request_dict.get("value"),
                is_cached=request_dict.get("is_cached"),
                expiration_time_sec=request_dict.get("expiration_time_sec")
            )

            node = self.server.node
            if node:
                self._common_request_handler(method_name, common_request, node)

            else:
                logger.error("[Server] Node not found.")
                encoded_response = bytes(json.dumps({"error_message": "Node not found."}),
                                         Constants.PICKLE_ENCODING)
                self.send_header("Content-Type", "application/octet-stream")
                self.end_headers()
                self.send_response(400)
                try:
                    self.wfile.write(encoded_response)
                except ConnectionRefusedError:
                    logger.error("[Server] Connection refused by client - we may have timed out.")
                except Exception as e:
                    logger.error(f"[Server] Exception sending response: {e}")


    def _common_request_handler(self,
                                method_name: str, common_request: CommonRequest, node):
        try:
            method: Callable = getattr(node, method_name)
            # Calls method, eg: server_store.
            response = method(common_request)

            # Fix protocols for JSON serialization
            if response.get("contacts"):
                for contact in response["contacts"]:
                    contact["protocol"] = contact["protocol"].encode()

            encoded_response = bytes(json.dumps(response), Constants.PICKLE_ENCODING)
            logger.debug(f"[Server] Sending encoded 200: {response}")
            self.send_response(code=200)
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()

            try:
                self.wfile.write(encoded_response)
                logger.debug("[Server] Writing response success!")
            except (ConnectionRefusedError, ConnectionAbortedError,
                    ConnectionResetError):
                logger.error("[Server] Connection refused by client (timeout?)")
            except Exception as e:
                logger.error(f"[Server] Write error: {e}")

        except Exception as e:
            logger.error(f"[Server] Handler exception: {e}")
            error_response = ErrorResponse(
                error_message=str(e),
                random_id=ID.random_id().value
            )
            encoded_response = bytes(json.dumps(error_response), Constants.PICKLE_ENCODING)

            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            self.send_response(code=400)
            try:
                self.wfile.write(encoded_response)
            except Exception as e:
                logger.error(f"[Server] Error sending failure response: {e}")

    def base_post_handling(self):
        logger.debug("[Server] POST Received.")

        routing_methods = {
            "/ping": PingRequest,  # "ping" should refer to type PingRequest
            "/store": StoreRequest,  # "store" should refer to type StoreRequest
            "/find_node": FindNodeRequest,  # "find_node" should refer to type FindNodeRequest
            "/find_value": FindValueRequest  # "find_value" should refer to type FindValueRequest
        }

        content_length = int(self.headers['Content-Length'])
        encoded_request: str = self.rfile.read(content_length).decode(Constants.PICKLE_ENCODING)
        decoded_request: dict = json.loads(encoded_request)
        # decode protocol
        decoded_request["protocol"] = decode_protocol(decoded_request["protocol"])
        logger.debug(f"[Server] Request received: {decoded_request}")

        request_dict = decoded_request
        path: str = self.path
        # Remove "/"
        # Prefix our call with "server_" so that the method name is unambiguous.
        method_name: str = "server_" + path[1:]  # path.substring(2)
        # What type is the request?
        try:
            # path is something like /ping or /find_node
            request_type: Optional[type] =  routing_methods[path]
        except KeyError:
            request_type: Optional[type] = None

        return request_type, request_dict, method_name


class TCPServer(BaseServer):
    def __init__(self, node: Node | None = None,
                 server_address: tuple[str, int] | None = None):
        """
        Creates a server using TCP, based on a Threading HTTP Server from http.server, the
        given node provides the IP and port tuple to start the server.
        :param node:
        """

        if (server_address and node) or (not server_address and not node):
            raise ValueError("Must provide either a node or a subnet server address.")

        if server_address:
            self.routing_methods: dict[str, type] = {
                "/ping": PingSubnetRequest,  # "ping" should refer to type PingSubnetRequest
                "/store": StoreSubnetRequest,  # "store" should refer to type StoreSubnetRequest
                "/find_node": FindNodeSubnetRequest,  # "find_node" should refer to type FindNodeSubnetRequest
                "/find_value": FindValueSubnetRequest  # "find_value" should refer to type FindValueSubnetRequest
            }
            super().__init__(
                server_address=server_address,
                request_handler_class=HTTPRequestHandler
            )

        elif node:
            self.node: Node = node
            if isinstance(self.node.our_contact.protocol, TCPProtocol):
                server_address: tuple[str, int] = (self.node.our_contact.protocol.url,
                                                   self.node.our_contact.protocol.port)
            else:
                raise IncorrectProtocolError("Invalid protocol.")
            super().__init__(
                server_address=server_address,
                request_handler_class=HTTPRequestHandler
            )

    def register_protocol(self, subnet: int, node):
        with self.subnet_lock:
            self.subnets[subnet] = node


class HTTPSubnetRequestHandler(HTTPRequestHandler):

    def _common_request_handler(self,
                                method_name: str, common_request: CommonRequest, node):

        # Test what happens if a node does not respond
        if (node.our_contact.protocol.type == "TCPSubnetProtocol"
                and not node.our_contact.protocol.responds):
            # Exceeds 500ms timeout
            logger.warning("[Server] Does not respond, sleeping for timeout.")
            sleep(Constants.REQUEST_TIMEOUT_SEC + 0.01)

        HTTPRequestHandler._common_request_handler(self, method_name, common_request, node)

    def do_POST(self):
        request_type, request_dict, method_name = self.base_post_handling()

        # if we know what the request wants (if it's a ping/find_node RPC etc.)
        if request_type:
            common_request: CommonRequest = CommonRequest(
                protocol=request_dict.get("protocol"),
                random_id=request_dict.get("random_id"),
                sender=request_dict.get("sender"),
                key=request_dict.get("key"),
                value=request_dict.get("value"),
                is_cached=request_dict.get("is_cached"),
                expiration_time_sec=request_dict.get("expiration_time_sec")
            )

            subnet: int = request_dict["subnet"]
            # If we know the node on the subnet, this should always happen right?
            # Because this is for testing on the same PC.
            self.server: TCPSubnetServer
            with self.server.subnet_lock:
                node = self.server.subnets.get(subnet)
            if node:
                self._common_request_handler(method_name, common_request, node)

            else:
                logger.error("[Server] Subnet node not found.")
                encoded_response = bytes(json.dumps({"error_message": "Subnet node not found."}),
                                         Constants.PICKLE_ENCODING)
                self.send_header("Content-Type", "application/octet-stream")
                self.end_headers()
                self.send_response(400)
                try:
                    self.wfile.write(encoded_response)
                except ConnectionRefusedError:
                    logger.error("[Server] Connection refused by client - we may have timed out.")
                except Exception as e:
                    logger.error(f"[Server] Exception sending response: {e}")


class TCPSubnetServer(BaseServer):
    def __init__(self, server_address: tuple[str, int]):

        self.routing_methods: dict[str, type] = {
            "/ping": PingSubnetRequest,  # "ping" should refer to type PingSubnetRequest
            "/store": StoreSubnetRequest,  # "store" should refer to type StoreSubnetRequest
            "/find_node": FindNodeSubnetRequest,  # "find_node" should refer to type FindNodeSubnetRequest
            "/find_value": FindValueSubnetRequest  # "find_value" should refer to type FindValueSubnetRequest
        }

        super().__init__(
            server_address=server_address,
            request_handler_class=HTTPSubnetRequestHandler
        )

    def register_protocol(self, subnet: int, node):
        self.subnets[subnet] = node


from aiohttp import web

class AsyncServer:
    routes = web.RouteTableDef()

    def __init__(self, host: str, port: int):
        self.host, self.port = host, port
        self.subnets: dict[int, Node] = {}
        self.subnet_lock = Lock()
        self.loop = None

        self.number_of_requests = 0
        self.counter_lock = Lock()

        self.app = web.Application(
            client_max_size=Constants.PIECE_LENGTH * 10)
        self.app.add_routes([
            web.post('/ping', self.handle_ping),
            web.post('/store', self.handle_store),
            web.post('/find_node', self.handle_find_node),
            web.post('/find_value', self.handle_find_value)
        ])
        self.runner = None
        self.site = None

    async def start(self):
        """Starts the server"""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        logger.info(f"Server started on {self.host}:{self.port}")

    async def end(self):
        try:
            if self.site:
                await self.site.stop()
        except Exception as e:
            logger.error(f"Error stopping site: {e}")

        try:
            if self.runner:
                await self.runner.cleanup()
        except Exception as e:
            logger.error(f"Error cleaning up runner: {e}")

    async def register_protocol(self, subnet: int, node):
        async with self.subnet_lock:
            self.subnets[subnet] = node

    async def handle_ping(self, request: web.Request):
        return await self.handle_rpc(request, "ping")

    async def handle_store(self, request: web.Request):
        return await self.handle_rpc(request, "store")

    async def handle_find_node(self, request: web.Request):
        return await self.handle_rpc(request, "find_node")

    async def handle_find_value(self, request: web.Request):
        return await self.handle_rpc(request, "find_value")

    async def handle_rpc(self, request: web.Request,method_name:str):
        try:
            async with self.counter_lock:
                self.number_of_requests += 1
                print(f"Received {method_name} request, current number of requests: {self.number_of_requests}")
            request_dict: dict = await request.json()
            common_request: CommonRequest = CommonRequest(
                protocol=request_dict.get("protocol"),
                random_id=request_dict.get("random_id"),
                sender=request_dict.get("sender"),
                key=request_dict.get("key"),
                value=request_dict.get("value"),
                is_cached=request_dict.get("is_cached"),
                expiration_time_sec=request_dict.get("expiration_time_sec")
            )
            subnet = request_dict.get("subnet")

            if common_request["protocol"]:
                common_request["protocol"] = decode_protocol(common_request["protocol"])

            async with self.subnet_lock:
                node: Node = self.subnets.get(subnet)

            if node:
                # For unit testing, I don't see why else we'd call this.
                if Constants.DEBUG and hasattr(node.our_contact.protocol, "responds"):
                    if not node.our_contact.protocol.responds:
                        logger.warning(
                            "[Server] Does not respond, sleeping for timeout.")
                        await asyncio.sleep(Constants.REQUEST_TIMEOUT_SEC + 0.01)

                method: Callable = getattr(node, "server_" + method_name)
                loop = asyncio.get_event_loop()
                # this calls method(common_request)
                response = await loop.run_in_executor(None,
                                                      method, common_request)
                if response.get("contacts"):
                    for contact in response["contacts"]:
                        contact["protocol"] = contact["protocol"].encode()

                ret = web.json_response(response)
                async with self.counter_lock:
                    self.number_of_requests -= 1
                return ret

            else:
                logger.error("AsyncServer: Subnet node not found.")
                async with self.counter_lock:
                    self.number_of_requests -= 1
                return web.json_response({
                    "status": "error",
                    "error_message": "Subnet node not found."
                }, status=400)

        except Exception as e:
            logger.error(f"AsyncServer: Error handling {method_name} request: "
                         f"{type(e)}: {e}, {e.__dict__}")
            async with self.counter_lock:
                self.number_of_requests -= 1
            return web.json_response({
                "status": "error",
                "error_message": str(e)
            }, status=500)

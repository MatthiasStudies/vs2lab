"""
Client and server using classes
"""
import json
import logging
import socket
from abc import ABC
from typing import TypedDict, Literal, Any

import const_cs
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)  # init loging channels for the lab

# pylint: disable=logging-not-lazy, line-too-long


type QueryType = Literal["GET", "GETALL"]


def parse_query(request: bytes) -> tuple[QueryType, dict[str, Any]]:
    request_data = json.loads(request)

    if "type" not in request_data:
        raise ValueError("Missing 'type' in request data")

    return (
        request_data["type"],
        request_data.get("data", {}),
    )

type Response = dict[str, Any]

def encode_response(response: Response) -> bytes:
    return json.dumps(response).encode('utf-8')

def encode_request(query_type: QueryType, data: dict[str, Any]) -> bytes:
    return json.dumps({"type": query_type, "data": data}).encode('utf-8')

def decode_response(response: bytes) -> Response:
    return json.loads(response)

def require_data(data: dict[str, Any], *keys: str) -> tuple[...]:
    """ Require data keys """
    values = []
    for key in keys:
        if key not in data:
            raise ValueError(f"Missing required data key: {key}")
        values.append(data[key])
    return tuple(values)

def response(detail: dict[str, Any]) -> Response:
    """ Return response """
    return {"data": detail}

class Server:
    """ The server """
    _logger = logging.getLogger("vs2lab.lab1.clientserver.Server")
    _serving = True

    def __init__(self, db: dict[str, str] = None):
        self.db: dict[str, str] = db or {}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # prevents errors due to "addresses in use"
        self.sock.bind((const_cs.HOST, const_cs.PORT))
        self.sock.settimeout(3)  # time out in order not to block forever
        self._logger.info("Server bound to socket " + str(self.sock))

    def handle_query(self, query_type: QueryType, data: dict[str, Any]) -> Response:
        """ Handle query """
        match query_type:
            case "GET":
                (name,) = require_data(data, "name")
                number = self.db.get(name)
                if number is None:
                    return {"error": "Name not found"}
                return response({
                    "name": name,
                    "number": number
                })
            case "GETALL":
                return response(self.db)
            case _:
                return {"error": "Invalid query type"}

    def listen(self):
        # pylint: disable=unused-variable
        (connection, address) = self.sock.accept()  # returns new socket and address of client
        while True:  # forever
            data = connection.recv(1024)  # receive data from client
            if not data:
                break  # stop if client stopped

            self._logger.info(f"Received {data.decode("utf-8")} from {address}")

            try:
                (query_type, detail) = parse_query(data)
                response = self.handle_query(query_type, detail)
                connection.send(encode_response(response))
                self._logger.info(f"Sent {response} to {address}")
            except Exception as e:
                self._logger.error(f"Error handling query: {e}")
                connection.send(encode_response({
                    "error": f"An error occurred: {e}"
                }))

        connection.close()  # close the connection
        self._logger.info(f"Connection closed from {address}")

    def serve(self):
        """ Serve echo """
        self.sock.listen(1)
        self._logger.info("Server listening")
        while self._serving:  # as long as _serving (checked after connections or socket timeouts)
            try:
                self.listen()
            except socket.timeout:
                pass  # ignore timeouts
        self.sock.close()
        self._logger.info("Server down.")

    def stop(self):
        """ Stop server """
        self._serving = False
        self.sock.close()
        self._logger.info("Server stopped.")


class Client:
    """ The client """
    logger = logging.getLogger("vs2lab.a1_layers.clientserver.Client")

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((const_cs.HOST, const_cs.PORT))
        self.logger.info("Client connected to socket " + str(self.sock))

    def call(self, query_type: QueryType, data: dict[str, Any] | None = None) -> Response:
        """ Call server """
        data = data or {}

        self.logger.info(f"Calling server with '{query_type}' query and {data}")

        request = encode_request(query_type, data)
        self.sock.send(request)  # send encoded string as data
        data = self.sock.recv(1024)  # receive the response

        if not data:
            return {}

        response = decode_response(data)

        self.logger.info(f"Received response: {response}")

        return decode_response(data)

    def close(self):
        """ Close socket """
        self.sock.close()
        self.logger.info("Client closed socket")

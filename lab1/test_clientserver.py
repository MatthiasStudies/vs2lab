"""
Simple client server unit test
"""

import logging
import threading
import unittest

import clientserver
from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)


class TestEchoService(unittest.TestCase):
    """The test"""
    _server = clientserver.Server({
        "John Doe": "123456789",
        "Jane Doe": "1122334455"
    })  # create single server in class variable
    _server_thread = threading.Thread(target=_server.serve)  # define thread for running server

    @classmethod
    def setUpClass(cls):
        cls._server_thread.start()  # start server loop in a thread (called only once)

    def setUp(self):
        super().setUp()
        self.client = clientserver.Client()  # create new client for each test

    def test_returns_an_error_if_query_type_is_missing(self):
        response = self.client.call("") # type: Ignore
        self.assertEqual(response, {'error': 'Invalid query type'})

    def test_returns_an_error_if_query_type_is_invalid(self):
        response = self.client.call("INVALID") # type: Ignore
        self.assertEqual(response, {'error': 'Invalid query type'})

    def test_returns_an_error_if_get_query_misses_name(self):
        response = self.client.call("GET")
        self.assertEqual(response, {'error': 'An error occurred: Missing required data key: name'})

    def test_returns_an_error_if_name_was_not_found(self):
        response = self.client.call("GET", {"name": "abc"})
        self.assertEqual(response, {'error': 'Name not found'})

    def test_returns_the_number_for_a_known_name(self):
        response = self.client.call("GET", {"name": "John Doe"})
        self.assertEqual(response, {"data":{'name': 'John Doe', 'number': '123456789'}})

    def test_returns_all_known_names_and_numbers(self):
        response = self.client.call("GETALL")
        self.assertEqual(response, {
            "data":[
                {'name': 'John Doe', 'number': '123456789'},
                {'name': 'Jane Doe', 'number': '1122334455'}
            ],
        })


    def tearDown(self):
        self.client.close()  # terminate client after each test

    @classmethod
    def tearDownClass(cls):
        cls._server._serving = False  # break out of server loop. pylint: disable=protected-access
        cls._server_thread.join()  # wait for server thread to terminate


if __name__ == '__main__':
    unittest.main()

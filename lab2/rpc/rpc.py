import threading
import time

from typing import Callable

import constRPC

from context import lab_channel


class DBList:
    def __init__(self, basic_list):
        self.value = list(basic_list)

    def append(self, data):
        self.value = self.value + [data]
        return self


class Client:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.client = self.chan.join('client')
        self.server = None

    def run(self):
        self.chan.bind(self.client)
        self.server = self.chan.subgroup('server')

    def stop(self):
        self.chan.leave('client')

    def append(self, data: str, db_list: DBList, callback_on_result: Callable[[DBList], None]) -> str:
        assert isinstance(db_list, DBList)
        msglst = (constRPC.APPEND, data, db_list)  # message payload
        self.chan.send_to(self.server, msglst)  # send msg to server
        msgrcv = self.chan.receive_from(self.server)  # wait for response

        assert msgrcv[1] == constRPC.ACK

        def wait():
            rcv = self.chan.receive_from(self.server)  # wait for response
            callback_on_result(rcv[1])  # call the callback with the result

        threading.Thread(target=wait, daemon=True).start()
        return constRPC.ACK


class Server:
    def __init__(self):
        self.chan = lab_channel.Channel()
        self.server = self.chan.join('server')
        self.timeout = 3


    @staticmethod
    def append(data, db_list):
        assert isinstance(db_list, DBList)  # - Make sure we have a list
        time.sleep(5)
        return db_list.append(data)  # - Append data to list and return new list

    def wrap(self, fn: Callable[[], ...], client: str):
        def run():
            result = fn()
            self.chan.send_to({client}, result)

        threading.Thread(target=run, daemon=True).start()
        return constRPC.ACK


    def run(self):
        self.chan.bind(self.server)
        while True:
            msgreq = self.chan.receive_from_any(self.timeout)  # wait for any request
            if msgreq is None:
                continue

            client, msgrpc = msgreq  # see who is the caller

            match msgrpc[0]:
                case constRPC.APPEND:
                    result = self.wrap(lambda: self.append(msgrpc[1], msgrpc[2]), client)
                    self.chan.send_to({client}, result)  # return response

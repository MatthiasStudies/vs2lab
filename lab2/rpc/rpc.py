import threading
import time

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

    def append(self, data, db_list, callback_on_result):
        assert isinstance(db_list, DBList)
        msglst = (constRPC.APPEND, data, db_list)  # message payload
        self.chan.send_to(self.server, msglst)  # send msg to server
        msgrcv = self.chan.receive_from(self.server)  # wait for response

        assert msgrcv[1] == constRPC.OK

        def wait():
            msgrcv = self.chan.receive_from(self.server)  # wait for response
            callback_on_result(msgrcv[1])  # call the callback with the result

        threading.Thread(target=wait, daemon=True).start()
        return constRPC.OK


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

    def wrap(self, fn, client):
        def run():
            result = fn()
            self.chan.send_to({client}, result)

        threading.Thread(target=run, daemon=True).start()
        return constRPC.OK


    def run(self):
        self.chan.bind(self.server)
        while True:
            msgreq = self.chan.receive_from_any(self.timeout)  # wait for any request
            if msgreq is not None:
                client = msgreq[0]  # see who is the caller
                msgrpc = msgreq[1]  # fetch call & parameters
                if constRPC.APPEND == msgrpc[0]:  # check what is being requested
                    result = self.wrap(lambda: self.append(msgrpc[1], msgrpc[2]), client)
                    self.chan.send_to({client}, result)  # return response
                else:
                    pass

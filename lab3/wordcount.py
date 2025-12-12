import time
from dataclasses import dataclass
import threading
import zmq

print_q = []
def thread_safe_print(*args, **kwargs):
    global print_q
    print_q += [(args, kwargs)]

def process_print_queue():
    global print_q
    while True:
        if print_q:
            args, kwargs = print_q.pop(0)
            print(*args, **kwargs)


class Splitter:
    def __init__(self, file: str, mapper_addresses: list[str]):
        self.file = file
        context = zmq.Context()
        self.mapper_sockets: list[zmq.Socket] = []

        for address in mapper_addresses:
            socket = context.socket(zmq.PUSH)
            socket.connect(address)
            self.mapper_sockets.append(socket)

    def split_and_send(self):
        current_socket_index = 0
        with open(self.file, 'r', encoding='utf-8') as f:
            for line in f:
                socket_info = self.mapper_sockets[current_socket_index]
                socket_info.send_string(line.strip())
                current_socket_index = (current_socket_index + 1) % len(self.mapper_sockets)

        for socket in self.mapper_sockets:
            socket.send_string("__EOF__")  # Send termination signal

class Reducer:
    def __init__(self, address: str, number: int):
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.bind(address)
        self.word_count = {}
        self.number = number

    def reduce(self):
        while True:
            word = self.socket.recv_string()
            if word == "__EOF__":
                break
            if word in self.word_count:
                self.word_count[word] += 1
            else:
                self.word_count[word] = 1

            thread_safe_print(f"[{self.number}] Word '{word}' count: {self.word_count[word]}")



class Mapper:
    def __init__(self, address: str, reducer_addresses: list[str]):
        context = zmq.Context()
        self.socket = context.socket(zmq.PULL)
        self.socket.bind(address)
        self.reducer_sockets: list[zmq.Socket] = []
        for address in reducer_addresses:
            sock = context.socket(zmq.PUSH)
            sock.connect(address)
            self.reducer_sockets.append(sock)

    def word_to_reducer(self, word: str) -> zmq.Socket:
        index = hash(word) % len(self.reducer_sockets)
        return self.reducer_sockets[index]

    def map(self):
        while True:
            line = self.socket.recv_string()
            if line == "__EOF__":
                for socket in self.reducer_sockets:
                    socket.send_string("__EOF__")  # Send termination signal to reducers
                break

            words = line.split()
            for word in words:
                socket = self.word_to_reducer(word)
                socket.send_string(word)

if __name__ == "__main__":
    reducer_addresses = [
        f"tcp://127.0.0.1:5001",
        f"tcp://127.0.0.1:5002",
    ]
    mapper_addresses = [
        f"tcp://127.0.0.1:6001",
        f"tcp://127.0.0.1:6002",
        f"tcp://127.0.0.1:6003",
    ]

    reducers = []
    for i, address in enumerate(reducer_addresses):
        reducers.append(Reducer(address, i))

    mappers= []
    for address in mapper_addresses:
        mappers.append(Mapper(address, reducer_addresses))

    splitter = Splitter("input.txt", mapper_addresses)

    t_split = threading.Thread(target=splitter.split_and_send, daemon=True)
    t_mappers = []
    t_reducers = []
    for mapper in mappers:
        t_mappers.append(threading.Thread(target=mapper.map, daemon=True))
    for reducer in reducers:
        t_reducers.append(threading.Thread(target=reducer.reduce, daemon=True))

    t_print = threading.Thread(target=process_print_queue, daemon=True)
    t_print.start()

    threads = [t_split] + t_mappers + t_reducers
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    time.sleep(1)  # Allow time for all prints to be processed
    print("Final word counts:")
    for i, reducer in enumerate(reducers):
        print(f"Reducer {i} counts:")
        for word, count in reducer.word_count.items():
            print(f"  {word}: {count}")


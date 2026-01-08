import zmq
import multiprocessing
import time

from const import *

def splitter(filename):
    context = zmq.Context()
    sender = context.socket(zmq.PUSH)
    sender.bind(f"tcp://*:{SPLITTER_PORT}")

    time.sleep(1)

    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            sender.send_string(line.strip())

    for _ in range(NUM_MAPPERS):
        sender.send_string("__EOF__")

    sender.close()
    context.term()

def mapper():
    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect(f"tcp://localhost:{SPLITTER_PORT}")

    reducer_sockets = []
    for port in REDUCER_PORTS:
        sender = context.socket(zmq.PUSH)
        sender.connect(f"tcp://localhost:{port}")
        reducer_sockets.append(sender)

    while True:
        message = receiver.recv_string()

        if message == "__EOF__":
            for sock in reducer_sockets:
                sock.send_string("__EOF__")
            break

        words = message.split()
        for word in words:
            reducer_idx = hash(word) % NUM_REDUCERS
            target_socket = reducer_sockets[reducer_idx]
            target_socket.send_string(word)

    receiver.close()
    for sock in reducer_sockets:
        sock.close()
    context.term()



if __name__ == "__main__":
    processes = []

    for i in range(NUM_MAPPERS):
        p = multiprocessing.Process(target=mapper)
        p.start()
        processes.append(p)

    p_splitter = multiprocessing.Process(target=splitter, args=("input.txt",))
    p_splitter.start()
    processes.append(p_splitter)

    for p in processes:
        p.join()

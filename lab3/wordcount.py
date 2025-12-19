import zmq
import multiprocessing
import time

NUM_MAPPERS = 3
NUM_REDUCERS = 2
SPLITTER_PORT = 5555
REDUCER_PORTS = [5556, 5557]

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

def reducer(reducer_id, port):
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.bind(f"tcp://*:{port}")

    counts = {}
    mappers_finished_count = 0

    while True:
        message = receiver.recv_string()

        if message == "__EOF__":
            mappers_finished_count += 1
            if mappers_finished_count == NUM_MAPPERS:
                break
        else:
            word = message
            counts[word] = counts.get(word, 0) + 1
            print(f" -> [{reducer_id}] '{word}': {counts[word]}")

    print(f"\nReducer {reducer_id} final wordcount:")
    for word, count in counts.items():
        print(f"{word}: {count}")
    print("----------------------------------\n")

    receiver.close()
    context.term()

if __name__ == "__main__":
    processes = []

    for i in range(NUM_REDUCERS):
        p = multiprocessing.Process(target=reducer, args=(i, REDUCER_PORTS[i]))
        p.start()
        processes.append(p)

    for i in range(NUM_MAPPERS):
        p = multiprocessing.Process(target=mapper)
        p.start()
        processes.append(p)

    p_splitter = multiprocessing.Process(target=splitter, args=("input.txt",))
    p_splitter.start()
    processes.append(p_splitter)

    for p in processes:
        p.join()

import sys

import zmq

from const import *


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
    reducer_num = int(sys.argv[1])
    reducer(reducer_num, REDUCER_PORTS[reducer_num])
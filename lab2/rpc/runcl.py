import threading
import time
from pprint import pprint

import constRPC
import rpc
import logging

from context import lab_logging

lab_logging.setup(stream_level=logging.INFO)

cl = rpc.Client()
cl.run()

result: rpc.DBList | None = None

def on_response(_result):
    global result
    result = _result
    print("\nCallback Response:", end=" ")
    # pprint(result)
    print("Result: {}".format(result.value))


start = time.time()
base_list = rpc.DBList({'foo'})

ack = cl.append('bar', base_list, on_response)

print("ACK:", ack)

print("Waiting for result", end='', flush=True)
while True:
    print('.', end='', flush=True)
    time.sleep(0.5)

stop = time.time()

print()

print("Completed in {:.2f} seconds".format(stop - start))
print("Result: {}".format(result.value))

cl.stop()

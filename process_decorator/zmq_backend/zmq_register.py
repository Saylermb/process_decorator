from random import randint
from time import time

import zmq
from zmq.devices import ProcessDevice


def run_zmq_server():
    streamerdevice = ProcessDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)
    zmq_in = f"ipc:///tmp/pd_in_{int(time())}_{randint(0, 1000)}"
    zmq_out = f"ipc:///tmp/pd_in_{int(time())}_{randint(0, 1000)}"
    streamerdevice.bind_in(zmq_in)
    streamerdevice.bind_out(zmq_out)
    streamerdevice.setsockopt_in(zmq.IDENTITY, b'PULL')
    streamerdevice.setsockopt_out(zmq.IDENTITY, b'PUSH')
    streamerdevice.start()
    return streamerdevice, zmq_in, zmq_out
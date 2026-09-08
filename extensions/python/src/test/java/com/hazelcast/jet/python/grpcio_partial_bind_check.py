"""Shows the grpcio partial-bind behaviour without Jet.
Takes an ephemeral port P on 127.0.0.1 with a plain socket (this plays the phone-back
socket of another Jet tasklet). Then asks grpcio to bind "localhost:P" and "127.0.0.1:P".
    python3 -m venv v && v/bin/pip install grpcio==1.80.0 && v/bin/python grpcio_partial_bind_check.py
grpcio 1.73.0 .. 1.78.1: both binds fail                       -> SAFE
grpcio 1.80.0:           "localhost:P" reports success although  -> HARMFUL
                         only [::1]:P is bound;
                         "127.0.0.1:P" fails                     -> SAFE (what the fixed server uses)
"""

import socket
from concurrent import futures

import grpc


def check(host):
    occupant = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    occupant.bind(("127.0.0.1", 0))
    occupant.listen(1)
    port = occupant.getsockname()[1]

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=1), options=[("grpc.so_reuseport", 0)]
    )
    try:
        result = server.add_insecure_port("%s:%d" % (host, port))
    except RuntimeError:
        result = 0
    if result == port:
        verdict = (
            "HARMFUL: reports port %d although 127.0.0.1:%d belongs to another socket"
            % (port, port)
        )
    else:
        verdict = "SAFE: bind fails"
    print(
        "grpcio %s: add_insecure_port('%s:%d') -> %s   %s"
        % (grpc.__version__, host, port, result, verdict)
    )

    if result:
        server.stop(0)
    occupant.close()


check("localhost")
check("127.0.0.1")

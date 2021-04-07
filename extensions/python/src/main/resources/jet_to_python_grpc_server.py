import grpc
import sys
import os
import socket
import logging
import importlib
from concurrent import futures

import jet_to_python_pb2
import jet_to_python_pb2_grpc

logger = logging.getLogger('Python PID %d' % os.getpid())

class JetToPythonServicer(jet_to_python_pb2_grpc.JetToPythonServicer):

    def __init__(self, handler_function):
        self._handler_function = handler_function

    def streamingCall(self, request_iterator, context):
        for request in request_iterator:
            output_list = self._handler_function(request.inputValue)
            output_item = jet_to_python_pb2.OutputMessage(outputValue = output_list)
            yield output_item
        logger.info('gRPC call completed')


def load_handler_function(handler_module_name, handler_function_name):
    try:
        handler_module = importlib.import_module(handler_module_name)
    except ImportError as e:
        raise RuntimeError("Cannot import module %s" % (handler_module_name), e)
    if not hasattr(handler_module, handler_function_name):
        raise RuntimeError("Handler function %s.%s doesn't exist" % (handler_module_name, handler_function_name))
    return getattr(handler_module, handler_function_name)


def serve(phoneback_port, handler_module_name, handler_function_name):
    # Fail as soon as possible for any simple problem with passed-in arguments
    phoneback_port_int = int(phoneback_port)
    handler_function = load_handler_function(handler_module_name, handler_function_name)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=[
        ('grpc.max_send_message_length', 100 * 1024 * 1024),
        ('grpc.max_receive_message_length', 100 * 1024 * 1024),
        ('grpc.so_reuseport', 0)
    ])
    jet_to_python_pb2_grpc.add_JetToPythonServicer_to_server(
        JetToPythonServicer(handler_function),
        server
    )
    listen_port = server.add_insecure_port('localhost:0')
    if listen_port == 0:
        logger.error("Couldn't find a port to bind to")
        return
    phoneback_message = ('%d\n' % listen_port).encode('utf-8')
    server.start()
    logger.info('started listening on port %d', listen_port)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('localhost', phoneback_port_int))
        s.sendall(phoneback_message)

    # Wait for a stop signal in stdin
    stdin_message = input()
    if stdin_message == 'stop':
        logger.info('Received a "stop" message from stdin. Stopping the server.')
    else:
        logger.info('Received an unexpected message from stdin: "%s"' % stdin_message)
    server.stop(0).wait()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, format='%(asctime)s %(levelname)s [%(name)s] %(threadName)s - %(message)s', level=logging.INFO)
    # Expecting these command-line parameters:
    # - $1 is the port where Jet is listening for the Python process to
    #   'phone back' and tell Jet on which port it started its gRPC endpoint.
    # - $2.$3 is the module.function of the handler function that will handle
    #   the input from Jet.
    serve(phoneback_port=sys.argv[1], handler_module_name=sys.argv[2], handler_function_name=sys.argv[3])

import os

import zmq


LOG_MODE = os.getenv("BROKER_LOG_MODE", os.getenv("SERVER_LOG_MODE", "presentation")).strip().lower()


def _log_verbose(message: str) -> None:
    if LOG_MODE == "verbose":
        print(message, flush=True)

context = zmq.Context()
poller = zmq.Poller()

client_socket = context.socket(zmq.ROUTER)
client_socket.bind("tcp://*:5555")
poller.register(client_socket, zmq.POLLIN)

server_socket = context.socket(zmq.DEALER)
server_socket.bind("tcp://*:5556")
poller.register(server_socket, zmq.POLLIN)

pending_client_id = None

while True:
    socks = dict(poller.poll())

    if socks.get(client_socket) == zmq.POLLIN:
        frames = client_socket.recv_multipart()
        if not frames:
            continue
        pending_client_id = frames[0]
        payload = frames[1:]
        if payload:
            server_socket.send_multipart(payload)
        _log_verbose("Client -> Server")

    if socks.get(server_socket) == zmq.POLLIN:
        frames = server_socket.recv_multipart()
        if pending_client_id is not None:
            client_socket.send_multipart([pending_client_id] + frames)
            pending_client_id = None
            _log_verbose("Server -> Client")

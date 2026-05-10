import zmq

context = zmq.Context()
poller = zmq.Poller()

client_socket = context.socket(zmq.ROUTER)
client_socket.bind("tcp://*:5555")
poller.register(client_socket, zmq.POLLIN)

server_socket = context.socket(zmq.DEALER)
server_socket.bind("tcp://*:5556")
poller.register(server_socket, zmq.POLLIN)

while True:
    socks = dict(poller.poll())

    if socks.get(client_socket) == zmq.POLLIN:
        frames = client_socket.recv_multipart()
        if len(frames) < 2:
            continue
        client_id = frames[0]
        payload = frames[1:]
        server_socket.send_multipart([client_id] + payload)
        print("Client -> Server", flush=True)

    if socks.get(server_socket) == zmq.POLLIN:
        frames = server_socket.recv_multipart()
        if len(frames) < 2:
            continue
        client_socket.send_multipart(frames)
        print("Server -> Client", flush=True)

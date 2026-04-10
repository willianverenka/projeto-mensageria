import zmq


context = zmq.Context()
xsub_socket = context.socket(zmq.XSUB)
xsub_socket.bind("tcp://*:5557")

xpub_socket = context.socket(zmq.XPUB)
xpub_socket.bind("tcp://*:5558")

zmq.proxy(xsub_socket, xpub_socket)

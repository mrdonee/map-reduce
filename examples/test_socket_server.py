# test_socket_server.py
#
# Start server (listener) in the background
# $ python test_socket_server.py &
# Run client to send a message
# $ python test_socket_client.py
# 
# Reference: https://docs.python.org/3/howto/sockets.html

import socket

if __name__ == "__main__":

    # create an INET, STREAMing socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # bind the socket to the server
    # NOTE: use socket.gethostname() if you want server to listen on 0.0.0.0
    s.bind(("localhost", 8000))

    # become a server socket
    s.listen(5)

    while True:
        # connect
        clientsocket, address = s.accept()
        print("Server: connection from ", address)

        # Receive a message
        message = clientsocket.recv(1024)
        clientsocket.close()
        print(message)

# test_socket_client.py
#
# See test_socket_server.py for how-to
#
# Reference: https://docs.python.org/3/howto/sockets.html

import socket

if __name__ == "__main__":

    # create an INET, STREAMing socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # connect to the server
    s.connect(("localhost", 8000))

    # send a message
    message = "hello world!"
    s.sendall(message)
    s.close()

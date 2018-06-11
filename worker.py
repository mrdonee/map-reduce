from helper import group
import json
import os
import sh
import socket
import sys
from threading import Thread
from time import sleep

def sendHeartbeat(HBPort):
    HBSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    pid = os.getpid()
    HB = json.dumps({"message_type": "heartbeat",
                     "worker_pid": pid})
    while True:
        HBSock.sendto(HB.encode("utf-8"), ("localhost", HBPort))
        sleep(2)


class Worker:
    def __init__(self, master_port, worker_port):
        pid = os.getpid()
        workerSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        workerSock.connect(("localhost", int(master_port)))

        message = json.dumps({"message_type": "register",
                              "worker_host": "localhost",
                              "worker_port": int(worker_port),
                              "worker_pid": pid})

        workerSock.sendall(message.encode("utf-8"))
        workerSock.close()

        workerSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        workerSock.bind(("localhost", int(worker_port)))
        workerSock.listen(5)

        while True: #wait for ack!
            WSock, address = workerSock.accept()
            message = ''

            while True:
                data = WSock.recv(1024)
                message += data.decode("utf-8")

                if len(data) != 1024:
                    break
            message = json.loads(message)
            if (message['message_type'] == "register_ack"):
                HBThread = Thread(target = sendHeartbeat, args = (int(master_port) - 1, ))
                HBThread.start()
            elif (message['message_type'] == "new_worker_job"):
                outputFiles = []
                for inFile in message['input_files']:
                    exe = sh.Command(message['executable'])
                    f = open(inFile,'r')
                    exe(_in = f, _out=message['output_directory'] + '/' + inFile.rsplit('/', 1)[-1])
                    outputFiles.append(message['output_directory'] + '/' + inFile.rsplit('/', 1)[-1])
                doneSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                doneSock.connect(("localhost", int(master_port)))
                finishedMsg = json.dumps({"message_type": "status",
                                          "output_files" : outputFiles,
                                          "status": "finished",
                                          "worker_pid": pid})
                doneSock.sendall(finishedMsg.encode("utf-8"))
                doneSock.close()

            elif (message['message_type'] == "new_sort_job"):
                # helper.py function that does the grouping
                group(message['input_files'], message['output_file'])
                # this is all to send the correct message back to the master
                doneSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                doneSock.connect(("localhost", int(master_port)))
                finishedMsg = json.dumps({"message_type": "status",
                                          "output_file": message['output_file'],
                                          "status": "finished",
                                          "worker_pid": pid})
                doneSock.sendall(finishedMsg.encode("utf-8"))
                doneSock.close()
            elif (message['message_type'] == "shutdown"):
                os._exit(0)
            else:
                print("This is super bad dude")


if __name__ == '__main__':
    master_port = sys.argv[1]
    worker_port = sys.argv[2]
    Worker(master_port, worker_port)

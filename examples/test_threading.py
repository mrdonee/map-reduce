# test_threading.py
#
# First, start this program, which starts 5 threads
# $ python3 test_threading.py
# Then, view the processes and threads running on your system
# $ ps -axM
#
# Reference: https://docs.python.org/3/library/threading.html

import os
import threading
import time

def worker(worker_id):
    pid=os.getpid()
    print("Worker {}  pid={}".format(worker_id, pid))
    while True:
        # spin
        pass

if __name__ == "__main__":
    threads = []
    for i in range(5):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

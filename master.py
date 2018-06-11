from helper import group, splitGrouping
import json
import math
import os
import shutil
import socket
import sys
from threading import Thread
import time
from time import sleep


allWorkers = {} # all registered workers, dictionary of pids to Regdworkers
allJobs = {} # All original new_master_job messages that haven't completed
currJobs = {} # current jobs in execution, dictionary of jobCounter to number of workers executing that job
jobFiles = {} # dictionary of jobCounter to all output files created by workers for that job
jobQueue = [] # jobs waitiing to be exeucted, stored as their new_master_job JSON object
jobCounter = -1 # current number of new_master_job requests received

class RegdWorker:
    def __init__(self, workerPort, timeStamp):
        self.worker_port = workerPort
        self.tStamp = timeStamp
        self.status = "ready"
        self.work = []


def faultTolerance():
    while True:
        for i in list(allWorkers):
            if (allWorkers[i].tStamp < time.time() - 10 and allWorkers[i].status != "dead"):
                if (allWorkers[i].status == "busy"):
                    reassignWork = Thread(target = reassign, args = (allWorkers[i].work, ))
                    reassignWork.start()
                    allWorkers[i].work = []
                allWorkers[i].status = "dead"


def heartbeatListener(port):
    HBSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    HBSock.bind(("localhost", int(port) - 1))

    while True:
        data, address = HBSock.recvfrom(4096) # 4096 = UDP port
        if data:
            data = data.decode("utf-8")
            data = json.loads(data)
            allWorkers[data['worker_pid']].tStamp = time.time()


def reassign(work):
    pids = []
    for worker in allWorkers:
        if (allWorkers[worker].status == "ready"):
            pids.append(worker)
    if not len(pids):
        for worker in allWorkers:
            if (allWorkers[worker].status == "busy"):
                pids.append(worker)
    i = 0
    for job in work:
        newSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        newSock.connect(("localhost", allWorkers[pids[i]].worker_port))

        allWorkers[pids[i]].status = "busy"
        allWorkers[pids[i]].work.append(job)

        newSock.sendall(job.encode("utf-8"))
        newSock.close()
        i = (i + 1) % len(pids)


def handleMessage(message):
    if (message['message_type'] == 'register'):

        ackSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ackSock.connect(("localhost", int(message['worker_port'])))

        ack = json.dumps({"message_type": "register_ack",
                          "worker_host": "localhost",
                          "worker_port": int(message['worker_port']),
                          "worker_pid" : int(message['worker_pid'])})

        allWorkers[message['worker_pid']] = RegdWorker(message['worker_port'], time.time())

        ackSock.sendall(ack.encode("utf-8"))
        ackSock.close()
        if len(allWorkers) == 1 and len(jobQueue):
            message = jobQueue[0]
            mapToWorker(message)

    elif (message['message_type'] == 'new_master_job'):
        global jobCounter
        jobCounter = jobCounter + 1
        os.makedirs('var/job-' + str(jobCounter))
        os.makedirs('var/job-' + str(jobCounter) + '/mapper-output')
        os.makedirs('var/job-' + str(jobCounter) + '/grouper-output')
        os.makedirs('var/job-' + str(jobCounter) + '/reducer-output')

        allJobs[jobCounter] = message

        workersReady = False
        for worker in allWorkers:
            if allWorkers[worker].status == "ready":
                workersReady = True
                break

        if len(jobQueue) or not workersReady:
            jobQueue.append(message)
        else:
            mapToWorker(jobCounter)

    elif (message['message_type'] == 'status'):
        allWorkers[message['worker_pid']].status = "ready"
        allWorkers[message['worker_pid']].work.pop(0)

        try:
            jCounter = message['output_files'][0].split('/')[1].split('-')[1]
            currJobs[int(jCounter)] -= 1

            if (message['output_files'][0].split('/')[2] == "mapper-output"):
                for f in message['output_files']:
                    if int(jCounter) in jobFiles:
                        jobFiles[int(jCounter)].append(f)
                    else:
                        jobFiles[int(jCounter)] = [f]

                if (not currJobs[int(jCounter)]):
                    groupToWorker(int(jCounter))
            elif (message['output_files'][0].split('/')[2] == "reducer-output"):
                if (not currJobs[int(jCounter)]):
                    finisher(int(jCounter))
                    del currJobs[int(jCounter)]
                    del allJobs[int(jCounter)]
                    if (len(jobQueue)):
                        jobQueue.pop(0)
                        mapToWorker(jobCounter - len(jobQueue))

        except:
            jCounter = message['output_file'].split('/')[1].split('-')[1]
            currJobs[int(jCounter)] -= 1
            if int(jCounter) in jobFiles:
                jobFiles[int(jCounter)].append(message['output_file'])
            else:
                jobFiles[int(jCounter)] = [message['output_file']]

            if (not currJobs[int(jCounter)]):
                numKeys = group(jobFiles[int(jCounter)], "var/job-" + str(jCounter) + "/grouper-output/master")
                splitGrouping("var/job-" + jCounter + "/grouper-output/master", allJobs[int(jCounter)]['num_reducers'], int(jCounter), numKeys)
                reduceToWorker(int(jCounter))
                del jobFiles[int(jCounter)]


    elif (message['message_type'] == 'shutdown'):
        for i in allWorkers:
            shutdownSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            shutdownSock.connect(("localhost", int(allWorkers[i].worker_port)))
            shutdown = json.dumps({"message_type": "shutdown"})
            shutdownSock.sendall(ack.encode("utf-8"))
            shutdownSock.close()
        os._exit(0)

    else:
        print("Something has gone very wrong")


def mapToWorker(jCounter):
    message = allJobs[jCounter]

    mappers = [[] for x in range(message['num_mappers'])]
    i = 0
    for filename in os.listdir(message['input_directory']):
        mappers[i].append(message['input_directory'] + '/' + filename)
        i = (i + 1) % message['num_mappers']

    pids = []
    for worker in allWorkers:
        if (allWorkers[worker].status == "ready"):
            pids.append(worker)
    i = 0
    for mapper in mappers:
        jobSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        jobSock.connect(("localhost", allWorkers[pids[i]].worker_port))

        newWorkerJob = json.dumps({"message_type": "new_worker_job",
                                   "input_files": mapper,
                                   "executable": message['mapper_executable'],
                                   "output_directory": "var/job-" + str(jCounter) + "/mapper-output",
                                   "worker_pid" : pids[i]})

        if jobCounter in currJobs:
            currJobs[jCounter] += 1
        else:
            currJobs[jCounter] = 1
        allWorkers[pids[i]].status = "busy"
        allWorkers[pids[i]].work.append(newWorkerJob)

        jobSock.sendall(newWorkerJob.encode("utf-8"))
        jobSock.close()
        i = (i + 1) % len(pids)

def groupToWorker(jCounter):
    files = jobFiles[jCounter]
    del jobFiles[jCounter]

    pids = []
    for worker in allWorkers:
        if (allWorkers[worker].status == "ready"):
            pids.append(worker)

    #assign certain files to a pid
    assign = [[] for x in range(len(pids))]
    i = 0
    for f in files:
        assign[i].append(f)
        i = (i + 1) % len(pids)

    i = 0
    for a in assign:
        groupSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        groupSock.connect(("localhost", allWorkers[pids[i]].worker_port))

        groupJob = json.dumps({"message_type": "new_sort_job",
                               "input_files": a,
                               "output_file": "var/job-" + str(jCounter) + "/grouper-output/" + str(pids[i]),
                               "worker_pid" : pids[i]})

        currJobs[jCounter] += 1
        allWorkers[pids[i]].status = "busy"
        allWorkers[pids[i]].work.append(groupJob)

        groupSock.sendall(groupJob.encode("utf-8"))
        groupSock.close()
        i = (i + 1) % len(pids)


def reduceToWorker(jCounter):
    message = allJobs[int(jCounter)]
    reducers = [[] for x in range(message['num_reducers'])]
    i = 0
    for n in range(message['num_reducers']):
        reducers[i].append("var/job-" + str(jCounter) + "/grouper-output/input_" + str(n))
        i = (i + 1) % message['num_reducers']

    pids = []
    for worker in allWorkers:
        if (allWorkers[worker].status == "ready"):
            pids.append(worker)
    i = 0
    for reducer in reducers:
        redSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        redSock.connect(("localhost", allWorkers[pids[i]].worker_port))

        newWorkerJob = json.dumps({"message_type": "new_worker_job",
                                   "input_files": reducer,
                                   "executable": message['reducer_executable'],
                                   "output_directory": "var/job-" + str(jCounter) + "/reducer-output",
                                   "worker_pid" : pids[i]})

        currJobs[jCounter] += 1
        allWorkers[pids[i]].status = "busy"
        allWorkers[pids[i]].work.append(newWorkerJob)

        redSock.sendall(newWorkerJob.encode("utf-8"))
        redSock.close()
        i = (i + 1) % len(pids)

def finisher(jCounter):
    outDir = allJobs[int(jCounter)]['output_directory']
    numFiles = allJobs[int(jCounter)]['num_reducers']
    if os.path.exists(outDir):
        shutil.rmtree(outDir)
    os.makedirs(outDir)
    i = 0
    while i < numFiles:
        currFile = "var/job-" + str(jCounter) + "/reducer-output/input_" + str(i)
        os.rename(currFile, outDir + "/input_" + str(i))
        i += 1

class Master:
    def __init__(self, port_number):
        if os.path.exists('var'):
            shutil.rmtree('var')
        os.makedirs('var')

        HBThread = Thread(target = heartbeatListener, args = (int(port_number), ))
        HBThread.start()

        masterSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        masterSock.bind(("localhost", int(port_number)))
        masterSock.listen(5)

        FTThread = Thread(target = faultTolerance, )
        FTThread.start()

        while True: #wait for incoming messages!
            MSock, address = masterSock.accept()
            message = ''

            while True:
                data = MSock.recv(1024)
                message += data.decode("utf-8")

                if len(data) != 1024:
                    break

            MSock.close()
            message = json.loads(message)
            handleMessage(message)

if __name__ == '__main__':
    port_number = sys.argv[1]
    Master(port_number)

import math

def group(inputFiles, outputFile):
    # this is the list of the lines in all the input files
    sortedFile = []
    for inFile in inputFiles:
        f = open(inFile,'r')
        # adds all the files lines to the list to be sorted later
        sortedFile += f.readlines()
    sortedFile.sort()
    outFile = open(outputFile,"w")
    numKeys = 1
    compareWord = sortedFile[0].split(' ')[0]
    for line in sortedFile:
        key = line.split(' ')[0]
        # if the current key is different
        # increment numKeys and change the compare word
        if key != compareWord:
            numKeys += 1
            compareWord = key
        outFile.write(line)
    return numKeys

def splitGrouping(inputFile, numReducers, jobNum, numKeys):
    # numKeys = 1 # we know that there will be at least one key in the file
    f = open(inputFile, 'r')
    lineList = f.readlines()
    compareWord = lineList[0].split(' ')[0] # variable to compare current key with
    if lineList:
        # must be guaranteed to not return a fraction
        numKeysPerReduce = math.ceil(numKeys/numReducers)
        counter = 1 # counts the number of keys per file, which is at least 1
        # this creates the first file since we know it has to exist
        outputFile = "var/job-" + str(jobNum) + "/grouper-output/" + "input_0"
        outFile = open(outputFile,'w')
        compareWord = lineList[0].split(' ')[0]
        i = 0 # keeps track of what file it is on
        for line in lineList:
            key = line.split(' ')[0]
            if key != compareWord:
                compareWord = key
                counter += 1
                if counter > numKeysPerReduce:
                    # the current file has reached its key limit
                    # so we change the file and set the counter to 0
                    i += 1
                    counter = 1 # each file has at least one key
                    outputFile = "var/job-" + str(jobNum) + "/grouper-output/" + "input_" + str(i)
                    outFile = open(outputFile,'w')
            #adds the line to output file
            outFile.write(line)

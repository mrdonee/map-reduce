#!/usr/bin/python
#
# Input: <text>
# Output: <1><tab><line>

import sys, re

# Define our query term
if len(sys.argv) == 1:
    query = "money"
else:
    query = sys.argv[1]

# Loop over every line in standard in
for line in sys.stdin:
    line = line.strip()

    if len(line) == 0:
        continue

    # Print key, value pair if query present in this line
    if query in line.lower():
        print "1\t" + line

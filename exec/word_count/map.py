#!/usr/bin/python
#
# Input: <text>
# Output: List of <word><tab><1>

import sys, re

# Loop over every line in standard in
for line in sys.stdin:
    # Strip all extra white space
    line = line.strip()

    # Loop over every word in every line (separated by space)
    for word in line.split(" "):
        # Print (word, 1) but lowercase
        print word.lower() + "\t1"

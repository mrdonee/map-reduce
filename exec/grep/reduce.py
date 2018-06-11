#!/usr/bin/python
#
# Input: <1><tab><line>
# Output: List of <line>

import sys, re

# Loop over every line in standard in
for line in sys.stdin:
    # Strip all extra white space and split by the tab
    line = line.strip()
    sep = line.split("\t")

    # Skip if empty newline (length must be 2 for the pair)
    if len(sep) != 2:
        continue

    # Read (key, value) from current line
    key, value = sep[0], sep[1]

    # Just print the value because this line has our query
    print value

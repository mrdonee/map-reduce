#!/usr/bin/python
#
# Input: List of <word><tab><1>
# Output: List of <word><tab><total_count>

import sys, re

# Remember current word and its count
curr_word = ""
curr_count = 0

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

    # Set current word if first loop iteration
    if curr_word == "":
        curr_word = key

    # Print the output if seeing a new key in this iteration
    # Will not run if this iteration is first time curr_word is set
    if key != curr_word:
        print curr_word + "\t" + str(curr_count)

        # Reset values for next key
        curr_word = key
        curr_count = 0

    # Increment current count
    curr_count += 1

# Print last word
print curr_word + "\t" + str(curr_count)

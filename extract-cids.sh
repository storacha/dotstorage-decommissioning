#!/bin/bash

# Check if filename argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <filename>"
    echo "Extracts CIDs from lines like 'hash_cid.car' and outputs as comma-separated quoted list"
    exit 1
fi

filename="$1"

# Check if file exists
if [ ! -f "$filename" ]; then
    echo "Error: File '$filename' not found"
    exit 1
fi

# Extract CIDs and format as comma-separated quoted list
sed 's/.*_\([^.]*\)\.car.*/'"'"'\1'"'"'/g' "$filename" | paste -s -d ',' -
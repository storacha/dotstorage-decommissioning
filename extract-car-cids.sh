#!/bin/bash

# Extract CIDs from CSV lines with format:
# blockmultihash,carpath
# Example: zQmaSZ2PtWvYgMTEE6pFNHTc3xgUknuYdDsuA3ZiDRYNVq9,us-west-2/dotstorage-prod-1/complete/bafybeiftz5gn7hzg3ahxfvj5f54iso6wjsfuqoqo7wlina6eq5eofprnsa.car

filename=${1:-/dev/stdin}

# Extract the part between the last "/" and the first "."
# Cut on comma to get carpath, then use sed to extract CID
cut -d',' -f2 "$filename" | sed 's/.*\/\([^/.]*\)\..*$/\1/g'
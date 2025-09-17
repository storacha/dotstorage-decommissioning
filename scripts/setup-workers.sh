#!/bin/bash

# set up 4 worker directories

mkdir -p worker-{1..4}/data
for n in {1..4}; do cp .env worker-$n; done
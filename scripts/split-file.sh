#!/bin/bash

# Check if arguments are provided
if [ $# -lt 2 ]; then
    echo "Usage: $0 <filename> <number_of_chunks>"
    echo "Splits a file into the specified number of equal chunks"
    exit 1
fi

filename="$1"
num_chunks="$2"

# Validate number of chunks
if ! [[ "$num_chunks" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: Number of chunks must be a positive integer"
    exit 1
fi

# Check if file exists
if [ ! -f "$filename" ]; then
    echo "Error: File '$filename' not found"
    exit 1
fi

# Get total number of lines
total_lines=$(wc -l < "$filename")
echo "Total lines: $total_lines"

# Calculate lines per chunk (rounded up)
lines_per_chunk=$(( (total_lines + num_chunks - 1) / num_chunks ))
echo "Lines per chunk: $lines_per_chunk"

# Get base filename without extension
base_name="${filename%.*}"
extension="${filename##*.}"

# If no extension, just use the filename
if [ "$extension" = "$filename" ]; then
    extension=""
    base_name="$filename"
else
    extension=".$extension"
fi

# Split the file into chunks
for (( i=1; i<=num_chunks; i++ )); do
    start_line=$(( (i - 1) * lines_per_chunk + 1 ))
    output_file="${base_name}_chunk_${i}${extension}"
    
    # Use sed to extract lines for this chunk
    sed -n "${start_line},$((start_line + lines_per_chunk - 1))p" "$filename" > "$output_file"
    
    # Count actual lines in the chunk
    actual_lines=$(wc -l < "$output_file")
    echo "Created $output_file with $actual_lines lines"
done

echo "File split complete!"
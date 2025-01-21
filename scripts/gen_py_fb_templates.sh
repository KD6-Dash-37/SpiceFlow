#!/bin/bash

# Script to generate Python FlatBuffers templates

# Define the input file and output directory
INPUT_FILE="flat-buffer/client/order_book.fbs"
OUTPUT_DIR="client"

# Run the FlatBuffers compiler
flatc --python -o "$OUTPUT_DIR" "$INPUT_FILE"

# Check if the command was successful
if [ $? -eq 0 ]; then
  echo "Python FlatBuffers templates generated successfully in $OUTPUT_DIR"
else
  echo "Failed to generate Python FlatBuffers templates" >&2
  exit 1
fi

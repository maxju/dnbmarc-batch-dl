#!/bin/bash

# Check if a PDF file is provided as an argument
if [ $# -lt 1 ]; then
    echo "Usage: $0 <pdf_file> [search_term]"
    exit 1
fi

pdf_file="$1"
search_term="${2:-abstract}"

# Check if the file exists
if [ ! -f "$pdf_file" ]; then
    echo "Error: File '$pdf_file' not found."
    exit 1
fi

# Convert PDF to text
text_content=$(pdftotext -layout "$pdf_file" -)

# Count occurrences of the search term (case-insensitive)
count=$(echo "$text_content" | grep -ci "$search_term")

# Find the first occurrence and calculate its position
total_pages=$(pdfinfo "$pdf_file" | grep "Pages:" | awk '{print $2}')
first_occurrence=$(echo "$text_content" | grep -ni "$search_term" | head -n 1)

if [ -n "$first_occurrence" ]; then
    line_number=$(echo "$first_occurrence" | cut -d':' -f1)
    total_lines=$(echo "$text_content" | wc -l)
    position=$(awk "BEGIN {printf \"%.2f\", ($line_number / $total_lines) * 100}")
    
    echo "$pdf_file,$count,$position"
else
    echo "$pdf_file,0,0"
fi

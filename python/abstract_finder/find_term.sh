#!/bin/bash

pdf_file="$1"
max_pages="$2"

# Check if the file exists
if [ ! -f "$pdf_file" ]; then
    echo "Error: File '$pdf_file' not found."
    exit 1
fi

# Convert first 25 pages of PDF to text
text_content=$(pdftotext -f 1 -l "$max_pages" -layout "$pdf_file" -)

# Count occurrences and find positions for both terms
abstract_count=$(echo "$text_content" | grep -ci "abstract")
abstract_position=$(echo "$text_content" | grep -ni "abstract" | head -n 1 | cut -d':' -f1)
total_lines=$(echo "$text_content" | wc -l)

# Calculate positions as percentages
abstract_pos=$(awk "BEGIN {printf \"%.2f\", ($abstract_position / $total_lines) * 100}")

echo "$pdf_file,$abstract_count,$abstract_pos"
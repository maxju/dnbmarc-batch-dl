#!/bin/bash

pdf_file="$1"
max_pages="$2"

# Check if the file exists
if [ ! -f "$pdf_file" ]; then
    echo "Error: File '$pdf_file' not found."
    exit 1
fi

# Convert first 25 pages of PDF to text
text_content=$(pdftotext -f 1 -l "$max_pages" -layout "$pdf_file" - 2>/dev/null)

# Count occurrences and find position for abstract
abstract_count=$(echo "$text_content" | grep -ci "abstract" || echo "0")
abstract_position=$(echo "$text_content" | grep -ni "abstract" | head -n 1 | cut -d':' -f1)

total_lines=$(echo "$text_content" | wc -l)

# Calculate position as percentage, defaulting to 0 if not found or on error
abstract_pos=$([ -n "$abstract_position" ] && awk "BEGIN {printf \"%.2f\", ($abstract_position / $total_lines) * 100}" 2>/dev/null || echo "0.00")

# Ensure output is on a single line with no extra spaces
echo "${pdf_file},${abstract_count},${abstract_pos}" | tr -d '\n' | tr -s ' '
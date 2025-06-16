#!/bin/zsh

# Count original lines
echo "Original line count:"
wc -l claude.convo.log

# Strip lines starting with more than 2 spaces and count
echo -e "\nStripped line count:"
grep -vE '^ {3,}' claude.convo.log | wc -l

# Save to stripped version
grep -vE '^ {3,}' claude.convo.log > claude.convo.stripped


# Now compact the conversation
echo -e "\nCompacting conversation..."

# Compact conversation to max 3 lines per LLM response
awk '
    /⏺/ || /^>/ {
        if (current_block != "") {
            print current_block
            current_block = ""
        }
        if (/^>/) {
            print
            next
        }
        current_block = $0
        line_count = 1
    }
    !/⏺/ && !/^>/ {
        if (line_count <= 3) {
            current_block = current_block "\n" $0
            line_count++
        }
    }
    END {
        if (current_block != "") {
            print current_block
        }
    }
' claude.convo.stripped > claude.convo.compacted

# Show final compacted line count
echo -e "\nCompacted line count:"
wc -l claude.convo.compacted

# Post-process to add formatting and truncation message
# Post-process to add formatting and truncation message
sed -E "s/(⏺.*$)/\n\1\n<truncated to three lines to save tokens>/g
     s/^>/\n&/g
     s/^⏺/^\n&/g" claude.convo.compacted > claude.convo.final

# Show final compacted line count
echo -e "\nFinal compacted line count:"
wc -l claude.convo.final

# Context Compaction Process

## What Happens During Compaction

**Automatic Trigger**: When context left drops to 0% Claude's infrastructure automatically triggers compaction
- **Warning Signs**: "Context left until auto-compact: 11%" or similar
- **Timing**: Cannot be forced by user or Claude - happens automatically
- **Duration**: Brief interruption (~1-2 minutes) where Claude (Haiku) creates summary
- **Manual Intervention**: User can see it get to <10% and i will warn you and cat this file so we can prepare for compaction

## Pre-Compaction Protocol

**When Context Drops Below 10%:**

1. **IMMEDIATE STATE DOCUMENTATION**: Write current session state to bottom of CLAUDE.md
   - Current layer/task status
   - What's working vs what's broken
   - Critical quality gate violations (like linting failures)
   - Next immediate tasks
   - Key files modified and their status
   - Any architectural insights discovered
   - Any ways of working or tips that are key communications between the wet mind and the digital mind

2. **CONTINUE WORKING**: Keep coding until compaction actually triggers
   - Don't stop work early "preparing for compaction"
   - Make progress on current layer/task
   - User will continue running commands until compaction hits

## Post-Compaction Recovery

**User Recovery Process:**
1. User runs `./read-all-claud.sh` to show all *.md files
2. User asks "did you see that? you reloaded?" 
3. Claude acknowledges seeing comprehensive documentation
4. Claude continues from documented state

**Claude Recovery Process:**
1. Read ALL documentation files shown (not just CLAUDE.md)
2. Understand current state from "CURRENT SESSION STATE" section
3. Resume work from exact point documented
4. Follow quality gates and methodology as documented

## Key Principles

- **Documentation is recovery**: State in CLAUDE.md survives compaction
- **Work until triggered**: Don't stop early, compaction timing is unpredictable
- **Comprehensive context**: User shows ALL .md files for full project understanding
- **Knowledge and Expertise Persists**: Accrued knowledge and expertise is retained through the entire project. 

## Critical Reminders

- **Always document quality gate violations** (like incomplete linting)
- **Capture exact test counts and status** (this will be out of date when you compact so you should take it as "we go at least this far")
- **Note what's working vs what needs implementation**
- **Include next specific tasks to resume**
- **Document any architectural insights discovered during session**


## Claude Retro Suggestions
<!-- claude-retro-auto -->
- Before implementing features requiring data queries, always ask 'what does the UI actually display with this data?' and verify the data source can provide it, rather than building the feature and discovering mid-implementation that the data structure doesn't support it.
- When debugging backend data bugs, check the data source (database query, API response, log output) directly with a quick query FIRST before investigating application code.
- When the user says 'do it', 'continue', 'just finish it', or 'DO EVERYTHING', work autonomously without asking for clarification or pausing for approval. Only return when blocked or asking would save significant wasted work.
- Before proposing large rewrites (UI redesigns, architectural changes, multi-file refactors), spend the first 1-2 turns validating the core problem with the user: 'What are the 2-3 specific things that need to change?' rather than proposing comprehensive redesigns.
- When setting up external tool integrations (MCP, Jira, credentials) in the middle of a session, immediately ask the user for configuration details rather than attempting multiple failed calls. A 2-minute credential clarification beats 60 turns of failed attempts.
<!-- claude-retro-auto -->

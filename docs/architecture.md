# Architecture Notes
- Channel (= business) vs Source (= system) separation
- Reconciliation Engine loads source parsers -> normalizes -> channel matcher
- Workers handle long-running jobs (parse large files, match at scale)
- Metrics, logging, and tracing should be wired in production

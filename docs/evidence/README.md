# Validation evidence

Measured claims belong here only after the corresponding workload and reconciliation have
completed. Do not commit `.env`, credentials, host usernames, absolute local paths, or unredacted
screenshots.

## Required acceptance run

From one clean, confirmation-protected reset:

```powershell
pwsh ./scripts/coldchain.ps1 reset
pwsh ./scripts/coldchain.ps1 demo
pwsh ./scripts/coldchain.ps1 demo
pwsh ./scripts/coldchain.ps1 benchmark
pwsh ./scripts/coldchain.ps1 failure-drill
```

Then perform the v1-to-v2 procedure in [the evolution guide](../evolution.md). The two demos must
pass consecutively without another reset. Preserve each generated
`data/runs/<run-id>/manifest.json` before any later reset.

## Pack layout

Create `docs/evidence/YYYY-MM-DD/` only for a completed run:

```text
docs/evidence/YYYY-MM-DD/
├── summary.md
├── summary.json
├── verifier/
│   ├── demo-1.txt
│   ├── demo-2.txt
│   ├── benchmark.txt
│   ├── failure-drill.txt
│   └── evolution-v2.txt
├── flink/
│   ├── jobs.json
│   └── checkpoints.json
└── screenshots/
    ├── flink-overview.png
    └── grafana-streaming.png
```

Copy the template directory, fill every required value, and remove any unused placeholders. The
summary must include:

- commit SHA and dirty-worktree status;
- CPU, physical RAM, OS, Docker version, and Docker CPU/RAM allocation;
- every run ID, manifest attempted/delivered totals, and achieved publish rate;
- exact bronze and terminal-silver reconciliation results;
- finalized gold comparison result;
- completed and failed checkpoint totals;
- peak/final Kafka lag and the measured time until lag returned to zero;
- TaskManager recovery time and RPO result;
- v1/v2 schema IDs, savepoint path with bucket credentials removed, partition specs, and snapshot
  IDs.

The weekly/manual GitHub workflow uploads equivalent unsanitized runtime artifacts for debugging;
review and sanitize them before committing an evidence pack.

# The plans

This directory holds one document for each milestone. Each document
gives the problem, the design, the reasons for each decision, and what
the tests measured when the work ran. [00-design.md](00-design.md) is
the design overview.

A document's directory states its status:

- `completed/` holds the milestones that are built.
- `rejected/` holds the milestones that were built and then removed. The
  document stays as the record of what came out and why.
- The markdown files in this directory are the milestones that are not
  built yet.

A plan closes in the commit that builds it. That commit moves the
document to `completed/` and states what the tests measured.

## Milestones

1. [High availability](01-high-availability.md): one primary and its replicas, failover, and
   switchover before a node drains.
2. [Backups](02-backups.md) to object storage or a volume, and restore
   into a new deployment.
3. [Adoption](03-adoption.md): replicate from an existing Redis or
   Valkey, then promote.
4. [Upgrades and downgrades](04-upgrades.md), staged so they lose no
   data.

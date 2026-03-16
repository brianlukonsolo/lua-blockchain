# Security

## Reporting

This repository does not yet have a private coordinated disclosure channel.

Until one exists:

- do not post live secrets, private keys, or exploit details in public issues
- open a minimal issue that states a security concern exists
- rotate any exposed node credentials before sharing reproduction details

## Current Boundary

The repo now includes:

- TLS on the native TCP P2P transport
- signed node identities on P2P hello messages
- peer admission controls with optional peer ID allowlists
- per-IP and per-subnet peer caps
- rate limiting and admin/peer shared-secret controls on HTTP endpoints

The repo does not yet claim:

- an independent external security audit
- a hardened public-internet discovery transport
- battle-tested public-chain consensus behavior
- production wallet custody or HSM integration

## Deployment Guidance

- run with `BLOCKCHAIN_MODE=production`
- set strong values for `BLOCKCHAIN_ADMIN_TOKEN` and `BLOCKCHAIN_PEER_SHARED_SECRET`
- keep `BLOCKCHAIN_P2P_TLS_ENABLED=true`
- restrict or disable UDP gossip on public internet deployments
- back up `/app/data/` and protect it as node-critical state
- use `POST /api/admin/backup` with an admin token for consistent in-process snapshots

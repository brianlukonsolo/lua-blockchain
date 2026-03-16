# Threat Model

## Assets

- chain state in `/app/data/blockchain.db`
- node identity keys
- native P2P TLS private key
- admin token and peer shared secret
- mempool contents and block propagation path

## Trust Boundaries

- public HTTP API to OpenResty
- peer-authenticated HTTP network endpoints
- native TCP P2P transport
- optional UDP gossip discovery transport
- persistent local volume mounted at `/app/data`

## Mitigations Present

- `chain_id` isolation across peers
- signed transactions with nonce and balance enforcement
- most-cumulative-work fork choice
- SQLite WAL plus integrity checks
- signed native P2P hello messages
- TLS on native P2P sessions
- peer reputation, cooldown, and ban windows
- optional peer ID allowlists
- peer caps per observed IP and subnet
- admin authentication and route rate limiting

## Open Risks

- UDP gossip is still plaintext discovery traffic
- the node is single-process and not a hardened async mesh
- consensus and networking have not had independent security review
- SQLite is stronger than JSON snapshots but still not public-chain node storage
- the repo has no formal key-management or HSM story

## Recommended Review Focus

- P2P handshake and peer admission logic
- block and transaction validation edge cases
- replay, eclipse, and resource-exhaustion scenarios
- backup, restore, and disaster recovery procedures

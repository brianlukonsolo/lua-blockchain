# lua-blockchain

Lua blockchain node implemented on OpenResty with:

- browser-generated wallets and locally signed transactions
- account balances, nonces, mining rewards, fee handling, and mempool validation
- signed native TCP peer transport over TLS for handshake, headers sync, block push, and transaction relay
- UDP gossip announcements for directory fan-out and fast peer discovery
- persisted peer records with health, reputation, cooldown, and ban state
- peer admission controls with peer ID allowlists plus per-IP and per-subnet limits
- peer directory exchange, gossip seeding, bootstrap peers, and autonomous background maintenance
- most-cumulative-work fork choice with retargeted prefix-based proof of work
- production guardrails for rate limiting, admin authentication, peer authentication, and network isolation by `chain_id`
- SQLite-backed transactional state with a dedicated persistent data volume
- review scaffolding in `SECURITY.md` and `docs/`
- containerized runtime with a dedicated persistent data volume

## Stack

- OpenResty / LuaJIT
- pure Lua SHA-256 via the vendored `pure_lua_SHA` module
- OpenSSL CLI for key generation and signature verification
- LuaSocket + LuaSec for native TCP peer transport, local loopback RPC, and UDP gossip transport
- Docker Compose for local runtime

## Local Usage

1. Build the image:
   `./docker-build.sh`
2. Start the application:
   `./start.sh`
3. Open the frontend:
   `http://127.0.0.1:8080/`
4. Run the backend tests:
   `./test.sh`
5. Open a shell in the container when needed:
   `./connect.sh`
6. Stop the application:
   `./stop.sh`

`start.sh` now starts the node in detached mode.

## Production Configuration

Copy `.env.example` to `.env` and replace the secrets and public URLs before starting the stack in production mode.

Important variables:

- `BLOCKCHAIN_MODE=production`
- `BLOCKCHAIN_NODE_URL=https://node-1.example.com`
- `BLOCKCHAIN_CHAIN_ID=lua-blockchain-mainnet`
- `BLOCKCHAIN_DATA_FILE=/app/data/blockchain.db`
- `BLOCKCHAIN_DIFFICULTY=0000`
- `BLOCKCHAIN_TARGET_BLOCK_SECONDS=30`
- `BLOCKCHAIN_DIFFICULTY_ADJUSTMENT_WINDOW=10`
- `BLOCKCHAIN_MIN_DIFFICULTY_PREFIX_LENGTH=2`
- `BLOCKCHAIN_MAX_DIFFICULTY_PREFIX_LENGTH=6`
- `BLOCKCHAIN_ADMIN_TOKEN=<long random secret>`
- `BLOCKCHAIN_PEER_SHARED_SECRET=<shared peer secret>`
- `BLOCKCHAIN_ALLOWED_PEER_IDS=<comma-separated peer id allowlist>`
- `BLOCKCHAIN_ALLOW_PLAINTEXT_GOSSIP=false`
- `BLOCKCHAIN_REQUIRE_HTTPS_PEERS=true`
- `BLOCKCHAIN_ENABLE_SERVER_WALLETS=false`
- `BLOCKCHAIN_BOOTSTRAP_PEERS=https://node-2.example.com,https://node-3.example.com`
- `BLOCKCHAIN_BACKUP_DIR=/app/data/backups`
- `BLOCKCHAIN_PEER_DISCOVERY_FANOUT=8`
- `BLOCKCHAIN_PEER_ADVERTISED_LIMIT=16`
- `BLOCKCHAIN_PEER_BACKOFF_BASE_SECONDS=15`
- `BLOCKCHAIN_PEER_BAN_SECONDS=300`
- `BLOCKCHAIN_PEER_MAX_FAILURES_BEFORE_BAN=5`
- `BLOCKCHAIN_MAX_PEERS_PER_IP=4`
- `BLOCKCHAIN_MAX_PEERS_PER_SUBNET=8`
- `BLOCKCHAIN_PEER_MAINTENANCE_ENABLED=true`
- `BLOCKCHAIN_PEER_MAINTENANCE_INTERVAL_SECONDS=30`
- `BLOCKCHAIN_P2P_ENABLED=true`
- `BLOCKCHAIN_P2P_PORT=19100`
- `BLOCKCHAIN_P2P_ADVERTISE_HOST=node-1.example.com`
- `BLOCKCHAIN_P2P_SEEDS=node-2.example.com:19100,node-3.example.com:19100`
- `BLOCKCHAIN_P2P_DIAL_DISCOVERED_PEERS=false`
- `BLOCKCHAIN_P2P_CONNECT_INTERVAL_SECONDS=5`
- `BLOCKCHAIN_P2P_POLL_INTERVAL_SECONDS=2`
- `BLOCKCHAIN_P2P_TLS_ENABLED=true`
- `BLOCKCHAIN_P2P_TLS_CERT_PATH=/app/data/node_p2p_cert.pem`
- `BLOCKCHAIN_P2P_TLS_KEY_PATH=/app/data/node_p2p_key.pem`
- `BLOCKCHAIN_NODE_IDENTITY_PRIVATE_KEY_PATH=/app/data/node_identity_private.pem`
- `BLOCKCHAIN_NODE_IDENTITY_PUBLIC_KEY_PATH=/app/data/node_identity_public.pem`
- `BLOCKCHAIN_GOSSIP_ENABLED=false`
- `BLOCKCHAIN_GOSSIP_PORT=19090`
- `BLOCKCHAIN_GOSSIP_ADVERTISE_HOST=node-1.example.com`
- `BLOCKCHAIN_GOSSIP_SEEDS=node-2.example.com:19090,node-3.example.com:19090`
- `BLOCKCHAIN_GOSSIP_FANOUT=3`
- `BLOCKCHAIN_GOSSIP_INTERVAL_SECONDS=5`
- `BLOCKCHAIN_GOSSIP_MESSAGE_TTL_SECONDS=30`
- `BLOCKCHAIN_GOSSIP_MAX_HOPS=3`

In production mode the node will refuse readiness if:

- `BLOCKCHAIN_ADMIN_TOKEN` is missing
- `BLOCKCHAIN_ADMIN_TOKEN` is shorter than 32 characters
- `BLOCKCHAIN_PEER_SHARED_SECRET` is missing
- `BLOCKCHAIN_PEER_SHARED_SECRET` is shorter than 32 characters
- `BLOCKCHAIN_NODE_URL` is missing or not `https`
- `BLOCKCHAIN_REQUIRE_HTTPS_PEERS` is not `true`
- neither `BLOCKCHAIN_ALLOWED_PEER_IDS` nor `BLOCKCHAIN_ALLOWED_PEER_HOSTS` is configured
- `BLOCKCHAIN_P2P_DIAL_DISCOVERED_PEERS=true` without `BLOCKCHAIN_ALLOWED_PEER_IDS`
- `BLOCKCHAIN_BOOTSTRAP_PEERS` contains a non-`https` URL
- native P2P TLS is disabled
- UDP gossip is enabled without explicitly allowing plaintext gossip
- server-side wallet generation is enabled

Persistent chain data is stored in the Docker volume mounted at `/app/data/blockchain.db`.

## API Overview

- `GET /api/health`
- `GET /api/ready`
- `GET /api/info`
- `GET /api/chain`
- `GET /api/headers`
- `GET /api/locator`
- `GET /api/network/peers`
- `POST /api/network/p2p/peer-update`
- `POST /api/network/p2p/peer-failure`
- `POST /api/network/gossip/announce`
- `GET /api/stats`
- `GET /api/blocks/{index}`
- `GET /api/blocks/hash/{hash}`
- `GET /api/accounts`
- `GET /api/accounts/{address}`
- `GET /api/transactions/pending`
- `POST /api/wallets`
- `POST /api/admin/backup`
- `POST /api/transactions`
- `POST /api/mine`
- `GET /api/validate`
- `GET /api/peers`
- `POST /api/peers`
- `POST /api/consensus/resolve`
- `POST /api/network/transactions`
- `POST /api/network/gossip/announce`
- `POST /api/network/inventory`
- `POST /api/network/blocks`
- `GET /api/hash/sha256/{string}`

Operational behavior:

- `POST /api/transactions` is public and rate limited
- `POST /api/network/*` requires the peer shared secret when configured
- peers identify themselves with signed node identities over the native TCP transport, advertise TLS fingerprints in the signed hello payload, and persist observed peer IP/TLS metadata through the local `/api/network/p2p/*` bridge
- peers fetch missing headers with `get_headers`, push missing blocks with `block`, and relay signed transactions over native TCP sessions
- peers can still exchange directories over `GET /api/network/peers`, track health/cooldown state, and bootstrap autonomous maintenance from `BLOCKCHAIN_BOOTSTRAP_PEERS`, `BLOCKCHAIN_P2P_SEEDS`, and `BLOCKCHAIN_GOSSIP_SEEDS`
- peer advertisement deliberately strips observed IP and subnet metadata before forwarding discovered peers to the wider network
- `POST /api/admin/backup` creates a consistent SQLite snapshot plus node identity and TLS key material copies under `BLOCKCHAIN_BACKUP_DIR`
- `POST /api/mine`, `POST /api/peers`, `POST /api/consensus/resolve`, and `POST /api/wallets` are admin-protected when an admin token is configured
- `POST /api/admin/backup` is also admin-protected
- `POST /api/wallets` is disabled by default

Legacy routes such as `/get_chain`, `/mine_block`, `/validate_chain`, and `/hashing/sha256/{string}` still resolve for compatibility.

## Data Model

Each block stores:

- `index`
- `timestamp`
- `transactions`
- `proof`
- `difficulty_prefix`
- `work`
- `cumulative_work`
- `hash_format`
- `previous_hash`
- `hash`
- `mined_by`

Each signed transfer stores:

- `sender`
- `recipient`
- `amount`
- `fee`
- `nonce`
- `timestamp`
- `note`
- `public_key`
- `signature`

Persistent state is stored in SQLite tables for:

- `metadata`
- `blocks`
- `transactions`
- `block_transactions`
- `pending_transactions`
- `peers` with discovery source, success/failure counters, cooldown windows, last error, and advertised capabilities

The checked-in `src/main/lua/com/brianlukonsolo/blockchain_data.json` file is only a legacy repo artifact. Runtime state is stored in `/app/data/blockchain.db` inside the container volume.

## Frontend

The frontend lives under `src/main/lua/com/brianlukonsolo/static/` and provides:

- local wallet generation and import/export
- browser-side ECDSA transaction signing
- account balance and nonce visibility
- mining controls
- peer registration, health visibility, and consensus sync
- mempool, account leaderboard, and block explorer views

## Tests

`src/test/lua/unit/blockchain_spec.lua` is a plain Lua integration-style test script executed inside the container with `luajit`.

The Postman collection for the upgraded API is in:

`src/test/postman/lua-blockchain_BrianLukonsolo.postman_collection.json`

## Security Review Scaffolding

- `SECURITY.md` describes disclosure expectations and the current security boundary.
- `docs/threat-model.md` captures assets, trust boundaries, and mitigations.
- `docs/security-review-checklist.md` lists the review items still required before claiming independent public-chain readiness.
- the admin backup route provides an in-repo recovery primitive, but backup shipping and retention policy are still an operator responsibility

## Scope

This repo now has materially stronger public-network hardening than the original demo, but there are still clear boundaries:

- peer networking now uses signed native TCP sessions over TLS for headers, blocks, and transaction relay, plus UDP gossip for discovery
- peer admission now enforces optional peer ID allowlists and caps peers per observed IP and subnet, which improves eclipse and Sybil resistance but does not eliminate those attacks
- peer discovery beyond configured seeds is intentionally conservative by default because the current daemon is single-process and not yet a full async mesh network
- consensus is now most-cumulative-work with retargeted prefix-based PoW, but it is still not a battle-tested public-chain fork-choice implementation
- storage is now transactional and SQLite-backed, but still not RocksDB/LevelDB-class node storage
- the UDP gossip channel is still plaintext discovery transport; public internet deployments should restrict it or disable it until a native authenticated/encrypted discovery layer exists
- there is no smart-contract VM, validator set management, or independent external security review

Treat this as a hardened blockchain node with documented review work still remaining, not a Bitcoin/Ethereum replacement.

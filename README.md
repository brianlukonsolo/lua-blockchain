# lua-blockchain

Lua blockchain node implemented on OpenResty with:

- browser-generated wallets and locally signed transactions
- account balances, nonces, mining rewards, fee handling, and mempool validation
- authenticated peer-to-peer propagation and longest-valid-chain consensus
- production guardrails for rate limiting, admin authentication, peer authentication, and network isolation by `chain_id`
- containerized runtime with a dedicated persistent data volume

## Stack

- OpenResty / LuaJIT
- pure Lua SHA-256 via the vendored `pure_lua_SHA` module
- OpenSSL CLI for key generation and signature verification
- LuaSocket + LuaSec for peer-to-peer HTTP requests
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
- `BLOCKCHAIN_ADMIN_TOKEN=<long random secret>`
- `BLOCKCHAIN_PEER_SHARED_SECRET=<shared peer secret>`
- `BLOCKCHAIN_REQUIRE_HTTPS_PEERS=true`
- `BLOCKCHAIN_ENABLE_SERVER_WALLETS=false`

In production mode the node will refuse readiness if:

- `BLOCKCHAIN_ADMIN_TOKEN` is missing
- `BLOCKCHAIN_PEER_SHARED_SECRET` is missing
- `BLOCKCHAIN_NODE_URL` is missing or not `https`
- server-side wallet generation is enabled

Persistent chain data is stored in the Docker volume mounted at `/app/data/blockchain_data.json`.

## API Overview

- `GET /api/health`
- `GET /api/ready`
- `GET /api/info`
- `GET /api/chain`
- `GET /api/stats`
- `GET /api/blocks/{index}`
- `GET /api/accounts`
- `GET /api/accounts/{address}`
- `GET /api/transactions/pending`
- `POST /api/wallets`
- `POST /api/transactions`
- `POST /api/mine`
- `GET /api/validate`
- `GET /api/peers`
- `POST /api/peers`
- `POST /api/consensus/resolve`
- `POST /api/network/transactions`
- `POST /api/network/blocks`
- `GET /api/hash/sha256/{string}`

Operational behavior:

- `POST /api/transactions` is public and rate limited
- `POST /api/network/*` requires the peer shared secret when configured
- `POST /api/mine`, `POST /api/peers`, `POST /api/consensus/resolve`, and `POST /api/wallets` are admin-protected when an admin token is configured
- `POST /api/wallets` is disabled by default

Legacy routes such as `/get_chain`, `/mine_block`, `/validate_chain`, and `/hashing/sha256/{string}` still resolve for compatibility.

## Data Model

Each block stores:

- `index`
- `timestamp`
- `transactions`
- `proof`
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

Persistent state contains:

- `meta`
- `chain`
- `pending_transactions`
- `peers`

The checked-in `src/main/lua/com/brianlukonsolo/blockchain_data.json` file is only a repo seed artifact. Runtime state is stored in `/app/data/blockchain_data.json` inside the container volume.

## Frontend

The frontend lives under `src/main/lua/com/brianlukonsolo/static/` and provides:

- local wallet generation and import/export
- browser-side ECDSA transaction signing
- account balance and nonce visibility
- mining controls
- peer registration and consensus sync
- mempool, account leaderboard, and block explorer views

## Tests

`src/test/lua/unit/blockchain_spec.lua` is a plain Lua integration-style test script executed inside the container with `luajit`.

The Postman collection for the upgraded API is in:

`src/test/postman/lua-blockchain_BrianLukonsolo.postman_collection.json`

## Scope

This repo is now much closer to a deployable private blockchain node than the original demo, but there are still clear boundaries:

- consensus is still longest valid chain over authenticated HTTP peers, not a mature public-network protocol
- storage is still JSON-backed, not RocksDB/LevelDB-class node storage
- there is no smart-contract VM, validator set management, or formal fork-choice implementation

Treat this as a hardened private-chain service, not a Bitcoin/Ethereum replacement.

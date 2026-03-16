# lua-blockchain

Lua blockchain node implemented on OpenResty with:

- a browser frontend at `http://127.0.0.1:8080/`
- browser-generated wallets and locally signed transactions
- account balances, nonces, mining rewards, and fee handling
- peer registration, block propagation, transaction propagation, and longest-valid-chain consensus
- JSON persistence and Docker-based local development

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

## API Overview

- `GET /api/health`
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

Persistent state is stored in `src/main/lua/com/brianlukonsolo/blockchain_data.json` and contains:

- `meta`
- `chain`
- `pending_transactions`
- `peers`

Older array-only chain files are migrated automatically on load.

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

This repo is now a much fuller blockchain system than the original demo, but it is still a compact educational node rather than a production public-chain implementation. Consensus is longest valid chain over HTTP peers, storage is still JSON-backed, and there is no smart-contract VM.

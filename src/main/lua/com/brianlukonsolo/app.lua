local cjson = require("classes.json")
local canonical_json = require("classes.canonical_json")
local crypto = require("classes.crypto")
local resty_lock = require("resty.lock")
local sha = require("cryptography.pure_lua_SHA.sha2")
local Blockchain = require("classes.blockchain")
local requests = require("classes.requests")

local APP_VERSION = "3.0.0"
local DATA_FILE = os.getenv("BLOCKCHAIN_DATA_FILE") or "/app/blockchain_data.json"
local DIFFICULTY = os.getenv("BLOCKCHAIN_DIFFICULTY") or "0000"
local REWARD = tonumber(os.getenv("BLOCKCHAIN_REWARD")) or 1
local NODE_ID = os.getenv("BLOCKCHAIN_NODE_ID") or "demo-node-1"
local NODE_URL = os.getenv("BLOCKCHAIN_NODE_URL") or ""

local function build_blockchain()
    return Blockchain.new({
        file_name = DATA_FILE,
        difficulty_prefix = DIFFICULTY,
        mining_reward = REWARD,
        node_id = NODE_ID,
        node_url = NODE_URL,
        version = APP_VERSION
    })
end

local function send_json(status, payload)
    ngx.status = status
    ngx.header["Content-Type"] = "application/json; charset=utf-8"
    ngx.say(canonical_json.encode(payload))
    return ngx.exit(status)
end

local function trim(value)
    return tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

local function read_request_body()
    ngx.req.read_body()

    local body = ngx.req.get_body_data()
    if not body then
        local body_file = ngx.req.get_body_file()
        if body_file then
            local handle = io.open(body_file, "r")
            if handle then
                body = handle:read("*a")
                handle:close()
            end
        end
    end

    if not body or body == "" then
        return {}
    end

    local decoded, err = cjson.decode(body)
    if not decoded then
        return nil, "Invalid JSON body: " .. tostring(err)
    end

    return decoded
end

local function build_snapshot(blockchain)
    local valid, reason = blockchain:validate_chain(blockchain:get_chain())
    return {
        meta = blockchain:get_meta(),
        stats = blockchain:get_stats(),
        validation = { valid = valid, reason = reason },
        chain = blockchain:get_chain(),
        pending_transactions = blockchain:get_pending_transactions(),
        peers = blockchain:get_peers(),
        accounts = blockchain:get_accounts()
    }
end

local function with_locked_blockchain(callback)
    local lock, lock_err = resty_lock:new("blockchain_locks", {
        timeout = 5,
        exptime = 10
    })
    if not lock then
        return nil, "Unable to create state lock: " .. tostring(lock_err)
    end

    local elapsed, acquire_err = lock:lock("blockchain_state")
    if not elapsed then
        return nil, "Unable to acquire state lock: " .. tostring(acquire_err)
    end

    local ok, result = xpcall(function()
        return callback(build_blockchain())
    end, debug.traceback)

    local unlocked, unlock_err = lock:unlock()
    if not unlocked then
        return nil, "Unable to release state lock: " .. tostring(unlock_err)
    end

    if not ok then
        return nil, result
    end

    return true, result
end

local function send_peer_post(peer, path, payload)
    local response = requests.send_post_request(
        peer .. path,
        canonical_json.encode(payload),
        { ["Content-Type"] = "application/json" }
    )

    return {
        peer = peer,
        ok = response.ok,
        status_code = response.status_code,
        error = response.ok and nil or (response.status_text or ("HTTP " .. tostring(response.status_code)))
    }
end

local function broadcast_to_peers(peers, path, payload, excluded_peer)
    local results = {}

    for _, peer in ipairs(peers or {}) do
        if peer ~= excluded_peer then
            results[#results + 1] = send_peer_post(peer, path, payload)
        end
    end

    return results
end

local function fetch_chain_payload(peer)
    local response = requests.send_get_request(peer .. "/api/chain")
    if not response.ok then
        return nil, response.status_text or ("HTTP " .. tostring(response.status_code))
    end

    local payload, err = cjson.decode(response.body)
    if not payload then
        return nil, "Peer returned invalid JSON: " .. tostring(err)
    end

    return payload
end

local function sync_from_source_peer(blockchain, source_peer)
    if trim(source_peer) == "" then
        return false, "No source peer supplied"
    end

    local payload, err = fetch_chain_payload(source_peer)
    if not payload then
        return false, err
    end

    return blockchain:replace_chain(payload.chain or payload)
end

local function handle_hash_route(uri)
    if uri == "/hashing/sha256" then
        return send_json(200, {
            message = "Use /hashing/sha256/{string} or /api/hash/sha256/{string}"
        })
    end

    local input = uri:match("^/hashing/sha256/(.+)$") or uri:match("^/api/hash/sha256/(.+)$")
    if input then
        input = ngx.unescape_uri(input)
        return send_json(200, {
            input = input,
            hash = sha.sha256(input)
        })
    end
end

local function handle_request()
    local method = ngx.req.get_method()
    local uri = ngx.var.uri
    local args = ngx.req.get_uri_args()

    local hash_response = handle_hash_route(uri)
    if hash_response then
        return hash_response
    end

    if method == "GET" and (uri == "/api/health" or uri == "/health") then
        return send_json(200, {
            status = "ok",
            service = "lua-blockchain",
            version = APP_VERSION,
            node_id = NODE_ID,
            node_url = NODE_URL,
            timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
        })
    end

    if method == "GET" and (uri == "/api/info" or uri == "/configuration") then
        local blockchain = build_blockchain()
        return send_json(200, {
            service = {
                name = "lua-blockchain",
                version = APP_VERSION,
                runtime = "OpenResty / LuaJIT",
                node_id = NODE_ID,
                node_url = NODE_URL
            },
            capabilities = {
                signed_transactions = true,
                nonce_enforcement = true,
                account_balances = true,
                peer_propagation = true,
                longest_valid_chain_consensus = true,
                frontend_console = true
            },
            api = {
                health = "/api/health",
                info = "/api/info",
                chain = "/api/chain",
                stats = "/api/stats",
                accounts = "/api/accounts",
                account = "/api/accounts/{address}",
                pending_transactions = "/api/transactions/pending",
                create_wallet = "POST /api/wallets",
                create_transaction = "POST /api/transactions",
                mine = "POST /api/mine",
                validate = "/api/validate",
                peers = "/api/peers",
                consensus = "POST /api/consensus/resolve",
                receive_transaction = "POST /api/network/transactions",
                receive_block = "POST /api/network/blocks"
            },
            snapshot = build_snapshot(blockchain)
        })
    end

    if method == "GET" and (uri == "/api/chain" or uri == "/get_chain") then
        local blockchain = build_blockchain()
        return send_json(200, build_snapshot(blockchain))
    end

    if method == "GET" and uri == "/api/stats" then
        local blockchain = build_blockchain()
        return send_json(200, {
            meta = blockchain:get_meta(),
            stats = blockchain:get_stats()
        })
    end

    if method == "GET" and (uri == "/api/validate" or uri == "/validate_chain") then
        local blockchain = build_blockchain()
        local valid, reason = blockchain:validate_chain(blockchain:get_chain())
        return send_json(200, {
            valid = valid,
            reason = reason
        })
    end

    local block_index = uri:match("^/api/blocks/(%d+)$")
    if method == "GET" and block_index then
        local blockchain = build_blockchain()
        local block = blockchain:get_block(tonumber(block_index))
        if not block then
            return send_json(404, { error = "Block not found" })
        end

        return send_json(200, { block = block })
    end

    if method == "GET" and (uri == "/api/transactions/pending" or uri == "/api/mempool") then
        local blockchain = build_blockchain()
        local pending_transactions = blockchain:get_pending_transactions()
        return send_json(200, {
            pending_transactions = pending_transactions,
            count = #pending_transactions
        })
    end

    if method == "GET" and uri == "/api/accounts" then
        local blockchain = build_blockchain()
        local accounts = blockchain:get_accounts()
        return send_json(200, {
            accounts = accounts,
            count = #accounts
        })
    end

    local account_address = uri:match("^/api/accounts/(lbc_[0-9a-f]+)$")
    if method == "GET" and account_address then
        local blockchain = build_blockchain()
        local account = blockchain:get_account(account_address)
        if not account then
            return send_json(404, { error = "Account not found" })
        end

        return send_json(200, { account = account })
    end

    if method == "POST" and uri == "/api/wallets" then
        local wallet, err = crypto.create_wallet()
        if not wallet then
            return send_json(500, { error = err })
        end

        return send_json(201, {
            message = "Wallet generated",
            wallet = wallet
        })
    end

    if method == "POST" and uri == "/api/transactions" then
        local payload, err = read_request_body()
        if not payload then
            return send_json(400, { error = err })
        end

        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local transaction, next_block_index_or_error = locked_blockchain:add_transaction(payload)
            if not transaction then
                return {
                    status = next_block_index_or_error == "transaction already exists" and 409 or 400,
                    body = { error = next_block_index_or_error }
                }
            end

            return {
                status = 201,
                body = {
                    message = "Signed transaction queued for mining",
                    next_block_index = next_block_index_or_error,
                    transaction = transaction,
                    pending_transactions = locked_blockchain:get_pending_transactions(),
                    account = locked_blockchain:get_account(transaction.sender)
                },
                peers = locked_blockchain:get_peers(),
                transaction = transaction
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        if result.transaction then
            result.body.propagation = broadcast_to_peers(result.peers, "/api/network/transactions", {
                source_peer = NODE_URL,
                transaction = result.transaction
            })
        end

        return send_json(result.status, result.body)
    end

    if method == "POST" and uri == "/api/network/transactions" then
        local payload, err = read_request_body()
        if not payload then
            return send_json(400, { error = err })
        end

        local source_peer = trim(payload.source_peer)
        local transaction_payload = payload.transaction or payload
        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local transaction, next_block_index_or_error = locked_blockchain:add_transaction(transaction_payload)
            if not transaction then
                if next_block_index_or_error == "transaction already exists" then
                    return {
                        status = 200,
                        body = {
                            accepted = false,
                            duplicate = true,
                            message = "Transaction already present"
                        }
                    }
                end

                return {
                    status = 400,
                    body = { error = next_block_index_or_error }
                }
            end

            return {
                status = 202,
                body = {
                    accepted = true,
                    duplicate = false,
                    next_block_index = next_block_index_or_error,
                    transaction = transaction
                },
                peers = locked_blockchain:get_peers(),
                transaction = transaction
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        if result.transaction then
            result.body.propagation = broadcast_to_peers(result.peers, "/api/network/transactions", {
                source_peer = NODE_URL,
                transaction = result.transaction
            }, source_peer ~= "" and source_peer or nil)
        end

        return send_json(result.status, result.body)
    end

    if (method == "POST" and uri == "/api/mine") or (method == "GET" and uri == "/mine_block") then
        local miner = trim(args.miner)
        if method == "POST" then
            local payload, err = read_request_body()
            if not payload then
                return send_json(400, { error = err })
            end
            miner = trim(payload.miner or miner)
        end

        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local block, mine_err = locked_blockchain:mine_block(miner)
            if not block then
                return {
                    status = 400,
                    body = { error = mine_err }
                }
            end

            return {
                status = 201,
                body = {
                    message = "New block mined",
                    block = block,
                    snapshot = build_snapshot(locked_blockchain)
                },
                peers = locked_blockchain:get_peers(),
                block = block
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        if result.block then
            result.body.propagation = broadcast_to_peers(result.peers, "/api/network/blocks", {
                source_peer = NODE_URL,
                block = result.block
            })
        end

        return send_json(result.status, result.body)
    end

    if method == "GET" and uri == "/api/peers" then
        local blockchain = build_blockchain()
        local peers = blockchain:get_peers()
        return send_json(200, {
            peers = peers,
            count = #peers
        })
    end

    if method == "POST" and uri == "/api/peers" then
        local payload, err = read_request_body()
        if not payload then
            return send_json(400, { error = err })
        end

        local peer_url = payload.peer or payload.url
        local handshake_enabled = payload.handshake ~= false
        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local peers, register_err = locked_blockchain:register_peer(peer_url)
            if not peers then
                return {
                    status = 400,
                    body = { error = register_err }
                }
            end

            return {
                status = 201,
                body = {
                    message = "Peer registered",
                    peers = peers
                }
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        if handshake_enabled and trim(peer_url) ~= "" and NODE_URL ~= "" and trim(peer_url):gsub("/+$", "") ~= NODE_URL then
            result.body.handshake = send_peer_post(trim(peer_url):gsub("/+$", ""), "/api/peers", {
                peer = NODE_URL,
                handshake = false
            })
        end

        return send_json(result.status, result.body)
    end

    if method == "POST" and uri == "/api/consensus/resolve" then
        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local replaced, consensus_result = locked_blockchain:resolve_conflicts(fetch_chain_payload)

            return {
                status = 200,
                body = {
                    replaced = replaced,
                    result = consensus_result,
                    snapshot = build_snapshot(locked_blockchain)
                }
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        return send_json(result.status, result.body)
    end

    if method == "POST" and uri == "/api/network/blocks" then
        local payload, err = read_request_body()
        if not payload then
            return send_json(400, { error = err })
        end

        local source_peer = trim(payload.source_peer)
        local block_payload = payload.block or payload
        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local appended, append_result = locked_blockchain:append_block(block_payload)
            local replaced = false
            local replace_message = nil

            if not appended and source_peer ~= "" then
                replaced, replace_message = sync_from_source_peer(locked_blockchain, source_peer)
            end

            if appended or replaced then
                return {
                    status = 202,
                    body = {
                        accepted = true,
                        appended = appended,
                        replaced = replaced,
                        message = appended and append_result or replace_message,
                        snapshot = build_snapshot(locked_blockchain)
                    }
                }
            end

            return {
                status = 409,
                body = {
                    accepted = false,
                    error = append_result
                }
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        return send_json(result.status, result.body)
    end

    if method == "GET" and uri == "/get_remote_chain" then
        local blockchain = build_blockchain()
        return send_json(200, {
            peers = blockchain:get_peers(),
            message = "Peers now exchange signed transactions and newly mined blocks over /api/network/* routes."
        })
    end

    return send_json(404, {
        error = "Route not found",
        method = method,
        path = uri
    })
end

local ok, err = xpcall(handle_request, debug.traceback)
if not ok then
    send_json(500, {
        error = "Unhandled server error",
        details = err
    })
end

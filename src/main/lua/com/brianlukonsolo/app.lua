local cjson = require("classes.json")
local canonical_json = require("classes.canonical_json")
local config = require("classes.config")
local bit = require("bit")
local resty_lock = require("resty.lock")
local sha = require("cryptography.pure_lua_SHA.sha2")
local Blockchain = require("classes.blockchain")
local crypto = require("classes.crypto")
local requests = require("classes.requests")

local APP_VERSION = "4.0.0"
local CONFIG = config.load(APP_VERSION)

local ADMIN_ROUTES = {
    ["/api/wallets"] = true,
    ["/api/mine"] = true,
    ["/mine_block"] = true,
    ["/api/peers"] = true,
    ["/api/consensus/resolve"] = true
}

local function trim(value)
    return tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

local function build_blockchain()
    return Blockchain.new({
        file_name = CONFIG.data_file,
        difficulty_prefix = CONFIG.difficulty_prefix,
        mining_reward = CONFIG.mining_reward,
        node_id = CONFIG.node_id,
        node_url = CONFIG.node_url,
        chain_id = CONFIG.chain_id,
        max_peers = CONFIG.max_peers,
        max_pending_transactions = CONFIG.max_pending_transactions,
        max_transactions_per_block = CONFIG.max_transactions_per_block,
        min_transaction_fee = CONFIG.min_transaction_fee,
        max_transaction_note_bytes = CONFIG.max_transaction_note_bytes,
        require_https_peers = CONFIG.require_https_peers,
        allowed_peer_host_map = CONFIG.allowed_peer_host_map,
        version = APP_VERSION
    })
end

local function send_json(status, payload)
    ngx.status = status
    ngx.header["Content-Type"] = "application/json; charset=utf-8"
    ngx.say(canonical_json.encode(payload))
    return ngx.exit(status)
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

local function current_headers()
    return ngx.req.get_headers(64) or {}
end

local function constant_time_equals(left, right)
    left = tostring(left or "")
    right = tostring(right or "")

    if #left ~= #right then
        return false
    end

    local diff = 0
    for index = 1, #left do
        diff = bit.bor(diff, bit.bxor(left:byte(index), right:byte(index)))
    end

    return diff == 0
end

local function get_client_identity()
    return trim(ngx.var.remote_addr) ~= "" and ngx.var.remote_addr or "unknown"
end

local function check_rate_limit(bucket, limit)
    if not limit or limit <= 0 then
        return true
    end

    local dict = ngx.shared.blockchain_rate_limits
    if not dict then
        return false, "rate limiting is unavailable", 500
    end

    local now_bucket = math.floor(ngx.now() / 60)
    local key = table.concat({ bucket, get_client_identity(), tostring(now_bucket) }, ":")
    local current, err = dict:incr(key, 1, 0, 61)
    if not current then
        return false, "rate limiting failed: " .. tostring(err), 500
    end

    if current > limit then
        return false, "rate limit exceeded", 429
    end

    ngx.header["X-RateLimit-Limit"] = tostring(limit)
    ngx.header["X-RateLimit-Remaining"] = tostring(math.max(limit - current, 0))

    return true
end

local function is_admin_route(method, uri)
    return ADMIN_ROUTES[uri] == true and (method == "POST" or uri == "/mine_block")
end

local function is_peer_route(uri)
    return uri:match("^/api/network/") ~= nil
end

local function require_admin_access()
    if CONFIG.admin_token == "" then
        return true
    end

    local headers = current_headers()
    local candidate = headers["x-blockchain-admin-token"] or headers["authorization"] or ""
    candidate = candidate:gsub("^Bearer%s+", "")

    if constant_time_equals(candidate, CONFIG.admin_token) then
        return true
    end

    return false, "admin token is required"
end

local function require_peer_access()
    if CONFIG.peer_shared_secret == "" then
        return true
    end

    local candidate = current_headers()["x-blockchain-peer-secret"] or ""
    if constant_time_equals(candidate, CONFIG.peer_shared_secret) then
        return true
    end

    return false, "peer shared secret is required"
end

local function enforce_request_policy(method, uri)
    if is_peer_route(uri) then
        local allowed, err = require_peer_access()
        if not allowed then
            return false, 401, err
        end

        local rate_ok, rate_err, rate_status = check_rate_limit("peer", CONFIG.peer_rate_limit_per_minute)
        if not rate_ok then
            return false, rate_status, rate_err
        end

        return true
    end

    if is_admin_route(method, uri) then
        local allowed, err = require_admin_access()
        if not allowed then
            return false, 401, err
        end

        local rate_ok, rate_err, rate_status = check_rate_limit("admin", CONFIG.admin_rate_limit_per_minute)
        if not rate_ok then
            return false, rate_status, rate_err
        end

        return true
    end

    local rate_ok, rate_err, rate_status = check_rate_limit("public", CONFIG.public_rate_limit_per_minute)
    if not rate_ok then
        return false, rate_status, rate_err
    end

    return true
end

local function ensure_config_is_ready()
    if CONFIG.is_valid then
        return true
    end

    return false, {
        status = "error",
        message = "node configuration is invalid",
        errors = CONFIG.errors
    }
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

local function peer_headers()
    local headers = {
        ["Content-Type"] = "application/json"
    }

    if CONFIG.peer_shared_secret ~= "" then
        headers["X-Blockchain-Peer-Secret"] = CONFIG.peer_shared_secret
    end

    return headers
end

local function send_peer_post(peer, path, payload)
    local response = requests.send_post_request(
        peer .. path,
        canonical_json.encode(payload),
        peer_headers(),
        { timeout_seconds = CONFIG.peer_timeout_seconds }
    )

    return {
        peer = peer,
        ok = response.ok,
        status_code = response.status_code,
        error = response.ok and nil or (response.status_text or ("HTTP " .. tostring(response.status_code)))
    }
end

local function broadcast_to_peers(peers, path, payload, excluded_peer)
    local outcomes = {}

    for _, peer in ipairs(peers or {}) do
        if peer ~= excluded_peer then
            outcomes[#outcomes + 1] = send_peer_post(peer, path, payload)
        end
    end

    return outcomes
end

local function fetch_peer_json(peer, path)
    local response = requests.send_get_request(
        peer .. path,
        peer_headers(),
        { timeout_seconds = CONFIG.peer_timeout_seconds }
    )
    if not response.ok then
        return nil, response.status_text or ("HTTP " .. tostring(response.status_code))
    end

    local payload, err = cjson.decode(response.body)
    if not payload then
        return nil, "Peer returned invalid JSON: " .. tostring(err)
    end

    return payload
end

local function fetch_chain_payload(peer)
    local payload, err = fetch_peer_json(peer, "/api/chain")
    if not payload then
        return nil, err
    end

    local chain_id = payload.meta and payload.meta.chain_id or nil
    if chain_id and chain_id ~= CONFIG.chain_id then
        return nil, "peer chain_id does not match local chain_id"
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

local function verify_peer_compatibility(peer)
    peer = trim(peer)
    if peer == "" then
        return false, "peer is required"
    end

    local payload, err = fetch_peer_json(peer, "/api/info")
    if not payload then
        return false, err
    end

    local snapshot = payload.snapshot or {}
    local meta = snapshot.meta or {}
    if meta.chain_id and meta.chain_id ~= CONFIG.chain_id then
        return false, "peer chain_id does not match local chain_id"
    end

    return true
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

local function handle_health()
    return send_json(200, {
        status = "ok",
        service = "lua-blockchain",
        version = APP_VERSION,
        mode = CONFIG.mode,
        node_id = CONFIG.node_id,
        node_url = CONFIG.node_url,
        chain_id = CONFIG.chain_id,
        config_valid = CONFIG.is_valid,
        config_errors = CONFIG.errors,
        timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
    })
end

local function handle_ready()
    local ready, config_error = ensure_config_is_ready()
    if not ready then
        return send_json(503, config_error)
    end

    local blockchain = build_blockchain()
    local valid, reason = blockchain:validate_chain(blockchain:get_chain())
    if not valid then
        return send_json(503, {
            status = "error",
            message = "chain validation failed",
            reason = reason
        })
    end

    return send_json(200, {
        status = "ready",
        chain_id = CONFIG.chain_id,
        node_id = CONFIG.node_id,
        timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
    })
end

local function handle_request()
    local method = ngx.req.get_method()
    local uri = ngx.var.uri
    local args = ngx.req.get_uri_args()

    if method == "GET" and (uri == "/api/health" or uri == "/health") then
        return handle_health()
    end

    if method == "GET" and uri == "/api/ready" then
        return handle_ready()
    end

    local allowed, status, err = enforce_request_policy(method, uri)
    if not allowed then
        return send_json(status, { error = err })
    end

    local config_ok, config_error = ensure_config_is_ready()
    if not config_ok then
        return send_json(503, config_error)
    end

    local hash_response = handle_hash_route(uri)
    if hash_response then
        return hash_response
    end

    if method == "GET" and (uri == "/api/info" or uri == "/configuration") then
        local blockchain = build_blockchain()
        return send_json(200, {
            service = {
                name = "lua-blockchain",
                version = APP_VERSION,
                runtime = "OpenResty / LuaJIT",
                mode = CONFIG.mode,
                node_id = CONFIG.node_id,
                node_url = CONFIG.node_url,
                chain_id = CONFIG.chain_id
            },
            capabilities = {
                signed_transactions = true,
                nonce_enforcement = true,
                account_balances = true,
                peer_propagation = true,
                longest_valid_chain_consensus = true,
                admin_authentication = CONFIG.admin_token ~= "",
                peer_authentication = CONFIG.peer_shared_secret ~= "",
                rate_limiting = true,
                frontend_console = true
            },
            api = {
                health = "/api/health",
                ready = "/api/ready",
                info = "/api/info",
                chain = "/api/chain",
                stats = "/api/stats",
                accounts = "/api/accounts",
                account = "/api/accounts/{address}",
                pending_transactions = "/api/transactions/pending",
                create_wallet = CONFIG.enable_server_wallets and "POST /api/wallets" or "disabled",
                create_transaction = "POST /api/transactions",
                mine = "POST /api/mine",
                validate = "/api/validate",
                peers = "POST /api/peers (admin)",
                consensus = "POST /api/consensus/resolve (admin)",
                receive_transaction = "POST /api/network/transactions (peer-authenticated)",
                receive_block = "POST /api/network/blocks (peer-authenticated)"
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
        if not CONFIG.enable_server_wallets then
            return send_json(403, {
                error = "server-side wallet generation is disabled"
            })
        end

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
                source_peer = CONFIG.node_url,
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
                source_peer = CONFIG.node_url,
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
                source_peer = CONFIG.node_url,
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
        local compatible, compatibility_err = verify_peer_compatibility(peer_url)
        if not compatible then
            return send_json(400, { error = compatibility_err })
        end

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
            message = "Peers exchange authenticated traffic over /api/network/* and must match the local chain_id."
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

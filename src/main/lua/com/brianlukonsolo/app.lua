local cjson = require("classes.json")
local canonical_json = require("classes.canonical_json")
local config = require("classes.config")
local bit = require("bit")
local resty_lock = require("resty.lock")
local sha = require("cryptography.pure_lua_SHA.sha2")
local Blockchain = require("classes.blockchain")
local crypto = require("classes.crypto")
local network = require("classes.network")

local APP_VERSION = "9.0.0"
local CONFIG = config.load(APP_VERSION)

local ADMIN_ROUTES = {
    ["/api/wallets"] = true,
    ["/api/mine"] = true,
    ["/mine_block"] = true,
    ["/api/peers"] = true,
    ["/api/consensus/resolve"] = true,
    ["/api/admin/backup"] = true
}

local function trim(value)
    return tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

local function build_blockchain()
    return Blockchain.new({
        file_name = CONFIG.data_file,
        difficulty_prefix = CONFIG.difficulty_prefix,
        target_block_seconds = CONFIG.target_block_seconds,
        difficulty_adjustment_window = CONFIG.difficulty_adjustment_window,
        min_difficulty_prefix_length = CONFIG.min_difficulty_prefix_length,
        max_difficulty_prefix_length = CONFIG.max_difficulty_prefix_length,
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
        allowed_peer_id_map = CONFIG.allowed_peer_id_map,
        bootstrap_peers = CONFIG.bootstrap_peers,
        peer_discovery_fanout = CONFIG.peer_discovery_fanout,
        peer_advertised_limit = CONFIG.peer_advertised_limit,
        peer_backoff_base_seconds = CONFIG.peer_backoff_base_seconds,
        peer_ban_seconds = CONFIG.peer_ban_seconds,
        peer_max_failures_before_ban = CONFIG.peer_max_failures_before_ban,
        max_peers_per_ip = CONFIG.max_peers_per_ip,
        max_peers_per_subnet = CONFIG.max_peers_per_subnet,
        p2p_enabled = CONFIG.p2p_enabled,
        p2p_bind_host = CONFIG.p2p_bind_host,
        p2p_port = CONFIG.p2p_port,
        p2p_advertise_host = CONFIG.p2p_advertise_host,
        p2p_endpoint = CONFIG.p2p_endpoint,
        p2p_seeds = CONFIG.p2p_seeds,
        p2p_tls_enabled = CONFIG.p2p_tls_enabled,
        p2p_tls_cert_path = CONFIG.p2p_tls_cert_path,
        p2p_tls_key_path = CONFIG.p2p_tls_key_path,
        node_identity_private_key_path = CONFIG.node_identity_private_key_path,
        node_identity_public_key_path = CONFIG.node_identity_public_key_path,
        backup_dir = CONFIG.backup_dir,
        gossip_enabled = CONFIG.gossip_enabled,
        gossip_bind_host = CONFIG.gossip_bind_host,
        gossip_port = CONFIG.gossip_port,
        gossip_advertise_host = CONFIG.gossip_advertise_host,
        gossip_endpoint = CONFIG.gossip_endpoint,
        gossip_seeds = CONFIG.gossip_seeds,
        gossip_fanout = CONFIG.gossip_fanout,
        gossip_interval_seconds = CONFIG.gossip_interval_seconds,
        gossip_message_ttl_seconds = CONFIG.gossip_message_ttl_seconds,
        gossip_max_hops = CONFIG.gossip_max_hops,
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
        peer_records = blockchain:get_public_peer_records(),
        accounts = blockchain:get_accounts()
    }
end

local function persist_peer_outcomes(outcomes)
    if type(outcomes) ~= "table" or #outcomes == 0 then
        return
    end

    local ok, result = with_locked_blockchain(function(locked_blockchain)
        network.record_peer_outcomes(locked_blockchain, outcomes)
        return true
    end)
    if not ok then
        ngx.log(ngx.ERR, "unable to persist peer outcomes: ", tostring(result))
    end
end

local function parse_hash_list(raw_value)
    return network.parse_hash_list(raw_value)
end

local function merge_tables(base, overrides)
    local merged = {}

    if type(base) == "table" then
        for key, value in pairs(base) do
            merged[key] = value
        end
    end

    if type(overrides) == "table" then
        for key, value in pairs(overrides) do
            merged[key] = value
        end
    end

    return merged
end

local function build_inventory_summary(blockchain)
    return network.build_inventory_summary(blockchain)
end

local function broadcast_to_peers(peers, path, payload, excluded_peer)
    return network.broadcast_to_peers(CONFIG, peers, path, payload, excluded_peer)
end

local function sync_from_source_peer(blockchain, source_peer)
    return network.sync_from_source_peer(CONFIG, blockchain, source_peer)
end

local function resolve_conflicts_headers_first(blockchain)
    return network.resolve_conflicts_headers_first(CONFIG, blockchain)
end

local function verify_peer_compatibility(peer, blockchain)
    return network.verify_peer_compatibility(CONFIG, peer, blockchain)
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
                peer_discovery = true,
                peer_reputation = true,
                inventory_announcements = true,
                native_p2p_transport = CONFIG.p2p_enabled == true,
                gossip_transport = CONFIG.gossip_enabled == true,
                block_fetch_by_hash = true,
                headers_first_sync = true,
                retargeted_prefix_pow = true,
                most_cumulative_work_consensus = true,
                background_maintenance = CONFIG.peer_maintenance_enabled == true,
                admin_authentication = CONFIG.admin_token ~= "",
                peer_authentication = CONFIG.peer_shared_secret ~= "",
                rate_limiting = true,
                frontend_console = true,
                frontend_learn = true
            },
            api = {
                health = "/api/health",
                ready = "/api/ready",
                info = "/api/info",
                chain = "/api/chain",
                headers = "/api/headers?from_height=1&limit=32",
                locator = "/api/locator",
                network_peers = "GET /api/network/peers (peer-authenticated)",
                p2p_peer_update = "POST /api/network/p2p/peer-update (peer-authenticated local daemon)",
                p2p_peer_failure = "POST /api/network/p2p/peer-failure (peer-authenticated local daemon)",
                gossip_announce = "POST /api/network/gossip/announce (peer-authenticated local daemon)",
                block_by_hash = "/api/blocks/hash/{hash}",
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
                backup = "POST /api/admin/backup (admin)",
                peer_directory = "GET /api/network/peers (peer-authenticated)",
                receive_p2p_peer_update = "POST /api/network/p2p/peer-update (peer-authenticated local daemon)",
                receive_p2p_peer_failure = "POST /api/network/p2p/peer-failure (peer-authenticated local daemon)",
                receive_gossip = "POST /api/network/gossip/announce (peer-authenticated local daemon)",
                receive_inventory = "POST /api/network/inventory (peer-authenticated)",
                receive_transaction = "POST /api/network/transactions (peer-authenticated)",
                receive_block = "POST /api/network/blocks (peer-authenticated)"
            },
            snapshot = build_snapshot(blockchain)
        })
    end

    if method == "GET" and uri == "/api/headers" then
        local blockchain = build_blockchain()
        local locator_hashes = parse_hash_list(args.locator)
        local headers = #locator_hashes > 0
            and blockchain:get_headers_after_locator(locator_hashes, args.limit)
            or blockchain:get_headers(args.from_height or args.start, args.limit)
        return send_json(200, {
            headers = headers,
            count = #headers,
            cumulative_work = blockchain:get_chain_work(blockchain:get_chain())
        })
    end

    if method == "GET" and uri == "/api/locator" then
        local blockchain = build_blockchain()
        return send_json(200, {
            meta = blockchain:get_meta(),
            locator = blockchain:get_locator(args.limit)
        })
    end

    if method == "GET" and uri == "/api/network/peers" then
        local blockchain = build_blockchain()
        return send_json(200, network.build_peer_directory_payload(
            CONFIG,
            blockchain,
            trim(args.exclude),
            args.limit
        ))
    end

    if method == "POST" and uri == "/api/network/p2p/peer-update" then
        local payload, err = read_request_body()
        if not payload then
            return send_json(400, { error = err })
        end

        local source_peer = trim(trim(payload.source_peer) ~= "" and payload.source_peer or payload.node_url)
        local chain_id = trim(payload.chain_id)
        local peer_id = trim(payload.peer_id)
        local p2p_endpoint = trim(payload.p2p_endpoint)
        local gossip_endpoint = trim(payload.gossip_endpoint)
        local remote_ip = trim(payload.remote_ip)
        local network_group = trim(payload.network_group)
        local tls_cert_fingerprint = trim(payload.tls_cert_fingerprint):lower()
        local capabilities = merge_tables(payload.capabilities, {})

        if source_peer == "" then
            return send_json(400, { error = "source_peer is required" })
        end

        if chain_id ~= "" and chain_id ~= CONFIG.chain_id then
            return send_json(400, { error = "peer chain_id does not match the local chain" })
        end

        if trim(payload.public_key) ~= "" then
            local derived_peer_id, peer_id_err = crypto.address_from_public_key(payload.public_key)
            if not derived_peer_id then
                return send_json(400, { error = peer_id_err })
            end

            if peer_id ~= "" and peer_id ~= derived_peer_id then
                return send_json(400, { error = "peer_id does not match the supplied public key" })
            end

            peer_id = derived_peer_id
            capabilities.signed_identity = {
                peer_id = peer_id,
                public_key = payload.public_key
            }
        end

        if p2p_endpoint ~= "" then
            capabilities.p2p_transport = {
                protocol = "tcp",
                endpoint = p2p_endpoint
            }
        end

        if gossip_endpoint ~= "" then
            capabilities.gossip_transport = {
                protocol = "udp",
                endpoint = gossip_endpoint
            }
        end

        if remote_ip ~= "" then
            capabilities.network_address = remote_ip
        end

        if network_group ~= "" then
            capabilities.network_group = network_group
        end

        if payload.tls_enabled ~= nil or tls_cert_fingerprint ~= "" then
            local transport_security = type(capabilities.transport_security) == "table"
                and merge_tables(capabilities.transport_security, {})
                or {}
            transport_security.tls = payload.tls_enabled == true
            if tls_cert_fingerprint ~= "" then
                transport_security.tls_cert_fingerprint = tls_cert_fingerprint
            end
            capabilities.transport_security = transport_security
        end

        capabilities.peer_discovery = true
        capabilities.headers_first_sync = true
        capabilities.block_fetch_by_hash = true
        capabilities.transaction_relay = true

        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local record, update_err = locked_blockchain:note_peer_success(source_peer, {
                source = "discovered",
                node_id = payload.node_id,
                node_url = payload.node_url or source_peer,
                version = payload.version,
                chain_id = chain_id ~= "" and chain_id or CONFIG.chain_id,
                capabilities = capabilities,
                last_advertised_height = payload.last_advertised_height,
                last_cumulative_work = payload.last_cumulative_work
            })
            if not record then
                return {
                    status = 400,
                    body = { error = update_err }
                }
            end

            local discovered = locked_blockchain:record_discovered_peers(payload.peers or payload.peer_records, source_peer)

            return {
                status = 200,
                body = {
                    accepted = true,
                    peer = record,
                    discovered = discovered
                }
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        return send_json(result.status, result.body)
    end

    if method == "POST" and uri == "/api/network/p2p/peer-failure" then
        local payload, err = read_request_body()
        if not payload then
            return send_json(400, { error = err })
        end

        local source_peer = trim(trim(payload.source_peer) ~= "" and payload.source_peer or payload.node_url)
        if source_peer == "" then
            return send_json(400, { error = "source_peer is required" })
        end

        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local record, failure_err = locked_blockchain:note_peer_failure(source_peer, payload.error or "native p2p transport failure", {
                source = "discovered",
                node_id = payload.node_id,
                node_url = payload.node_url or source_peer,
                version = payload.version,
                chain_id = payload.chain_id
            })
            if not record then
                return {
                    status = 400,
                    body = { error = failure_err }
                }
            end

            return {
                status = 200,
                body = {
                    accepted = true,
                    peer = record
                }
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        return send_json(result.status, result.body)
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

    local block_hash = uri:match("^/api/blocks/hash/([0-9a-f]+)$")
    if method == "GET" and block_hash then
        local blockchain = build_blockchain()
        local block = blockchain:get_block_by_hash(block_hash)
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

    if method == "POST" and uri == "/api/admin/backup" then
        local payload, err = read_request_body()
        if not payload then
            return send_json(400, { error = err })
        end

        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local manifest, backup_err = locked_blockchain:create_backup({
                backup_dir = CONFIG.backup_dir,
                label = payload.label
            })
            if not manifest then
                return {
                    status = 500,
                    body = { error = backup_err }
                }
            end

            return {
                status = 201,
                body = {
                    message = "Backup created",
                    backup = manifest
                }
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        return send_json(result.status, result.body)
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

        if result.transaction and not CONFIG.p2p_enabled then
            result.body.propagation = broadcast_to_peers(result.peers, "/api/network/transactions", {
                source_peer = CONFIG.node_url,
                transaction = result.transaction
            })
            persist_peer_outcomes(result.body.propagation)
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

        if result.transaction and not CONFIG.p2p_enabled then
            result.body.propagation = broadcast_to_peers(result.peers, "/api/network/transactions", {
                source_peer = CONFIG.node_url,
                transaction = result.transaction
            }, source_peer ~= "" and source_peer or nil)
            persist_peer_outcomes(result.body.propagation)
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
                block = block,
                inventory = build_inventory_summary(locked_blockchain),
                advertised_peers = locked_blockchain:get_advertised_peers(CONFIG.peer_advertised_limit),
                meta = locked_blockchain:get_meta()
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        if result.block and not CONFIG.p2p_enabled then
            result.body.propagation = broadcast_to_peers(result.peers, "/api/network/inventory", {
                source_peer = CONFIG.node_url,
                meta = result.meta,
                inventory = result.inventory,
                peers = result.advertised_peers
            })
            persist_peer_outcomes(result.body.propagation)
        end

        return send_json(result.status, result.body)
    end

    if method == "GET" and uri == "/api/peers" then
        local blockchain = build_blockchain()
        local peers = blockchain:get_peers()
        return send_json(200, {
            active_peers = peers,
            peers = peers,
            peer_records = blockchain:get_public_peer_records(),
            counts = {
                active = blockchain:get_active_peer_count(),
                total = blockchain:get_total_peer_count(),
                states = blockchain:get_peer_state_counts()
            }
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
            locked_blockchain:seed_bootstrap_peers(CONFIG.bootstrap_peers)
            local discovery = network.discover_from_peers(CONFIG, locked_blockchain)
            local replaced, consensus_result = resolve_conflicts_headers_first(locked_blockchain)

            return {
                status = 200,
                body = {
                    replaced = replaced,
                    discovery = discovery,
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

    if method == "POST" and uri == "/api/network/inventory" then
        local payload, err = read_request_body()
        if not payload then
            return send_json(400, { error = err })
        end

        local source_peer = trim(payload.source_peer)
        if source_peer == "" then
            return send_json(400, { error = "source_peer is required" })
        end

        local inventory = payload.inventory or payload
        local announced_locator = {
            hashes = parse_hash_list(inventory.locator or inventory.hashes),
            height = tonumber(inventory.height) or 0,
            cumulative_work = tonumber(inventory.cumulative_work) or 0,
            tip_hash = trim(inventory.tip_hash):lower()
        }
        local announced_peers = payload.peers or inventory.peers

        local ok, result = with_locked_blockchain(function(locked_blockchain)
            locked_blockchain:record_discovered_peers(announced_peers, source_peer)
            local updated, message, details = sync_from_source_peer(locked_blockchain, source_peer)
            local status = updated and 202 or ((details and details.status) == "error" and 409 or 200)

            return {
                status = status,
                body = {
                    accepted = updated,
                    updated = updated,
                    status = details and details.status or (updated and "updated" or "noop"),
                    message = message,
                    advertised_locator = announced_locator,
                    discovered = announced_peers,
                    snapshot = updated and build_snapshot(locked_blockchain) or nil
                }
            }
        end)
        if not ok then
            return send_json(500, { error = result })
        end

        return send_json(result.status, result.body)
    end

    if method == "POST" and uri == "/api/network/gossip/announce" then
        local payload, err = read_request_body()
        if not payload then
            return send_json(400, { error = err })
        end

        local ok, result = with_locked_blockchain(function(locked_blockchain)
            local updated, message, details = network.ingest_gossip_announcement(CONFIG, locked_blockchain, payload)
            local status = updated and 202 or ((details and details.status) == "error" and 409 or 200)

            return {
                status = status,
                body = {
                    accepted = true,
                    updated = updated,
                    status = details and details.status or (updated and "updated" or "noop"),
                    message = message,
                    source_peer = payload.source_peer,
                    source_gossip_endpoint = payload.source_gossip_endpoint,
                    discovered = details and details.discovered or 0,
                    snapshot = updated and build_snapshot(locked_blockchain) or nil
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
            message = "Peers exchange authenticated traffic over /api/network/*, announce inventory, fetch headers after locators, and fetch missing blocks by hash."
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

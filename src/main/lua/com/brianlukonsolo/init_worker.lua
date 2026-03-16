local config = require("classes.config")
local resty_lock = require("resty.lock")
local Blockchain = require("classes.blockchain")
local network = require("classes.network")

local APP_VERSION = "9.0.0"
local CONFIG = config.load(APP_VERSION)

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

local function with_locked_blockchain(callback)
    local lock, lock_err = resty_lock:new("blockchain_locks", {
        timeout = 1,
        exptime = 10
    })
    if not lock then
        return nil, "Unable to create maintenance lock: " .. tostring(lock_err)
    end

    local elapsed, acquire_err = lock:lock("blockchain_state")
    if not elapsed then
        return nil, "Unable to acquire maintenance lock: " .. tostring(acquire_err)
    end

    local ok, result = xpcall(function()
        return callback(build_blockchain())
    end, debug.traceback)

    local unlocked, unlock_err = lock:unlock()
    if not unlocked then
        return nil, "Unable to release maintenance lock: " .. tostring(unlock_err)
    end

    if not ok then
        return nil, result
    end

    return true, result
end

local function maintenance_tick(premature)
    if premature then
        return
    end

    if not CONFIG.peer_maintenance_enabled then
        return
    end

    if not CONFIG.is_valid then
        ngx.log(ngx.WARN, "peer maintenance skipped because configuration is invalid")
        return
    end

    local ok, result = with_locked_blockchain(function(blockchain)
        blockchain:seed_bootstrap_peers(CONFIG.bootstrap_peers)
        local discovery = network.discover_from_peers(CONFIG, blockchain)
        local replaced, consensus = network.resolve_conflicts_headers_first(CONFIG, blockchain)

        return {
            discovery = discovery,
            replaced = replaced,
            consensus = consensus,
            peers = blockchain:get_total_peer_count()
        }
    end)

    if not ok then
        ngx.log(ngx.ERR, "peer maintenance failed: ", tostring(result))
        return
    end

    local discovered_count = result.discovery and #(result.discovery.discovered or {}) or 0
    if result.replaced or discovered_count > 0 then
        ngx.log(
            ngx.NOTICE,
            "peer maintenance updated state; replaced=",
            tostring(result.replaced),
            ", discovered=",
            tostring(discovered_count),
            ", peers=",
            tostring(result.peers)
        )
    end
end

if ngx.worker.id() == 0 and CONFIG.peer_maintenance_enabled then
    local initial_ok, initial_err = ngx.timer.at(0, maintenance_tick)
    if not initial_ok then
        ngx.log(ngx.ERR, "unable to schedule initial peer maintenance tick: ", tostring(initial_err))
    end

    local every_ok, every_err = ngx.timer.every(CONFIG.peer_maintenance_interval_seconds, maintenance_tick)
    if not every_ok then
        ngx.log(ngx.ERR, "unable to schedule recurring peer maintenance: ", tostring(every_err))
    end
end

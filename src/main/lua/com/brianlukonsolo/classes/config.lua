local config = {}

local function trim(value)
    return tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

local function parse_boolean(value, default)
    if value == nil then
        return default
    end

    local normalized = trim(value):lower()
    if normalized == "1" or normalized == "true" or normalized == "yes" or normalized == "on" then
        return true
    end

    if normalized == "0" or normalized == "false" or normalized == "no" or normalized == "off" then
        return false
    end

    return default
end

local function parse_number(value, default)
    local parsed = tonumber(value)
    if parsed == nil then
        return default
    end

    return parsed
end

local function parse_list(value)
    local list = {}
    local raw = trim(value)
    if raw == "" then
        return list
    end

    for item in raw:gmatch("([^,]+)") do
        local normalized = trim(item)
        if normalized ~= "" then
            list[#list + 1] = normalized
        end
    end

    return list
end

local function is_sqlite_path(path)
    local normalized = trim(path):lower()
    return normalized:match("%.db$") ~= nil or normalized:match("%.sqlite$") ~= nil or normalized:match("%.sqlite3$") ~= nil
end

local function parse_port(value, default)
    local parsed = tonumber(value)
    if not parsed then
        return default
    end

    parsed = math.floor(parsed)
    if parsed < 1 or parsed > 65535 then
        return default
    end

    return parsed
end

local function all_peers_match_scheme(peers, scheme)
    for _, peer in ipairs(peers or {}) do
        if not tostring(peer):match("^" .. scheme .. "://") then
            return false, peer
        end
    end

    return true
end

local function has_any_entries(map)
    return type(map) == "table" and next(map) ~= nil
end

function config.load(app_version)
    local mode = trim(os.getenv("BLOCKCHAIN_MODE"))
    if mode == "" then
        mode = "development"
    end

    local cfg = {
        version = app_version or "9.0.0",
        mode = mode,
        data_file = os.getenv("BLOCKCHAIN_DATA_FILE") or "/app/data/blockchain.db",
        difficulty_prefix = os.getenv("BLOCKCHAIN_DIFFICULTY") or "0000",
        target_block_seconds = parse_number(os.getenv("BLOCKCHAIN_TARGET_BLOCK_SECONDS"), 30),
        difficulty_adjustment_window = parse_number(os.getenv("BLOCKCHAIN_DIFFICULTY_ADJUSTMENT_WINDOW"), 10),
        min_difficulty_prefix_length = parse_number(os.getenv("BLOCKCHAIN_MIN_DIFFICULTY_PREFIX_LENGTH"), 2),
        max_difficulty_prefix_length = parse_number(os.getenv("BLOCKCHAIN_MAX_DIFFICULTY_PREFIX_LENGTH"), 6),
        mining_reward = parse_number(os.getenv("BLOCKCHAIN_REWARD"), 1),
        node_id = os.getenv("BLOCKCHAIN_NODE_ID") or "demo-node-1",
        node_url = os.getenv("BLOCKCHAIN_NODE_URL") or "",
        chain_id = os.getenv("BLOCKCHAIN_CHAIN_ID") or "lua-blockchain-mainnet",
        peer_timeout_seconds = parse_number(os.getenv("BLOCKCHAIN_PEER_TIMEOUT_SECONDS"), 5),
        max_peers = parse_number(os.getenv("BLOCKCHAIN_MAX_PEERS"), 64),
        max_pending_transactions = parse_number(os.getenv("BLOCKCHAIN_MAX_PENDING_TRANSACTIONS"), 5000),
        max_transactions_per_block = parse_number(os.getenv("BLOCKCHAIN_MAX_TRANSACTIONS_PER_BLOCK"), 250),
        min_transaction_fee = parse_number(os.getenv("BLOCKCHAIN_MIN_TRANSACTION_FEE"), 0.01),
        max_transaction_note_bytes = parse_number(os.getenv("BLOCKCHAIN_MAX_TRANSACTION_NOTE_BYTES"), 280),
        require_https_peers = parse_boolean(os.getenv("BLOCKCHAIN_REQUIRE_HTTPS_PEERS"), false),
        enable_server_wallets = parse_boolean(os.getenv("BLOCKCHAIN_ENABLE_SERVER_WALLETS"), false),
        public_rate_limit_per_minute = parse_number(os.getenv("BLOCKCHAIN_PUBLIC_RATE_LIMIT_PER_MINUTE"), 240),
        admin_rate_limit_per_minute = parse_number(os.getenv("BLOCKCHAIN_ADMIN_RATE_LIMIT_PER_MINUTE"), 60),
        peer_rate_limit_per_minute = parse_number(os.getenv("BLOCKCHAIN_PEER_RATE_LIMIT_PER_MINUTE"), 600),
        admin_token = trim(os.getenv("BLOCKCHAIN_ADMIN_TOKEN")),
        peer_shared_secret = trim(os.getenv("BLOCKCHAIN_PEER_SHARED_SECRET")),
        allowed_peer_hosts = parse_list(os.getenv("BLOCKCHAIN_ALLOWED_PEER_HOSTS")),
        allowed_peer_ids = parse_list(os.getenv("BLOCKCHAIN_ALLOWED_PEER_IDS")),
        allow_plaintext_gossip = parse_boolean(os.getenv("BLOCKCHAIN_ALLOW_PLAINTEXT_GOSSIP"), false),
        bootstrap_peers = parse_list(os.getenv("BLOCKCHAIN_BOOTSTRAP_PEERS")),
        backup_dir = trim(os.getenv("BLOCKCHAIN_BACKUP_DIR")),
        peer_discovery_fanout = parse_number(os.getenv("BLOCKCHAIN_PEER_DISCOVERY_FANOUT"), 8),
        peer_advertised_limit = parse_number(os.getenv("BLOCKCHAIN_PEER_ADVERTISED_LIMIT"), 16),
        peer_backoff_base_seconds = parse_number(os.getenv("BLOCKCHAIN_PEER_BACKOFF_BASE_SECONDS"), 15),
        peer_ban_seconds = parse_number(os.getenv("BLOCKCHAIN_PEER_BAN_SECONDS"), 300),
        peer_max_failures_before_ban = parse_number(os.getenv("BLOCKCHAIN_PEER_MAX_FAILURES_BEFORE_BAN"), 5),
        max_peers_per_ip = parse_number(os.getenv("BLOCKCHAIN_MAX_PEERS_PER_IP"), 4),
        max_peers_per_subnet = parse_number(os.getenv("BLOCKCHAIN_MAX_PEERS_PER_SUBNET"), 8),
        peer_maintenance_enabled = parse_boolean(os.getenv("BLOCKCHAIN_PEER_MAINTENANCE_ENABLED"), true),
        peer_maintenance_interval_seconds = parse_number(os.getenv("BLOCKCHAIN_PEER_MAINTENANCE_INTERVAL_SECONDS"), 30),
        p2p_enabled = parse_boolean(os.getenv("BLOCKCHAIN_P2P_ENABLED"), true),
        p2p_bind_host = trim(os.getenv("BLOCKCHAIN_P2P_BIND_HOST")),
        p2p_port = parse_port(os.getenv("BLOCKCHAIN_P2P_PORT"), 19100),
        p2p_advertise_host = trim(os.getenv("BLOCKCHAIN_P2P_ADVERTISE_HOST")),
        p2p_seeds = parse_list(os.getenv("BLOCKCHAIN_P2P_SEEDS")),
        p2p_dial_discovered_peers = parse_boolean(os.getenv("BLOCKCHAIN_P2P_DIAL_DISCOVERED_PEERS"), false),
        p2p_connect_interval_seconds = parse_number(os.getenv("BLOCKCHAIN_P2P_CONNECT_INTERVAL_SECONDS"), 5),
        p2p_poll_interval_seconds = parse_number(os.getenv("BLOCKCHAIN_P2P_POLL_INTERVAL_SECONDS"), 2),
        p2p_max_message_bytes = parse_number(os.getenv("BLOCKCHAIN_P2P_MAX_MESSAGE_BYTES"), 1048576),
        p2p_tls_enabled = parse_boolean(os.getenv("BLOCKCHAIN_P2P_TLS_ENABLED"), true),
        p2p_tls_cert_path = trim(os.getenv("BLOCKCHAIN_P2P_TLS_CERT_PATH")),
        p2p_tls_key_path = trim(os.getenv("BLOCKCHAIN_P2P_TLS_KEY_PATH")),
        node_identity_private_key_path = trim(os.getenv("BLOCKCHAIN_NODE_IDENTITY_PRIVATE_KEY_PATH")),
        node_identity_public_key_path = trim(os.getenv("BLOCKCHAIN_NODE_IDENTITY_PUBLIC_KEY_PATH")),
        gossip_enabled = parse_boolean(os.getenv("BLOCKCHAIN_GOSSIP_ENABLED"), true),
        gossip_bind_host = trim(os.getenv("BLOCKCHAIN_GOSSIP_BIND_HOST")),
        gossip_port = parse_port(os.getenv("BLOCKCHAIN_GOSSIP_PORT"), 19090),
        gossip_advertise_host = trim(os.getenv("BLOCKCHAIN_GOSSIP_ADVERTISE_HOST")),
        gossip_seeds = parse_list(os.getenv("BLOCKCHAIN_GOSSIP_SEEDS")),
        gossip_fanout = parse_number(os.getenv("BLOCKCHAIN_GOSSIP_FANOUT"), 3),
        gossip_interval_seconds = parse_number(os.getenv("BLOCKCHAIN_GOSSIP_INTERVAL_SECONDS"), 5),
        gossip_message_ttl_seconds = parse_number(os.getenv("BLOCKCHAIN_GOSSIP_MESSAGE_TTL_SECONDS"), 30),
        gossip_max_hops = parse_number(os.getenv("BLOCKCHAIN_GOSSIP_MAX_HOPS"), 3)
    }

    if cfg.gossip_bind_host == "" then
        cfg.gossip_bind_host = "0.0.0.0"
    end

    if cfg.p2p_bind_host == "" then
        cfg.p2p_bind_host = "0.0.0.0"
    end

    if cfg.p2p_advertise_host == "" and cfg.node_url ~= "" then
        cfg.p2p_advertise_host = trim(cfg.node_url:match("^https?://([^:/]+)"))
    end

    if cfg.p2p_advertise_host ~= "" then
        cfg.p2p_endpoint = cfg.p2p_advertise_host .. ":" .. tostring(cfg.p2p_port)
    else
        cfg.p2p_endpoint = nil
    end

    if cfg.gossip_advertise_host == "" and cfg.node_url ~= "" then
        cfg.gossip_advertise_host = trim(cfg.node_url:match("^https?://([^:/]+)"))
    end

    if cfg.gossip_advertise_host ~= "" then
        cfg.gossip_endpoint = cfg.gossip_advertise_host .. ":" .. tostring(cfg.gossip_port)
    else
        cfg.gossip_endpoint = nil
    end

    if cfg.node_identity_private_key_path == "" then
        cfg.node_identity_private_key_path = "/app/data/node_identity_private.pem"
    end

    if cfg.node_identity_public_key_path == "" then
        cfg.node_identity_public_key_path = "/app/data/node_identity_public.pem"
    end

    if cfg.p2p_tls_cert_path == "" then
        cfg.p2p_tls_cert_path = "/app/data/node_p2p_cert.pem"
    end

    if cfg.p2p_tls_key_path == "" then
        cfg.p2p_tls_key_path = "/app/data/node_p2p_key.pem"
    end

    if cfg.backup_dir == "" then
        cfg.backup_dir = "/app/data/backups"
    end

    cfg.allowed_peer_host_map = {}
    for _, host in ipairs(cfg.allowed_peer_hosts) do
        cfg.allowed_peer_host_map[host:lower()] = true
    end

    cfg.allowed_peer_id_map = {}
    for _, peer_id in ipairs(cfg.allowed_peer_ids) do
        cfg.allowed_peer_id_map[peer_id:lower()] = true
    end

    local errors = {}
    if cfg.chain_id == "" then
        errors[#errors + 1] = "BLOCKCHAIN_CHAIN_ID is required"
    end

    if cfg.data_file == "" then
        errors[#errors + 1] = "BLOCKCHAIN_DATA_FILE is required"
    elseif not is_sqlite_path(cfg.data_file) then
        errors[#errors + 1] = "BLOCKCHAIN_DATA_FILE must point to a SQLite database file"
    end

    if not tostring(cfg.difficulty_prefix):match("^0+$") then
        errors[#errors + 1] = "BLOCKCHAIN_DIFFICULTY must contain only zero characters"
    end

    if cfg.target_block_seconds <= 0 then
        errors[#errors + 1] = "BLOCKCHAIN_TARGET_BLOCK_SECONDS must be > 0"
    end

    if cfg.difficulty_adjustment_window < 2 then
        errors[#errors + 1] = "BLOCKCHAIN_DIFFICULTY_ADJUSTMENT_WINDOW must be >= 2"
    end

    if cfg.min_difficulty_prefix_length < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_MIN_DIFFICULTY_PREFIX_LENGTH must be >= 1"
    end

    if cfg.max_difficulty_prefix_length < cfg.min_difficulty_prefix_length then
        errors[#errors + 1] = "BLOCKCHAIN_MAX_DIFFICULTY_PREFIX_LENGTH must be >= BLOCKCHAIN_MIN_DIFFICULTY_PREFIX_LENGTH"
    end

    if #cfg.difficulty_prefix < cfg.min_difficulty_prefix_length or #cfg.difficulty_prefix > cfg.max_difficulty_prefix_length then
        errors[#errors + 1] = "BLOCKCHAIN_DIFFICULTY length must fall between the configured min and max difficulty prefix lengths"
    end

    if cfg.max_peers < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_MAX_PEERS must be >= 1"
    end

    if cfg.max_pending_transactions < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_MAX_PENDING_TRANSACTIONS must be >= 1"
    end

    if cfg.max_transactions_per_block < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_MAX_TRANSACTIONS_PER_BLOCK must be >= 1"
    end

    if cfg.min_transaction_fee < 0 then
        errors[#errors + 1] = "BLOCKCHAIN_MIN_TRANSACTION_FEE must be >= 0"
    end

    if cfg.max_transaction_note_bytes < 0 then
        errors[#errors + 1] = "BLOCKCHAIN_MAX_TRANSACTION_NOTE_BYTES must be >= 0"
    end

    if cfg.peer_timeout_seconds <= 0 then
        errors[#errors + 1] = "BLOCKCHAIN_PEER_TIMEOUT_SECONDS must be > 0"
    end

    if cfg.peer_discovery_fanout < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_PEER_DISCOVERY_FANOUT must be >= 1"
    end

    if cfg.peer_advertised_limit < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_PEER_ADVERTISED_LIMIT must be >= 1"
    end

    if cfg.peer_backoff_base_seconds < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_PEER_BACKOFF_BASE_SECONDS must be >= 1"
    end

    if cfg.peer_ban_seconds < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_PEER_BAN_SECONDS must be >= 1"
    end

    if cfg.peer_max_failures_before_ban < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_PEER_MAX_FAILURES_BEFORE_BAN must be >= 1"
    end

    if cfg.max_peers_per_ip < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_MAX_PEERS_PER_IP must be >= 1"
    end

    if cfg.max_peers_per_subnet < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_MAX_PEERS_PER_SUBNET must be >= 1"
    end

    if cfg.peer_maintenance_interval_seconds < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_PEER_MAINTENANCE_INTERVAL_SECONDS must be >= 1"
    end

    if cfg.p2p_port < 1 or cfg.p2p_port > 65535 then
        errors[#errors + 1] = "BLOCKCHAIN_P2P_PORT must be between 1 and 65535"
    end

    if cfg.p2p_connect_interval_seconds < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_P2P_CONNECT_INTERVAL_SECONDS must be >= 1"
    end

    if cfg.p2p_poll_interval_seconds < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_P2P_POLL_INTERVAL_SECONDS must be >= 1"
    end

    if cfg.p2p_max_message_bytes < 1024 then
        errors[#errors + 1] = "BLOCKCHAIN_P2P_MAX_MESSAGE_BYTES must be >= 1024"
    end

    if cfg.p2p_tls_cert_path == "" then
        errors[#errors + 1] = "BLOCKCHAIN_P2P_TLS_CERT_PATH is required"
    end

    if cfg.p2p_tls_key_path == "" then
        errors[#errors + 1] = "BLOCKCHAIN_P2P_TLS_KEY_PATH is required"
    end

    if cfg.node_identity_private_key_path == "" then
        errors[#errors + 1] = "BLOCKCHAIN_NODE_IDENTITY_PRIVATE_KEY_PATH is required"
    end

    if cfg.node_identity_public_key_path == "" then
        errors[#errors + 1] = "BLOCKCHAIN_NODE_IDENTITY_PUBLIC_KEY_PATH is required"
    end

    if cfg.gossip_port < 1 or cfg.gossip_port > 65535 then
        errors[#errors + 1] = "BLOCKCHAIN_GOSSIP_PORT must be between 1 and 65535"
    end

    if cfg.gossip_fanout < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_GOSSIP_FANOUT must be >= 1"
    end

    if cfg.gossip_interval_seconds < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_GOSSIP_INTERVAL_SECONDS must be >= 1"
    end

    if cfg.gossip_message_ttl_seconds < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_GOSSIP_MESSAGE_TTL_SECONDS must be >= 1"
    end

    if cfg.gossip_max_hops < 1 then
        errors[#errors + 1] = "BLOCKCHAIN_GOSSIP_MAX_HOPS must be >= 1"
    end

    if cfg.backup_dir == "" then
        errors[#errors + 1] = "BLOCKCHAIN_BACKUP_DIR is required"
    end

    if cfg.mode == "production" then
        if cfg.admin_token == "" then
            errors[#errors + 1] = "BLOCKCHAIN_ADMIN_TOKEN is required in production mode"
        elseif #cfg.admin_token < 32 then
            errors[#errors + 1] = "BLOCKCHAIN_ADMIN_TOKEN must be at least 32 characters in production mode"
        end

        if cfg.peer_shared_secret == "" then
            errors[#errors + 1] = "BLOCKCHAIN_PEER_SHARED_SECRET is required in production mode"
        elseif #cfg.peer_shared_secret < 32 then
            errors[#errors + 1] = "BLOCKCHAIN_PEER_SHARED_SECRET must be at least 32 characters in production mode"
        end

        if cfg.node_url == "" then
            errors[#errors + 1] = "BLOCKCHAIN_NODE_URL is required in production mode"
        elseif not cfg.node_url:match("^https://") then
            errors[#errors + 1] = "BLOCKCHAIN_NODE_URL must use https in production mode"
        end

        if not cfg.require_https_peers then
            errors[#errors + 1] = "BLOCKCHAIN_REQUIRE_HTTPS_PEERS must be true in production mode"
        end

        local bootstrap_ok, invalid_bootstrap_peer = all_peers_match_scheme(cfg.bootstrap_peers, "https")
        if not bootstrap_ok then
            errors[#errors + 1] = "BLOCKCHAIN_BOOTSTRAP_PEERS must only contain https URLs in production mode: " .. tostring(invalid_bootstrap_peer)
        end

        if cfg.p2p_enabled and not cfg.p2p_tls_enabled then
            errors[#errors + 1] = "BLOCKCHAIN_P2P_TLS_ENABLED must be true in production mode"
        end

        if cfg.gossip_enabled and not cfg.allow_plaintext_gossip then
            errors[#errors + 1] = "BLOCKCHAIN_GOSSIP_ENABLED must be false in production mode unless BLOCKCHAIN_ALLOW_PLAINTEXT_GOSSIP=true"
        end

        if cfg.p2p_dial_discovered_peers and not has_any_entries(cfg.allowed_peer_id_map) then
            errors[#errors + 1] = "BLOCKCHAIN_ALLOWED_PEER_IDS must be configured when BLOCKCHAIN_P2P_DIAL_DISCOVERED_PEERS=true in production mode"
        end

        if not has_any_entries(cfg.allowed_peer_id_map) and not has_any_entries(cfg.allowed_peer_host_map) then
            errors[#errors + 1] = "configure BLOCKCHAIN_ALLOWED_PEER_IDS or BLOCKCHAIN_ALLOWED_PEER_HOSTS in production mode"
        end

        if cfg.enable_server_wallets then
            errors[#errors + 1] = "BLOCKCHAIN_ENABLE_SERVER_WALLETS must be false in production mode"
        end
    end

    cfg.errors = errors
    cfg.is_valid = #errors == 0

    return cfg
end

return config

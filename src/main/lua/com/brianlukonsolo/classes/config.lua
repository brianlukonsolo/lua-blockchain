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

function config.load(app_version)
    local mode = trim(os.getenv("BLOCKCHAIN_MODE"))
    if mode == "" then
        mode = "development"
    end

    local cfg = {
        version = app_version or "4.0.0",
        mode = mode,
        data_file = os.getenv("BLOCKCHAIN_DATA_FILE") or "/app/data/blockchain_data.json",
        difficulty_prefix = os.getenv("BLOCKCHAIN_DIFFICULTY") or "0000",
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
        allowed_peer_hosts = parse_list(os.getenv("BLOCKCHAIN_ALLOWED_PEER_HOSTS"))
    }

    cfg.allowed_peer_host_map = {}
    for _, host in ipairs(cfg.allowed_peer_hosts) do
        cfg.allowed_peer_host_map[host:lower()] = true
    end

    local errors = {}
    if cfg.chain_id == "" then
        errors[#errors + 1] = "BLOCKCHAIN_CHAIN_ID is required"
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

    if cfg.mode == "production" then
        if cfg.admin_token == "" then
            errors[#errors + 1] = "BLOCKCHAIN_ADMIN_TOKEN is required in production mode"
        end

        if cfg.peer_shared_secret == "" then
            errors[#errors + 1] = "BLOCKCHAIN_PEER_SHARED_SECRET is required in production mode"
        end

        if cfg.node_url == "" then
            errors[#errors + 1] = "BLOCKCHAIN_NODE_URL is required in production mode"
        elseif not cfg.node_url:match("^https://") then
            errors[#errors + 1] = "BLOCKCHAIN_NODE_URL must use https in production mode"
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

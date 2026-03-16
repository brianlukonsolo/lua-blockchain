local canonical_json = require("classes.canonical_json")
local crypto = require("classes.crypto")
local hashing = require("cryptography.pure_lua_SHA.sha2")
local StateStore = require("classes.state_store")
local storage = require("classes.storage")

local Blockchain = {}
Blockchain.__index = Blockchain

local EPSILON = 0.000001

local function utc_now()
    return os.date("!%Y-%m-%dT%H:%M:%SZ")
end

local function trim(value)
    return tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

local function deep_copy(value)
    if type(value) ~= "table" then
        return value
    end

    local copy = {}
    for key, item in pairs(value) do
        copy[key] = deep_copy(item)
    end

    return copy
end

local function copy_map(map)
    local clone = {}
    for key, value in pairs(map or {}) do
        clone[key] = value
    end

    return clone
end

local function is_array(tbl)
    if type(tbl) ~= "table" then
        return false
    end

    local count = 0
    local max = 0
    for key in pairs(tbl) do
        if type(key) ~= "number" or key < 1 or key % 1 ~= 0 then
            return false
        end

        if key > max then
            max = key
        end
        count = count + 1
    end

    return max == count
end

local function round_currency(amount)
    local parsed = tonumber(amount)
    if not parsed then
        return nil
    end

    return math.floor(parsed * 100 + 0.5) / 100
end

local function normalize_integer(value)
    local parsed = tonumber(value)
    if not parsed or parsed % 1 ~= 0 then
        return nil
    end

    return parsed
end

local function append_unique(list, seen, value)
    if value and not seen[value] then
        seen[value] = true
        list[#list + 1] = value
    end
end

local function normalize_peer_url(self, peer)
    local normalized = trim(peer):gsub("/+$", "")
    if normalized == "" then
        return nil, "peer is required"
    end

    if not normalized:match("^https?://[%w%.%-_:]+") then
        return nil, "peer must start with http:// or https://"
    end

    if self.require_https_peers and not normalized:match("^https://") then
        return nil, "peer must use https"
    end

    local host = normalized:match("^https?://([^/%?]+)")
    if next(self.allowed_peer_host_map) and (not host or not self.allowed_peer_host_map[host:lower()]) then
        return nil, "peer host is not allowed"
    end

    return normalized
end

local function parse_utc_timestamp(value)
    local year, month, day, hour, minute, second = tostring(value or ""):match("^(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d)Z$")
    if not year then
        return nil
    end

    return os.time({
        year = tonumber(year),
        month = tonumber(month),
        day = tonumber(day),
        hour = tonumber(hour),
        min = tonumber(minute),
        sec = tonumber(second)
    })
end

local function utc_from_epoch(epoch)
    return os.date("!%Y-%m-%dT%H:%M:%SZ", tonumber(epoch) or os.time())
end

local function normalize_peer_source(source)
    local normalized = trim(source):lower()
    if normalized == "" then
        return "manual"
    end

    if normalized == "manual" or normalized == "bootstrap" or normalized == "discovered" then
        return normalized
    end

    return "manual"
end

local ipv4_network_group

local function normalize_peer_capabilities(capabilities)
    if type(capabilities) ~= "table" then
        return {}
    end

    local normalized = deep_copy(capabilities)
    if trim(normalized.network_group) == "" and trim(normalized.network_address) ~= "" then
        normalized.network_group = ipv4_network_group(normalized.network_address)
    end

    return normalized
end

local function sanitize_advertised_peer_capabilities(capabilities)
    local sanitized = normalize_peer_capabilities(capabilities)
    sanitized.network_address = nil
    sanitized.network_group = nil
    return sanitized
end

local function peer_signed_identity(capabilities)
    local normalized = type(capabilities) == "table" and capabilities or {}
    return type(normalized.signed_identity) == "table" and normalized.signed_identity or {}
end

local function peer_identity_id(capabilities)
    return trim(peer_signed_identity(capabilities).peer_id):lower()
end

local function peer_network_address(capabilities)
    local normalized = type(capabilities) == "table" and capabilities or {}
    return trim(normalized.network_address)
end

local function peer_network_group(capabilities)
    local normalized = type(capabilities) == "table" and capabilities or {}
    return trim(normalized.network_group)
end

ipv4_network_group = function(address)
    local first, second, third = tostring(address or ""):match("^(%d+)%.(%d+)%.(%d+)%.%d+$")
    if first then
        return table.concat({ first, second, third, "0/24" }, ".")
    end

    return nil
end

local function peer_status_rank(status)
    if status == "active" then
        return 0
    end

    if status == "backoff" then
        return 1
    end

    if status == "banned" then
        return 2
    end

    return 3
end

local function sort_peer_records(records)
    table.sort(records, function(left, right)
        local left_status = peer_status_rank(left.state)
        local right_status = peer_status_rank(right.state)
        if left_status ~= right_status then
            return left_status < right_status
        end

        local left_score = tonumber(left.score) or 0
        local right_score = tonumber(right.score) or 0
        if left_score ~= right_score then
            return left_score > right_score
        end

        local left_success = parse_utc_timestamp(left.last_success_at) or 0
        local right_success = parse_utc_timestamp(right.last_success_at) or 0
        if left_success ~= right_success then
            return left_success > right_success
        end

        local left_seen = parse_utc_timestamp(left.last_seen_at) or 0
        local right_seen = parse_utc_timestamp(right.last_seen_at) or 0
        if left_seen ~= right_seen then
            return left_seen > right_seen
        end

        return tostring(left.url or "") < tostring(right.url or "")
    end)
end

local function zero_prefix(length)
    local count = math.max(0, normalize_integer(length) or 0)
    return string.rep("0", count)
end

local function normalize_difficulty_prefix(self, value)
    local prefix = trim(value)
    if prefix == "" then
        prefix = self.difficulty_prefix
    end

    if not prefix:match("^0+$") then
        return nil, "difficulty prefix must contain only zero characters"
    end

    if #prefix < self.min_difficulty_prefix_length or #prefix > self.max_difficulty_prefix_length then
        return nil, "difficulty prefix length is outside the configured bounds"
    end

    return prefix
end

local function block_work_from_prefix(prefix)
    local work = 1
    for _ = 1, #(prefix or "") do
        work = work * 16
    end

    return work
end

local function header_from_block(block)
    return {
        cumulative_work = tonumber(block.cumulative_work) or 0,
        difficulty_prefix = block.difficulty_prefix,
        hash = block.hash,
        index = block.index,
        mined_by = block.mined_by,
        previous_hash = block.previous_hash,
        proof = block.proof,
        timestamp = block.timestamp,
        transaction_count = #(block.transactions or {}),
        work = tonumber(block.work) or 0
    }
end

local function transaction_cost(transaction)
    return round_currency((tonumber(transaction.amount) or 0) + (tonumber(transaction.fee) or 0)) or 0
end

local function transaction_id_payload(transaction)
    if transaction.kind == "reward" then
        return canonical_json.encode({
            amount = transaction.amount,
            fee = transaction.fee,
            kind = transaction.kind,
            nonce = transaction.nonce,
            note = transaction.note,
            recipient = transaction.recipient,
            sender = transaction.sender,
            timestamp = transaction.timestamp
        })
    end

    return crypto.build_signing_message(transaction) .. "|" .. tostring(transaction.signature or "")
end

local function sanitize_backup_label(label)
    local normalized = trim(label):gsub("[^%w%-_]+", "-"):gsub("%-+", "-"):gsub("^%-+", ""):gsub("%-+$", "")
    if normalized == "" then
        return "snapshot"
    end

    return normalized
end

function Blockchain.new(options)
    local self = setmetatable({}, Blockchain)

    self.name = options.name or "lua-blockchain"
    self.version = options.version or "9.0.0"
    self.file_name = options.file_name or "blockchain.db"
    self.difficulty_prefix = options.difficulty_prefix or "0000"
    self.target_block_seconds = tonumber(options.target_block_seconds) or 30
    self.difficulty_adjustment_window = tonumber(options.difficulty_adjustment_window) or 10
    self.min_difficulty_prefix_length = tonumber(options.min_difficulty_prefix_length) or math.max(1, #self.difficulty_prefix - 2)
    self.max_difficulty_prefix_length = tonumber(options.max_difficulty_prefix_length) or math.max(#self.difficulty_prefix, #self.difficulty_prefix + 2)
    self.mining_reward = tonumber(options.mining_reward) or 1
    self.node_id = options.node_id or "node-local"
    self.node_url = options.node_url or ""
    self.chain_id = options.chain_id or "lua-blockchain-mainnet"
    self.max_peers = tonumber(options.max_peers) or 64
    self.max_pending_transactions = tonumber(options.max_pending_transactions) or 5000
    self.max_transactions_per_block = tonumber(options.max_transactions_per_block) or 250
    self.min_transaction_fee = tonumber(options.min_transaction_fee) or 0
    self.max_transaction_note_bytes = tonumber(options.max_transaction_note_bytes) or 280
    self.require_https_peers = options.require_https_peers == true
    self.allowed_peer_host_map = options.allowed_peer_host_map or {}
    self.allowed_peer_id_map = options.allowed_peer_id_map or {}
    self.bootstrap_peers = options.bootstrap_peers or {}
    self.peer_discovery_fanout = tonumber(options.peer_discovery_fanout) or 8
    self.peer_advertised_limit = tonumber(options.peer_advertised_limit) or 16
    self.peer_backoff_base_seconds = tonumber(options.peer_backoff_base_seconds) or 15
    self.peer_ban_seconds = tonumber(options.peer_ban_seconds) or 300
    self.peer_max_failures_before_ban = tonumber(options.peer_max_failures_before_ban) or 5
    self.max_peers_per_ip = tonumber(options.max_peers_per_ip) or 4
    self.max_peers_per_subnet = tonumber(options.max_peers_per_subnet) or 8
    self.p2p_enabled = options.p2p_enabled ~= false
    self.p2p_bind_host = options.p2p_bind_host or "0.0.0.0"
    self.p2p_port = tonumber(options.p2p_port) or 19100
    self.p2p_advertise_host = options.p2p_advertise_host
    self.p2p_endpoint = options.p2p_endpoint
    self.p2p_seeds = options.p2p_seeds or {}
    self.p2p_tls_enabled = options.p2p_tls_enabled ~= false
    self.p2p_tls_cert_path = options.p2p_tls_cert_path
    self.p2p_tls_key_path = options.p2p_tls_key_path
    self.node_identity_private_key_path = options.node_identity_private_key_path
    self.node_identity_public_key_path = options.node_identity_public_key_path
    self.backup_dir = options.backup_dir or "/app/data/backups"
    self.gossip_enabled = options.gossip_enabled ~= false
    self.gossip_bind_host = options.gossip_bind_host or "0.0.0.0"
    self.gossip_port = tonumber(options.gossip_port) or 19090
    self.gossip_advertise_host = options.gossip_advertise_host
    self.gossip_endpoint = options.gossip_endpoint
    self.gossip_seeds = options.gossip_seeds or {}
    self.gossip_fanout = tonumber(options.gossip_fanout) or 3
    self.gossip_interval_seconds = tonumber(options.gossip_interval_seconds) or 5
    self.gossip_message_ttl_seconds = tonumber(options.gossip_message_ttl_seconds) or 30
    self.gossip_max_hops = tonumber(options.gossip_max_hops) or 3
    self.chain = {}
    self.pending_transactions = {}
    self.peers = {}
    self.peer_records = {}
    self.meta = {}
    self.store = StateStore.new(self.file_name)
    self.normalized_node_url = nil

    if trim(self.node_url) ~= "" then
        self.normalized_node_url = trim(self.node_url):gsub("/+$", "")
    end

    self:load()
    self:seed_bootstrap_peers(self.bootstrap_peers)

    return self
end

function Blockchain:get_meta()
    return deep_copy(self.meta)
end

function Blockchain:get_chain()
    return deep_copy(self.chain)
end

function Blockchain:get_pending_transactions()
    return deep_copy(self.pending_transactions)
end

function Blockchain:get_peers()
    return deep_copy(self.peers)
end

function Blockchain:get_peer_records()
    return deep_copy(self.peer_records)
end

function Blockchain:get_backup_dir()
    return self.backup_dir
end

function Blockchain:get_peer(peer)
    local normalized, err = normalize_peer_url(self, peer)
    if not normalized then
        return nil, err
    end

    for _, record in ipairs(self.peer_records) do
        if record.url == normalized then
            return deep_copy(record)
        end
    end

    return nil
end

function Blockchain:get_active_peer_count()
    return #self.peers
end

function Blockchain:get_total_peer_count()
    return #self.peer_records
end

function Blockchain:get_peer_state_counts()
    local counts = {
        active = 0,
        backoff = 0,
        banned = 0
    }

    for _, record in ipairs(self.peer_records) do
        local status = tostring(record.state or "active")
        counts[status] = (counts[status] or 0) + 1
    end

    return counts
end

function Blockchain:refresh_peer_views()
    local active = {}
    local now_epoch = os.time()

    for _, record in ipairs(self.peer_records) do
        local backoff_until = parse_utc_timestamp(record.backoff_until)
        local ban_until = parse_utc_timestamp(record.ban_until)

        if backoff_until and backoff_until <= now_epoch then
            record.backoff_until = nil
            backoff_until = nil
        end

        if ban_until and ban_until <= now_epoch then
            record.ban_until = nil
            ban_until = nil
        end

        if ban_until and ban_until > now_epoch then
            record.state = "banned"
        elseif backoff_until and backoff_until > now_epoch then
            record.state = "backoff"
        else
            record.state = "active"
        end
    end

    sort_peer_records(self.peer_records)

    for _, record in ipairs(self.peer_records) do
        if record.state == "active" then
            active[#active + 1] = record.url
        end
    end

    self.peers = active
end

function Blockchain:find_peer_record_index(peer)
    local normalized, err = normalize_peer_url(self, peer)
    if not normalized then
        return nil, err
    end

    for index, record in ipairs(self.peer_records) do
        if record.url == normalized then
            return index
        end
    end

    return nil
end

function Blockchain:count_peer_records_by_identity(peer_id, excluded_url)
    local count = 0

    for _, record in ipairs(self.peer_records) do
        if record.url ~= excluded_url and peer_identity_id(record.capabilities) == peer_id then
            count = count + 1
        end
    end

    return count
end

function Blockchain:count_peer_records_by_network_address(network_address, excluded_url)
    local count = 0

    for _, record in ipairs(self.peer_records) do
        if record.url ~= excluded_url and peer_network_address(record.capabilities) == network_address then
            count = count + 1
        end
    end

    return count
end

function Blockchain:count_peer_records_by_network_group(network_group, excluded_url)
    local count = 0

    for _, record in ipairs(self.peer_records) do
        if record.url ~= excluded_url and peer_network_group(record.capabilities) == network_group then
            count = count + 1
        end
    end

    return count
end

function Blockchain:validate_peer_candidate(normalized_url, options)
    local capabilities = normalize_peer_capabilities(options and options.capabilities)
    local peer_id = peer_identity_id(capabilities)
    if peer_id ~= "" then
        if next(self.allowed_peer_id_map) and not self.allowed_peer_id_map[peer_id] then
            return false, "peer_id is not allowed"
        end

        if self:count_peer_records_by_identity(peer_id, normalized_url) >= 1 then
            return false, "peer_id already belongs to another registered peer"
        end
    end

    local network_address = peer_network_address(capabilities)
    if network_address ~= "" and self:count_peer_records_by_network_address(network_address, normalized_url) >= self.max_peers_per_ip then
        return false, "peer admission rejected because too many peers share the same IP address"
    end

    local network_group = peer_network_group(capabilities)
    if network_group == "" and network_address ~= "" then
        network_group = ipv4_network_group(network_address) or ""
    end

    if network_group ~= "" and self:count_peer_records_by_network_group(network_group, normalized_url) >= self.max_peers_per_subnet then
        return false, "peer admission rejected because too many peers share the same network group"
    end

    return true
end

function Blockchain:build_peer_record(peer, options)
    local normalized, err = normalize_peer_url(self, peer)
    if not normalized then
        return nil, err
    end

    if self.normalized_node_url and normalized == self.normalized_node_url then
        return nil, "peer must not reference this node"
    end

    local valid_peer, validate_err = self:validate_peer_candidate(normalized, options or {})
    if not valid_peer then
        return nil, validate_err
    end

    local now = utc_now()
    local record = {
        url = normalized,
        source = normalize_peer_source(options and options.source),
        discovered_at = trim(options and options.discovered_at or "") ~= "" and tostring(options.discovered_at) or now,
        last_seen_at = trim(options and options.last_seen_at or ""),
        last_success_at = trim(options and options.last_success_at or ""),
        last_failure_at = trim(options and options.last_failure_at or ""),
        success_count = math.max(tonumber(options and options.success_count) or 0, 0),
        failure_count = math.max(tonumber(options and options.failure_count) or 0, 0),
        score = tonumber(options and options.score) or 0,
        backoff_until = trim(options and options.backoff_until or ""),
        ban_until = trim(options and options.ban_until or ""),
        last_error = trim(options and options.last_error or ""),
        node_id = trim(options and options.node_id or ""),
        node_url = trim(options and options.node_url or ""),
        version = trim(options and options.version or ""),
        chain_id = trim(options and options.chain_id or ""),
        capabilities = normalize_peer_capabilities(options and options.capabilities),
        last_advertised_height = math.max(tonumber(options and options.last_advertised_height) or 0, 0),
        last_cumulative_work = tonumber(options and options.last_cumulative_work) or 0,
        state = "active"
    }

    if record.last_seen_at == "" then
        record.last_seen_at = nil
    end
    if record.last_success_at == "" then
        record.last_success_at = nil
    end
    if record.last_failure_at == "" then
        record.last_failure_at = nil
    end
    if record.backoff_until == "" then
        record.backoff_until = nil
    end
    if record.ban_until == "" then
        record.ban_until = nil
    end
    if record.last_error == "" then
        record.last_error = nil
    end
    if record.node_id == "" then
        record.node_id = nil
    end
    if record.node_url == "" then
        record.node_url = nil
    end
    if record.version == "" then
        record.version = nil
    end
    if record.chain_id == "" then
        record.chain_id = nil
    end

    return record
end

function Blockchain:update_peer_record(record, options)
    if type(record) ~= "table" or type(options) ~= "table" then
        return
    end

    if options.source ~= nil then
        record.source = normalize_peer_source(options.source)
    end

    local timestamp_fields = {
        "discovered_at",
        "last_seen_at",
        "last_success_at",
        "last_failure_at",
        "backoff_until",
        "ban_until"
    }
    for _, field in ipairs(timestamp_fields) do
        if options[field] ~= nil then
            local value = trim(options[field])
            record[field] = value ~= "" and value or nil
        end
    end

    local scalar_fields = {
        "node_id",
        "node_url",
        "version",
        "chain_id",
        "last_error"
    }
    for _, field in ipairs(scalar_fields) do
        if options[field] ~= nil then
            local value = trim(options[field])
            record[field] = value ~= "" and value or nil
        end
    end

    if options.success_count ~= nil then
        record.success_count = math.max(tonumber(options.success_count) or 0, 0)
    end

    if options.failure_count ~= nil then
        record.failure_count = math.max(tonumber(options.failure_count) or 0, 0)
    end

    if options.score ~= nil then
        record.score = tonumber(options.score) or 0
    end

    if options.last_advertised_height ~= nil then
        record.last_advertised_height = math.max(tonumber(options.last_advertised_height) or 0, 0)
    end

    if options.last_cumulative_work ~= nil then
        record.last_cumulative_work = tonumber(options.last_cumulative_work) or 0
    end

    if options.capabilities ~= nil then
        record.capabilities = normalize_peer_capabilities(options.capabilities)
    end
end

function Blockchain:upsert_peer_record(peer, options)
    local normalized, err = normalize_peer_url(self, peer)
    if not normalized then
        return nil, err, false
    end

    local valid_peer, validate_err = self:validate_peer_candidate(normalized, options or {})
    if not valid_peer then
        return nil, validate_err, false
    end

    local existing_index = self:find_peer_record_index(normalized)
    if existing_index then
        local record = self.peer_records[existing_index]
        self:update_peer_record(record, options or {})
        self:refresh_peer_views()
        return record, nil, false
    end

    if #self.peer_records >= self.max_peers then
        return nil, "peer limit reached", false
    end

    local record, build_err = self:build_peer_record(normalized, options)
    if not record then
        return nil, build_err, false
    end

    self.peer_records[#self.peer_records + 1] = record
    self:refresh_peer_views()

    return record, nil, true
end

function Blockchain:get_advertised_peers(limit, excluded_peer)
    local normalized_excluded = nil
    if trim(excluded_peer) ~= "" then
        normalized_excluded = trim(excluded_peer):gsub("/+$", "")
    end

    local advertised = {}
    local max_count = math.min(math.max(normalize_integer(limit) or self.peer_advertised_limit or 16, 1), self.max_peers)

    for _, record in ipairs(self.peer_records) do
        if record.state == "active" and record.url ~= normalized_excluded then
            advertised[#advertised + 1] = record.url
            if #advertised >= max_count then
                break
            end
        end
    end

    return advertised
end

function Blockchain:get_advertised_peer_records(limit, excluded_peer)
    local normalized_excluded = nil
    if trim(excluded_peer) ~= "" then
        normalized_excluded = trim(excluded_peer):gsub("/+$", "")
    end

    local advertised = {}
    local max_count = math.min(math.max(normalize_integer(limit) or self.peer_advertised_limit or 16, 1), self.max_peers)

    for _, record in ipairs(self.peer_records) do
        if record.state == "active" and record.url ~= normalized_excluded then
            advertised[#advertised + 1] = {
                url = record.url,
                node_id = record.node_id,
                node_url = record.node_url,
                version = record.version,
                chain_id = record.chain_id,
                capabilities = sanitize_advertised_peer_capabilities(record.capabilities),
                last_advertised_height = record.last_advertised_height,
                last_cumulative_work = record.last_cumulative_work
            }
            if #advertised >= max_count then
                break
            end
        end
    end

    return advertised
end

function Blockchain:seed_bootstrap_peers(peers)
    local dirty = false

    for _, peer in ipairs(peers or {}) do
        local _, err, created = self:upsert_peer_record(peer, {
            source = "bootstrap",
            discovered_at = utc_now()
        })
        if created then
            dirty = true
        elseif err and err ~= "peer limit reached" then
            -- Ignore invalid bootstrap peers to keep startup resilient.
        end
    end

    if dirty then
        self:save()
    end
end

function Blockchain:note_peer_success(peer, options)
    local now = utc_now()
    local upsert_options = deep_copy(options or {})
    upsert_options.source = upsert_options.source or "discovered"
    upsert_options.discovered_at = upsert_options.discovered_at or now

    local record, err = self:upsert_peer_record(peer, upsert_options)
    if not record then
        return nil, err
    end

    self:update_peer_record(record, options or {})
    record.last_seen_at = now
    record.last_success_at = now
    record.success_count = (tonumber(record.success_count) or 0) + 1
    record.failure_count = 0
    record.backoff_until = nil
    record.ban_until = nil
    record.last_error = nil
    record.score = math.min((tonumber(record.score) or 0) + 10, 1000)
    self:refresh_peer_views()
    self:save()

    return deep_copy(record)
end

function Blockchain:note_peer_failure(peer, error_message, options)
    local now_epoch = os.time()
    local now = utc_from_epoch(now_epoch)
    local upsert_options = deep_copy(options or {})
    upsert_options.source = upsert_options.source or "discovered"
    upsert_options.discovered_at = upsert_options.discovered_at or now

    local record, err = self:upsert_peer_record(peer, upsert_options)
    if not record then
        return nil, err
    end

    self:update_peer_record(record, options or {})
    record.last_seen_at = now
    record.last_failure_at = now
    record.failure_count = (tonumber(record.failure_count) or 0) + 1
    record.last_error = trim(error_message)
    record.score = math.max((tonumber(record.score) or 0) - 20, -1000)

    local backoff_seconds = self.peer_backoff_base_seconds
    for _ = 2, record.failure_count do
        backoff_seconds = math.min(backoff_seconds * 2, self.peer_ban_seconds)
    end
    record.backoff_until = utc_from_epoch(now_epoch + backoff_seconds)

    if record.failure_count >= self.peer_max_failures_before_ban then
        record.ban_until = utc_from_epoch(now_epoch + self.peer_ban_seconds)
    end

    self:refresh_peer_views()
    self:save()

    return deep_copy(record)
end

function Blockchain:record_discovered_peers(peers, source_peer)
    local discovered = {}
    local dirty = false

    for _, peer in ipairs(peers or {}) do
        local peer_url = type(peer) == "table" and peer.url or peer
        local peer_options = {
            source = "discovered",
            discovered_at = utc_now()
        }

        if type(peer) == "table" then
            peer_options.node_id = peer.node_id
            peer_options.node_url = peer.node_url
            peer_options.version = peer.version
            peer_options.chain_id = peer.chain_id
            peer_options.capabilities = peer.capabilities
            peer_options.last_advertised_height = peer.last_advertised_height
            peer_options.last_cumulative_work = peer.last_cumulative_work
        end

        local record, err, created = self:upsert_peer_record(peer_url, peer_options)
        if record then
            discovered[#discovered + 1] = record.url
        elseif err == "peer limit reached" then
            break
        end

        if created then
            dirty = true
        end
    end

    if dirty then
        self:save()
    end

    return discovered
end

function Blockchain:get_previous_block()
    return self.chain[#self.chain]
end

function Blockchain:get_block(index)
    return deep_copy(self.chain[index])
end

function Blockchain:get_block_index_by_hash(hash)
    local normalized_hash = trim(hash):lower()
    if normalized_hash == "" then
        return nil
    end

    for index, block in ipairs(self.chain) do
        if tostring(block.hash or ""):lower() == normalized_hash then
            return index
        end
    end

    return nil
end

function Blockchain:get_block_by_hash(hash)
    local index = self:get_block_index_by_hash(hash)
    if not index then
        return nil
    end

    return self:get_block(index)
end

function Blockchain:get_sha256_hash_of_string(value)
    return hashing.sha256(value)
end

function Blockchain:get_stats()
    local committed_transactions = 0
    local committed_value = 0
    local pending_value = 0
    local queued_fees = 0
    local account_set = {}
    local account_count = 0

    for _, block in ipairs(self.chain) do
        for _, transaction in ipairs(block.transactions or {}) do
            committed_transactions = committed_transactions + 1
            committed_value = round_currency(committed_value + (tonumber(transaction.amount) or 0)) or committed_value

            if transaction.sender and transaction.sender ~= "NETWORK" and not account_set[transaction.sender] then
                account_set[transaction.sender] = true
                account_count = account_count + 1
            end

            if transaction.recipient and not account_set[transaction.recipient] then
                account_set[transaction.recipient] = true
                account_count = account_count + 1
            end
        end
    end

    for _, transaction in ipairs(self.pending_transactions) do
        pending_value = round_currency(pending_value + (tonumber(transaction.amount) or 0)) or pending_value
        queued_fees = round_currency(queued_fees + (tonumber(transaction.fee) or 0)) or queued_fees
    end

    local accounts = self:get_accounts()
    local circulating_supply = 0
    for _, account in ipairs(accounts) do
        circulating_supply = round_currency(circulating_supply + (tonumber(account.confirmed_balance) or 0)) or circulating_supply
    end

    local average_block_time_seconds = self:get_average_block_time_seconds(self.chain)
    if average_block_time_seconds then
        average_block_time_seconds = math.floor(average_block_time_seconds * 100 + 0.5) / 100
    end

    local tip = self.chain[#self.chain]
    local peer_state_counts = self:get_peer_state_counts()

    return {
        blocks = #self.chain,
        pending_transactions = #self.pending_transactions,
        committed_transactions = committed_transactions,
        peers = #self.peers,
        known_peers = #self.peer_records,
        peer_states = peer_state_counts,
        accounts = account_count,
        mining_reward = self.mining_reward,
        difficulty_prefix = self.difficulty_prefix,
        tip_difficulty_prefix = tip and tip.difficulty_prefix or self.difficulty_prefix,
        target_block_seconds = self.target_block_seconds,
        difficulty_adjustment_window = self.difficulty_adjustment_window,
        cumulative_work = self:get_chain_work(self.chain),
        average_block_time_seconds = average_block_time_seconds,
        committed_value = committed_value,
        pending_value = pending_value,
        queued_fees = queued_fees,
        circulating_supply = circulating_supply,
        last_block_hash = tip and tip.hash or nil
    }
end

function Blockchain:get_accounts()
    local confirmed_state = self:calculate_confirmed_state(self.chain)
    if not confirmed_state then
        return {}
    end

    local pending_result = self:simulate_pending_transactions(confirmed_state, self.pending_transactions)
    local account_order = {}
    local seen = {}

    for address in pairs(confirmed_state.balances) do
        append_unique(account_order, seen, address)
    end
    for address in pairs(pending_result.state.balances) do
        append_unique(account_order, seen, address)
    end
    for _, transaction in ipairs(self.pending_transactions) do
        append_unique(account_order, seen, transaction.sender ~= "NETWORK" and transaction.sender or nil)
        append_unique(account_order, seen, transaction.recipient)
    end

    local accounts = {}
    for _, address in ipairs(account_order) do
        local summary = self:get_account(address, confirmed_state, pending_result)
        if summary then
            accounts[#accounts + 1] = summary
        end
    end

    table.sort(accounts, function(left, right)
        if left.pending_balance == right.pending_balance then
            return left.address < right.address
        end

        return left.pending_balance > right.pending_balance
    end)

    return accounts
end

function Blockchain:get_account(address, confirmed_state, pending_result)
    local normalized_address = crypto.normalize_address(address)
    if not normalized_address then
        return nil
    end

    local committed_state = confirmed_state or self:calculate_confirmed_state(self.chain)
    if not committed_state then
        return nil
    end

    local queued_state = pending_result or self:simulate_pending_transactions(committed_state, self.pending_transactions)
    local next_confirmed_nonce = committed_state.nonces[normalized_address] or 1
    local next_pending_nonce = queued_state.state.nonces[normalized_address] or next_confirmed_nonce

    local pending_outgoing = 0
    local pending_incoming = 0
    for _, transaction in ipairs(queued_state.accepted) do
        if transaction.sender == normalized_address then
            pending_outgoing = pending_outgoing + 1
        end
        if transaction.recipient == normalized_address then
            pending_incoming = pending_incoming + 1
        end
    end

    return {
        address = normalized_address,
        confirmed_balance = round_currency(committed_state.balances[normalized_address] or 0) or 0,
        pending_balance = round_currency(queued_state.state.balances[normalized_address] or 0) or 0,
        next_confirmed_nonce = next_confirmed_nonce,
        next_nonce = next_pending_nonce,
        pending_outgoing = pending_outgoing,
        pending_incoming = pending_incoming
    }
end

function Blockchain:serialize_block_for_hash(block)
    local payload = {
        hash = nil,
        index = block.index,
        mined_by = block.mined_by,
        network = self.chain_id,
        previous_hash = tostring(block.previous_hash or "0"),
        proof = block.proof,
        timestamp = block.timestamp,
        transactions = deep_copy(block.transactions or {})
    }

    if block.hash_format ~= "legacy-v1" then
        payload.difficulty_prefix = block.difficulty_prefix
    end

    return payload
end

function Blockchain:calculate_hash(block)
    return self:get_sha256_hash_of_string(canonical_json.encode(self:serialize_block_for_hash(block)))
end

function Blockchain:is_valid_proof(previous_proof, proof, previous_hash, difficulty_prefix)
    local payload = table.concat({
        tostring(previous_proof),
        tostring(proof),
        tostring(previous_hash)
    }, ":")

    local hash = self:get_sha256_hash_of_string(payload)
    local prefix = difficulty_prefix or self.difficulty_prefix
    return hash:sub(1, #prefix) == prefix
end

function Blockchain:proof_of_work(previous_proof, previous_hash, difficulty_prefix)
    local proof = 1

    while not self:is_valid_proof(previous_proof, proof, previous_hash, difficulty_prefix) do
        proof = proof + 1
    end

    return proof
end

function Blockchain:normalize_transaction(input)
    if type(input) ~= "table" then
        return nil, "transaction must be an object"
    end

    local kind = trim(input.kind)
    if kind == "" then
        kind = trim(input.sender) == "NETWORK" and "reward" or "transfer"
    end

    if kind ~= "transfer" and kind ~= "reward" then
        return nil, "unsupported transaction kind"
    end

    local amount = round_currency(input.amount)
    if not amount or amount <= 0 then
        return nil, "amount must be a positive number"
    end

    local fee = round_currency(input.fee or 0)
    if fee == nil or fee < 0 then
        return nil, "fee must be zero or a positive number"
    end

    local timestamp = trim(input.timestamp)
    if timestamp == "" then
        timestamp = utc_now()
    end

    local note = trim(input.note or "")
    local sender = trim(input.sender)
    local recipient = trim(input.recipient)
    local nonce = 0
    local public_key = nil
    local signature = nil

    if kind == "reward" then
        sender = "NETWORK"
        fee = 0
        nonce = 0
    else
        if fee + EPSILON < self.min_transaction_fee then
            return nil, "fee must be at least " .. tostring(self.min_transaction_fee)
        end

        local normalized_sender = crypto.normalize_address(sender)
        if not normalized_sender then
            return nil, "sender must be a valid address"
        end

        sender = normalized_sender
        nonce = normalize_integer(input.nonce)
        if not nonce or nonce < 1 then
            return nil, "nonce must be a positive integer"
        end

        local public_key_err
        public_key, public_key_err = crypto.normalize_public_key_pem(input.public_key)
        if not public_key then
            return nil, public_key_err
        end

        local signature_err
        signature, signature_err = crypto.normalize_signature(input.signature)
        if not signature then
            return nil, signature_err
        end
    end

    local normalized_recipient = crypto.normalize_address(recipient)
    if not normalized_recipient then
        return nil, "recipient must be a valid address"
    end
    recipient = normalized_recipient

    if #note > self.max_transaction_note_bytes then
        return nil, "note exceeds max length of " .. tostring(self.max_transaction_note_bytes) .. " bytes"
    end

    local transaction = {
        amount = amount,
        fee = fee,
        kind = kind,
        nonce = nonce,
        note = note,
        recipient = recipient,
        sender = sender,
        timestamp = timestamp
    }

    if public_key then
        transaction.public_key = public_key
    end

    if signature then
        transaction.signature = signature
    end

    local expected_id = self:get_sha256_hash_of_string(transaction_id_payload(transaction))
    local transaction_id = trim(input.id)
    if transaction_id ~= "" and transaction_id ~= expected_id then
        return nil, "transaction id does not match the signed payload"
    end

    transaction.id = expected_id

    return transaction
end

function Blockchain:normalize_block(input, previous_hash, index, previous_cumulative_work)
    if type(input) ~= "table" then
        return nil, "block must be an object"
    end

    local block = {
        index = normalize_integer(input.index) or index or (#self.chain + 1),
        timestamp = trim(input.timestamp) ~= "" and trim(input.timestamp) or utc_now(),
        transactions = {},
        proof = normalize_integer(input.proof) or 1,
        previous_hash = tostring(input.previous_hash or previous_hash or "0"),
        mined_by = trim(input.mined_by) ~= "" and trim(input.mined_by) or nil,
        hash_format = trim(input.hash_format) ~= "" and trim(input.hash_format) or "v2"
    }

    local difficulty_prefix, difficulty_err = normalize_difficulty_prefix(self, input.difficulty_prefix)
    if not difficulty_prefix then
        return nil, difficulty_err
    end
    block.difficulty_prefix = difficulty_prefix

    if is_array(input.transactions) then
        if #input.transactions > self.max_transactions_per_block then
            return nil, "block exceeds the max transaction count of " .. tostring(self.max_transactions_per_block)
        end

        for transaction_index, transaction in ipairs(input.transactions) do
            local normalized_transaction, transaction_err = self:normalize_transaction(transaction)
            if not normalized_transaction then
                return nil, "invalid transaction at index " .. tostring(transaction_index) .. ": " .. tostring(transaction_err)
            end
            block.transactions[transaction_index] = normalized_transaction
        end
    end

    block.hash = trim(input.hash) ~= "" and tostring(input.hash) or self:calculate_hash(block)
    block.work = block_work_from_prefix(block.difficulty_prefix)
    block.cumulative_work = (tonumber(previous_cumulative_work) or 0) + block.work

    return block
end

function Blockchain:normalize_chain(chain)
    if not is_array(chain) then
        return nil, "chain must be an array"
    end

    local normalized = {}
    local previous_hash = "0"
    local previous_cumulative_work = 0

    for index, block in ipairs(chain) do
        local normalized_block, err = self:normalize_block(block, previous_hash, index, previous_cumulative_work)
        if not normalized_block then
            return nil, err
        end

        normalized[index] = normalized_block
        previous_hash = normalized_block.hash
        previous_cumulative_work = normalized_block.cumulative_work
    end

    return normalized
end

function Blockchain:calculate_block_work(difficulty_prefix)
    return block_work_from_prefix(difficulty_prefix or self.difficulty_prefix)
end

function Blockchain:get_chain_work(chain)
    local candidate_chain = chain or self.chain
    local tip = candidate_chain and candidate_chain[#candidate_chain] or nil
    return tip and (tonumber(tip.cumulative_work) or 0) or 0
end

function Blockchain:get_average_block_time_seconds(chain)
    local candidate_chain = chain or self.chain
    local interval_count = math.min(math.max(self.difficulty_adjustment_window - 1, 1), math.max(#candidate_chain - 2, 0))
    if interval_count < 1 then
        return nil
    end

    local start_index = #candidate_chain - interval_count
    local start_epoch = parse_utc_timestamp(candidate_chain[start_index].timestamp)
    local end_epoch = parse_utc_timestamp(candidate_chain[#candidate_chain].timestamp)
    if not start_epoch or not end_epoch or end_epoch <= start_epoch then
        return nil
    end

    return (end_epoch - start_epoch) / interval_count
end

function Blockchain:get_next_difficulty_prefix(chain)
    local candidate_chain = chain or self.chain
    local current_prefix = self.difficulty_prefix
    if candidate_chain and candidate_chain[#candidate_chain] and candidate_chain[#candidate_chain].difficulty_prefix then
        current_prefix = candidate_chain[#candidate_chain].difficulty_prefix
    end

    local current_length = #current_prefix
    local mined_blocks = math.max(#candidate_chain - 1, 0)
    if mined_blocks < self.difficulty_adjustment_window or mined_blocks % self.difficulty_adjustment_window ~= 0 then
        return current_prefix
    end

    local average = self:get_average_block_time_seconds(candidate_chain)
    if not average then
        return current_prefix
    end

    if average <= self.target_block_seconds * 0.5 and current_length < self.max_difficulty_prefix_length then
        current_length = current_length + 1
    elseif average >= self.target_block_seconds * 2 and current_length > self.min_difficulty_prefix_length then
        current_length = current_length - 1
    end

    return zero_prefix(current_length)
end

function Blockchain:compare_chain_priority(candidate_chain, local_chain)
    local current_chain = local_chain or self.chain
    local candidate_work = self:get_chain_work(candidate_chain)
    local local_work = self:get_chain_work(current_chain)

    if candidate_work ~= local_work then
        return candidate_work > local_work, candidate_work > local_work and "higher cumulative work" or "lower cumulative work"
    end

    if #candidate_chain ~= #current_chain then
        return #candidate_chain > #current_chain, #candidate_chain > #current_chain and "same work but longer chain" or "same work but shorter chain"
    end

    local candidate_tip_hash = candidate_chain[#candidate_chain] and tostring(candidate_chain[#candidate_chain].hash or "") or ""
    local current_tip_hash = current_chain[#current_chain] and tostring(current_chain[#current_chain].hash or "") or ""
    if candidate_tip_hash ~= current_tip_hash then
        return candidate_tip_hash < current_tip_hash, candidate_tip_hash < current_tip_hash and "same work and height, lower tip hash" or "same work and height, higher tip hash"
    end

    return false, "same cumulative work and height"
end

function Blockchain:get_headers(start_index, limit)
    local headers = {}
    local first_index = math.max(normalize_integer(start_index) or 1, 1)
    local max_headers = math.min(math.max(normalize_integer(limit) or 32, 1), 256)

    for index = first_index, math.min(#self.chain, first_index + max_headers - 1) do
        headers[#headers + 1] = header_from_block(self.chain[index])
    end

    return headers
end

function Blockchain:get_headers_after_locator(locator_hashes, limit)
    local first_index = 1

    if type(locator_hashes) == "table" and #locator_hashes > 0 then
        for _, hash in ipairs(locator_hashes) do
            local match_index = self:get_block_index_by_hash(hash)
            if match_index then
                first_index = match_index + 1
                break
            end
        end
    end

    return self:get_headers(first_index, limit)
end

function Blockchain:get_locator(limit)
    local hashes = {}
    local max_hashes = math.min(math.max(normalize_integer(limit) or 16, 1), 64)
    local index = #self.chain
    local step = 1
    local emitted = 0

    while index >= 1 and emitted < max_hashes do
        hashes[#hashes + 1] = self.chain[index].hash
        emitted = emitted + 1
        index = index - step
        if emitted >= 5 then
            step = step * 2
        end
    end

    if hashes[#hashes] ~= self.chain[1].hash then
        hashes[#hashes + 1] = self.chain[1].hash
    end

    return {
        hashes = hashes,
        height = #self.chain,
        cumulative_work = self:get_chain_work(self.chain),
        tip_hash = self.chain[#self.chain] and self.chain[#self.chain].hash or nil
    }
end

function Blockchain:import_blocks(candidate_blocks)
    if not is_array(candidate_blocks) or #candidate_blocks == 0 then
        return false, "candidate blocks must be a non-empty array"
    end

    local first_candidate = candidate_blocks[1]
    local ancestor_hash = tostring(first_candidate.previous_hash or "")
    local ancestor_index = self:get_block_index_by_hash(ancestor_hash)
    if not ancestor_index then
        return false, "candidate branch does not share a known ancestor"
    end

    local first_index = normalize_integer(first_candidate.index)
    if not first_index or first_index ~= ancestor_index + 1 then
        return false, "candidate branch does not start after the common ancestor"
    end

    local candidate_chain = {}
    for index = 1, ancestor_index do
        candidate_chain[index] = deep_copy(self.chain[index])
    end

    for _, block in ipairs(candidate_blocks) do
        candidate_chain[#candidate_chain + 1] = deep_copy(block)
    end

    local normalized_chain, normalize_err = self:normalize_chain(candidate_chain)
    if not normalized_chain then
        return false, normalize_err
    end

    local valid, reason = self:validate_chain(normalized_chain)
    if not valid then
        return false, reason
    end

    local preferred, preference_reason = self:compare_chain_priority(normalized_chain, self.chain)
    if not preferred then
        return false, "Candidate blocks are not preferred: " .. tostring(preference_reason)
    end

    local committed_ids = {}
    for _, block in ipairs(candidate_blocks) do
        for _, transaction in ipairs(block.transactions or {}) do
            committed_ids[transaction.id] = true
        end
    end

    self.chain = normalized_chain
    self:remove_pending_by_ids(committed_ids)
    self:revalidate_pending_transactions()
    self:save()

    return true, "Local chain updated from fetched blocks"
end

function Blockchain:build_meta(updated_at)
    return {
        chain_id = self.chain_id,
        consensus = "most-cumulative-work",
        difficulty_prefix = self.difficulty_prefix,
        target_block_seconds = self.target_block_seconds,
        difficulty_adjustment_window = self.difficulty_adjustment_window,
        min_difficulty_prefix_length = self.min_difficulty_prefix_length,
        max_difficulty_prefix_length = self.max_difficulty_prefix_length,
        limits = {
            max_peers = self.max_peers,
            max_peers_per_ip = self.max_peers_per_ip,
            max_peers_per_subnet = self.max_peers_per_subnet,
            max_pending_transactions = self.max_pending_transactions,
            max_transactions_per_block = self.max_transactions_per_block,
            min_transaction_fee = self.min_transaction_fee,
            max_transaction_note_bytes = self.max_transaction_note_bytes
        },
        mining_reward = self.mining_reward,
        name = self.name,
        node_id = self.node_id,
        node_url = self.node_url,
        transports = {
            http = {
                endpoint = self.node_url
            },
            p2p = self.p2p_enabled and {
                protocol = "tcp",
                endpoint = self.p2p_endpoint,
                port = self.p2p_port,
                tls = self.p2p_tls_enabled == true
            } or nil,
            gossip = self.gossip_enabled and {
                protocol = "udp",
                endpoint = self.gossip_endpoint,
                port = self.gossip_port,
                fanout = self.gossip_fanout
            } or nil
        },
        fork_choice = "most-cumulative-work",
        schema_version = 8,
        storage_engine = "sqlite",
        updated_at = updated_at or utc_now(),
        version = self.version
    }
end

function Blockchain:build_state(chain)
    return {
        meta = deep_copy(self.meta),
        chain = deep_copy(chain or self.chain),
        pending_transactions = deep_copy(self.pending_transactions),
        peer_records = deep_copy(self.peer_records)
    }
end

function Blockchain:save()
    self.meta = self:build_meta(utc_now())
    return self.store:save_state(self:build_state())
end

function Blockchain:create_genesis_block()
    local block = {
        index = 1,
        timestamp = "genesis block",
        transactions = {},
        proof = 1,
        previous_hash = "0",
        difficulty_prefix = self.difficulty_prefix,
        hash_format = "v2"
    }
    block.hash = self:calculate_hash(block)
    block.work = self:calculate_block_work(block.difficulty_prefix)
    block.cumulative_work = block.work

    return block
end

function Blockchain:reset()
    self.chain = { self:create_genesis_block() }
    self.pending_transactions = {}
    self.peers = {}
    self.peer_records = {}
    self.meta = self:build_meta()

    local ok, err = self:save()
    if not ok then
        error("Unable to initialize blockchain state: " .. tostring(err))
    end
end

function Blockchain:create_backup(options)
    local backup_dir = trim(options and options.backup_dir or self.backup_dir)
    if backup_dir == "" then
        return nil, "backup directory is required"
    end

    local timestamp = os.date("!%Y%m%dT%H%M%SZ")
    local label = sanitize_backup_label(options and options.label or "")
    local prefix = backup_dir .. "/" .. label .. "-" .. timestamp
    local database_path = prefix .. ".db"
    local manifest_path = prefix .. ".json"

    local backup_path, backup_err = self.store:backup_to(database_path)
    if not backup_path then
        return nil, backup_err
    end

    local files = {
        database = backup_path
    }

    local function copy_optional(source_path, suffix)
        local source = trim(source_path)
        if source == "" or not storage.file_exists(source) then
            return true
        end

        local destination = prefix .. "-" .. suffix
        local copied, copy_err = storage.copy_file(source, destination)
        if not copied then
            return nil, copy_err
        end

        files[suffix] = destination
        return true
    end

    local copied, copy_err = copy_optional(self.node_identity_private_key_path, "node-identity-private.pem")
    if not copied then
        return nil, copy_err
    end

    copied, copy_err = copy_optional(self.node_identity_public_key_path, "node-identity-public.pem")
    if not copied then
        return nil, copy_err
    end

    copied, copy_err = copy_optional(self.p2p_tls_key_path, "p2p-tls-key.pem")
    if not copied then
        return nil, copy_err
    end

    copied, copy_err = copy_optional(self.p2p_tls_cert_path, "p2p-tls-cert.pem")
    if not copied then
        return nil, copy_err
    end

    local manifest = {
        created_at = utc_now(),
        label = label,
        chain = {
            chain_id = self.chain_id,
            blocks = #self.chain,
            cumulative_work = self:get_chain_work(self.chain),
            tip_hash = self.chain[#self.chain] and self.chain[#self.chain].hash or nil
        },
        node = {
            node_id = self.node_id,
            node_url = self.node_url
        },
        files = files
    }

    local wrote_manifest, manifest_err = storage.atomic_write_json(manifest_path, manifest)
    if not wrote_manifest then
        return nil, manifest_err
    end

    manifest.manifest = manifest_path
    return manifest
end

function Blockchain:load()
    local loaded_state, err, err_type = self.store:load_state()
    if not loaded_state then
        if err_type == "missing" then
            self:reset()
            return
        end

        error("Unable to load blockchain state from " .. tostring(self.file_name) .. ": " .. tostring(err))
    end

    local loaded_meta = type(loaded_state.meta) == "table" and loaded_state.meta or {}
    local dirty = false
    self.meta = self:build_meta(loaded_meta.updated_at or utc_now())

    if loaded_meta.schema_version ~= self.meta.schema_version or loaded_meta.storage_engine ~= self.meta.storage_engine then
        dirty = true
    end

    self.peers = {}
    self.peer_records = {}
    local seen_peers = {}
    local loaded_peer_values = nil
    if is_array(loaded_state.peer_records) then
        loaded_peer_values = loaded_state.peer_records
    elseif is_array(loaded_state.peers) then
        loaded_peer_values = loaded_state.peers
    elseif loaded_state.peer_records ~= nil or loaded_state.peers ~= nil then
        dirty = true
    end

    if loaded_peer_values then
        for _, value in ipairs(loaded_peer_values) do
            local peer_url = type(value) == "table" and value.url or value
            local normalized_peer = normalize_peer_url(self, peer_url)
            if normalized_peer and not seen_peers[normalized_peer] and #self.peer_records < self.max_peers then
                local record, build_err = self:build_peer_record(normalized_peer, type(value) == "table" and value or {
                    source = "manual"
                })
                if record and not build_err then
                    seen_peers[normalized_peer] = true
                    self.peer_records[#self.peer_records + 1] = record
                else
                    dirty = true
                end
            else
                dirty = true
            end
        end
    end
    self:refresh_peer_views()

    local normalized_chain, normalize_err = self:normalize_chain(loaded_state.chain)
    if not normalized_chain or #normalized_chain == 0 then
        error("Persisted chain is invalid: " .. tostring(normalize_err or "chain is empty"))
    end

    local valid_chain, validation_err = self:validate_chain(normalized_chain)
    if not valid_chain then
        error("Persisted chain failed validation: " .. tostring(validation_err))
    end

    self.chain = normalized_chain
    self.pending_transactions = {}
    if is_array(loaded_state.pending_transactions) then
        for _, transaction in ipairs(loaded_state.pending_transactions) do
            local normalized_transaction = self:normalize_transaction(transaction)
            if normalized_transaction and normalized_transaction.kind == "transfer" and not self:has_transaction(normalized_transaction.id) and #self.pending_transactions < self.max_pending_transactions then
                self.pending_transactions[#self.pending_transactions + 1] = normalized_transaction
            else
                dirty = true
            end
        end
    elseif loaded_state.pending_transactions ~= nil then
        dirty = true
    end

    local pending_count = #self.pending_transactions
    self:revalidate_pending_transactions()
    if #self.pending_transactions ~= pending_count then
        dirty = true
    end

    if dirty then
        local saved, save_err = self:save()
        if not saved then
            error("Unable to rewrite normalized blockchain state: " .. tostring(save_err))
        end
    end
end

function Blockchain:apply_transfer_to_state(state, transaction)
    if state.transaction_ids[transaction.id] then
        return false, "transaction already exists on chain or in the pending pool"
    end

    local verified, verify_err = crypto.verify_transaction_signature(transaction)
    if not verified then
        return false, verify_err
    end

    local expected_nonce = state.nonces[transaction.sender] or 1
    if transaction.nonce ~= expected_nonce then
        return false, "expected nonce " .. tostring(expected_nonce) .. " for " .. transaction.sender
    end

    local balance = round_currency(state.balances[transaction.sender] or 0) or 0
    local cost = transaction_cost(transaction)
    if balance + EPSILON < cost then
        return false, "insufficient balance for " .. transaction.sender
    end

    state.balances[transaction.sender] = round_currency(balance - cost) or 0
    state.balances[transaction.recipient] = round_currency((state.balances[transaction.recipient] or 0) + transaction.amount) or 0
    state.nonces[transaction.sender] = expected_nonce + 1
    state.transaction_ids[transaction.id] = true

    return true
end

function Blockchain:calculate_confirmed_state(chain)
    local state = {
        balances = {},
        nonces = {},
        transaction_ids = {}
    }

    local candidate_chain = chain or self.chain
    if type(candidate_chain) ~= "table" or #candidate_chain == 0 then
        return nil, "chain is empty"
    end

    for index = 2, #candidate_chain do
        local block = candidate_chain[index]
        local reward_seen = false
        local total_fees = 0

        for transaction_index, transaction in ipairs(block.transactions or {}) do
            if state.transaction_ids[transaction.id] then
                return nil, "duplicate transaction id " .. tostring(transaction.id)
            end

            if transaction.kind == "reward" then
                if reward_seen then
                    return nil, "block " .. tostring(index) .. " contains more than one reward transaction"
                end

                if transaction_index ~= #block.transactions then
                    return nil, "reward transaction must be the last transaction in block " .. tostring(index)
                end

                local expected_reward = round_currency(self.mining_reward + total_fees) or self.mining_reward
                if math.abs((tonumber(transaction.amount) or 0) - expected_reward) > EPSILON then
                    return nil, "reward amount mismatch in block " .. tostring(index)
                end

                state.balances[transaction.recipient] = round_currency((state.balances[transaction.recipient] or 0) + transaction.amount) or 0
                state.transaction_ids[transaction.id] = true
                reward_seen = true
            else
                local applied, apply_err = self:apply_transfer_to_state(state, transaction)
                if not applied then
                    return nil, "invalid transaction " .. tostring(transaction.id) .. ": " .. tostring(apply_err)
                end

                total_fees = round_currency(total_fees + transaction.fee) or total_fees
            end
        end

        if not reward_seen then
            return nil, "block " .. tostring(index) .. " does not contain a reward transaction"
        end
    end

    return state
end

function Blockchain:simulate_pending_transactions(confirmed_state, pending_transactions)
    local state = {
        balances = copy_map((confirmed_state or {}).balances),
        nonces = copy_map((confirmed_state or {}).nonces),
        transaction_ids = copy_map((confirmed_state or {}).transaction_ids)
    }

    local accepted = {}
    local rejected = {}

    for _, transaction in ipairs(pending_transactions or {}) do
        local applied, apply_err = self:apply_transfer_to_state(state, transaction)
        if applied then
            accepted[#accepted + 1] = deep_copy(transaction)
        else
            rejected[#rejected + 1] = {
                transaction = deep_copy(transaction),
                error = apply_err
            }
        end
    end

    return {
        accepted = accepted,
        rejected = rejected,
        state = state
    }
end

function Blockchain:validate_chain(chain)
    if type(chain) ~= "table" or #chain == 0 then
        return false, "Chain is empty"
    end

    local first_block = chain[1]
    if tonumber(first_block.index) ~= 1 then
        return false, "Genesis block index must be 1"
    end

    if tostring(first_block.previous_hash) ~= "0" then
        return false, "Genesis block previous_hash must be 0"
    end

    if first_block.difficulty_prefix ~= self.difficulty_prefix then
        return false, "Genesis block difficulty_prefix must match the configured network difficulty"
    end

    if self:calculate_hash(first_block) ~= first_block.hash then
        return false, "Genesis block hash is invalid"
    end

    if tonumber(first_block.work) ~= self:calculate_block_work(first_block.difficulty_prefix) then
        return false, "Genesis block work is invalid"
    end

    if tonumber(first_block.cumulative_work) ~= tonumber(first_block.work) then
        return false, "Genesis block cumulative work is invalid"
    end

    local validated_chain = { first_block }

    for index = 2, #chain do
        local previous_block = chain[index - 1]
        local block = chain[index]
        local expected_difficulty_prefix = self:get_next_difficulty_prefix(validated_chain)

        if tonumber(block.index) ~= index then
            return false, "Block index mismatch at block " .. tostring(index)
        end

        if block.hash_format ~= "legacy-v1" and block.hash_format ~= "v2" then
            return false, "hash_format is invalid at block " .. tostring(index)
        end

        if block.difficulty_prefix ~= expected_difficulty_prefix then
            return false, "difficulty_prefix mismatch at block " .. tostring(index)
        end

        if tostring(block.previous_hash) ~= tostring(previous_block.hash) then
            return false, "previous_hash mismatch at block " .. tostring(index)
        end

        local calculated_hash = self:calculate_hash(block)
        if calculated_hash ~= block.hash then
            return false, "Block hash mismatch at block " .. tostring(index)
        end

        if not self:is_valid_proof(previous_block.proof, block.proof, previous_block.hash, block.difficulty_prefix) then
            return false, "Proof of work is invalid at block " .. tostring(index)
        end

        local expected_work = self:calculate_block_work(block.difficulty_prefix)
        if tonumber(block.work) ~= expected_work then
            return false, "Work mismatch at block " .. tostring(index)
        end

        local expected_cumulative_work = (tonumber(previous_block.cumulative_work) or 0) + expected_work
        if tonumber(block.cumulative_work) ~= expected_cumulative_work then
            return false, "Cumulative work mismatch at block " .. tostring(index)
        end

        validated_chain[#validated_chain + 1] = block
    end

    local confirmed_state, state_err = self:calculate_confirmed_state(chain)
    if not confirmed_state then
        return false, state_err
    end

    return true, "Chain is valid"
end

function Blockchain:revalidate_pending_transactions()
    local confirmed_state = self:calculate_confirmed_state(self.chain)
    if not confirmed_state then
        self.pending_transactions = {}
        return {}
    end

    local result = self:simulate_pending_transactions(confirmed_state, self.pending_transactions)
    self.pending_transactions = result.accepted

    return result.rejected
end

function Blockchain:has_transaction(transaction_id)
    if not transaction_id or transaction_id == "" then
        return false
    end

    for _, block in ipairs(self.chain) do
        for _, transaction in ipairs(block.transactions or {}) do
            if transaction.id == transaction_id then
                return true
            end
        end
    end

    for _, transaction in ipairs(self.pending_transactions) do
        if transaction.id == transaction_id then
            return true
        end
    end

    return false
end

function Blockchain:add_transaction(input)
    local transaction, err = self:normalize_transaction(input)
    if not transaction then
        return nil, err
    end

    if transaction.kind ~= "transfer" then
        return nil, "only signed transfer transactions can be queued"
    end

    if self:has_transaction(transaction.id) then
        return nil, "transaction already exists"
    end

    if #self.pending_transactions >= self.max_pending_transactions then
        return nil, "pending transaction pool is full"
    end

    local rejected = self:revalidate_pending_transactions()
    local confirmed_state = self:calculate_confirmed_state(self.chain)
    if not confirmed_state then
        return nil, "local chain is invalid"
    end

    local pending_result = self:simulate_pending_transactions(confirmed_state, self.pending_transactions)
    local applied, apply_err = self:apply_transfer_to_state(pending_result.state, transaction)
    if not applied then
        if #rejected > 0 then
            self:save()
        end
        return nil, apply_err
    end

    self.pending_transactions[#self.pending_transactions + 1] = transaction
    table.sort(self.pending_transactions, function(left, right)
        if left.fee == right.fee then
            if left.timestamp == right.timestamp then
                return left.id < right.id
            end

            return left.timestamp < right.timestamp
        end

        return left.fee > right.fee
    end)
    self:save()

    return deep_copy(transaction), #self.chain + 1
end

function Blockchain:create_reward_transaction(recipient, total_fees)
    return self:normalize_transaction({
        amount = round_currency(self.mining_reward + (tonumber(total_fees) or 0)) or self.mining_reward,
        fee = 0,
        kind = "reward",
        nonce = 0,
        note = "Block reward for block " .. tostring(#self.chain + 1),
        recipient = recipient,
        sender = "NETWORK",
        timestamp = utc_now()
    })
end

function Blockchain:remove_pending_by_ids(transaction_ids)
    if not transaction_ids or not next(transaction_ids) then
        return
    end

    local remaining = {}
    for _, transaction in ipairs(self.pending_transactions) do
        if not transaction_ids[transaction.id] then
            remaining[#remaining + 1] = transaction
        end
    end
    self.pending_transactions = remaining
end

function Blockchain:mine_block(miner_address)
    local normalized_miner = crypto.normalize_address(miner_address)
    if not normalized_miner then
        return nil, "miner must be a valid address"
    end

    self:revalidate_pending_transactions()

    local previous_block = self:get_previous_block()
    local confirmed_state = self:calculate_confirmed_state(self.chain)
    if not confirmed_state then
        return nil, "local chain is invalid"
    end

    local working_state = {
        balances = copy_map(confirmed_state.balances),
        nonces = copy_map(confirmed_state.nonces),
        transaction_ids = copy_map(confirmed_state.transaction_ids)
    }

    local selected_transactions = {}
    local deferred_transactions = {}
    local total_fees = 0

    local max_transfer_count = math.max(self.max_transactions_per_block - 1, 0)
    for _, transaction in ipairs(self.pending_transactions) do
        if #selected_transactions >= max_transfer_count then
            deferred_transactions[#deferred_transactions + 1] = deep_copy(transaction)
        else
            local applied = self:apply_transfer_to_state(working_state, transaction)
            if applied then
                selected_transactions[#selected_transactions + 1] = deep_copy(transaction)
                total_fees = round_currency(total_fees + transaction.fee) or total_fees
            else
                deferred_transactions[#deferred_transactions + 1] = deep_copy(transaction)
            end
        end
    end

    local reward_transaction, reward_err = self:create_reward_transaction(normalized_miner, total_fees)
    if not reward_transaction then
        return nil, reward_err
    end

    selected_transactions[#selected_transactions + 1] = reward_transaction

    local difficulty_prefix = self:get_next_difficulty_prefix(self.chain)
    local proof = self:proof_of_work(previous_block.proof, previous_block.hash, difficulty_prefix)
    local block = {
        index = #self.chain + 1,
        timestamp = utc_now(),
        transactions = selected_transactions,
        proof = proof,
        previous_hash = previous_block.hash,
        mined_by = normalized_miner,
        difficulty_prefix = difficulty_prefix,
        hash_format = "v2"
    }
    block.hash = self:calculate_hash(block)
    block.work = self:calculate_block_work(block.difficulty_prefix)
    block.cumulative_work = (tonumber(previous_block.cumulative_work) or 0) + block.work

    self.chain[#self.chain + 1] = block
    self.pending_transactions = deferred_transactions
    self:revalidate_pending_transactions()
    self:save()

    return deep_copy(block)
end

function Blockchain:register_peer(peer)
    local _, err = self:upsert_peer_record(peer, {
        source = "manual",
        discovered_at = utc_now()
    })
    if err then
        return nil, err
    end

    self:save()

    return self:get_peers()
end

function Blockchain:replace_chain(candidate_chain)
    local normalized_chain, err = self:normalize_chain(candidate_chain)
    if not normalized_chain then
        return false, err
    end

    local valid, reason = self:validate_chain(normalized_chain)
    if not valid then
        return false, reason
    end

    local preferred, preference_reason = self:compare_chain_priority(normalized_chain, self.chain)
    if not preferred then
        return false, "Candidate chain is not preferred: " .. tostring(preference_reason)
    end

    self.chain = normalized_chain
    self:revalidate_pending_transactions()
    self:save()

    return true, "Local chain replaced with a higher-work valid chain"
end

function Blockchain:append_block(candidate_block)
    local previous_block = self:get_previous_block()
    local normalized_block, err = self:normalize_block(candidate_block, previous_block.hash, #self.chain + 1, previous_block.cumulative_work)
    if not normalized_block then
        return false, err
    end

    if normalized_block.index <= #self.chain then
        local existing = self.chain[normalized_block.index]
        if existing and existing.hash == normalized_block.hash then
            return true, "Block already present locally"
        end
        return false, "Block height conflicts with the local chain"
    end

    if normalized_block.index ~= #self.chain + 1 then
        return false, "Block does not immediately extend the local chain"
    end

    if normalized_block.previous_hash ~= previous_block.hash then
        return false, "Block previous_hash does not match the local tip"
    end

    local candidate_chain = deep_copy(self.chain)
    candidate_chain[#candidate_chain + 1] = normalized_block
    local valid, reason = self:validate_chain(candidate_chain)
    if not valid then
        return false, reason
    end

    local committed_ids = {}
    for _, transaction in ipairs(normalized_block.transactions or {}) do
        committed_ids[transaction.id] = true
    end

    self.chain = candidate_chain
    self:remove_pending_by_ids(committed_ids)
    self:revalidate_pending_transactions()
    self:save()

    return true, "Block appended"
end

function Blockchain:resolve_conflicts(fetch_chain_callback)
    local best_chain = nil
    local best_source = nil
    local inspected = {}

    for _, peer in ipairs(self.peers) do
        local payload, err = fetch_chain_callback(peer)
        if not payload then
            inspected[#inspected + 1] = { peer = peer, ok = false, error = err }
        else
            local candidate_chain = payload.chain or payload
            local normalized_chain, normalize_err = self:normalize_chain(candidate_chain)
            if not normalized_chain then
                inspected[#inspected + 1] = { peer = peer, ok = false, error = normalize_err }
            else
                local valid, reason = self:validate_chain(normalized_chain)
                local error_message = nil
                if not valid then
                    error_message = reason
                end
                inspected[#inspected + 1] = {
                    peer = peer,
                    ok = valid,
                    error = error_message,
                    blocks = #normalized_chain,
                    cumulative_work = self:get_chain_work(normalized_chain)
                }
                local preferred = valid and self:compare_chain_priority(normalized_chain, best_chain or self.chain)
                if preferred then
                    best_chain = normalized_chain
                    best_source = peer
                end
            end
        end
    end

    if best_chain then
        self.chain = best_chain
        self:revalidate_pending_transactions()
        self:save()
        return true, {
            source_peer = best_source,
            blocks = #best_chain,
            cumulative_work = self:get_chain_work(best_chain),
            inspected = inspected
        }
    end

    return false, {
        source_peer = nil,
        blocks = #self.chain,
        cumulative_work = self:get_chain_work(self.chain),
        inspected = inspected
    }
end

return Blockchain

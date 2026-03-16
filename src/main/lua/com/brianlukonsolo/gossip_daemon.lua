package.path = "/app/?.lua;/app/?/init.lua;" .. package.path
package.cpath = "/usr/lib/x86_64-linux-gnu/lua/5.1/?.so;" .. package.cpath

local socket = require("socket")
local cjson = require("classes.json")
local canonical_json = require("classes.canonical_json")
local config = require("classes.config")
local requests = require("classes.requests")
local sha = require("cryptography.pure_lua_SHA.sha2")

local APP_VERSION = "9.0.0"
local CONFIG = config.load(APP_VERSION)
local LOCAL_NODE_URL = "http://127.0.0.1:8080"

math.randomseed(os.time() + tonumber(tostring({}):match("0x(.*)"), 16))

local function trim(value)
    return tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

local function local_headers()
    local headers = {
        ["Content-Type"] = "application/json"
    }

    if trim(CONFIG.peer_shared_secret) ~= "" then
        headers["X-Blockchain-Peer-Secret"] = CONFIG.peer_shared_secret
    end

    return headers
end

local function local_get_json(path)
    local response = requests.send_get_request(
        LOCAL_NODE_URL .. path,
        local_headers(),
        { timeout_seconds = 2 }
    )
    if not response.ok then
        return nil, response.status_text or ("HTTP " .. tostring(response.status_code))
    end

    local payload, err = cjson.decode(response.body)
    if not payload then
        return nil, "Local node returned invalid JSON: " .. tostring(err)
    end

    return payload
end

local function local_post_json(path, payload)
    local response = requests.send_post_request(
        LOCAL_NODE_URL .. path,
        canonical_json.encode(payload),
        local_headers(),
        { timeout_seconds = 2 }
    )
    if not response.ok then
        return nil, response.status_text or ("HTTP " .. tostring(response.status_code))
    end

    local decoded, err = cjson.decode(response.body)
    if not decoded then
        return nil, "Local node returned invalid JSON: " .. tostring(err)
    end

    return decoded
end

local function parse_endpoint(endpoint)
    local host, port = trim(endpoint):match("^([^:]+):(%d+)$")
    if not host then
        return nil
    end

    port = tonumber(port)
    if not port or port < 1 or port > 65535 then
        return nil
    end

    return host, port
end

local function normalize_endpoint(endpoint)
    local host, port = parse_endpoint(endpoint)
    if not host then
        return nil
    end

    return host .. ":" .. tostring(port)
end

local function resolve_udp_host(host)
    host = trim(host)
    if host == "" then
        return nil
    end

    if host:match("^%d+%.%d+%.%d+%.%d+$") then
        return host
    end

    local ip = socket.dns.toip(host)
    if type(ip) == "string" and trim(ip) ~= "" then
        return trim(ip)
    end

    return nil
end

local function cleanup_seen_messages(seen_messages, now)
    for message_id, expires_at in pairs(seen_messages) do
        if expires_at <= now then
            seen_messages[message_id] = nil
        end
    end
end

local function endpoint_from_peer_record(record)
    local capabilities = type(record) == "table" and type(record.capabilities) == "table" and record.capabilities or {}
    local gossip = type(capabilities.gossip_transport) == "table" and capabilities.gossip_transport or {}
    return normalize_endpoint(gossip.endpoint)
end

local function collect_targets(directory_payload)
    local targets = {}
    local seen = {}

    local function append_target(endpoint)
        local normalized = normalize_endpoint(endpoint)
        if normalized and normalized ~= CONFIG.gossip_endpoint and not seen[normalized] then
            seen[normalized] = true
            targets[#targets + 1] = normalized
        end
    end

    for _, seed in ipairs(CONFIG.gossip_seeds or {}) do
        append_target(seed)
    end

    local directory = directory_payload
    if not directory then
        directory = local_get_json("/api/network/peers?limit=" .. tostring(CONFIG.peer_advertised_limit))
    end

    if type(directory) == "table" then
        for _, record in ipairs(directory.peer_records or {}) do
            append_target(endpoint_from_peer_record(record))
        end
    end

    return targets, directory
end

local function choose_targets(all_targets)
    local selected = {}
    for _, target in ipairs(all_targets or {}) do
        selected[#selected + 1] = target
    end

    for index = #selected, 2, -1 do
        local swap_index = math.random(index)
        selected[index], selected[swap_index] = selected[swap_index], selected[index]
    end

    while #selected > CONFIG.gossip_fanout do
        table.remove(selected)
    end

    return selected
end

local function build_announce(directory)
    if type(directory) ~= "table" or type(directory.locator) ~= "table" then
        return nil
    end

    local timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
    local tip_hash = tostring(directory.locator.tip_hash or "")
    local message_id = sha.sha256(table.concat({
        CONFIG.node_id,
        CONFIG.node_url,
        tostring(CONFIG.gossip_endpoint or ""),
        tip_hash,
        timestamp,
        tostring(math.random(100000, 999999))
    }, "|"))

    return {
        type = "gossip_announce",
        message_id = message_id,
        timestamp = timestamp,
        chain_id = CONFIG.chain_id,
        node_id = CONFIG.node_id,
        version = APP_VERSION,
        source_peer = CONFIG.node_url,
        source_gossip_endpoint = CONFIG.gossip_endpoint,
        hops = 0,
        max_hops = CONFIG.gossip_max_hops,
        inventory = {
            locator = directory.locator.hashes or {},
            height = directory.locator.height,
            cumulative_work = directory.locator.cumulative_work,
            tip_hash = directory.locator.tip_hash
        },
        peers = directory.peer_records or {}
    }
end

local function send_announce(udp, target_endpoint, payload)
    local host, port = parse_endpoint(target_endpoint)
    if not host then
        return false
    end

    local resolved_host = resolve_udp_host(host)
    if not resolved_host then
        io.stderr:write("gossip send skipped for unresolved host: " .. tostring(host) .. "\n")
        return false
    end

    local encoded = canonical_json.encode(payload)
    local ok = udp:sendto(encoded, resolved_host, port)
    return ok ~= nil
end

local function forward_message(udp, payload, sender_endpoint)
    local hops = (tonumber(payload.hops) or 0) + 1
    local max_hops = math.min(tonumber(payload.max_hops) or CONFIG.gossip_max_hops, CONFIG.gossip_max_hops)
    if hops > max_hops then
        return
    end

    local forwarded = {}
    for key, value in pairs(payload) do
        forwarded[key] = value
    end
    forwarded.hops = hops

    local targets = choose_targets(select(1, collect_targets()))
    for _, target in ipairs(targets) do
        if target ~= sender_endpoint then
            send_announce(udp, target, forwarded)
        end
    end
end

local function process_incoming_message(udp, seen_messages, payload, ip, port)
    if type(payload) ~= "table" then
        return
    end

    if trim(payload.chain_id) ~= CONFIG.chain_id then
        return
    end

    local message_id = trim(payload.message_id)
    if message_id == "" or seen_messages[message_id] then
        return
    end

    seen_messages[message_id] = socket.gettime() + CONFIG.gossip_message_ttl_seconds

    local sender_endpoint = normalize_endpoint(payload.source_gossip_endpoint) or normalize_endpoint(ip .. ":" .. tostring(port))
    if sender_endpoint then
        payload.source_gossip_endpoint = sender_endpoint
    end

    local ok, err = local_post_json("/api/network/gossip/announce", payload)
    if not ok and err then
        io.stderr:write("gossip ingest failed: " .. tostring(err) .. "\n")
    end

    forward_message(udp, payload, sender_endpoint)
end

if not CONFIG.gossip_enabled then
    os.exit(0)
end

if not CONFIG.is_valid then
    io.stderr:write("gossip daemon exiting because configuration is invalid\n")
    os.exit(1)
end

local udp = assert(socket.udp())
assert(udp:setsockname(CONFIG.gossip_bind_host, CONFIG.gossip_port))
assert(udp:settimeout(0))

local seen_messages = {}
local next_broadcast_at = 0

while true do
    local now = socket.gettime()
    cleanup_seen_messages(seen_messages, now)

    local packet, ip, port = udp:receivefrom()
    while packet do
        local ok, payload = pcall(cjson.decode, packet)
        if ok then
            process_incoming_message(udp, seen_messages, payload, ip, port)
        end
        packet, ip, port = udp:receivefrom()
    end

    if now >= next_broadcast_at then
        local targets, directory = collect_targets()
        local announce = build_announce(directory)
        if announce then
            seen_messages[announce.message_id] = now + CONFIG.gossip_message_ttl_seconds
            for _, target in ipairs(choose_targets(targets)) do
                send_announce(udp, target, announce)
            end
        end

        next_broadcast_at = now + CONFIG.gossip_interval_seconds
    end

    socket.sleep(0.2)
end

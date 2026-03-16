package.path = "/app/?.lua;/app/?/init.lua;" .. package.path
package.cpath = "/usr/lib/x86_64-linux-gnu/lua/5.1/?.so;" .. package.cpath

local socket = require("socket")
local ssl = require("ssl")
local cjson = require("classes.json")
local canonical_json = require("classes.canonical_json")
local config = require("classes.config")
local node_identity = require("classes.node_identity")
local requests = require("classes.requests")
local tls_identity = require("classes.tls_identity")

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
        { timeout_seconds = math.max(tonumber(CONFIG.peer_timeout_seconds) or 5, 2) }
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
        { timeout_seconds = math.max(tonumber(CONFIG.peer_timeout_seconds) or 5, 2) }
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

local function socket_peer_address(client)
    local ok, peer = pcall(function()
        return { client:getpeername() }
    end)
    if not ok or type(peer) ~= "table" then
        return nil
    end

    local address = trim(peer[1])
    if address == "" then
        return nil
    end

    return address
end

local function resolve_host(host)
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

local function endpoint_from_peer_record(record)
    local capabilities = type(record) == "table" and type(record.capabilities) == "table" and record.capabilities or {}
    local p2p_transport = type(capabilities.p2p_transport) == "table" and capabilities.p2p_transport or {}
    return normalize_endpoint(p2p_transport.endpoint)
end

local function build_tls_parameters(mode)
    return {
        mode = mode,
        protocol = "tlsv1_2",
        key = CONFIG.p2p_tls_key_path,
        certificate = CONFIG.p2p_tls_cert_path,
        verify = "none",
        options = "all"
    }
end

local function peer_certificate_fingerprint(client)
    local ok, peer_cert = pcall(function()
        return client:getpeercertificate()
    end)
    if not ok or not peer_cert then
        return nil
    end

    local digest_ok, digest = pcall(function()
        return peer_cert:digest("sha256")
    end)
    if not digest_ok or not digest then
        return nil
    end

    local normalized = trim(digest):lower()
    if normalized == "" then
        return nil
    end

    return normalized
end

local function wrap_tls_socket(client, mode, server_name)
    if not CONFIG.p2p_tls_enabled then
        return client, nil
    end

    local wrapped, wrap_err = ssl.wrap(client, build_tls_parameters(mode))
    if not wrapped then
        pcall(function()
            client:close()
        end)
        return nil, wrap_err
    end

    wrapped:settimeout(math.max(tonumber(CONFIG.peer_timeout_seconds) or 5, 2))

    if mode == "client" and trim(server_name) ~= "" then
        pcall(function()
            wrapped:sni(server_name)
        end)
    end

    local ok, handshake_err = wrapped:dohandshake()
    if not ok then
        pcall(function()
            wrapped:close()
        end)
        return nil, handshake_err
    end

    local fingerprint = peer_certificate_fingerprint(wrapped)
    if mode == "client" and not fingerprint then
        pcall(function()
            wrapped:close()
        end)
        return nil, "peer did not present a TLS certificate"
    end

    return wrapped, fingerprint
end

local function collect_targets(directory_payload)
    local targets = {}
    local seen = {}

    local function append_target(endpoint)
        local normalized = normalize_endpoint(endpoint)
        if normalized and normalized ~= CONFIG.p2p_endpoint and not seen[normalized] then
            seen[normalized] = true
            targets[#targets + 1] = normalized
        end
    end

    for _, seed in ipairs(CONFIG.p2p_seeds or {}) do
        append_target(seed)
    end

    if CONFIG.p2p_dial_discovered_peers and type(directory_payload) == "table" then
        for _, record in ipairs(directory_payload.peer_records or {}) do
            append_target(endpoint_from_peer_record(record))
        end
    end

    return targets
end

local function send_message(client, message)
    local encoded = canonical_json.encode(message)
    if #encoded > CONFIG.p2p_max_message_bytes then
        return nil, "message exceeds BLOCKCHAIN_P2P_MAX_MESSAGE_BYTES"
    end

    local ok, err = client:send(encoded .. "\n")
    if not ok then
        return nil, err
    end

    return true
end

local function receive_message(client)
    local line, err, partial = client:receive("*l")
    local data = line or partial

    if not data or data == "" then
        return nil, err
    end

    if #data > CONFIG.p2p_max_message_bytes then
        return nil, "message exceeds BLOCKCHAIN_P2P_MAX_MESSAGE_BYTES"
    end

    local message, decode_err = cjson.decode(data)
    if not message then
        return nil, "received invalid JSON: " .. tostring(decode_err)
    end

    return message
end

local function build_local_directory()
    return local_get_json("/api/network/peers?limit=" .. tostring(CONFIG.peer_advertised_limit))
end

local function build_hello_payload(identity, directory_payload, tls_id)
    local locator = type(directory_payload) == "table" and type(directory_payload.locator) == "table" and directory_payload.locator or {}
    local peer_records = type(directory_payload) == "table" and type(directory_payload.peer_records) == "table" and directory_payload.peer_records or {}

    return {
        chain_id = CONFIG.chain_id,
        version = APP_VERSION,
        node_id = CONFIG.node_id,
        node_url = CONFIG.node_url,
        peer_id = identity.peer_id,
        public_key = identity.public_key,
        p2p_endpoint = CONFIG.p2p_endpoint,
        gossip_endpoint = CONFIG.gossip_endpoint,
        capabilities = {
            peer_discovery = true,
            headers_first_sync = true,
            block_fetch_by_hash = true,
            transaction_relay = true,
            transport_security = {
                tls = CONFIG.p2p_tls_enabled == true,
                tls_cert_fingerprint = tls_id and tls_id.fingerprint or nil
            }
        },
        locator = {
            hashes = locator.hashes or {},
            height = locator.height or 0,
            cumulative_work = locator.cumulative_work or 0,
            tip_hash = locator.tip_hash or ""
        },
        peers = peer_records,
        timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        nonce = tostring(os.time()) .. "-" .. tostring(math.random(100000, 999999))
    }
end

local function build_hello_message(identity, directory_payload, tls_id)
    return node_identity.build_signed_envelope(identity, "hello", build_hello_payload(identity, directory_payload, tls_id))
end

local function verify_hello_message(message, live_peer_fingerprint)
    if type(message) ~= "table" or message.type ~= "hello" then
        return nil, "expected a hello message"
    end

    if type(message.payload) ~= "table" then
        return nil, "hello payload is required"
    end

    if trim(message.payload.chain_id) ~= CONFIG.chain_id then
        return nil, "peer chain_id does not match the local chain"
    end

    if trim(message.payload.node_url) == "" then
        return nil, "peer node_url is required"
    end

    if not trim(message.payload.node_url):match("^https?://") then
        return nil, "peer node_url must be an http(s) URL"
    end

    local verified, verify_err = node_identity.verify_signed_payload(message.payload, message.signature)
    if not verified then
        return nil, verify_err
    end

    local capabilities = type(message.payload.capabilities) == "table" and message.payload.capabilities or {}
    local transport_security = type(capabilities.transport_security) == "table" and capabilities.transport_security or {}
    local advertised_tls = transport_security.tls == true
    local advertised_fingerprint = trim(transport_security.tls_cert_fingerprint):lower()

    if CONFIG.p2p_tls_enabled then
        if not advertised_tls then
            return nil, "peer hello must advertise TLS when native P2P TLS is enabled"
        end

        if advertised_fingerprint == "" then
            return nil, "peer hello is missing a TLS certificate fingerprint"
        end

        if live_peer_fingerprint and advertised_fingerprint ~= live_peer_fingerprint then
            return nil, "peer TLS certificate fingerprint does not match the signed hello payload"
        end
    end

    return message.payload
end

local function notify_peer_success(payload, observation)
    observation = type(observation) == "table" and observation or {}
    return local_post_json("/api/network/p2p/peer-update", {
        source_peer = payload.node_url,
        node_id = payload.node_id,
        node_url = payload.node_url,
        version = payload.version,
        chain_id = payload.chain_id,
        peer_id = payload.peer_id,
        public_key = payload.public_key,
        p2p_endpoint = payload.p2p_endpoint,
        gossip_endpoint = payload.gossip_endpoint,
        last_advertised_height = payload.locator and payload.locator.height or 0,
        last_cumulative_work = payload.locator and payload.locator.cumulative_work or 0,
        peers = payload.peers,
        capabilities = payload.capabilities,
        remote_ip = observation.remote_ip,
        tls_enabled = observation.tls_enabled == true,
        tls_cert_fingerprint = observation.tls_cert_fingerprint
    })
end

local function notify_peer_failure(peer_payload, error_message)
    if type(peer_payload) ~= "table" or trim(peer_payload.node_url) == "" then
        return
    end

    local_post_json("/api/network/p2p/peer-failure", {
        source_peer = peer_payload.node_url,
        node_id = peer_payload.node_id,
        node_url = peer_payload.node_url,
        version = peer_payload.version,
        chain_id = peer_payload.chain_id,
        error = error_message
    })
end

local function respond_with_headers(client, payload)
    local locator = type(payload) == "table" and type(payload.locator) == "table" and payload.locator or {}
    local limit = math.max(1, math.min(tonumber(payload and payload.limit) or 64, 128))
    local query = "/api/headers?limit=" .. tostring(limit)

    if #(locator or {}) > 0 then
        query = query .. "&locator=" .. table.concat(locator, ",")
    end

    local response_payload, err = local_get_json(query)
    if not response_payload then
        return nil, err
    end

    return send_message(client, {
        type = "headers",
        payload = {
            headers = response_payload.headers or {}
        }
    })
end

local function respond_with_block(client, payload)
    local block_hash = trim(type(payload) == "table" and payload.hash or "")
    if block_hash == "" then
        return nil, "block hash is required"
    end

    local response_payload, err = local_get_json("/api/blocks/hash/" .. block_hash)
    if not response_payload then
        return nil, err
    end

    return send_message(client, {
        type = "block",
        payload = {
            block = response_payload.block
        }
    })
end

local function relay_transaction(payload)
    if type(payload) ~= "table" or type(payload.transaction) ~= "table" then
        return nil, "transaction payload is required"
    end

    return local_post_json("/api/network/transactions", {
        source_peer = payload.source_peer or payload.node_url,
        transaction = payload.transaction
    })
end

local function relay_block(payload)
    if type(payload) ~= "table" or type(payload.block) ~= "table" then
        return nil, "block payload is required"
    end

    return local_post_json("/api/network/blocks", {
        source_peer = payload.source_peer or payload.node_url,
        block = payload.block
    })
end

local function request_headers(client, locator_hashes)
    local sent, send_err = send_message(client, {
        type = "get_headers",
        payload = {
            locator = locator_hashes or {},
            limit = 128
        }
    })
    if not sent then
        return nil, send_err
    end

    while true do
        local message, err = receive_message(client)
        if not message then
            return nil, err
        end

        if message.type == "headers" then
            return type(message.payload) == "table" and message.payload.headers or {}
        elseif message.type == "tx" then
            relay_transaction(message.payload)
        end
    end
end

local function request_block(client, block_hash)
    local sent, send_err = send_message(client, {
        type = "get_block",
        payload = {
            hash = block_hash
        }
    })
    if not sent then
        return nil, send_err
    end

    while true do
        local message, err = receive_message(client)
        if not message then
            return nil, err
        end

        if message.type == "block" then
            return type(message.payload) == "table" and message.payload.block or nil
        elseif message.type == "tx" then
            relay_transaction(message.payload)
        end
    end
end

local function sync_from_peer_connection(client, peer_payload)
    local local_locator_payload, locator_err = local_get_json("/api/locator")
    if not local_locator_payload then
        return nil, locator_err
    end

    local local_locator = local_locator_payload.locator or {}
    local peer_locator = type(peer_payload.locator) == "table" and peer_payload.locator or {}
    local peer_work = tonumber(peer_locator.cumulative_work) or 0
    local local_work = tonumber(local_locator.cumulative_work) or 0

    if peer_work <= local_work then
        return true
    end

    local headers, headers_err = request_headers(client, local_locator.hashes or {})
    if not headers then
        return nil, headers_err
    end

    for _, header in ipairs(headers or {}) do
        local block, block_err = request_block(client, header.hash)
        if not block then
            return nil, block_err
        end

        local imported, import_err = local_post_json("/api/network/blocks", {
            source_peer = peer_payload.node_url,
            block = block
        })
        if not imported then
            return nil, import_err
        end
    end

    return true
end

local function push_missing_blocks_to_peer(client, peer_payload)
    local local_locator_payload, locator_err = local_get_json("/api/locator")
    if not local_locator_payload then
        return nil, locator_err
    end

    local local_locator = local_locator_payload.locator or {}
    local peer_locator = type(peer_payload.locator) == "table" and peer_payload.locator or {}
    local local_work = tonumber(local_locator.cumulative_work) or 0
    local peer_work = tonumber(peer_locator.cumulative_work) or 0

    if local_work <= peer_work then
        return true
    end

    local query = "/api/headers?limit=128"
    if type(peer_locator.hashes) == "table" and #peer_locator.hashes > 0 then
        query = query .. "&locator=" .. table.concat(peer_locator.hashes, ",")
    else
        query = query .. "&from_height=1"
    end

    local headers_payload, headers_err = local_get_json(query)
    if not headers_payload then
        return nil, headers_err
    end

    for _, header in ipairs(headers_payload.headers or {}) do
        local block_payload, block_err = local_get_json("/api/blocks/hash/" .. tostring(header.hash))
        if not block_payload then
            return nil, block_err
        end

        local sent, send_err = send_message(client, {
            type = "block",
            payload = {
                source_peer = CONFIG.node_url,
                block = block_payload.block
            }
        })
        if not sent then
            return nil, send_err
        end
    end

    return true
end

local function send_pending_transactions(client, pending_payload)
    local pending_transactions = type(pending_payload) == "table" and pending_payload.pending_transactions or {}
    local sent_count = 0

    for _, transaction in ipairs(pending_transactions or {}) do
        local sent, send_err = send_message(client, {
            type = "tx",
            payload = {
                source_peer = CONFIG.node_url,
                transaction = transaction
            }
        })
        if not sent then
            return nil, send_err
        end

        sent_count = sent_count + 1
        if sent_count >= 32 then
            break
        end
    end

    return true
end

local function handle_inbound_connection(client, identity, tls_id)
    local remote_ip = socket_peer_address(client)
    local live_peer_fingerprint = nil

    client, live_peer_fingerprint = wrap_tls_socket(client, "server")
    if not client then
        return nil, live_peer_fingerprint
    end

    client:settimeout(math.max(tonumber(CONFIG.peer_timeout_seconds) or 5, 2))

    local hello_message, hello_err = receive_message(client)
    if not hello_message then
        return nil, hello_err
    end

    local peer_payload, verify_err = verify_hello_message(hello_message, live_peer_fingerprint)
    if not peer_payload then
        return nil, verify_err
    end

    local recorded, record_err = notify_peer_success(peer_payload, {
        remote_ip = remote_ip,
        tls_enabled = CONFIG.p2p_tls_enabled == true,
        tls_cert_fingerprint = live_peer_fingerprint
    })
    if not recorded then
        return nil, record_err
    end

    local local_directory, directory_err = build_local_directory()
    if not local_directory then
        return nil, directory_err
    end

    local local_hello, hello_build_err = build_hello_message(identity, local_directory, tls_id)
    if not local_hello then
        return nil, hello_build_err
    end

    local sent, send_err = send_message(client, local_hello)
    if not sent then
        return nil, send_err
    end

    client:settimeout(1)

    while true do
        local message, err = receive_message(client)
        if not message then
            if err ~= "timeout" and err ~= "closed" then
                notify_peer_failure(peer_payload, err)
            end
            return true
        end

        if message.type == "get_headers" then
            local ok, headers_err = respond_with_headers(client, message.payload)
            if not ok then
                notify_peer_failure(peer_payload, headers_err)
                return nil, headers_err
            end
        elseif message.type == "get_block" then
            local ok, block_err = respond_with_block(client, message.payload)
            if not ok then
                notify_peer_failure(peer_payload, block_err)
                return nil, block_err
            end
        elseif message.type == "block" then
            local relayed, relay_err = relay_block(message.payload)
            if not relayed then
                notify_peer_failure(peer_payload, relay_err)
                return nil, relay_err
            end
        elseif message.type == "tx" then
            local relayed, relay_err = relay_transaction(message.payload)
            if not relayed then
                notify_peer_failure(peer_payload, relay_err)
                return nil, relay_err
            end
        end
    end
end

local function open_peer_connection(endpoint)
    local host, port = parse_endpoint(endpoint)
    if not host then
        return nil, "invalid p2p endpoint"
    end

    local ip = resolve_host(host)
    if not ip then
        return nil, "unable to resolve p2p host"
    end

    local client = assert(socket.tcp())
    client:settimeout(math.max(tonumber(CONFIG.peer_timeout_seconds) or 5, 2))

    local ok, err = client:connect(ip, port)
    if not ok then
        client:close()
        return nil, err
    end

    return client, nil, {
        remote_ip = ip,
        server_name = host
    }
end

local function handle_outbound_target(endpoint, identity, tls_id, local_directory, pending_payload)
    local client, connect_err, connection = open_peer_connection(endpoint)
    if not client then
        return nil, connect_err
    end

    local live_peer_fingerprint = nil
    client, live_peer_fingerprint = wrap_tls_socket(client, "client", connection and connection.server_name or nil)
    if not client then
        return nil, live_peer_fingerprint
    end

    local hello_message, hello_build_err = build_hello_message(identity, local_directory, tls_id)
    if not hello_message then
        client:close()
        return nil, hello_build_err
    end

    local sent, send_err = send_message(client, hello_message)
    if not sent then
        client:close()
        return nil, send_err
    end

    local response_message, response_err = receive_message(client)
    if not response_message then
        client:close()
        return nil, response_err
    end

    local peer_payload, verify_err = verify_hello_message(response_message, live_peer_fingerprint)
    if not peer_payload then
        client:close()
        return nil, verify_err
    end

    local recorded, record_err = notify_peer_success(peer_payload, {
        remote_ip = connection and connection.remote_ip or nil,
        tls_enabled = CONFIG.p2p_tls_enabled == true,
        tls_cert_fingerprint = live_peer_fingerprint
    })
    if not recorded then
        client:close()
        return nil, record_err
    end

    local synced, sync_err = sync_from_peer_connection(client, peer_payload)
    if not synced then
        notify_peer_failure(peer_payload, sync_err)
        client:close()
        return nil, sync_err
    end

    local pushed, push_err = push_missing_blocks_to_peer(client, peer_payload)
    if not pushed then
        notify_peer_failure(peer_payload, push_err)
        client:close()
        return nil, push_err
    end

    local relayed, relay_err = send_pending_transactions(client, pending_payload)
    if not relayed then
        notify_peer_failure(peer_payload, relay_err)
        client:close()
        return nil, relay_err
    end

    client:close()
    return true
end

if not CONFIG.p2p_enabled then
    os.exit(0)
end

if not CONFIG.is_valid then
    io.stderr:write("p2p daemon exiting because configuration is invalid\n")
    os.exit(1)
end

local identity, identity_err = node_identity.load_or_create(CONFIG)
if not identity then
    io.stderr:write("p2p daemon exiting because node identity could not be initialized: " .. tostring(identity_err) .. "\n")
    os.exit(1)
end

local tls_id = nil
if CONFIG.p2p_tls_enabled then
    tls_id, identity_err = tls_identity.load_or_create(CONFIG, identity.peer_id)
    if not tls_id then
        io.stderr:write("p2p daemon exiting because the TLS identity could not be initialized: " .. tostring(identity_err) .. "\n")
        os.exit(1)
    end
end

local server = assert(socket.bind(CONFIG.p2p_bind_host, CONFIG.p2p_port))
assert(server:settimeout(0))

local next_connect_at = 0

while true do
    local client = server:accept()
    while client do
        local ok, result, err = pcall(handle_inbound_connection, client, identity, tls_id)
        if not ok then
            io.stderr:write("p2p inbound handler failed: " .. tostring(result) .. "\n")
        elseif not result and err then
            io.stderr:write("p2p inbound connection error: " .. tostring(err) .. "\n")
        end

        pcall(function()
            client:close()
        end)
        client = server:accept()
    end

    local now = socket.gettime()
    if now >= next_connect_at then
        local local_directory = build_local_directory()
        local pending_payload = local_get_json("/api/transactions/pending")

        if type(local_directory) == "table" then
            for _, target in ipairs(collect_targets(local_directory)) do
                local ok, err = handle_outbound_target(target, identity, tls_id, local_directory, pending_payload or {})
                if not ok and err then
                    io.stderr:write("p2p outbound connection failed for " .. tostring(target) .. ": " .. tostring(err) .. "\n")
                end
            end
        end

        next_connect_at = now + CONFIG.p2p_connect_interval_seconds
    end

    socket.sleep(CONFIG.p2p_poll_interval_seconds)
end

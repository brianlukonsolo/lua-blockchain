local cjson = require("classes.json")
local canonical_json = require("classes.canonical_json")
local requests = require("classes.requests")

local network = {}

local function trim(value)
    return tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

function network.parse_hash_list(raw_value)
    local hashes = {}
    local seen = {}

    local function append_hash(value)
        local normalized = trim(value):lower()
        if normalized ~= "" and normalized:match("^[0-9a-f]+$") and not seen[normalized] then
            seen[normalized] = true
            hashes[#hashes + 1] = normalized
        end
    end

    local function consume(value)
        if type(value) == "table" then
            for _, item in ipairs(value) do
                consume(item)
            end
            return
        end

        if value == nil then
            return
        end

        for item in tostring(value):gmatch("([^,]+)") do
            append_hash(item)
        end
    end

    consume(raw_value)
    return hashes
end

local function parse_peer_list(raw_value)
    local peers = {}
    local seen = {}

    local function append_peer(value)
        local normalized = trim(value):gsub("/+$", "")
        if normalized ~= "" and not seen[normalized] then
            seen[normalized] = true
            peers[#peers + 1] = normalized
        end
    end

    if type(raw_value) == "table" then
        for _, value in ipairs(raw_value) do
            append_peer(value)
        end
    else
        append_peer(raw_value)
    end

    return peers
end

local function parse_peer_directory(raw_value)
    local peers = {}
    local seen = {}

    local function append_entry(value)
        local entry = nil
        if type(value) == "table" then
            local url = trim(value.url):gsub("/+$", "")
            if url == "" or seen[url] then
                return
            end
            entry = {
                url = url,
                node_id = trim(value.node_id),
                node_url = trim(value.node_url),
                version = trim(value.version),
                chain_id = trim(value.chain_id),
                capabilities = value.capabilities,
                last_advertised_height = tonumber(value.last_advertised_height) or 0,
                last_cumulative_work = tonumber(value.last_cumulative_work) or 0
            }
            seen[url] = true
        else
            local normalized = trim(value):gsub("/+$", "")
            if normalized == "" or seen[normalized] then
                return
            end
            entry = { url = normalized }
            seen[normalized] = true
        end

        peers[#peers + 1] = entry
    end

    if type(raw_value) == "table" then
        for _, value in ipairs(raw_value) do
            append_entry(value)
        end
    else
        append_entry(raw_value)
    end

    return peers
end

local function peer_headers(config)
    local headers = {
        ["Content-Type"] = "application/json"
    }

    if trim(config.peer_shared_secret) ~= "" then
        headers["X-Blockchain-Peer-Secret"] = config.peer_shared_secret
    end

    return headers
end

local function peer_meta_is_compatible(config, meta)
    local peer_meta = type(meta) == "table" and meta or {}

    if peer_meta.chain_id and peer_meta.chain_id ~= config.chain_id then
        return false, "peer chain_id does not match local chain_id"
    end

    if peer_meta.difficulty_prefix and peer_meta.difficulty_prefix ~= config.difficulty_prefix then
        return false, "peer difficulty policy does not match the local network"
    end

    if tonumber(peer_meta.target_block_seconds) and tonumber(peer_meta.target_block_seconds) ~= config.target_block_seconds then
        return false, "peer target block time does not match the local network"
    end

    if tonumber(peer_meta.difficulty_adjustment_window) and tonumber(peer_meta.difficulty_adjustment_window) ~= config.difficulty_adjustment_window then
        return false, "peer difficulty adjustment window does not match the local network"
    end

    if tonumber(peer_meta.min_difficulty_prefix_length) and tonumber(peer_meta.min_difficulty_prefix_length) ~= config.min_difficulty_prefix_length then
        return false, "peer minimum difficulty bound does not match the local network"
    end

    if tonumber(peer_meta.max_difficulty_prefix_length) and tonumber(peer_meta.max_difficulty_prefix_length) ~= config.max_difficulty_prefix_length then
        return false, "peer maximum difficulty bound does not match the local network"
    end

    return true
end

local function peer_metadata_from_payload(payload)
    local service = type(payload.service) == "table" and payload.service or {}
    local meta = type(payload.meta) == "table" and payload.meta or {}
    local locator = type(payload.locator) == "table" and payload.locator or {}
    local snapshot_meta = type(payload.snapshot) == "table" and type(payload.snapshot.meta) == "table" and payload.snapshot.meta or {}
    local merged_meta = next(meta) and meta or snapshot_meta

    return {
        source = "discovered",
        node_id = service.node_id or merged_meta.node_id,
        node_url = service.node_url or merged_meta.node_url,
        version = service.version or merged_meta.version,
        chain_id = service.chain_id or merged_meta.chain_id,
        capabilities = payload.capabilities or (merged_meta.transports and {
            p2p_transport = merged_meta.transports.p2p,
            gossip_transport = merged_meta.transports.gossip
        }) or {},
        last_advertised_height = tonumber(locator.height) or 0,
        last_cumulative_work = tonumber(locator.cumulative_work) or 0
    }
end

function network.build_inventory_summary(blockchain)
    local locator = blockchain:get_locator(16)
    return {
        locator = locator.hashes,
        height = locator.height,
        cumulative_work = locator.cumulative_work,
        tip_hash = locator.tip_hash
    }
end

function network.build_peer_directory_payload(config, blockchain, excluded_peer, limit)
    local capabilities = {
        peer_discovery = true,
        headers_first_sync = true,
        inventory_announcements = true,
        block_fetch_by_hash = true,
        background_maintenance = config.peer_maintenance_enabled == true,
        p2p_transport = config.p2p_enabled and {
            protocol = "tcp",
            endpoint = config.p2p_endpoint,
            tls = config.p2p_tls_enabled == true
        } or nil,
        gossip_transport = config.gossip_enabled and {
            protocol = "udp",
            endpoint = config.gossip_endpoint
        } or nil
    }

    return {
        service = {
            name = "lua-blockchain",
            version = config.version,
            node_id = config.node_id,
            node_url = config.node_url,
            chain_id = config.chain_id,
            p2p_endpoint = config.p2p_endpoint,
            gossip_endpoint = config.gossip_endpoint
        },
        capabilities = capabilities,
        meta = blockchain:get_meta(),
        locator = blockchain:get_locator(16),
        peers = blockchain:get_advertised_peers(limit or config.peer_advertised_limit, excluded_peer),
        peer_records = blockchain:get_advertised_peer_records(limit or config.peer_advertised_limit, excluded_peer)
    }
end

function network.send_peer_post(config, peer, path, payload)
    local response = requests.send_post_request(
        peer .. path,
        canonical_json.encode(payload),
        peer_headers(config),
        { timeout_seconds = config.peer_timeout_seconds }
    )

    local error_message = nil
    if not response.ok then
        error_message = response.status_text or ("HTTP " .. tostring(response.status_code))
    end

    return {
        peer = peer,
        ok = response.ok,
        status_code = response.status_code,
        error = error_message
    }
end

function network.broadcast_to_peers(config, peers, path, payload, excluded_peer)
    local outcomes = {}

    for _, peer in ipairs(peers or {}) do
        if peer ~= excluded_peer then
            outcomes[#outcomes + 1] = network.send_peer_post(config, peer, path, payload)
        end
    end

    return outcomes
end

function network.record_peer_outcomes(blockchain, outcomes)
    for _, outcome in ipairs(outcomes or {}) do
        if outcome.ok then
            blockchain:note_peer_success(outcome.peer, {
                source = "discovered"
            })
        else
            blockchain:note_peer_failure(outcome.peer, outcome.error or ("HTTP " .. tostring(outcome.status_code)), {
                source = "discovered"
            })
        end
    end
end

function network.fetch_peer_json(config, peer, path, blockchain, success_metadata)
    local response = requests.send_get_request(
        peer .. path,
        peer_headers(config),
        { timeout_seconds = config.peer_timeout_seconds }
    )
    if not response.ok then
        if blockchain then
            blockchain:note_peer_failure(peer, response.status_text or ("HTTP " .. tostring(response.status_code)), {
                source = "discovered"
            })
        end
        return nil, response.status_text or ("HTTP " .. tostring(response.status_code))
    end

    local payload, err = cjson.decode(response.body)
    if not payload then
        if blockchain then
            blockchain:note_peer_failure(peer, "Peer returned invalid JSON: " .. tostring(err), {
                source = "discovered"
            })
        end
        return nil, "Peer returned invalid JSON: " .. tostring(err)
    end

    if blockchain and success_metadata then
        blockchain:note_peer_success(peer, success_metadata or {
            source = "discovered"
        })
    end

    return payload
end

function network.fetch_peer_directory(config, peer, blockchain)
    local path = "/api/network/peers?limit=" .. tostring(config.peer_advertised_limit)
    local payload, err = network.fetch_peer_json(config, peer, path, blockchain, false)
    if not payload then
        return nil, err
    end

    local compatible, compatibility_err = peer_meta_is_compatible(config, payload.meta)
    if not compatible then
        if blockchain then
            blockchain:note_peer_failure(peer, compatibility_err, {
                source = "discovered"
            })
        end
        return nil, compatibility_err
    end

    if blockchain then
        blockchain:note_peer_success(peer, peer_metadata_from_payload(payload))
    end

    return payload
end

function network.fetch_peer_locator(config, peer, blockchain)
    local payload, err = network.fetch_peer_json(config, peer, "/api/locator", blockchain, false)
    if not payload then
        return nil, err
    end

    local compatible, compatibility_err = peer_meta_is_compatible(config, payload.meta)
    if not compatible then
        if blockchain then
            blockchain:note_peer_failure(peer, compatibility_err, {
                source = "discovered"
            })
        end
        return nil, compatibility_err
    end

    local metadata = {
        source = "discovered",
        node_id = payload.meta and payload.meta.node_id,
        node_url = payload.meta and payload.meta.node_url,
        version = payload.meta and payload.meta.version,
        chain_id = payload.meta and payload.meta.chain_id,
        last_advertised_height = tonumber(payload.locator and payload.locator.height or payload.height) or 0,
        last_cumulative_work = tonumber(payload.locator and payload.locator.cumulative_work or payload.cumulative_work) or 0
    }
    if blockchain then
        blockchain:note_peer_success(peer, metadata)
    end

    return {
        meta = payload.meta or {},
        locator = {
            hashes = network.parse_hash_list(payload.locator and payload.locator.hashes or payload.hashes),
            height = tonumber(payload.locator and payload.locator.height or payload.height) or 0,
            cumulative_work = tonumber(payload.locator and payload.locator.cumulative_work or payload.cumulative_work) or 0,
            tip_hash = trim(payload.locator and payload.locator.tip_hash or payload.tip_hash):lower()
        }
    }
end

function network.fetch_peer_headers(config, peer, locator_hashes, limit, blockchain)
    local path = "/api/headers?limit=" .. tostring(limit or 64)
    if locator_hashes and #locator_hashes > 0 then
        path = path .. "&locator=" .. table.concat(locator_hashes, ",")
    end

    local payload, err = network.fetch_peer_json(config, peer, path, blockchain, false)
    if not payload then
        return nil, err
    end

    return payload.headers or {}
end

function network.fetch_peer_block_by_hash(config, peer, hash, blockchain)
    local payload, err = network.fetch_peer_json(config, peer, "/api/blocks/hash/" .. tostring(hash), blockchain, false)
    if not payload then
        return nil, err
    end

    return payload.block or payload
end

function network.validate_header_batch(headers, previous_header)
    local prior = previous_header

    for _, header in ipairs(headers or {}) do
        if type(header) ~= "table" then
            return false, "peer returned an invalid header entry"
        end

        if prior then
            if tonumber(header.index) ~= tonumber(prior.index) + 1 then
                return false, "peer returned non-contiguous headers"
            end

            if tostring(header.previous_hash or "") ~= tostring(prior.hash or "") then
                return false, "peer returned headers with a broken previous_hash link"
            end

            if tonumber(header.cumulative_work or 0) <= tonumber(prior.cumulative_work or 0) then
                return false, "peer returned headers with non-increasing cumulative work"
            end
        end

        prior = header
    end

    return true
end

function network.discover_from_peer(config, blockchain, peer)
    local payload, err = network.fetch_peer_directory(config, peer, blockchain)
    if not payload then
        return false, err, { discovered = 0 }
    end

    local directory_entries = {}
    local raw_entries = type(payload.peer_records) == "table" and payload.peer_records or payload.peers
    for _, entry in ipairs(parse_peer_directory(raw_entries)) do
        directory_entries[#directory_entries + 1] = entry
    end

    local discovered_peers = blockchain:record_discovered_peers(directory_entries, peer)

    return true, "Peer directory processed", {
        discovered = #discovered_peers,
        peers = discovered_peers
    }
end

function network.ingest_gossip_announcement(config, blockchain, payload)
    local source_peer = trim(payload.source_peer)
    local source_node_id = trim(payload.node_id)
    local source_version = trim(payload.version)
    local source_gossip_endpoint = trim(payload.source_gossip_endpoint)
    local source_chain_id = trim(payload.chain_id)
    local inventory = type(payload.inventory) == "table" and payload.inventory or {}

    if source_peer == "" then
        return false, "source_peer is required"
    end

    if source_chain_id ~= "" and source_chain_id ~= config.chain_id then
        return false, "gossip chain_id does not match local chain_id"
    end

    local announced_record = {
        url = source_peer,
        node_id = source_node_id,
        node_url = source_peer,
        version = source_version,
        chain_id = source_chain_id ~= "" and source_chain_id or config.chain_id,
        last_advertised_height = tonumber(inventory.height) or 0,
        last_cumulative_work = tonumber(inventory.cumulative_work) or 0,
        capabilities = {
            gossip_transport = source_gossip_endpoint ~= "" and {
                protocol = "udp",
                endpoint = source_gossip_endpoint
            } or nil,
            inventory_announcements = true,
            peer_discovery = true
        }
    }

    blockchain:note_peer_success(source_peer, announced_record)

    local discovered_entries = {}
    for _, entry in ipairs(parse_peer_directory(payload.peers or payload.peer_records)) do
        discovered_entries[#discovered_entries + 1] = entry
    end
    if #discovered_entries > 0 then
        blockchain:record_discovered_peers(discovered_entries, source_peer)
    end

    if announced_record.last_cumulative_work > blockchain:get_chain_work(blockchain:get_chain()) then
        return network.sync_from_source_peer(config, blockchain, source_peer)
    end

    return false, "Gossip announcement recorded", {
        status = "noop",
        discovered = #discovered_entries,
        cumulative_work = announced_record.last_cumulative_work
    }
end

function network.discover_from_peers(config, blockchain, preferred_peer)
    local discovered = {}
    local seen = {}
    local ordered_peers = {}

    local function append_peer(peer)
        local normalized = trim(peer)
        if normalized ~= "" and not seen[normalized] then
            seen[normalized] = true
            ordered_peers[#ordered_peers + 1] = normalized
        end
    end

    append_peer(preferred_peer)
    for _, peer in ipairs(blockchain:get_peers()) do
        append_peer(peer)
    end

    local fanout = math.min(#ordered_peers, math.max(tonumber(config.peer_discovery_fanout) or 1, 1))
    local inspected = {}

    for index = 1, fanout do
        local peer = ordered_peers[index]
        local ok, message, details = network.discover_from_peer(config, blockchain, peer)
        inspected[#inspected + 1] = {
            peer = peer,
            ok = ok,
            message = message,
            discovered = details and details.discovered or 0
        }
        if details and type(details.peers) == "table" then
            for _, discovered_peer in ipairs(details.peers) do
                discovered[#discovered + 1] = discovered_peer
            end
        end
    end

    return {
        discovered = discovered,
        inspected = inspected
    }
end

function network.sync_from_source_peer(config, blockchain, source_peer)
    if trim(source_peer) == "" then
        return false, "No source peer supplied", { status = "error" }
    end

    local locator_payload, locator_err = network.fetch_peer_locator(config, source_peer, blockchain)
    if not locator_payload then
        return false, locator_err, { status = "error" }
    end

    local peer_locator = locator_payload.locator
    if peer_locator.cumulative_work <= blockchain:get_chain_work(blockchain:get_chain()) then
        return false, "Peer does not advertise more cumulative work", {
            status = "noop",
            cumulative_work = peer_locator.cumulative_work,
            blocks = peer_locator.height
        }
    end

    local request_locator = blockchain:get_locator(16).hashes
    local headers = {}
    local last_header = nil
    local page_limit = 64

    while #headers < 1024 do
        local batch, headers_err = network.fetch_peer_headers(config, source_peer, request_locator, page_limit, blockchain)
        if not batch then
            return false, headers_err, { status = "error" }
        end

        if #batch == 0 then
            break
        end

        local valid_headers, valid_headers_err = network.validate_header_batch(batch, last_header)
        if not valid_headers then
            blockchain:note_peer_failure(source_peer, valid_headers_err, {
                source = "discovered"
            })
            return false, valid_headers_err, { status = "error" }
        end

        for _, header in ipairs(batch) do
            headers[#headers + 1] = header
        end

        last_header = headers[#headers]
        if #batch < page_limit then
            break
        end

        request_locator = { last_header.hash }
    end

    if #headers == 0 then
        return false, "Peer does not advertise missing headers", {
            status = "noop",
            cumulative_work = peer_locator.cumulative_work,
            blocks = peer_locator.height
        }
    end

    local blocks = {}
    for _, header in ipairs(headers) do
        local block, block_err = network.fetch_peer_block_by_hash(config, source_peer, header.hash, blockchain)
        if not block then
            return false, block_err, { status = "error", headers = #headers }
        end
        blocks[#blocks + 1] = block
    end

    local updated, import_result = blockchain:import_blocks(blocks)
    if not updated then
        return false, import_result, {
            status = "noop",
            headers = #headers,
            blocks = #blocks,
            cumulative_work = peer_locator.cumulative_work
        }
    end

    return true, import_result, {
        status = "updated",
        headers = #headers,
        blocks = #blocks,
        cumulative_work = blockchain:get_chain_work(blockchain:get_chain())
    }
end

function network.resolve_conflicts_headers_first(config, blockchain)
    local replaced = false
    local source_peer = nil
    local inspected = {}

    for _, peer in ipairs(blockchain:get_peers()) do
        local locator_payload, locator_err = network.fetch_peer_locator(config, peer, blockchain)
        if not locator_payload then
            inspected[#inspected + 1] = {
                peer = peer,
                ok = false,
                error = locator_err
            }
        else
            local updated, message, details = network.sync_from_source_peer(config, blockchain, peer)
            local status = details and details.status or (updated and "updated" or "noop")

            inspected[#inspected + 1] = {
                peer = peer,
                ok = status ~= "error",
                status = status,
                message = message,
                error = status == "error" and message or nil,
                blocks = locator_payload.locator.height,
                cumulative_work = locator_payload.locator.cumulative_work
            }

            if updated then
                replaced = true
                source_peer = peer
            end
        end
    end

    return replaced, {
        source_peer = source_peer,
        blocks = #blockchain:get_chain(),
        cumulative_work = blockchain:get_chain_work(blockchain:get_chain()),
        inspected = inspected
    }
end

function network.verify_peer_compatibility(config, peer, blockchain)
    peer = trim(peer)
    if peer == "" then
        return false, "peer is required"
    end

    local payload, err = network.fetch_peer_directory(config, peer, blockchain)
    if not payload then
        return false, err
    end

    return peer_meta_is_compatible(config, payload.meta)
end

return network

package.path = "/app/?.lua;/app/?/init.lua;" .. package.path
package.cpath = "/usr/lib/x86_64-linux-gnu/lua/5.1/?.so;" .. package.cpath

local Blockchain = require("classes.blockchain")
local crypto = require("classes.crypto")
local node_identity = require("classes.node_identity")

math.randomseed(os.time())

local function assert_true(value, message)
    if not value then
        error(message or "assertion failed")
    end
end

local function assert_equal(actual, expected, message)
    if actual ~= expected then
        error(message or ("expected " .. tostring(expected) .. ", got " .. tostring(actual)))
    end
end

local function cleanup(path)
    os.remove(path)
    os.remove(path .. ".tmp")
    os.remove(path .. "-wal")
    os.remove(path .. "-shm")
end

local function cleanup_tree(path)
    if path and path ~= "" then
        os.execute("rm -rf " .. string.format("%q", path))
    end
end

local function build_signed_transaction(wallet, recipient, amount, fee, nonce, note)
    local payload = {
        amount = amount,
        fee = fee,
        kind = "transfer",
        nonce = nonce,
        note = note or "",
        recipient = recipient,
        sender = wallet.address,
        timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
    }

    payload.public_key = wallet.public_key
    payload.signature = assert(crypto.sign_message(wallet.private_key, crypto.build_signing_message(payload)))

    return payload
end

local function build_reward_transaction(blockchain, recipient, block_index, timestamp)
    local transaction, err = blockchain:normalize_transaction({
        amount = blockchain.mining_reward,
        fee = 0,
        kind = "reward",
        nonce = 0,
        note = "Block reward for block " .. tostring(block_index),
        recipient = recipient,
        sender = "NETWORK",
        timestamp = timestamp
    })
    if not transaction then
        error(err)
    end

    return transaction
end

local function build_reward_block(blockchain, previous_block, index, timestamp, miner, difficulty_prefix)
    local block = {
        index = index,
        timestamp = timestamp,
        transactions = {
            build_reward_transaction(blockchain, miner, index, timestamp)
        },
        proof = blockchain:proof_of_work(previous_block.proof, previous_block.hash, difficulty_prefix),
        previous_hash = previous_block.hash,
        mined_by = miner,
        difficulty_prefix = difficulty_prefix,
        hash_format = "v2"
    }
    block.hash = blockchain:calculate_hash(block)
    block.work = blockchain:calculate_block_work(block.difficulty_prefix)
    block.cumulative_work = (previous_block.cumulative_work or 0) + block.work
    return block
end

local test_path = "/tmp/lua-blockchain-test-" .. tostring(math.random(1000000)) .. ".db"
cleanup(test_path)

local identity_private_path = test_path .. ".node-identity.private.pem"
local identity_public_path = test_path .. ".node-identity.public.pem"
os.remove(identity_private_path)
os.remove(identity_public_path)

local identity_one = assert(node_identity.load_or_create({
    node_identity_private_key_path = identity_private_path,
    node_identity_public_key_path = identity_public_path
}))
local identity_two = assert(node_identity.load_or_create({
    node_identity_private_key_path = identity_private_path,
    node_identity_public_key_path = identity_public_path
}))
assert_equal(identity_one.peer_id, identity_two.peer_id, "node identity should persist across reloads")
assert_equal(identity_one.public_key, identity_two.public_key, "node identity public key should persist across reloads")

local wallet_a = assert(crypto.create_wallet())
local wallet_b = assert(crypto.create_wallet())

local blockchain = Blockchain.new({
    file_name = test_path,
    difficulty_prefix = "00",
    target_block_seconds = 30,
    difficulty_adjustment_window = 2,
    mining_reward = 2,
    chain_id = "test-chain-a",
    node_id = "test-node",
    node_url = "http://127.0.0.1:8080",
    min_difficulty_prefix_length = 1,
    max_difficulty_prefix_length = 4,
    max_peers = 1,
    max_pending_transactions = 1,
    max_transactions_per_block = 2,
    min_transaction_fee = 0.1,
    max_transaction_note_bytes = 64,
    require_https_peers = true,
    allowed_peer_host_map = { ["node.example.com"] = true },
    p2p_enabled = true,
    p2p_port = 19100,
    p2p_endpoint = "node.example.com:19100",
    node_identity_private_key_path = identity_private_path,
    node_identity_public_key_path = identity_public_path,
    version = "test"
})

assert_equal(#blockchain:get_chain(), 1, "genesis block should be created")
assert_equal(blockchain:get_meta().transports.p2p.endpoint, "node.example.com:19100", "meta should expose the native p2p endpoint")
assert_true(blockchain:get_meta().transports.p2p.tls == true, "meta should expose native p2p TLS")

local first_block, first_mine_err = blockchain:mine_block(wallet_a.address)
assert_true(first_block ~= nil, first_mine_err)
assert_equal(first_block.index, 2, "first mined block should be the second block")
assert_equal(first_block.transactions[1].kind, "reward", "reward-only block should still include a reward transaction")
assert_equal(first_block.difficulty_prefix, "00", "the first mined block should use the initial difficulty")
assert_equal(first_block.work, 256, "block work should be derived from the difficulty prefix")

local account_a_after_reward = blockchain:get_account(wallet_a.address)
assert_true(account_a_after_reward ~= nil, "wallet A summary should exist after mining")
assert_equal(account_a_after_reward.confirmed_balance, 2, "mining reward should fund wallet A")
assert_equal(account_a_after_reward.next_nonce, 1, "wallet A next nonce should begin at 1")

local overspend_transfer = build_signed_transaction(wallet_a, wallet_b.address, 50, 0.10, 1, "overspend")
local overspend_result, overspend_err = blockchain:add_transaction(overspend_transfer)
assert_true(overspend_result == nil, "overspending transfers should be rejected")
assert_true(string.find(overspend_err or "", "insufficient balance", 1, true) ~= nil, "overspend rejection should mention balance")

local low_fee_transfer = build_signed_transaction(wallet_a, wallet_b.address, 0.25, 0.05, 1, "low fee")
local low_fee_result, low_fee_err = blockchain:add_transaction(low_fee_transfer)
assert_true(low_fee_result == nil, "transfers below the minimum fee should be rejected")
assert_equal(low_fee_err, "fee must be at least 0.1", "minimum fee rejection should be explicit")

local tampered_transfer = build_signed_transaction(wallet_a, wallet_b.address, 0.50, 0.10, 1, "tampered")
tampered_transfer.amount = 0.60
local tampered_result, tampered_err = blockchain:add_transaction(tampered_transfer)
assert_true(tampered_result == nil, "tampered signatures should be rejected")
assert_true(string.find(tampered_err or "", "signed payload", 1, true) ~= nil or string.find(tampered_err or "", "verification", 1, true) ~= nil, "tampered rejection should mention signature integrity")

local signed_transfer = build_signed_transaction(wallet_a, wallet_b.address, 1.25, 0.25, 1, "test transfer")
local queued_transaction, next_block_index = blockchain:add_transaction(signed_transfer)
assert_true(queued_transaction ~= nil, "signed transfer should be accepted")
assert_equal(next_block_index, 3, "queued transfer should target the next block")
assert_equal(#blockchain:get_pending_transactions(), 1, "signed transfer should enter the mempool")

local mempool_overflow = build_signed_transaction(wallet_a, wallet_b.address, 0.10, 0.10, 2, "pool full")
local overflow_result, overflow_err = blockchain:add_transaction(mempool_overflow)
assert_true(overflow_result == nil, "mempool limit should reject additional queued transfers")
assert_equal(overflow_err, "pending transaction pool is full", "mempool overflow should be explicit")

local duplicate_transaction, duplicate_err = blockchain:add_transaction(signed_transfer)
assert_true(duplicate_transaction == nil, "duplicate transfers should be rejected")
assert_equal(duplicate_err, "transaction already exists", "duplicate rejection should be explicit")

local second_block, second_mine_err = blockchain:mine_block(wallet_a.address)
assert_true(second_block ~= nil, second_mine_err)
assert_equal(second_block.index, 3, "second mined block should be the third block")
assert_equal(#second_block.transactions, 2, "transfer block should contain the transfer and reward")
assert_equal(second_block.transactions[1].kind, "transfer", "queued transfer should be mined before the reward")
assert_equal(second_block.transactions[2].kind, "reward", "reward should remain the last transaction in the block")
assert_equal(second_block.transactions[2].amount, 2.25, "mining reward should include transaction fees")
assert_equal(#blockchain:get_pending_transactions(), 0, "mempool should be empty after mining")
assert_equal(second_block.difficulty_prefix, "00", "difficulty should remain stable before the first retarget")
assert_equal(second_block.cumulative_work, 768, "cumulative work should include the genesis and mined blocks")

local valid, reason = blockchain:validate_chain(blockchain:get_chain())
assert_true(valid, reason)

local headers = blockchain:get_headers(2, 8)
assert_equal(#headers, 2, "headers endpoint should support ranged reads")
assert_equal(headers[2].cumulative_work, 768, "headers should expose cumulative work")

local headers_after_locator = blockchain:get_headers_after_locator({ first_block.hash }, 8)
assert_equal(#headers_after_locator, 1, "locator-based header reads should return only the missing suffix")
assert_equal(headers_after_locator[1].hash, second_block.hash, "locator-based header reads should begin after the common ancestor")

local locator = blockchain:get_locator(8)
assert_equal(locator.tip_hash, second_block.hash, "locator should point to the local chain tip")
assert_equal(locator.cumulative_work, 768, "locator should report chain work")

local fetched_by_hash = blockchain:get_block_by_hash(second_block.hash)
assert_equal(fetched_by_hash.index, second_block.index, "blocks should be retrievable by hash")

local stats = blockchain:get_stats()
assert_equal(stats.cumulative_work, 768, "stats should report cumulative work")
assert_equal(stats.tip_difficulty_prefix, "00", "stats should expose the tip difficulty")

local account_a = blockchain:get_account(wallet_a.address)
local account_b = blockchain:get_account(wallet_b.address)
assert_equal(account_a.confirmed_balance, 2.75, "wallet A should retain reward minus spend plus fee-adjusted reward")
assert_equal(account_a.next_nonce, 2, "wallet A should advance to nonce 2 after one confirmed transfer")
assert_equal(account_b.confirmed_balance, 1.25, "wallet B should receive the transfer amount")
assert_equal(account_b.next_nonce, 1, "wallet B should still be ready for its first outbound transfer")

local bad_nonce_transfer = build_signed_transaction(wallet_b, wallet_a.address, 0.50, 0.10, 2, "bad nonce")
local bad_nonce_result, bad_nonce_err = blockchain:add_transaction(bad_nonce_transfer)
assert_true(bad_nonce_result == nil, "future nonces should be rejected")
assert_true(string.find(bad_nonce_err or "", "expected nonce 1", 1, true) ~= nil, "nonce rejection should name the expected value")

local insecure_peer_result, insecure_peer_err = blockchain:register_peer("http://node.example.com")
assert_true(insecure_peer_result == nil, "non-https peers should be rejected when https is required")
assert_equal(insecure_peer_err, "peer must use https", "peer rejection should mention https")

local disallowed_peer_result, disallowed_peer_err = blockchain:register_peer("https://node-b.example.com")
assert_true(disallowed_peer_result == nil, "disallowed hosts should be rejected")
assert_equal(disallowed_peer_err, "peer host is not allowed", "peer rejection should mention the allowlist")

local peers = blockchain:register_peer("https://node.example.com/")
assert_equal(peers[1], "https://node.example.com", "peer URLs should be normalized")
assert_equal(blockchain:get_total_peer_count(), 1, "peer records should track the normalized peer")

local peer_record = blockchain:get_peer("https://node.example.com")
assert_equal(peer_record.source, "manual", "manually registered peers should be marked with the manual source")

local failed_peer = assert(blockchain:note_peer_failure("https://node.example.com", "timeout"))
assert_equal(failed_peer.state, "backoff", "failed peers should enter backoff")
assert_equal(#blockchain:get_peers(), 0, "backoff peers should be removed from the active peer set")

local recovered_peer = assert(blockchain:note_peer_success("https://node.example.com", {
    node_id = "peer-node-1",
    version = "peer-version",
    chain_id = "test-chain-a",
    capabilities = {
        peer_discovery = true,
        gossip_transport = {
            protocol = "udp",
            endpoint = "node.example.com:19090"
        }
    },
    last_advertised_height = 9,
    last_cumulative_work = 4096
}))
assert_equal(recovered_peer.state, "active", "successful peers should return to the active set")
assert_equal(#blockchain:get_peers(), 1, "successful peers should return to the active peer set")
assert_equal(recovered_peer.node_id, "peer-node-1", "peer metadata should be attached to peer records")
assert_equal(recovered_peer.last_advertised_height, 9, "peer metadata should retain advertised height")
assert_equal(recovered_peer.capabilities.gossip_transport.endpoint, "node.example.com:19090", "gossip transport metadata should persist on peer records")

local advertised_peer_record = blockchain:get_advertised_peer_records(4)[1]
assert_equal(advertised_peer_record.capabilities.gossip_transport.endpoint, "node.example.com:19090", "advertised peer records should expose gossip endpoints")

local peer_limit_result, peer_limit_err = blockchain:register_peer("https://node.example.com:8443")
assert_true(peer_limit_result == nil, "peer limit should be enforced")
assert_equal(peer_limit_err, "peer host is not allowed", "peer host allowlist should run before peer limit")

local reloaded = Blockchain.new({
    file_name = test_path,
    difficulty_prefix = "00",
    target_block_seconds = 30,
    difficulty_adjustment_window = 2,
    mining_reward = 2,
    chain_id = "test-chain-a",
    node_id = "test-node",
    node_url = "http://127.0.0.1:8080",
    min_difficulty_prefix_length = 1,
    max_difficulty_prefix_length = 4,
    max_peers = 1,
    max_pending_transactions = 1,
    max_transactions_per_block = 2,
    min_transaction_fee = 0.1,
    max_transaction_note_bytes = 64,
    require_https_peers = true,
    allowed_peer_host_map = { ["node.example.com"] = true },
    p2p_enabled = true,
    p2p_port = 19100,
    p2p_endpoint = "node.example.com:19100",
    node_identity_private_key_path = identity_private_path,
    node_identity_public_key_path = identity_public_path,
    version = "test"
})

local reloaded_valid, reloaded_reason = reloaded:validate_chain(reloaded:get_chain())
assert_true(reloaded_valid, reloaded_reason)
assert_equal(#reloaded:get_chain(), 3, "reloaded blockchain should preserve mined blocks")
assert_equal(reloaded:get_account(wallet_a.address).confirmed_balance, 2.75, "wallet A balance should persist to disk")
assert_equal(reloaded:get_account(wallet_b.address).confirmed_balance, 1.25, "wallet B balance should persist to disk")
assert_equal(reloaded:get_meta().storage_engine, "sqlite", "state should be persisted through sqlite")
assert_equal(reloaded:get_meta().schema_version, 8, "peer-aware schema metadata should persist")
assert_equal(reloaded:get_chain_work(reloaded:get_chain()), 768, "reloaded chain work should persist through normalization")
assert_equal(reloaded:get_total_peer_count(), 1, "reloaded blockchain should preserve peer records")
assert_equal(reloaded:get_peer("https://node.example.com").node_id, "peer-node-1", "peer metadata should persist to disk")
assert_equal(reloaded:get_peer("https://node.example.com").capabilities.gossip_transport.endpoint, "node.example.com:19090", "gossip transport metadata should survive persistence")
assert_equal(reloaded:get_meta().transports.p2p.endpoint, "node.example.com:19100", "reloaded metadata should preserve the native p2p endpoint")

local backup_dir = test_path .. ".backups"
cleanup_tree(backup_dir)
local backup_manifest = assert(reloaded:create_backup({
    backup_dir = backup_dir,
    label = "spec"
}))
assert_true(backup_manifest.files.database ~= nil, "backups should include a SQLite snapshot")
assert_true(backup_manifest.files["node-identity-private.pem"] ~= nil, "backups should include the node identity private key when present")
assert_true(backup_manifest.files["node-identity-public.pem"] ~= nil, "backups should include the node identity public key when present")
assert_true(io.open(backup_manifest.files.database, "r") ~= nil, "backup database file should exist")
assert_true(io.open(backup_manifest.manifest, "r") ~= nil, "backup manifest file should exist")

local restored_from_backup = Blockchain.new({
    file_name = backup_manifest.files.database,
    difficulty_prefix = "00",
    target_block_seconds = 30,
    difficulty_adjustment_window = 2,
    mining_reward = 2,
    chain_id = "test-chain-a",
    node_id = "test-node",
    node_url = "http://127.0.0.1:8080",
    min_difficulty_prefix_length = 1,
    max_difficulty_prefix_length = 4,
    version = "test"
})
assert_equal(#restored_from_backup:get_chain(), 3, "backup database should load as a valid blockchain snapshot")
assert_equal(restored_from_backup:get_chain_work(restored_from_backup:get_chain()), 768, "backup database should preserve cumulative work")

local admission_path = test_path .. ".admission.db"
cleanup(admission_path)
local admission_chain = Blockchain.new({
    file_name = admission_path,
    difficulty_prefix = "00",
    target_block_seconds = 30,
    difficulty_adjustment_window = 2,
    mining_reward = 2,
    chain_id = "test-chain-a",
    min_difficulty_prefix_length = 1,
    max_difficulty_prefix_length = 4,
    max_peers = 8,
    max_peers_per_ip = 1,
    max_peers_per_subnet = 1,
    require_https_peers = true,
    allowed_peer_host_map = {
        ["peer-a.example.com"] = true,
        ["peer-b.example.com"] = true,
        ["peer-c.example.com"] = true,
        ["peer-d.example.com"] = true
    },
    allowed_peer_id_map = {
        ["peer-alpha"] = true,
        ["peer-beta"] = true,
        ["peer-gamma"] = true
    },
    p2p_enabled = true,
    p2p_port = 19100,
    p2p_endpoint = "peer-self.example.com:19100",
    p2p_tls_enabled = true,
    version = "test"
})

local admitted_peer = assert(admission_chain:note_peer_success("https://peer-a.example.com", {
    node_id = "peer-a",
    chain_id = "test-chain-a",
    capabilities = {
        signed_identity = {
            peer_id = "peer-alpha",
            public_key = "placeholder-public-key"
        },
        network_address = "198.51.100.10",
        transport_security = {
            tls = true,
            tls_cert_fingerprint = "fingerprint-a"
        }
    }
}))
assert_equal(admitted_peer.capabilities.network_group, "198.51.100.0/24", "peer capabilities should derive a network group from IPv4 addresses")

local duplicate_identity_result, duplicate_identity_err = admission_chain:note_peer_success("https://peer-b.example.com", {
    node_id = "peer-b",
    chain_id = "test-chain-a",
    capabilities = {
        signed_identity = {
            peer_id = "peer-alpha",
            public_key = "placeholder-public-key-2"
        },
        network_address = "203.0.113.10"
    }
})
assert_true(duplicate_identity_result == nil, "duplicate peer identities should be rejected")
assert_equal(duplicate_identity_err, "peer_id already belongs to another registered peer", "duplicate peer identities should be explicit")

local disallowed_identity_result, disallowed_identity_err = admission_chain:note_peer_success("https://peer-c.example.com", {
    node_id = "peer-c",
    chain_id = "test-chain-a",
    capabilities = {
        signed_identity = {
            peer_id = "peer-rogue",
            public_key = "placeholder-public-key-3"
        },
        network_address = "203.0.113.11"
    }
})
assert_true(disallowed_identity_result == nil, "non-allowlisted peer identities should be rejected")
assert_equal(disallowed_identity_err, "peer_id is not allowed", "peer allowlist rejections should be explicit")

local duplicate_ip_result, duplicate_ip_err = admission_chain:note_peer_success("https://peer-b.example.com", {
    node_id = "peer-b",
    chain_id = "test-chain-a",
    capabilities = {
        signed_identity = {
            peer_id = "peer-beta",
            public_key = "placeholder-public-key-4"
        },
        network_address = "198.51.100.10"
    }
})
assert_true(duplicate_ip_result == nil, "too many peers on one IP should be rejected")
assert_equal(duplicate_ip_err, "peer admission rejected because too many peers share the same IP address", "per-IP admission rejections should be explicit")

local duplicate_subnet_result, duplicate_subnet_err = admission_chain:note_peer_success("https://peer-b.example.com", {
    node_id = "peer-b",
    chain_id = "test-chain-a",
    capabilities = {
        signed_identity = {
            peer_id = "peer-beta",
            public_key = "placeholder-public-key-5"
        },
        network_address = "198.51.100.11"
    }
})
assert_true(duplicate_subnet_result == nil, "too many peers in one subnet should be rejected")
assert_equal(duplicate_subnet_err, "peer admission rejected because too many peers share the same network group", "per-subnet admission rejections should be explicit")

local advertised_capabilities = admission_chain:get_advertised_peer_records(4)[1].capabilities
assert_true(advertised_capabilities.network_address == nil, "advertised peer records should not leak observed network addresses")
assert_true(advertised_capabilities.network_group == nil, "advertised peer records should not leak observed network groups")
assert_equal(advertised_capabilities.transport_security.tls_cert_fingerprint, "fingerprint-a", "advertised peer records should preserve TLS fingerprints")

local difficulty_probe_path = test_path .. ".difficulty.db"
cleanup(difficulty_probe_path)
local difficulty_probe = Blockchain.new({
    file_name = difficulty_probe_path,
    difficulty_prefix = "00",
    target_block_seconds = 30,
    difficulty_adjustment_window = 2,
    mining_reward = 2,
    chain_id = "test-chain-a",
    min_difficulty_prefix_length = 1,
    max_difficulty_prefix_length = 4,
    version = "test"
})
assert(difficulty_probe:mine_block(wallet_a.address))
assert(difficulty_probe:mine_block(wallet_a.address))
difficulty_probe.chain[2].timestamp = "2026-01-01T00:00:00Z"
difficulty_probe.chain[3].timestamp = "2026-01-01T00:05:00Z"
assert_equal(difficulty_probe:get_next_difficulty_prefix(), "0", "slow blocks should reduce the next difficulty")
difficulty_probe.chain[3].timestamp = "2026-01-01T00:00:10Z"
assert_equal(difficulty_probe:get_next_difficulty_prefix(), "000", "fast blocks should increase the next difficulty")

local fork_path = test_path .. ".fork.db"
cleanup(fork_path)
local fork_chain = Blockchain.new({
    file_name = fork_path,
    difficulty_prefix = "00",
    target_block_seconds = 30,
    difficulty_adjustment_window = 2,
    mining_reward = 2,
    chain_id = "test-chain-a",
    min_difficulty_prefix_length = 1,
    max_difficulty_prefix_length = 4,
    version = "test"
})

local genesis = fork_chain:get_chain()[1]
local slow_block_two = build_reward_block(fork_chain, genesis, 2, "2026-01-01T00:00:00Z", wallet_a.address, "00")
local slow_block_three = build_reward_block(fork_chain, slow_block_two, 3, "2026-01-01T00:05:00Z", wallet_a.address, "00")
local slow_block_four = build_reward_block(fork_chain, slow_block_three, 4, "2026-01-01T00:10:00Z", wallet_a.address, "0")
local fast_block_two = build_reward_block(fork_chain, genesis, 2, "2026-01-01T00:00:00Z", wallet_a.address, "00")
local fast_block_three = build_reward_block(fork_chain, fast_block_two, 3, "2026-01-01T00:00:10Z", wallet_a.address, "00")
local fast_block_four = build_reward_block(fork_chain, fast_block_three, 4, "2026-01-01T00:00:20Z", wallet_a.address, "000")

local slow_chain = { genesis, slow_block_two, slow_block_three, slow_block_four }
local fast_chain = { genesis, fast_block_two, fast_block_three, fast_block_four }
assert_true(select(1, fork_chain:validate_chain(slow_chain)), "slow synthetic chain should validate")
assert_true(select(1, fork_chain:validate_chain(fast_chain)), "fast synthetic chain should validate")
assert_true(fork_chain:get_chain_work(fast_chain) > fork_chain:get_chain_work(slow_chain), "fast synthetic chain should have more cumulative work")
fork_chain.chain = slow_chain
local replaced, replace_reason = fork_chain:import_blocks({ fast_block_two, fast_block_three, fast_block_four })
assert_true(replaced, replace_reason)
assert_equal(fork_chain:get_chain()[#fork_chain:get_chain()].difficulty_prefix, "000", "higher-work chain should replace the local chain")

local alternate_path = test_path .. ".chain-id.db"
cleanup(alternate_path)
local alternate_chain = Blockchain.new({
    file_name = alternate_path,
    difficulty_prefix = "00",
    target_block_seconds = 30,
    difficulty_adjustment_window = 2,
    mining_reward = 2,
    chain_id = "test-chain-b",
    min_difficulty_prefix_length = 1,
    max_difficulty_prefix_length = 4,
    version = "test"
})
assert_true(alternate_chain:get_chain()[1].hash ~= blockchain:get_chain()[1].hash, "chain_id should influence the genesis hash")

cleanup(test_path)
cleanup(difficulty_probe_path)
cleanup(fork_path)
cleanup(alternate_path)
cleanup(admission_path)
cleanup_tree(backup_dir)
os.remove(identity_private_path)
os.remove(identity_public_path)

print("All blockchain tests passed")

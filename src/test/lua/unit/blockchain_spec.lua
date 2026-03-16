package.path = "/app/?.lua;/app/?/init.lua;" .. package.path
package.cpath = "/usr/lib/x86_64-linux-gnu/lua/5.1/?.so;" .. package.cpath

local Blockchain = require("classes.blockchain")
local crypto = require("classes.crypto")

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

local test_path = "/tmp/lua-blockchain-test-" .. tostring(math.random(1000000)) .. ".json"
cleanup(test_path)

local wallet_a = assert(crypto.create_wallet())
local wallet_b = assert(crypto.create_wallet())

local blockchain = Blockchain.new({
    file_name = test_path,
    difficulty_prefix = "00",
    mining_reward = 2,
    chain_id = "test-chain-a",
    node_id = "test-node",
    node_url = "http://127.0.0.1:8080",
    max_peers = 1,
    max_pending_transactions = 1,
    max_transactions_per_block = 2,
    min_transaction_fee = 0.1,
    max_transaction_note_bytes = 64,
    require_https_peers = true,
    allowed_peer_host_map = { ["node.example.com"] = true },
    version = "test"
})

assert_equal(#blockchain:get_chain(), 1, "genesis block should be created")

local first_block, first_mine_err = blockchain:mine_block(wallet_a.address)
assert_true(first_block ~= nil, first_mine_err)
assert_equal(first_block.index, 2, "first mined block should be the second block")
assert_equal(first_block.transactions[1].kind, "reward", "reward-only block should still include a reward transaction")

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

local valid, reason = blockchain:validate_chain(blockchain:get_chain())
assert_true(valid, reason)

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

local peer_limit_result, peer_limit_err = blockchain:register_peer("https://node.example.com:8443")
assert_true(peer_limit_result == nil, "peer limit should be enforced")
assert_equal(peer_limit_err, "peer host is not allowed", "peer host allowlist should run before peer limit")

local reloaded = Blockchain.new({
    file_name = test_path,
    difficulty_prefix = "00",
    mining_reward = 2,
    chain_id = "test-chain-a",
    node_id = "test-node",
    node_url = "http://127.0.0.1:8080",
    max_peers = 1,
    max_pending_transactions = 1,
    max_transactions_per_block = 2,
    min_transaction_fee = 0.1,
    max_transaction_note_bytes = 64,
    require_https_peers = true,
    allowed_peer_host_map = { ["node.example.com"] = true },
    version = "test"
})

local reloaded_valid, reloaded_reason = reloaded:validate_chain(reloaded:get_chain())
assert_true(reloaded_valid, reloaded_reason)
assert_equal(#reloaded:get_chain(), 3, "reloaded blockchain should preserve mined blocks")
assert_equal(reloaded:get_account(wallet_a.address).confirmed_balance, 2.75, "wallet A balance should persist to disk")
assert_equal(reloaded:get_account(wallet_b.address).confirmed_balance, 1.25, "wallet B balance should persist to disk")

local alternate_path = test_path .. ".chain-id"
cleanup(alternate_path)
local alternate_chain = Blockchain.new({
    file_name = alternate_path,
    difficulty_prefix = "00",
    mining_reward = 2,
    chain_id = "test-chain-b",
    version = "test"
})
assert_true(alternate_chain:get_chain()[1].hash ~= blockchain:get_chain()[1].hash, "chain_id should influence the genesis hash")

cleanup(test_path)
cleanup(alternate_path)

print("All blockchain tests passed")

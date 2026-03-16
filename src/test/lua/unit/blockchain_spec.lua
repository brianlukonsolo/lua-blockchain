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
    node_id = "test-node",
    node_url = "http://127.0.0.1:8080",
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

local signed_transfer = build_signed_transaction(wallet_a, wallet_b.address, 1.25, 0.25, 1, "test transfer")
local queued_transaction, next_block_index = blockchain:add_transaction(signed_transfer)
assert_true(queued_transaction ~= nil, "signed transfer should be accepted")
assert_equal(next_block_index, 3, "queued transfer should target the next block")
assert_equal(#blockchain:get_pending_transactions(), 1, "signed transfer should enter the mempool")

local duplicate_transaction, duplicate_err = blockchain:add_transaction(signed_transfer)
assert_true(duplicate_transaction == nil, "duplicate transfers should be rejected")
assert_equal(duplicate_err, "transaction already exists", "duplicate rejection should be explicit")

local overspend_transfer = build_signed_transaction(wallet_a, wallet_b.address, 50, 0.10, 2, "overspend")
local overspend_result, overspend_err = blockchain:add_transaction(overspend_transfer)
assert_true(overspend_result == nil, "overspending transfers should be rejected")
assert_true(string.find(overspend_err or "", "insufficient balance", 1, true) ~= nil, "overspend rejection should mention balance")

local tampered_transfer = build_signed_transaction(wallet_a, wallet_b.address, 0.50, 0.05, 2, "tampered")
tampered_transfer.amount = 0.60
local tampered_result, tampered_err = blockchain:add_transaction(tampered_transfer)
assert_true(tampered_result == nil, "tampered signatures should be rejected")
assert_true(string.find(tampered_err or "", "signed payload", 1, true) ~= nil or string.find(tampered_err or "", "verification", 1, true) ~= nil, "tampered rejection should mention signature integrity")

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

local bad_nonce_transfer = build_signed_transaction(wallet_b, wallet_a.address, 0.50, 0.05, 2, "bad nonce")
local bad_nonce_result, bad_nonce_err = blockchain:add_transaction(bad_nonce_transfer)
assert_true(bad_nonce_result == nil, "future nonces should be rejected")
assert_true(string.find(bad_nonce_err or "", "expected nonce 1", 1, true) ~= nil, "nonce rejection should name the expected value")

local peers = blockchain:register_peer("http://127.0.0.1:8081/")
assert_equal(peers[1], "http://127.0.0.1:8081", "peer URLs should be normalized")

local reloaded = Blockchain.new({
    file_name = test_path,
    difficulty_prefix = "00",
    mining_reward = 2,
    node_id = "test-node",
    node_url = "http://127.0.0.1:8080",
    version = "test"
})

local reloaded_valid, reloaded_reason = reloaded:validate_chain(reloaded:get_chain())
assert_true(reloaded_valid, reloaded_reason)
assert_equal(#reloaded:get_chain(), 3, "reloaded blockchain should preserve mined blocks")
assert_equal(reloaded:get_account(wallet_a.address).confirmed_balance, 2.75, "wallet A balance should persist to disk")
assert_equal(reloaded:get_account(wallet_b.address).confirmed_balance, 1.25, "wallet B balance should persist to disk")

cleanup(test_path)

print("All blockchain tests passed")

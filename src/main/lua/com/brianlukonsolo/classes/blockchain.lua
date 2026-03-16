local canonical_json = require("classes.canonical_json")
local crypto = require("classes.crypto")
local hashing = require("cryptography.pure_lua_SHA.sha2")
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

function Blockchain.new(options)
    local self = setmetatable({}, Blockchain)

    self.name = options.name or "lua-blockchain"
    self.version = options.version or "3.0.0"
    self.file_name = options.file_name or "blockchain_data.json"
    self.difficulty_prefix = options.difficulty_prefix or "0000"
    self.mining_reward = tonumber(options.mining_reward) or 1
    self.node_id = options.node_id or "node-local"
    self.node_url = options.node_url or ""
    self.chain = {}
    self.pending_transactions = {}
    self.peers = {}
    self.meta = {}

    self:load()

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

function Blockchain:get_previous_block()
    return self.chain[#self.chain]
end

function Blockchain:get_block(index)
    return deep_copy(self.chain[index])
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

    return {
        blocks = #self.chain,
        pending_transactions = #self.pending_transactions,
        committed_transactions = committed_transactions,
        peers = #self.peers,
        accounts = account_count,
        mining_reward = self.mining_reward,
        difficulty_prefix = self.difficulty_prefix,
        committed_value = committed_value,
        pending_value = pending_value,
        queued_fees = queued_fees,
        circulating_supply = circulating_supply,
        last_block_hash = self.chain[#self.chain] and self.chain[#self.chain].hash or nil
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
    return {
        hash = nil,
        index = block.index,
        mined_by = block.mined_by,
        previous_hash = tostring(block.previous_hash or "0"),
        proof = block.proof,
        timestamp = block.timestamp,
        transactions = deep_copy(block.transactions or {})
    }
end

function Blockchain:calculate_hash(block)
    return self:get_sha256_hash_of_string(canonical_json.encode(self:serialize_block_for_hash(block)))
end

function Blockchain:is_valid_proof(previous_proof, proof, previous_hash)
    local payload = table.concat({
        tostring(previous_proof),
        tostring(proof),
        tostring(previous_hash)
    }, ":")

    local hash = self:get_sha256_hash_of_string(payload)
    return hash:sub(1, #self.difficulty_prefix) == self.difficulty_prefix
end

function Blockchain:proof_of_work(previous_proof, previous_hash)
    local proof = 1

    while not self:is_valid_proof(previous_proof, proof, previous_hash) do
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

function Blockchain:normalize_block(input, previous_hash, index)
    if type(input) ~= "table" then
        return nil, "block must be an object"
    end

    local block = {
        index = normalize_integer(input.index) or index or (#self.chain + 1),
        timestamp = trim(input.timestamp) ~= "" and trim(input.timestamp) or utc_now(),
        transactions = {},
        proof = normalize_integer(input.proof) or 1,
        previous_hash = tostring(input.previous_hash or previous_hash or "0"),
        mined_by = trim(input.mined_by) ~= "" and trim(input.mined_by) or nil
    }

    if is_array(input.transactions) then
        for transaction_index, transaction in ipairs(input.transactions) do
            local normalized_transaction, transaction_err = self:normalize_transaction(transaction)
            if not normalized_transaction then
                return nil, "invalid transaction at index " .. tostring(transaction_index) .. ": " .. tostring(transaction_err)
            end
            block.transactions[transaction_index] = normalized_transaction
        end
    end

    block.hash = trim(input.hash) ~= "" and tostring(input.hash) or self:calculate_hash(block)

    return block
end

function Blockchain:normalize_chain(chain)
    if not is_array(chain) then
        return nil, "chain must be an array"
    end

    local normalized = {}
    local previous_hash = "0"

    for index, block in ipairs(chain) do
        local normalized_block, err = self:normalize_block(block, previous_hash, index)
        if not normalized_block then
            return nil, err
        end

        normalized[index] = normalized_block
        previous_hash = normalized_block.hash
    end

    return normalized
end

function Blockchain:build_state(chain)
    return {
        meta = {
            consensus = "longest-valid-chain",
            difficulty_prefix = self.difficulty_prefix,
            mining_reward = self.mining_reward,
            name = self.name,
            node_id = self.node_id,
            node_url = self.node_url,
            schema_version = 3,
            updated_at = utc_now(),
            version = self.version
        },
        chain = deep_copy(chain or self.chain),
        pending_transactions = deep_copy(self.pending_transactions),
        peers = deep_copy(self.peers)
    }
end

function Blockchain:save()
    self.meta.updated_at = utc_now()
    return storage.atomic_write_json(self.file_name, self:build_state())
end

function Blockchain:create_genesis_block()
    local block = {
        index = 1,
        timestamp = "genesis block",
        transactions = {},
        proof = 1,
        previous_hash = "0"
    }
    block.hash = self:calculate_hash(block)

    return block
end

function Blockchain:reset()
    self.chain = { self:create_genesis_block() }
    self.pending_transactions = {}
    self.peers = {}
    self.meta = {
        consensus = "longest-valid-chain",
        difficulty_prefix = self.difficulty_prefix,
        mining_reward = self.mining_reward,
        name = self.name,
        node_id = self.node_id,
        node_url = self.node_url,
        schema_version = 3,
        updated_at = utc_now(),
        version = self.version
    }

    self:save()
end

function Blockchain:migrate_state(decoded)
    if is_array(decoded) then
        return {
            meta = {
                consensus = "longest-valid-chain",
                difficulty_prefix = self.difficulty_prefix,
                mining_reward = self.mining_reward,
                name = self.name,
                node_id = self.node_id,
                node_url = self.node_url,
                schema_version = 3,
                updated_at = utc_now(),
                version = self.version
            },
            chain = decoded,
            pending_transactions = {},
            peers = {}
        }
    end

    if type(decoded) ~= "table" then
        return nil, "stored blockchain data must be an object or array"
    end

    return {
        meta = decoded.meta or {},
        chain = decoded.chain or {},
        pending_transactions = decoded.pending_transactions or {},
        peers = decoded.peers or {}
    }
end

function Blockchain:load()
    local decoded = storage.read_json(self.file_name)
    if not decoded then
        local existing_contents = storage.read_file(self.file_name)
        if existing_contents and existing_contents ~= "" then
            storage.backup_file(self.file_name)
        end
        self:reset()
        return
    end

    local migrated = self:migrate_state(decoded)
    if not migrated then
        storage.backup_file(self.file_name)
        self:reset()
        return
    end

    self.meta = {
        consensus = "longest-valid-chain",
        difficulty_prefix = self.difficulty_prefix,
        mining_reward = self.mining_reward,
        name = self.name,
        node_id = self.node_id,
        node_url = self.node_url,
        schema_version = 3,
        updated_at = migrated.meta.updated_at or utc_now(),
        version = self.version
    }

    self.peers = {}
    if is_array(migrated.peers) then
        for _, peer in ipairs(migrated.peers) do
            local normalized_peer = trim(peer):gsub("/+$", "")
            if normalized_peer ~= "" then
                self.peers[#self.peers + 1] = normalized_peer
            end
        end
    end
    table.sort(self.peers)

    local normalized_chain = self:normalize_chain(migrated.chain)
    if not normalized_chain or #normalized_chain == 0 then
        storage.backup_file(self.file_name)
        self:reset()
        return
    end

    local valid_chain = self:validate_chain(normalized_chain)
    if not valid_chain then
        storage.backup_file(self.file_name)
        self:reset()
        return
    end

    self.chain = normalized_chain
    self.pending_transactions = {}
    if is_array(migrated.pending_transactions) then
        for _, transaction in ipairs(migrated.pending_transactions) do
            local normalized_transaction = self:normalize_transaction(transaction)
            if normalized_transaction and normalized_transaction.kind == "transfer" and not self:has_transaction(normalized_transaction.id) then
                self.pending_transactions[#self.pending_transactions + 1] = normalized_transaction
            end
        end
    end

    self:revalidate_pending_transactions()
    self:save()
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

    if self:calculate_hash(first_block) ~= first_block.hash then
        return false, "Genesis block hash is invalid"
    end

    for index = 2, #chain do
        local previous_block = chain[index - 1]
        local block = chain[index]

        if tonumber(block.index) ~= index then
            return false, "Block index mismatch at block " .. tostring(index)
        end

        if tostring(block.previous_hash) ~= tostring(previous_block.hash) then
            return false, "previous_hash mismatch at block " .. tostring(index)
        end

        local calculated_hash = self:calculate_hash(block)
        if calculated_hash ~= block.hash then
            return false, "Block hash mismatch at block " .. tostring(index)
        end

        if not self:is_valid_proof(previous_block.proof, block.proof, previous_block.hash) then
            return false, "Proof of work is invalid at block " .. tostring(index)
        end
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

    for _, transaction in ipairs(self.pending_transactions) do
        local applied = self:apply_transfer_to_state(working_state, transaction)
        if applied then
            selected_transactions[#selected_transactions + 1] = deep_copy(transaction)
            total_fees = round_currency(total_fees + transaction.fee) or total_fees
        else
            deferred_transactions[#deferred_transactions + 1] = deep_copy(transaction)
        end
    end

    local reward_transaction, reward_err = self:create_reward_transaction(normalized_miner, total_fees)
    if not reward_transaction then
        return nil, reward_err
    end

    selected_transactions[#selected_transactions + 1] = reward_transaction

    local proof = self:proof_of_work(previous_block.proof, previous_block.hash)
    local block = {
        index = #self.chain + 1,
        timestamp = utc_now(),
        transactions = selected_transactions,
        proof = proof,
        previous_hash = previous_block.hash,
        mined_by = normalized_miner
    }
    block.hash = self:calculate_hash(block)

    self.chain[#self.chain + 1] = block
    self.pending_transactions = deferred_transactions
    self:revalidate_pending_transactions()
    self:save()

    return deep_copy(block)
end

function Blockchain:register_peer(peer)
    local normalized = trim(peer):gsub("/+$", "")
    if normalized == "" then
        return nil, "peer is required"
    end

    if not normalized:match("^https?://[%w%.%-_:]+") then
        return nil, "peer must start with http:// or https://"
    end

    for _, existing in ipairs(self.peers) do
        if existing == normalized then
            return self:get_peers()
        end
    end

    self.peers[#self.peers + 1] = normalized
    table.sort(self.peers)
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

    if #normalized_chain <= #self.chain then
        return false, "Candidate chain is not longer than the local chain"
    end

    self.chain = normalized_chain
    self:revalidate_pending_transactions()
    self:save()

    return true, "Local chain replaced with a longer valid chain"
end

function Blockchain:append_block(candidate_block)
    local previous_block = self:get_previous_block()
    local normalized_block, err = self:normalize_block(candidate_block, previous_block.hash, #self.chain + 1)
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
    local best_length = #self.chain
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
                inspected[#inspected + 1] = {
                    peer = peer,
                    ok = valid,
                    error = valid and nil or reason,
                    blocks = #normalized_chain
                }
                if valid and #normalized_chain > best_length then
                    best_chain = normalized_chain
                    best_source = peer
                    best_length = #normalized_chain
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
            blocks = best_length,
            inspected = inspected
        }
    end

    return false, {
        source_peer = nil,
        blocks = #self.chain,
        inspected = inspected
    }
end

return Blockchain

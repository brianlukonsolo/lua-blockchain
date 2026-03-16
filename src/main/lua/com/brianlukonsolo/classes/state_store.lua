local cjson = require("classes.json")
local canonical_json = require("classes.canonical_json")
local storage = require("classes.storage")
local luasql = require("luasql.sqlite3")

local sqlite_environment = assert(luasql.sqlite3())

local StateStore = {}
StateStore.__index = StateStore

local function sql_quote(value)
    if value == nil then
        return "NULL"
    end

    if type(value) == "number" then
        return tostring(value)
    end

    if type(value) == "boolean" then
        return value and "1" or "0"
    end

    return "'" .. tostring(value):gsub("'", "''") .. "'"
end

local function close_cursor(cursor)
    if cursor and type(cursor) == "userdata" then
        pcall(function()
            cursor:close()
        end)
    end
end

local function tonumber_or_nil(value)
    if value == nil or value == "" then
        return nil
    end

    return tonumber(value)
end

function StateStore.new(path)
    return setmetatable({
        path = path or "blockchain.db"
    }, StateStore)
end

function StateStore:execute(conn, sql)
    local result, err = conn:execute(sql)
    if not result then
        return nil, err
    end

    close_cursor(result)
    return result
end

function StateStore:fetch_rows(conn, sql)
    local cursor, err = conn:execute(sql)
    if not cursor then
        return nil, err
    end

    local rows = {}
    local row = cursor:fetch({}, "a")
    while row do
        rows[#rows + 1] = row
        row = cursor:fetch({}, "a")
    end

    cursor:close()
    return rows
end

function StateStore:fetch_one(conn, sql)
    local rows, err = self:fetch_rows(conn, sql)
    if not rows then
        return nil, err
    end

    return rows[1]
end

function StateStore:fetch_count(conn, table_name)
    local row, err = self:fetch_one(conn, "SELECT COUNT(*) AS count FROM " .. table_name)
    if not row then
        return nil, err
    end

    return tonumber(row.count) or 0
end

function StateStore:has_column(conn, table_name, column_name)
    local columns, err = self:fetch_rows(conn, "PRAGMA table_info(" .. table_name .. ")")
    if not columns then
        return nil, err
    end

    for _, column in ipairs(columns) do
        if column.name == column_name then
            return true
        end
    end

    return false
end

function StateStore:open()
    storage.ensure_parent_directory(self.path)

    local conn, err = sqlite_environment:connect(self.path)
    if not conn then
        return nil, err
    end

    local statements = {
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=FULL",
        "PRAGMA foreign_keys=ON",
        "PRAGMA busy_timeout=5000"
    }

    for _, statement in ipairs(statements) do
        local ok, statement_err = self:execute(conn, statement)
        if not ok then
            conn:close()
            return nil, statement_err
        end
    end

    local schema_statements = {
        [[
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            )
        ]],
        [[
            CREATE TABLE IF NOT EXISTS blocks (
                idx INTEGER PRIMARY KEY,
                hash TEXT NOT NULL,
                previous_hash TEXT NOT NULL,
                proof INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                mined_by TEXT
            )
        ]],
        [[
            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                amount REAL NOT NULL,
                fee REAL NOT NULL,
                nonce INTEGER NOT NULL,
                note TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                public_key TEXT,
                signature TEXT
            )
        ]],
        [[
            CREATE TABLE IF NOT EXISTS block_transactions (
                block_index INTEGER NOT NULL,
                position INTEGER NOT NULL,
                transaction_id TEXT NOT NULL,
                PRIMARY KEY (block_index, position),
                UNIQUE (transaction_id),
                FOREIGN KEY (block_index) REFERENCES blocks(idx) ON DELETE CASCADE,
                FOREIGN KEY (transaction_id) REFERENCES transactions(id) ON DELETE CASCADE
            )
        ]],
        [[
            CREATE TABLE IF NOT EXISTS pending_transactions (
                position INTEGER PRIMARY KEY,
                transaction_id TEXT NOT NULL UNIQUE,
                FOREIGN KEY (transaction_id) REFERENCES transactions(id) ON DELETE CASCADE
            )
        ]],
        [[
            CREATE TABLE IF NOT EXISTS peers (
                url TEXT PRIMARY KEY
            )
        ]],
        "CREATE INDEX IF NOT EXISTS idx_block_transactions_transaction_id ON block_transactions(transaction_id)",
        "CREATE INDEX IF NOT EXISTS idx_pending_transactions_transaction_id ON pending_transactions(transaction_id)"
    }

    for _, statement in ipairs(schema_statements) do
        local ok, schema_err = self:execute(conn, statement)
        if not ok then
            conn:close()
            return nil, schema_err
        end
    end

    local has_difficulty_prefix, column_err = self:has_column(conn, "blocks", "difficulty_prefix")
    if has_difficulty_prefix == nil then
        conn:close()
        return nil, column_err
    end

    if not has_difficulty_prefix then
        local migrated, migrate_err = self:execute(conn, "ALTER TABLE blocks ADD COLUMN difficulty_prefix TEXT NOT NULL DEFAULT '0000'")
        if not migrated then
            conn:close()
            return nil, migrate_err
        end
    end

    local has_hash_format, hash_format_err = self:has_column(conn, "blocks", "hash_format")
    if has_hash_format == nil then
        conn:close()
        return nil, hash_format_err
    end

    if not has_hash_format then
        local migrated, migrate_err = self:execute(conn, "ALTER TABLE blocks ADD COLUMN hash_format TEXT NOT NULL DEFAULT 'legacy-v1'")
        if not migrated then
            conn:close()
            return nil, migrate_err
        end
    end

    local peer_columns = {
        { name = "source", sql = "ALTER TABLE peers ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'" },
        { name = "discovered_at", sql = "ALTER TABLE peers ADD COLUMN discovered_at TEXT" },
        { name = "last_seen_at", sql = "ALTER TABLE peers ADD COLUMN last_seen_at TEXT" },
        { name = "last_success_at", sql = "ALTER TABLE peers ADD COLUMN last_success_at TEXT" },
        { name = "last_failure_at", sql = "ALTER TABLE peers ADD COLUMN last_failure_at TEXT" },
        { name = "success_count", sql = "ALTER TABLE peers ADD COLUMN success_count INTEGER NOT NULL DEFAULT 0" },
        { name = "failure_count", sql = "ALTER TABLE peers ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0" },
        { name = "score", sql = "ALTER TABLE peers ADD COLUMN score REAL NOT NULL DEFAULT 0" },
        { name = "backoff_until", sql = "ALTER TABLE peers ADD COLUMN backoff_until TEXT" },
        { name = "ban_until", sql = "ALTER TABLE peers ADD COLUMN ban_until TEXT" },
        { name = "last_error", sql = "ALTER TABLE peers ADD COLUMN last_error TEXT" },
        { name = "node_id", sql = "ALTER TABLE peers ADD COLUMN node_id TEXT" },
        { name = "node_url", sql = "ALTER TABLE peers ADD COLUMN node_url TEXT" },
        { name = "version", sql = "ALTER TABLE peers ADD COLUMN version TEXT" },
        { name = "chain_id", sql = "ALTER TABLE peers ADD COLUMN chain_id TEXT" },
        { name = "capabilities_json", sql = "ALTER TABLE peers ADD COLUMN capabilities_json TEXT NOT NULL DEFAULT '{}'" },
        { name = "last_advertised_height", sql = "ALTER TABLE peers ADD COLUMN last_advertised_height INTEGER NOT NULL DEFAULT 0" },
        { name = "last_cumulative_work", sql = "ALTER TABLE peers ADD COLUMN last_cumulative_work REAL NOT NULL DEFAULT 0" }
    }

    for _, column in ipairs(peer_columns) do
        local has_column, peer_column_err = self:has_column(conn, "peers", column.name)
        if has_column == nil then
            conn:close()
            return nil, peer_column_err
        end

        if not has_column then
            local migrated, migrate_err = self:execute(conn, column.sql)
            if not migrated then
                conn:close()
                return nil, migrate_err
            end
        end
    end

    local integrity_row, integrity_err = self:fetch_one(conn, "PRAGMA integrity_check(1)")
    if not integrity_row then
        conn:close()
        return nil, integrity_err
    end

    local integrity_status = integrity_row.integrity_check
    if integrity_status and integrity_status ~= "ok" then
        conn:close()
        return nil, "sqlite integrity_check failed: " .. tostring(integrity_status)
    end

    return conn
end

function StateStore:load_state()
    local conn, err = self:open()
    if not conn then
        return nil, err
    end

    local block_count, block_count_err = self:fetch_count(conn, "blocks")
    if block_count == nil then
        conn:close()
        return nil, block_count_err
    end

    local metadata_count, metadata_count_err = self:fetch_count(conn, "metadata")
    if metadata_count == nil then
        conn:close()
        return nil, metadata_count_err
    end

    if block_count == 0 and metadata_count == 0 then
        conn:close()
        return nil, "state store is empty", "missing"
    end

    if block_count == 0 then
        conn:close()
        return nil, "state store does not contain any blocks"
    end

    local metadata_row, metadata_err = self:fetch_one(conn, "SELECT value_json FROM metadata WHERE key = 'meta'")
    if metadata_err then
        conn:close()
        return nil, metadata_err
    end

    local meta = {}
    if metadata_row and metadata_row.value_json then
        meta, metadata_err = cjson.decode(metadata_row.value_json)
        if not meta then
            conn:close()
            return nil, "stored metadata is not valid JSON: " .. tostring(metadata_err)
        end
    end

    local chain_rows, chain_err = self:fetch_rows(conn, [[
        SELECT idx, hash, previous_hash, proof, timestamp, mined_by, difficulty_prefix, hash_format
        FROM blocks
        ORDER BY idx ASC
    ]])
    if not chain_rows then
        conn:close()
        return nil, chain_err
    end

    local chain = {}
    for _, row in ipairs(chain_rows) do
        chain[#chain + 1] = {
            index = tonumber(row.idx),
            timestamp = row.timestamp,
            transactions = {},
            proof = tonumber(row.proof),
            previous_hash = row.previous_hash,
            hash = row.hash,
            mined_by = row.mined_by,
            difficulty_prefix = row.difficulty_prefix,
            hash_format = row.hash_format
        }
    end

    local block_transaction_rows, block_transaction_err = self:fetch_rows(conn, [[
        SELECT
            bt.block_index,
            bt.position,
            t.id,
            t.kind,
            t.sender,
            t.recipient,
            t.amount,
            t.fee,
            t.nonce,
            t.note,
            t.timestamp,
            t.public_key,
            t.signature
        FROM block_transactions bt
        INNER JOIN transactions t ON t.id = bt.transaction_id
        ORDER BY bt.block_index ASC, bt.position ASC
    ]])
    if not block_transaction_rows then
        conn:close()
        return nil, block_transaction_err
    end

    for _, row in ipairs(block_transaction_rows) do
        local block = chain[tonumber(row.block_index)]
        if not block then
            conn:close()
            return nil, "transaction references missing block " .. tostring(row.block_index)
        end

        block.transactions[#block.transactions + 1] = {
            id = row.id,
            kind = row.kind,
            sender = row.sender,
            recipient = row.recipient,
            amount = tonumber_or_nil(row.amount),
            fee = tonumber_or_nil(row.fee),
            nonce = tonumber_or_nil(row.nonce),
            note = row.note,
            timestamp = row.timestamp,
            public_key = row.public_key,
            signature = row.signature
        }
    end

    local pending_rows, pending_err = self:fetch_rows(conn, [[
        SELECT
            p.position,
            t.id,
            t.kind,
            t.sender,
            t.recipient,
            t.amount,
            t.fee,
            t.nonce,
            t.note,
            t.timestamp,
            t.public_key,
            t.signature
        FROM pending_transactions p
        INNER JOIN transactions t ON t.id = p.transaction_id
        ORDER BY p.position ASC
    ]])
    if not pending_rows then
        conn:close()
        return nil, pending_err
    end

    local pending_transactions = {}
    for _, row in ipairs(pending_rows) do
        pending_transactions[#pending_transactions + 1] = {
            id = row.id,
            kind = row.kind,
            sender = row.sender,
            recipient = row.recipient,
            amount = tonumber_or_nil(row.amount),
            fee = tonumber_or_nil(row.fee),
            nonce = tonumber_or_nil(row.nonce),
            note = row.note,
            timestamp = row.timestamp,
            public_key = row.public_key,
            signature = row.signature
        }
    end

    local peer_rows, peer_err = self:fetch_rows(conn, [[
        SELECT
            url,
            source,
            discovered_at,
            last_seen_at,
            last_success_at,
            last_failure_at,
            success_count,
            failure_count,
            score,
            backoff_until,
            ban_until,
            last_error,
            node_id,
            node_url,
            version,
            chain_id,
            capabilities_json,
            last_advertised_height,
            last_cumulative_work
        FROM peers
        ORDER BY url ASC
    ]])
    if not peer_rows then
        conn:close()
        return nil, peer_err
    end

    local peer_records = {}
    for _, row in ipairs(peer_rows) do
        local capabilities = {}
        if row.capabilities_json and row.capabilities_json ~= "" then
            capabilities = cjson.decode(row.capabilities_json) or {}
        end

        peer_records[#peer_records + 1] = {
            url = row.url,
            source = row.source,
            discovered_at = row.discovered_at,
            last_seen_at = row.last_seen_at,
            last_success_at = row.last_success_at,
            last_failure_at = row.last_failure_at,
            success_count = tonumber_or_nil(row.success_count) or 0,
            failure_count = tonumber_or_nil(row.failure_count) or 0,
            score = tonumber_or_nil(row.score) or 0,
            backoff_until = row.backoff_until,
            ban_until = row.ban_until,
            last_error = row.last_error,
            node_id = row.node_id,
            node_url = row.node_url,
            version = row.version,
            chain_id = row.chain_id,
            capabilities = capabilities,
            last_advertised_height = tonumber_or_nil(row.last_advertised_height) or 0,
            last_cumulative_work = tonumber_or_nil(row.last_cumulative_work) or 0
        }
    end

    conn:close()
    return {
        meta = meta,
        chain = chain,
        pending_transactions = pending_transactions,
        peer_records = peer_records
    }
end

function StateStore:save_state(state)
    local conn, err = self:open()
    if not conn then
        return nil, err
    end

    local ok, begin_err = self:execute(conn, "BEGIN IMMEDIATE")
    if not ok then
        conn:close()
        return nil, begin_err
    end

    local success = false
    local save_err = nil

    local function fail(message)
        save_err = message
        return false
    end

    local cleanup_statements = {
        "DELETE FROM block_transactions",
        "DELETE FROM pending_transactions",
        "DELETE FROM peers",
        "DELETE FROM transactions",
        "DELETE FROM blocks",
        "DELETE FROM metadata"
    }

    for _, statement in ipairs(cleanup_statements) do
        local cleanup_ok, cleanup_err = self:execute(conn, statement)
        if not cleanup_ok then
            fail(cleanup_err)
            break
        end
    end

    if not save_err then
        local meta_json = canonical_json.encode(state.meta or {})
        local metadata_ok, metadata_err = self:execute(conn, "INSERT INTO metadata(key, value_json) VALUES ('meta', " .. sql_quote(meta_json) .. ")")
        if not metadata_ok then
            fail(metadata_err)
        end
    end

    local stored_transaction_ids = {}

    local function insert_transaction(transaction)
        if stored_transaction_ids[transaction.id] then
            return true
        end

        stored_transaction_ids[transaction.id] = true
        return self:execute(conn, table.concat({
            "INSERT INTO transactions(",
            "id, kind, sender, recipient, amount, fee, nonce, note, timestamp, public_key, signature",
            ") VALUES (",
            table.concat({
                sql_quote(transaction.id),
                sql_quote(transaction.kind),
                sql_quote(transaction.sender),
                sql_quote(transaction.recipient),
                sql_quote(transaction.amount),
                sql_quote(transaction.fee),
                sql_quote(transaction.nonce),
                sql_quote(transaction.note or ""),
                sql_quote(transaction.timestamp),
                sql_quote(transaction.public_key),
                sql_quote(transaction.signature)
            }, ", "),
            ")"
        }))
    end

    if not save_err then
        for _, block in ipairs(state.chain or {}) do
            local block_ok, block_err = self:execute(conn, table.concat({
                "INSERT INTO blocks(idx, hash, previous_hash, proof, timestamp, mined_by, difficulty_prefix, hash_format) VALUES (",
                table.concat({
                    sql_quote(block.index),
                    sql_quote(block.hash),
                    sql_quote(block.previous_hash),
                    sql_quote(block.proof),
                    sql_quote(block.timestamp),
                    sql_quote(block.mined_by),
                    sql_quote(block.difficulty_prefix),
                    sql_quote(block.hash_format)
                }, ", "),
                ")"
            }))
            if not block_ok then
                fail(block_err)
                break
            end

            for position, transaction in ipairs(block.transactions or {}) do
                local transaction_ok, transaction_err = insert_transaction(transaction)
                if not transaction_ok then
                    fail(transaction_err)
                    break
                end

                local block_transaction_ok, block_transaction_err = self:execute(conn, table.concat({
                    "INSERT INTO block_transactions(block_index, position, transaction_id) VALUES (",
                    table.concat({
                        sql_quote(block.index),
                        sql_quote(position),
                        sql_quote(transaction.id)
                    }, ", "),
                    ")"
                }))
                if not block_transaction_ok then
                    fail(block_transaction_err)
                    break
                end
            end

            if save_err then
                break
            end
        end
    end

    if not save_err then
        for position, transaction in ipairs(state.pending_transactions or {}) do
            local transaction_ok, transaction_err = insert_transaction(transaction)
            if not transaction_ok then
                fail(transaction_err)
                break
            end

            local pending_ok, pending_insert_err = self:execute(conn, table.concat({
                "INSERT INTO pending_transactions(position, transaction_id) VALUES (",
                table.concat({
                    sql_quote(position),
                    sql_quote(transaction.id)
                }, ", "),
                ")"
            }))
            if not pending_ok then
                fail(pending_insert_err)
                break
            end
        end
    end

    if not save_err then
        for _, peer in ipairs(state.peer_records or state.peers or {}) do
            local record = type(peer) == "table" and peer or { url = peer }
            local capabilities_json = canonical_json.encode(record.capabilities or {})
            local peer_ok, peer_insert_err = self:execute(conn, table.concat({
                "INSERT INTO peers(",
                "url, source, discovered_at, last_seen_at, last_success_at, last_failure_at, success_count, failure_count, score, backoff_until, ban_until, last_error, node_id, node_url, version, chain_id, capabilities_json, last_advertised_height, last_cumulative_work",
                ") VALUES (",
                table.concat({
                    sql_quote(record.url),
                    sql_quote(record.source or "manual"),
                    sql_quote(record.discovered_at),
                    sql_quote(record.last_seen_at),
                    sql_quote(record.last_success_at),
                    sql_quote(record.last_failure_at),
                    sql_quote(record.success_count or 0),
                    sql_quote(record.failure_count or 0),
                    sql_quote(record.score or 0),
                    sql_quote(record.backoff_until),
                    sql_quote(record.ban_until),
                    sql_quote(record.last_error),
                    sql_quote(record.node_id),
                    sql_quote(record.node_url),
                    sql_quote(record.version),
                    sql_quote(record.chain_id),
                    sql_quote(capabilities_json),
                    sql_quote(record.last_advertised_height or 0),
                    sql_quote(record.last_cumulative_work or 0)
                }, ", "),
                ")"
            }))
            if not peer_ok then
                fail(peer_insert_err)
                break
            end
        end
    end

    if not save_err then
        local commit_ok, commit_err = self:execute(conn, "COMMIT")
        if not commit_ok then
            fail(commit_err)
        else
            success = true
        end
    end

    if not success then
        self:execute(conn, "ROLLBACK")
        conn:close()
        return nil, save_err
    end

    conn:close()
    return true
end

function StateStore:backup_to(destination_path)
    if not destination_path or destination_path == "" then
        return nil, "backup destination is required"
    end

    local conn, err = self:open()
    if not conn then
        return nil, err
    end

    storage.ensure_parent_directory(destination_path)
    os.remove(destination_path)

    local ok, backup_err = self:execute(conn, "VACUUM INTO " .. sql_quote(destination_path))
    conn:close()
    if not ok then
        return nil, backup_err
    end

    return destination_path
end

return StateStore

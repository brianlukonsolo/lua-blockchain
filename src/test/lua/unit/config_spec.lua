package.path = "/app/?.lua;/app/?/init.lua;" .. package.path
package.cpath = "/usr/lib/x86_64-linux-gnu/lua/5.1/?.so;" .. package.cpath

local ffi = require("ffi")
local config = require("classes.config")

ffi.cdef([[
int setenv(const char *name, const char *value, int overwrite);
int unsetenv(const char *name);
]])

local function assert_true(value, message)
    if not value then
        error(message or "assertion failed")
    end
end

local function assert_contains(haystack, needle, message)
    for _, value in ipairs(haystack or {}) do
        if tostring(value) == tostring(needle) then
            return
        end
    end

    error(message or ("expected to find " .. tostring(needle)))
end

local ENV_KEYS = {
    "BLOCKCHAIN_MODE",
    "BLOCKCHAIN_NODE_URL",
    "BLOCKCHAIN_CHAIN_ID",
    "BLOCKCHAIN_DATA_FILE",
    "BLOCKCHAIN_DIFFICULTY",
    "BLOCKCHAIN_TARGET_BLOCK_SECONDS",
    "BLOCKCHAIN_DIFFICULTY_ADJUSTMENT_WINDOW",
    "BLOCKCHAIN_MIN_DIFFICULTY_PREFIX_LENGTH",
    "BLOCKCHAIN_MAX_DIFFICULTY_PREFIX_LENGTH",
    "BLOCKCHAIN_ADMIN_TOKEN",
    "BLOCKCHAIN_PEER_SHARED_SECRET",
    "BLOCKCHAIN_REQUIRE_HTTPS_PEERS",
    "BLOCKCHAIN_ENABLE_SERVER_WALLETS",
    "BLOCKCHAIN_ALLOWED_PEER_IDS",
    "BLOCKCHAIN_ALLOWED_PEER_HOSTS",
    "BLOCKCHAIN_ALLOW_PLAINTEXT_GOSSIP",
    "BLOCKCHAIN_BOOTSTRAP_PEERS",
    "BLOCKCHAIN_BACKUP_DIR",
    "BLOCKCHAIN_P2P_ENABLED",
    "BLOCKCHAIN_P2P_DIAL_DISCOVERED_PEERS",
    "BLOCKCHAIN_P2P_TLS_ENABLED",
    "BLOCKCHAIN_P2P_TLS_CERT_PATH",
    "BLOCKCHAIN_P2P_TLS_KEY_PATH",
    "BLOCKCHAIN_NODE_IDENTITY_PRIVATE_KEY_PATH",
    "BLOCKCHAIN_NODE_IDENTITY_PUBLIC_KEY_PATH",
    "BLOCKCHAIN_GOSSIP_ENABLED"
}

local function set_env(name, value)
    local ok
    if value == nil then
        ok = ffi.C.unsetenv(name) == 0
    else
        ok = ffi.C.setenv(name, tostring(value), 1) == 0
    end

    assert_true(ok, "failed to update environment for " .. tostring(name))
end

local function with_env(overrides, fn)
    local previous = {}
    for _, name in ipairs(ENV_KEYS) do
        previous[name] = os.getenv(name)
        set_env(name, nil)
    end

    for name, value in pairs(overrides or {}) do
        set_env(name, value)
    end

    local ok, result = pcall(fn)

    for _, name in ipairs(ENV_KEYS) do
        set_env(name, previous[name])
    end

    if not ok then
        error(result)
    end

    return result
end

local valid_production_env

local function merge_env(overrides)
    local merged = {}
    for key, value in pairs(valid_production_env or {}) do
        merged[key] = value
    end
    for key, value in pairs(overrides or {}) do
        merged[key] = value
    end
    return merged
end

valid_production_env = {
    BLOCKCHAIN_MODE = "production",
    BLOCKCHAIN_NODE_URL = "https://node-1.example.com",
    BLOCKCHAIN_CHAIN_ID = "prod-chain",
    BLOCKCHAIN_DATA_FILE = "/tmp/config-spec.db",
    BLOCKCHAIN_DIFFICULTY = "0000",
    BLOCKCHAIN_TARGET_BLOCK_SECONDS = "30",
    BLOCKCHAIN_DIFFICULTY_ADJUSTMENT_WINDOW = "10",
    BLOCKCHAIN_MIN_DIFFICULTY_PREFIX_LENGTH = "2",
    BLOCKCHAIN_MAX_DIFFICULTY_PREFIX_LENGTH = "6",
    BLOCKCHAIN_ADMIN_TOKEN = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    BLOCKCHAIN_PEER_SHARED_SECRET = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    BLOCKCHAIN_REQUIRE_HTTPS_PEERS = "true",
    BLOCKCHAIN_ENABLE_SERVER_WALLETS = "false",
    BLOCKCHAIN_ALLOWED_PEER_IDS = "peer-node-1,peer-node-2",
    BLOCKCHAIN_ALLOW_PLAINTEXT_GOSSIP = "false",
    BLOCKCHAIN_BOOTSTRAP_PEERS = "https://node-2.example.com,https://node-3.example.com",
    BLOCKCHAIN_BACKUP_DIR = "/tmp/config-spec-backups",
    BLOCKCHAIN_P2P_ENABLED = "true",
    BLOCKCHAIN_P2P_DIAL_DISCOVERED_PEERS = "false",
    BLOCKCHAIN_P2P_TLS_ENABLED = "true",
    BLOCKCHAIN_P2P_TLS_CERT_PATH = "/tmp/config-spec-cert.pem",
    BLOCKCHAIN_P2P_TLS_KEY_PATH = "/tmp/config-spec-key.pem",
    BLOCKCHAIN_NODE_IDENTITY_PRIVATE_KEY_PATH = "/tmp/config-spec-node-private.pem",
    BLOCKCHAIN_NODE_IDENTITY_PUBLIC_KEY_PATH = "/tmp/config-spec-node-public.pem",
    BLOCKCHAIN_GOSSIP_ENABLED = "false"
}

with_env(valid_production_env, function()
    local cfg = config.load("test")
    assert_true(cfg.is_valid, table.concat(cfg.errors or {}, "\n"))
end)

with_env(merge_env({
    BLOCKCHAIN_GOSSIP_ENABLED = "true"
}), function()
    local cfg = config.load("test")
    assert_true(not cfg.is_valid, "production config should reject plaintext gossip by default")
    assert_contains(cfg.errors, "BLOCKCHAIN_GOSSIP_ENABLED must be false in production mode unless BLOCKCHAIN_ALLOW_PLAINTEXT_GOSSIP=true")
end)

with_env(merge_env({
    BLOCKCHAIN_ADMIN_TOKEN = "short"
}), function()
    local cfg = config.load("test")
    assert_true(not cfg.is_valid, "production config should reject short admin tokens")
    assert_contains(cfg.errors, "BLOCKCHAIN_ADMIN_TOKEN must be at least 32 characters in production mode")
end)

with_env(merge_env({
    BLOCKCHAIN_ALLOWED_PEER_IDS = "",
    BLOCKCHAIN_ALLOWED_PEER_HOSTS = ""
}), function()
    local cfg = config.load("test")
    assert_true(not cfg.is_valid, "production config should require an explicit peer allowlist")
    assert_contains(cfg.errors, "configure BLOCKCHAIN_ALLOWED_PEER_IDS or BLOCKCHAIN_ALLOWED_PEER_HOSTS in production mode")
end)

with_env(merge_env({
    BLOCKCHAIN_ALLOWED_PEER_IDS = "",
    BLOCKCHAIN_P2P_DIAL_DISCOVERED_PEERS = "true"
}), function()
    local cfg = config.load("test")
    assert_true(not cfg.is_valid, "production config should require peer IDs when dialing discovered peers")
    assert_contains(cfg.errors, "BLOCKCHAIN_ALLOWED_PEER_IDS must be configured when BLOCKCHAIN_P2P_DIAL_DISCOVERED_PEERS=true in production mode")
end)

with_env(merge_env({
    BLOCKCHAIN_BOOTSTRAP_PEERS = "http://node-2.example.com"
}), function()
    local cfg = config.load("test")
    assert_true(not cfg.is_valid, "production config should reject non-https bootstrap peers")
    assert_contains(cfg.errors, "BLOCKCHAIN_BOOTSTRAP_PEERS must only contain https URLs in production mode: http://node-2.example.com")
end)

print("All config tests passed")

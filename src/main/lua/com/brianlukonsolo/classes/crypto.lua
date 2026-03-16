local canonical_json = require("classes.canonical_json")
local hashing = require("cryptography.pure_lua_SHA.sha2")
local storage = require("classes.storage")

local crypto = {}

math.randomseed(os.time() + tonumber(tostring({}):match("0x(.*)"), 16))

local function trim(value)
    return tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

local function shell_quote(value)
    return "'" .. tostring(value):gsub("'", "'\\''") .. "'"
end

local function normalize_newlines(value)
    return tostring(value or ""):gsub("\r\n", "\n"):gsub("\r", "\n")
end

local function encode_base64(value)
    return (hashing.bin_to_base64(tostring(value or "")):gsub("%s+", ""))
end

local function decode_base64(value)
    local normalized = trim(value):gsub("%s+", "")
    if normalized == "" then
        return nil
    end

    local ok, decoded = pcall(hashing.base64_to_bin, normalized)
    if not ok then
        return nil
    end

    return decoded
end

local function allocate_paths(prefix)
    local token = table.concat({
        tostring(os.time()),
        tostring(math.random(100000, 999999)),
        tostring(math.random(100000, 999999))
    }, "-")

    local base = "/tmp/lua-blockchain-" .. prefix .. "-" .. token
    return {
        private_key = base .. ".private.pem",
        public_key = base .. ".public.pem",
        message = base .. ".message",
        signature = base .. ".signature",
        stderr = base .. ".stderr"
    }
end

local function cleanup(paths)
    for _, path in pairs(paths) do
        os.remove(path)
    end
end

local function der_integer(value)
    local normalized = value
    while #normalized > 1 and normalized:byte(1) == 0 do
        normalized = normalized:sub(2)
    end

    if normalized:byte(1) >= 0x80 then
        normalized = string.char(0) .. normalized
    end

    return string.char(0x02, #normalized) .. normalized
end

local function raw_p256_signature_to_der(signature)
    if #signature ~= 64 then
        return signature
    end

    local r = der_integer(signature:sub(1, 32))
    local s = der_integer(signature:sub(33, 64))
    local sequence = r .. s

    return string.char(0x30, #sequence) .. sequence
end

local function execute(command, stderr_path)
    local ok, _, exit_code = os.execute(command .. " 1>/dev/null 2>" .. shell_quote(stderr_path))
    local stderr = trim(storage.read_file(stderr_path) or "")

    if ok == true then
        return (exit_code == nil or exit_code == 0), stderr
    end

    if type(ok) == "number" then
        return ok == 0, stderr
    end

    return false, stderr
end

local function normalize_pem(value, label, required_header_patterns)
    local normalized = trim(normalize_newlines(value))
    if normalized == "" then
        return nil, string.lower(label) .. " is required"
    end

    local has_header = false
    for _, pattern in ipairs(required_header_patterns) do
        if normalized:match(pattern) then
            has_header = true
            break
        end
    end

    if not has_header then
        return nil, string.lower(label) .. " is invalid"
    end

    return normalized .. "\n"
end

function crypto.normalize_public_key_pem(public_key)
    return normalize_pem(public_key, "public key", {
        "^%-%-%-%-%-BEGIN PUBLIC KEY%-%-%-%-%-"
    })
end

function crypto.normalize_private_key_pem(private_key)
    return normalize_pem(private_key, "private key", {
        "^%-%-%-%-%-BEGIN PRIVATE KEY%-%-%-%-%-",
        "^%-%-%-%-%-BEGIN EC PRIVATE KEY%-%-%-%-%-"
    })
end

function crypto.normalize_signature(signature)
    local normalized = trim(signature):gsub("%s+", "")
    if normalized == "" then
        return nil, "signature is required"
    end

    return normalized
end

function crypto.normalize_address(address)
    local normalized = trim(address):lower()
    if normalized == "" then
        return nil, "address is required"
    end

    if not normalized:match("^lbc_[0-9a-f]+$") then
        return nil, "address is invalid"
    end

    return normalized
end

function crypto.address_from_public_key(public_key)
    local normalized, err = crypto.normalize_public_key_pem(public_key)
    if not normalized then
        return nil, err
    end

    return "lbc_" .. hashing.sha256(normalized):sub(1, 40)
end

function crypto.build_signing_payload(transaction)
    return {
        amount = tonumber(transaction.amount) or transaction.amount,
        fee = tonumber(transaction.fee) or 0,
        kind = "transfer",
        nonce = tonumber(transaction.nonce) or transaction.nonce,
        note = tostring(transaction.note or ""),
        recipient = tostring(transaction.recipient or ""),
        sender = tostring(transaction.sender or ""),
        timestamp = tostring(transaction.timestamp or "")
    }
end

function crypto.build_signing_message(transaction)
    return canonical_json.encode(crypto.build_signing_payload(transaction))
end

function crypto.create_wallet()
    local paths = allocate_paths("wallet")

    local generate_ok, generate_err = execute(
        "openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out " .. shell_quote(paths.private_key),
        paths.stderr
    )
    if not generate_ok then
        cleanup(paths)
        return nil, generate_err ~= "" and generate_err or "Unable to generate private key"
    end

    local public_ok, public_err = execute(
        "openssl pkey -in " .. shell_quote(paths.private_key) .. " -pubout -out " .. shell_quote(paths.public_key),
        paths.stderr
    )
    if not public_ok then
        cleanup(paths)
        return nil, public_err ~= "" and public_err or "Unable to derive public key"
    end

    local private_key = storage.read_file(paths.private_key)
    local public_key = storage.read_file(paths.public_key)
    cleanup(paths)

    if not private_key or not public_key then
        return nil, "Unable to read generated wallet files"
    end

    local normalized_private, private_err = crypto.normalize_private_key_pem(private_key)
    if not normalized_private then
        return nil, private_err
    end

    local normalized_public, public_key_err = crypto.normalize_public_key_pem(public_key)
    if not normalized_public then
        return nil, public_key_err
    end

    local address, address_err = crypto.address_from_public_key(normalized_public)
    if not address then
        return nil, address_err
    end

    return {
        private_key = normalized_private,
        public_key = normalized_public,
        address = address
    }
end

function crypto.sign_message(private_key, message)
    local normalized_private, private_err = crypto.normalize_private_key_pem(private_key)
    if not normalized_private then
        return nil, private_err
    end

    local paths = allocate_paths("sign")
    storage.write_file(paths.private_key, normalized_private)
    storage.write_file(paths.message, tostring(message or ""))

    local ok, sign_err = execute(
        "openssl dgst -sha256 -sign " .. shell_quote(paths.private_key) ..
            " -out " .. shell_quote(paths.signature) ..
            " " .. shell_quote(paths.message),
        paths.stderr
    )
    if not ok then
        cleanup(paths)
        return nil, sign_err ~= "" and sign_err or "Unable to sign message"
    end

    local signature = storage.read_file(paths.signature)
    cleanup(paths)

    if not signature then
        return nil, "Unable to read generated signature"
    end

    return encode_base64(signature)
end

function crypto.verify_message(public_key, message, signature)
    local normalized_public, public_err = crypto.normalize_public_key_pem(public_key)
    if not normalized_public then
        return false, public_err
    end

    local normalized_signature, signature_err = crypto.normalize_signature(signature)
    if not normalized_signature then
        return false, signature_err
    end

    local decoded_signature = decode_base64(normalized_signature)
    if not decoded_signature or decoded_signature == "" then
        return false, "signature is not valid base64"
    end

    decoded_signature = raw_p256_signature_to_der(decoded_signature)

    local paths = allocate_paths("verify")
    storage.write_file(paths.public_key, normalized_public)
    storage.write_file(paths.message, tostring(message or ""))
    storage.write_file(paths.signature, decoded_signature)

    local ok, verify_err = execute(
        "openssl dgst -sha256 -verify " .. shell_quote(paths.public_key) ..
            " -signature " .. shell_quote(paths.signature) ..
            " " .. shell_quote(paths.message),
        paths.stderr
    )
    cleanup(paths)

    if not ok then
        return false, verify_err ~= "" and verify_err or "signature verification failed"
    end

    return true
end

function crypto.verify_transaction_signature(transaction)
    local sender = crypto.normalize_address(transaction.sender)
    if not sender then
        return false, "sender address is invalid"
    end

    local derived_address, address_err = crypto.address_from_public_key(transaction.public_key)
    if not derived_address then
        return false, address_err
    end

    if sender ~= derived_address then
        return false, "sender address does not match the supplied public key"
    end

    return crypto.verify_message(transaction.public_key, crypto.build_signing_message(transaction), transaction.signature)
end

return crypto

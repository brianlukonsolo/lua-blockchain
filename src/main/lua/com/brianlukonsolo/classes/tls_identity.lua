local ssl = require("ssl")
local storage = require("classes.storage")

local tls_identity = {}

local function trim(value)
    return tostring(value or ""):gsub("^%s+", ""):gsub("%s+$", "")
end

local function shell_quote(value)
    return "'" .. tostring(value):gsub("'", "'\\''") .. "'"
end

local function execute(command)
    local ok, _, exit_code = os.execute(command)
    if ok == true then
        return exit_code == nil or exit_code == 0
    end

    if type(ok) == "number" then
        return ok == 0
    end

    return false
end

local function load_existing(cert_path, key_path)
    local certificate = storage.read_file(cert_path)
    local key = storage.read_file(key_path)
    if not certificate or not key then
        return nil
    end

    local cert, cert_err = ssl.loadcertificate(certificate)
    if not cert then
        return nil, cert_err
    end

    return {
        certificate = certificate,
        key = key,
        fingerprint = trim(cert:digest("sha256")):lower(),
        certificate_path = cert_path,
        key_path = key_path
    }
end

function tls_identity.load_or_create(options, common_name)
    local cert_path = options.p2p_tls_cert_path or "/app/data/node_p2p_cert.pem"
    local key_path = options.p2p_tls_key_path or "/app/data/node_p2p_key.pem"

    local existing, err = load_existing(cert_path, key_path)
    if existing then
        return existing
    end

    storage.ensure_parent_directory(cert_path)
    storage.ensure_parent_directory(key_path)

    local subject = "/CN=" .. trim(common_name ~= "" and common_name or "lua-blockchain-p2p")
    local generated_key = execute(
        "openssl ecparam -name prime256v1 -genkey -noout -out " .. shell_quote(key_path)
    )
    if not generated_key then
        return nil, "unable to generate the P2P TLS private key"
    end

    local generated_cert = execute(
        "openssl req -new -x509 -sha256 -key " .. shell_quote(key_path) ..
            " -out " .. shell_quote(cert_path) ..
            " -days 3650 -subj " .. shell_quote(subject)
    )
    if not generated_cert then
        return nil, "unable to generate the P2P TLS certificate"
    end

    local created, create_err = load_existing(cert_path, key_path)
    if not created then
        return nil, create_err or err or "unable to load the generated P2P TLS certificate"
    end

    execute("chmod 600 " .. shell_quote(key_path))
    execute("chmod 644 " .. shell_quote(cert_path))

    return created
end

return tls_identity

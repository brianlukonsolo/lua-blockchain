local canonical_json = require("classes.canonical_json")
local crypto = require("classes.crypto")
local storage = require("classes.storage")

local node_identity = {}

local function load_existing(private_key_path, public_key_path)
    local private_key = storage.read_file(private_key_path)
    local public_key = storage.read_file(public_key_path)
    if not private_key or not public_key then
        return nil
    end

    local normalized_private, private_err = crypto.normalize_private_key_pem(private_key)
    if not normalized_private then
        return nil, private_err
    end

    local normalized_public, public_err = crypto.normalize_public_key_pem(public_key)
    if not normalized_public then
        return nil, public_err
    end

    local peer_id, peer_id_err = crypto.address_from_public_key(normalized_public)
    if not peer_id then
        return nil, peer_id_err
    end

    return {
        private_key = normalized_private,
        public_key = normalized_public,
        peer_id = peer_id,
        private_key_path = private_key_path,
        public_key_path = public_key_path
    }
end

function node_identity.load_or_create(options)
    local private_key_path = options.node_identity_private_key_path or "/app/data/node_identity_private.pem"
    local public_key_path = options.node_identity_public_key_path or "/app/data/node_identity_public.pem"

    local existing, err = load_existing(private_key_path, public_key_path)
    if existing then
        return existing
    end

    local generated, generate_err = crypto.create_wallet()
    if not generated then
        return nil, generate_err or err
    end

    local wrote_private, private_write_err = storage.write_file(private_key_path, generated.private_key)
    if not wrote_private then
        return nil, private_write_err
    end

    local wrote_public, public_write_err = storage.write_file(public_key_path, generated.public_key)
    if not wrote_public then
        return nil, public_write_err
    end

    return {
        private_key = generated.private_key,
        public_key = generated.public_key,
        peer_id = generated.address,
        private_key_path = private_key_path,
        public_key_path = public_key_path
    }
end

function node_identity.build_signed_envelope(identity, message_type, payload)
    local signature, sign_err = crypto.sign_message(identity.private_key, canonical_json.encode(payload))
    if not signature then
        return nil, sign_err
    end

    return {
        type = message_type,
        payload = payload,
        signature = signature
    }
end

function node_identity.verify_signed_payload(payload, signature)
    if type(payload) ~= "table" then
        return false, "payload must be a table"
    end

    local public_key = payload.public_key
    local peer_id = payload.peer_id
    if not public_key then
        return false, "payload public_key is required"
    end

    if not peer_id then
        return false, "payload peer_id is required"
    end

    local derived_peer_id, peer_id_err = crypto.address_from_public_key(public_key)
    if not derived_peer_id then
        return false, peer_id_err
    end

    if derived_peer_id ~= peer_id then
        return false, "payload peer_id does not match the supplied public key"
    end

    return crypto.verify_message(public_key, canonical_json.encode(payload), signature)
end

return node_identity

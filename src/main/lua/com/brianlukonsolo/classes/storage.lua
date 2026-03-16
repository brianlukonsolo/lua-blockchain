local cjson = require("classes.json")
local canonical_json = require("classes.canonical_json")

local storage = {}

local function ensure_parent_directory(path)
    local directory = tostring(path):match("^(.*)/[^/]+$")
    if directory and directory ~= "" then
        os.execute("mkdir -p " .. "'" .. directory:gsub("'", "'\\''") .. "'")
    end
end

storage.ensure_parent_directory = ensure_parent_directory

function storage.read_file(path)
    local handle, err = io.open(path, "r")
    if not handle then
        return nil, err
    end

    local contents = handle:read("*a")
    handle:close()

    return contents
end

function storage.file_exists(path)
    local handle = io.open(path, "r")
    if not handle then
        return false
    end

    handle:close()
    return true
end

function storage.write_file(path, contents)
    ensure_parent_directory(path)

    local handle, err = io.open(path, "w")
    if not handle then
        return nil, err
    end

    handle:write(contents)
    handle:close()

    return true
end

function storage.copy_file(source_path, destination_path)
    local contents, err = storage.read_file(source_path)
    if contents == nil then
        return nil, err
    end

    return storage.write_file(destination_path, contents)
end

function storage.read_json(path)
    local contents, err = storage.read_file(path)
    if not contents or contents == "" then
        return nil, err or "empty file"
    end

    return cjson.decode(contents)
end

function storage.atomic_write_json(path, value)
    local encoded = canonical_json.encode(value)

    local tmp_path = path .. ".tmp"
    local ok, write_err = storage.write_file(tmp_path, encoded)
    if not ok then
        return nil, write_err
    end

    local renamed, rename_err = os.rename(tmp_path, path)
    if not renamed then
        os.remove(tmp_path)
        return nil, rename_err
    end

    return true
end

function storage.backup_file(path)
    local contents = storage.read_file(path)
    if not contents then
        return nil
    end

    local backup_path = path .. ".bak-" .. os.date("!%Y%m%d%H%M%S")
    local ok, err = storage.write_file(backup_path, contents)
    if not ok then
        return nil, err
    end

    return backup_path
end

return storage

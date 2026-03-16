local ok, safe = pcall(require, "cjson.safe")
if ok and safe then
    return safe
end

local cjson_ok, cjson = pcall(require, "cjson")
if not cjson_ok then
    package.cpath = "/usr/lib/x86_64-linux-gnu/lua/5.1/?.so;" .. package.cpath
    cjson = require("cjson")
end

local json = {
    null = cjson.null
}

function json.encode(value)
    return cjson.encode(value)
end

function json.decode(value)
    local success, result = pcall(cjson.decode, value)
    if success then
        return result
    end

    return nil, result
end

return json

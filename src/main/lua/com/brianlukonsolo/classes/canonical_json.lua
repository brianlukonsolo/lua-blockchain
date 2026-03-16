local cjson = require("classes.json")

local canonical_json = {}

local function is_array(tbl)
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

local function encode(value)
    if value == cjson.null or value == nil then
        return "null"
    end

    local value_type = type(value)

    if value_type == "string" or value_type == "number" or value_type == "boolean" then
        local encoded = cjson.encode(value)
        if not encoded then
            error("Unable to encode JSON primitive")
        end
        return encoded
    end

    if value_type ~= "table" then
        error("Unsupported value type for canonical JSON: " .. value_type)
    end

    if is_array(value) then
        local parts = {}
        for index = 1, #value do
            parts[index] = encode(value[index])
        end
        return "[" .. table.concat(parts, ",") .. "]"
    end

    local keys = {}
    for key in pairs(value) do
        keys[#keys + 1] = key
    end
    table.sort(keys, function(left, right)
        return tostring(left) < tostring(right)
    end)

    local parts = {}
    for index, key in ipairs(keys) do
        parts[index] = cjson.encode(tostring(key)) .. ":" .. encode(value[key])
    end

    return "{" .. table.concat(parts, ",") .. "}"
end

canonical_json.encode = encode

return canonical_json

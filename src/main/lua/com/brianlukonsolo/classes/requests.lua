local http = require("socket.http")
local https = require("ssl.https")
local ltn12 = require("ltn12")

local requests = {}

local function choose_transport(url)
    if tostring(url):match("^https://") then
        return https
    end

    return http
end

local function send_request(method, url, payload, headers, options)
    local response_body = {}
    local transport = choose_transport(url)
    local request_headers = headers or {}
    local body = payload and tostring(payload) or nil
    local timeout_seconds = tonumber((options or {}).timeout_seconds) or 5

    transport.TIMEOUT = timeout_seconds

    if body then
        request_headers["Content-Length"] = #body
    end

    local _, status_code, response_headers, status_text = transport.request({
        url = url,
        method = method,
        headers = request_headers,
        source = body and ltn12.source.string(body) or nil,
        sink = ltn12.sink.table(response_body)
    })

    local numeric_status = tonumber(status_code) or 0

    return {
        ok = numeric_status >= 200 and numeric_status < 300,
        status_code = numeric_status,
        status_text = status_text,
        headers = response_headers or {},
        body = table.concat(response_body)
    }
end

function requests.send_get_request(url, headers, options)
    return send_request("GET", url, nil, headers, options)
end

function requests.send_post_request(url, payload, headers, options)
    local request_headers = headers or {}
    request_headers["Content-Type"] = request_headers["Content-Type"] or "application/json"
    return send_request("POST", url, payload, request_headers, options)
end

return requests

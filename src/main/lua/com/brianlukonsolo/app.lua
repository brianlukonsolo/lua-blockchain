local lapis = require("lapis")
local config = require("lapis.config").get()
local app = lapis.Application()
local blockchain = require("classes.blockchain")

--override error stacktrace function
function app:handle_error(err, trace)
  local response = { error = err }
  return { status = 500, content_type = "application/json", json = response }
end

--configuration information endpoint
app:get("/configuration", function()
  local response = {
    server = config.server,
    port = config.port,
    _name = config._name,
    num_workers = config.num_workers,
    logging = config.logging,
    session_name = config.session_name,
    code_cache = config.code_cache
  }
  return { content_type = "application/json", json = response }
end)

--default landing page
app:get("/", function()
  local response = { message = 'Welcome to lua-blockchain - Developed by Brian Lukonsolo' }
  return { content_type = "application/json", json = response }
end)

--hashing endpoints
app:get("/hashing/sha256", function()
  local response = {message = 'use the url format: http://example-host:port/sha256/{string}' }
  return { content_type = "application/json", json = response }
end)

app:get("/hashing/sha256/:str", function(self)
  local response = { input = self.params.str, hash = tostring(blockchain.getSha256HashOfString(self.params.str)) }
  return { content_type = "application/json", json = response }
end)

--blockchain endpoints
--TODO

return app

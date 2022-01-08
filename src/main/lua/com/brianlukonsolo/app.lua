local lapis = require("lapis")
local app = lapis.Application()
local blockchain = require("classes.blockchain")

app:get("/", function()
  return "Welcome to lua-blockchain. Running using lapis version " .. require("lapis.version")
end)

app:get("/hash", function()
  return "Hash test: string 'brian' when hashed with sha256 is: "
          .. tostring("\n" .. blockchain.getSha256HashOf('brian'))
end)

return app

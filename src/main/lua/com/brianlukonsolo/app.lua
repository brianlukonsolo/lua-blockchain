local lapis = require("lapis")
local config = require("lapis.config").get()
local app = lapis.Application()
local blockchain = require("classes.blockchain")
--initialize the blockchain with a name and version
blockchain.chain = blockchain.readBlockchainFromFile()
print("### chain items: " ..  #blockchain.chain)
if #blockchain.chain < 1 or blockchain.chain == nil then
    blockchain.init() --TODO:// if starting with empty table, the initial block generates an error and the 2 block's previous_hash is wrong. From then onwards all hashes are correct
end
--TODO: ensure that the initial block is added to the json file with the correct structure etc, perhaps make a class?
--[[
TODO: mining the initial block generates an error but correctly creates a block to replace the default file contents
{
    "error": "./app.lua:59: attempt to index local 'previousBlock' (a nil value)",
    "stacktrace": "\nstack traceback:\n\t./app.lua:59: in function 'handler'\n\t/usr/local/share/lua/5.1/lapis/application.lua:147: in function 'resolve'\n\t/usr/local/share/lua/5.1/lapis/application.lua:184: in function </usr/local/share/lua/5.1/lapis/application.lua:182>\n\t[C]: in function 'xpcall'\n\t/usr/local/share/lua/5.1/lapis/application.lua:190: in function 'dispatch'\n\t/usr/local/share/lua/5.1/lapis/nginx.lua:231: in function 'serve'\n\tcontent_by_lua(nginx.conf.compiled:22):2: in main chunk"
}
--]]

--override error stacktrace function
function app:handle_error(err, trace)
    local response = { error = err, stacktrace = trace }

    return { success = false, status = 500, content_type = "application/json", json = response }
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
    local response = { message = 'use the url format: http://example-host:port/sha256/{string}' }

    return { content_type = "application/json", json = response }
end)

app:get("/hashing/sha256/:str", function(self)
    local response = { input = self.params.str, hash = tostring(blockchain.getSha256HashOfString(self.params.str)) }

    return { content_type = "application/json", json = response }
end)

--blockchain endpoints
app:get("/mine_block", function()
    print("blockchain is >>>>> " .. tostring(blockchain.chain))
    --first need the proof from the last block in the chain
    local previousBlock = blockchain.getPreviousBlock()
    local previousProof = previousBlock.proof
    -- get the proof of work
    local proof = blockchain.proofOfWork(previousProof)
    local previousHash = blockchain.hash(previousBlock)
    --now I can create the block
    local block = blockchain.createBlock(proof, previousHash)
    table.insert(blockchain.chain, block)
    local response = {
        message = "new block has been mined and added to the blockchain",
        index = block.index,
        timestamp = block.timestamp,
        proof = block.proof,
        previous_hash = block.previous_hash
    }

    return { content_type = "application/json", json = response }
end)

app:get("/get_chain", function()
    local response = {
        chain = blockchain.chain,
        length = #blockchain.chain
    }

    return { content_type = "application/json", json = response }
end)

app:get("/validate_chain", function()
    local messageString = ''
    local isValid = blockchain.isChainValid(blockchain.chain)
    if isValid == true then
        messageString = 'blockchain is valid'
    else
        messageString = 'blockchain is invalid'
    end
    local response = { message = messageString }

    return { content_type = "application/json", json = response }
end)

return app

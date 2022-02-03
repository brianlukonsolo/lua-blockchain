---
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by brian.
--- DateTime: 08/01/2022 18:58
---
local hashing = require("cryptography.pure_lua_SHA.sha2")
local cjson = require("cjson")
local util = require "cjson.util"
--rules
local hashOperationLeadingZeros = '0000'
local _Blockchain = {}
_Blockchain.chain = {}
_Blockchain.name = "lua-blockchain"
_Blockchain.version = "1.0"
_Blockchain.file_name = "blockchain_data.json"

-- #### utility functions
local function LOG(message)
    print(message)
end

function _Blockchain.getWelcomeMessage()
    return "Hi Brian, your Lua module is working when imported into app.lua"
end

function _Blockchain.getSha256HashOfString(string)
    return hashing.sha256(string)
end

function _Blockchain.getTimeStamp()
    return os.date("date %x time %H:%M:%S:%m")
end

function _Blockchain.encodeTableToJson(table)
    return cjson.encode(table)
end

function _Blockchain.convertToStandardBlock(blockWithKeysInWrongOrder)
    LOG("### converting block to key ordered block ...")
    local blockWithKeysInOrder = {
        index = blockWithKeysInWrongOrder.index,
        timestamp =blockWithKeysInWrongOrder.timestamp,
        proof = blockWithKeysInWrongOrder.proof,
        previous_hash = blockWithKeysInWrongOrder.previous_hash
    }

    return blockWithKeysInOrder
end
--
--function _Blockchain.convertStandardBlockToSortedBlockJsonString(block)
--    local stringStart = '{'
--    local endOfString = '}'
--    local sortedTable = {}
--
--    for key in pairs(block) do table.insert(sortedTable, key) end
--    table.sort(sortedTable)
--    for index,key2 in ipairs(sortedTable) do
--        sortedTable[key2]=block[key2]
--        stringStart = stringStart .. "\"" .. key2 .. "\":" .. "\"" .. block[key2] .. "\","
--    end
--    local strWithTrailingComma = stringStart .. endOfString
--    local str = string.sub(strWithTrailingComma, 1, #strWithTrailingComma-2) .. "}"
--
--    return str
--end

function _Blockchain.saveBlockchainToFile(t, filename)
    LOG("### saving blockchain to file ...")
    local path = filename
    local file = io.open(path, "w")

    local convertedTable = {}
    for i, k in ipairs(t) do
        convertedTable[i] = _Blockchain.convertToStandardBlock(t[i])
    end

    if file then
        local contents = cjson.encode(t)
        file:write( contents )
        io.close( file )
        LOG("========> " .. tostring(contents))
        return true
    else
        return false
    end
end

function _Blockchain.readBlockchainFromFile()
    LOG("### loading blockchain from file ...")
    local jsonText = util.file_load(_Blockchain.file_name)
    local jsonBlockchainObj = cjson.decode(jsonText)

    return jsonBlockchainObj
end

-- #### blockchain methods
function _Blockchain.createBlock(proofInt, previousHashHexString)
    LOG("### creating new block ...")
    local block = {
        index = #_Blockchain.chain + 1,
        timestamp = _Blockchain.getTimeStamp(),
        proof = proofInt,
        previous_hash = previousHashHexString
    }
    local chain = _Blockchain.readBlockchainFromFile()
    table.insert(chain, _Blockchain.convertToStandardBlock(block))
    _Blockchain.saveBlockchainToFile(chain, _Blockchain.file_name)

    return block
end

function _Blockchain.init()
    LOG("### initialising blockchain ...")
    return _Blockchain.createBlock(1, 0)
end

function _Blockchain.getPreviousBlock()
    LOG("### getting previous block ...")
    return _Blockchain.convertToStandardBlock(_Blockchain.chain[#_Blockchain.chain]) -- get last (most recent) block in the table
end

function _Blockchain.proofOfWork(previousProof)
    LOG("### getting proof of work ...")
    local newProof = 1
    local checkProof = false

    while checkProof == false do
        local hashOperation = _Blockchain.getSha256HashOfString(tostring(newProof * 2 ^ 2 - previousProof * 2 ^ 2))
        -- As soon as we find the operation that results in a hash with 4 leading zeroes, the miner wins
        -- The more leading zeroes required, the harder it is to mine a block
        if string.sub(hashOperation,1,4) == hashOperationLeadingZeros then
            --TODO ^ensure hash operation^ can equal 0000
            checkProof = true
        else
            newProof = newProof + 1
        end
    end
    return newProof
end

--[[ function _Blockchain.hash(block)
    LOG("### calculating hash value ...")
    LOG( "JSON ======>>> " .. tostring(_Blockchain.encodeTableToJson(
            _Blockchain.convertToStandardBlock(block) --convert to avoid different hash caused by json key order
    )))
    return _Blockchain.getSha256HashOfString(
tostring(
        _Blockchain.encodeTableToJson((
                    _Blockchain.convertToStandardBlock(block) --convert to avoid different hash caused by json key order
            )
    )))
end ]]

 -- hash() replacement
function _Blockchain.hash(block)
    LOG("### calculating hash value ... by string concatenating")
    LOG( "JSON ======>>> " .. tostring(block.previous_hash) .. block.index)
    return _Blockchain.getSha256HashOfString(tostring(block.previous_hash .. block.index .. block.proof .. block.timestamp))
end


function _Blockchain.isChainValid(chain)
    LOG("### validating blockchain ...")
 --   local previousBlock = _Blockchain.chain[#_Blockchain.chain]
 --   local blockIndex = 1
   
    local blockIndex = #_Blockchain.chain
    local previousBlock = _Blockchain.chain[blockIndex - 1]
    
    while blockIndex > 1  do
        local block = _Blockchain.chain[blockIndex]
        LOG("### previousHash: " .. block.previous_hash)
        -- if the previous hash of the current block is not the same as the previous block, there is a problem
        if block.previous_hash ~= _Blockchain.hash(previousBlock) then
            return false
        end
        LOG("##############  validationcheck : " .. blockIndex .. "  :: TRUE")
        -- if proof starts with 4 leading zeroes (see get_proof_of_work) it is valid
        --[[ local previousProof = previousBlock.proof
        --local proof = block.proof

        local hashOperation = _Blockchain.getSha256HashOfString(tostring(newProof * 2 ^ 2 - previousProof * 2 ^ 2))
        if string.sub(hashOperation,1,4) ~= hashOperationLeadingZeros then
            --TODO ^ensure hash operation can equal 0000
            return false
        end ]]
        LOG("### BLOCK INDEX previousHash: " .. blockIndex)
        blockIndex = blockIndex - 1
        previousBlock = _Blockchain.chain[blockIndex - 1]
    end

    return true
end

return _Blockchain

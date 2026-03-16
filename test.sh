docker compose run --rm lua_blockchain sh -lc '
set -eu
luajit /tests/lua/unit/blockchain_spec.lua
luajit /tests/lua/unit/config_spec.lua
'

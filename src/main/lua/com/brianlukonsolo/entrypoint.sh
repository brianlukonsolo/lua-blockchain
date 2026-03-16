#!/bin/sh
set -eu

mkdir -p /app/data /app/logs /tests

if id nobody >/dev/null 2>&1; then
    chown -R nobody:nogroup /app/data /app/logs 2>/dev/null || chown -R nobody:nobody /app/data /app/logs 2>/dev/null || true
fi

chmod 0770 /app/data /app/logs 2>/dev/null || true

if [ "$#" -eq 0 ]; then
    set -- openresty -p /app/ -c /app/nginx.conf
fi

if [ "${1:-}" = "openresty" ]; then
    p2p_enabled="$(printf '%s' "${BLOCKCHAIN_P2P_ENABLED:-true}" | tr '[:upper:]' '[:lower:]')"
    gossip_enabled="$(printf '%s' "${BLOCKCHAIN_GOSSIP_ENABLED:-true}" | tr '[:upper:]' '[:lower:]')"
    if [ "$p2p_enabled" = "1" ] || [ "$p2p_enabled" = "true" ] || [ "$p2p_enabled" = "yes" ] || [ "$p2p_enabled" = "on" ]; then
        luajit /app/p2p_daemon.lua &
    fi
    if [ "$gossip_enabled" = "1" ] || [ "$gossip_enabled" = "true" ] || [ "$gossip_enabled" = "yes" ] || [ "$gossip_enabled" = "on" ]; then
        luajit /app/gossip_daemon.lua &
    fi
fi

exec "$@"

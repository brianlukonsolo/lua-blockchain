# Security Review Checklist

- Validate the native P2P TLS handshake, downgrade behavior, and certificate lifecycle.
- Review signed hello verification, peer identity binding, and peer record updates.
- Fuzz block, header, transaction, and inventory parsing paths.
- Load-test rate limits, mempool caps, and block-size limits.
- Exercise eclipse and Sybil scenarios against the per-IP and per-subnet admission rules.
- Verify backup and restore of `/app/data/blockchain.db` and key material.
- Confirm production envs disable server wallet generation.
- Review all admin and peer-authenticated routes for missing authorization checks.
- Run dependency and image scanning on the final container.
- Obtain an independent external review before marketing the node as public-grade.

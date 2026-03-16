const STORAGE_KEYS = {
  wallet: "lua-blockchain-wallet-session",
  adminToken: "lua-blockchain-admin-token",
  activeView: "lua-blockchain-active-view",
};
const LEGACY_WALLET_KEY = "lua-blockchain-wallet";
const CHAIN_RENDER_LIMIT = 12;
const textEncoder = new TextEncoder();

const LESSON_ORDER = [
  "lesson-overview",
  "lesson-architecture",
  "lesson-transactions",
  "lesson-mining",
  "lesson-consensus",
  "lesson-security",
  "lesson-review",
];

const REVIEW_FINDINGS = [
  {
    severity: "high",
    title: "Public snapshot endpoints are still too heavy",
    summary:
      "The public `/api/info` and `/api/chain` routes serialize the full chain, accounts, pending transactions, and peer views on every request. That is expensive and exposes more state than a public dashboard needs.",
    action: "Add paginated and role-scoped read models for public consumers.",
    source: "app.lua build_snapshot() and GET /api/info + GET /api/chain",
  },
  {
    severity: "medium",
    title: "Readiness and public reads rebuild chain state on the request path",
    summary:
      "The node constructs a blockchain instance and reloads SQLite state when serving readiness and snapshot-style GET requests. As chain size grows, probe cost and request latency rise linearly.",
    action: "Introduce cached read models or a long-lived state service for non-mutating requests.",
    source: "app.lua handle_ready(), build_blockchain(), blockchain.lua Blockchain.new() -> load()",
  },
  {
    severity: "medium",
    title: "Backups remain only a local recovery primitive until exported safely",
    summary:
      "Backups include the SQLite snapshot and node identity material. That is useful for recovery, but it also means backup compromise is effectively node compromise unless retention, encryption, and export policy are handled outside the app.",
    action: "Ship backups off-node, encrypt them, and rotate backup access separately from node access.",
    source: "blockchain.lua create_backup()",
  },
];

const state = {
  info: null,
  health: null,
  readiness: null,
  wallet: null,
  walletAccount: null,
  activeView: sessionStorage.getItem(STORAGE_KEYS.activeView) || "console-view",
  activeLesson: LESSON_ORDER[0],
};

function byId(id) {
  return document.getElementById(id);
}

const elements = {
  heroBadges: byId("hero-badges"),
  heroStats: byId("hero-stats"),
  heroRuntime: byId("hero-runtime"),
  openConsoleButton: byId("open-console-button"),
  openLearnButton: byId("open-learn-button"),
  snapshotGrid: byId("snapshot-grid"),
  securityGrid: byId("security-grid"),
  statusStrip: byId("status-strip"),
  adminTokenInput: byId("admin-token-input"),
  saveAdminButton: byId("save-admin-button"),
  clearAdminButton: byId("clear-admin-button"),
  operatorNote: byId("operator-note"),
  backupLabelInput: byId("backup-label-input"),
  backupButton: byId("backup-button"),
  backupOutput: byId("backup-output"),
  walletAddress: byId("wallet-address"),
  walletBalance: byId("wallet-balance"),
  walletTextarea: byId("wallet-textarea"),
  senderAddress: byId("sender-address"),
  minerAddress: byId("miner-address"),
  peerList: byId("peer-list"),
  accountList: byId("account-list"),
  pendingList: byId("pending-list"),
  chainList: byId("chain-list"),
  refreshButton: byId("refresh-button"),
  validateButton: byId("validate-button"),
  syncButton: byId("sync-button"),
  generateWalletButton: byId("generate-wallet-button"),
  exportWalletButton: byId("export-wallet-button"),
  importWalletButton: byId("import-wallet-button"),
  clearWalletButton: byId("clear-wallet-button"),
  transactionForm: byId("transaction-form"),
  mineForm: byId("mine-form"),
  mineButton: byId("mine-button"),
  peerForm: byId("peer-form"),
  peerSubmitButton: byId("peer-submit-button"),
  courseProgress: byId("course-progress"),
  courseProgressBar: byId("course-progress-bar"),
  courseLiveSummary: byId("course-live-summary"),
  courseReviewGrid: byId("course-review-grid"),
  architectureFacts: byId("architecture-facts"),
  transactionFacts: byId("transaction-facts"),
  miningFacts: byId("mining-facts"),
  consensusFacts: byId("consensus-facts"),
  securityFacts: byId("security-facts"),
  implementationFindings: byId("implementation-findings"),
  apiMap: byId("api-map"),
  viewSections: Array.from(document.querySelectorAll(".view-section")),
  viewButtons: Array.from(document.querySelectorAll("[data-view-target]")),
  lessonButtons: Array.from(document.querySelectorAll("[data-section-target]")),
  lessonSections: LESSON_ORDER.map((id) => byId(id)).filter(Boolean),
  revealNodes: Array.from(document.querySelectorAll(".reveal")),
};

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatAmount(value) {
  return Number(value || 0).toFixed(2);
}

function formatInteger(value) {
  return Number(value || 0).toLocaleString();
}

function shorten(value, keep = 10) {
  const text = String(value || "");
  if (text.length <= keep * 2) {
    return text;
  }
  return `${text.slice(0, keep)}...${text.slice(-keep)}`;
}

function trim(value) {
  return String(value || "").trim();
}

function isAddress(value) {
  return /^lbc_[0-9a-f]+$/i.test(trim(value));
}

function normalizeMoney(value) {
  return Number(Number(value || 0).toFixed(2));
}

function formatTimestamp(value) {
  if (!value) {
    return "n/a";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  return date.toLocaleString();
}

function capabilityBadge(label, tone = "badge-positive") {
  return `<span class="hero-badge ${tone}">${escapeHtml(label)}</span>`;
}

function metricCard(label, value, context = "") {
  return `
    <article class="metric-card">
      <div class="metric-label">${escapeHtml(label)}</div>
      <div class="metric-value">${escapeHtml(value)}</div>
      ${context ? `<div class="subtle">${escapeHtml(context)}</div>` : ""}
    </article>
  `;
}

function summaryCard(label, value, context = "") {
  return `
    <article class="summary-card">
      <strong>${escapeHtml(label)}</strong>
      <div class="summary-value">${escapeHtml(value)}</div>
      ${context ? `<div class="subtle">${escapeHtml(context)}</div>` : ""}
    </article>
  `;
}

function factCard(label, value, context = "") {
  return `
    <article class="fact-card">
      <strong>${escapeHtml(label)}</strong>
      <div class="fact-value">${escapeHtml(value)}</div>
      ${context ? `<div class="subtle">${escapeHtml(context)}</div>` : ""}
    </article>
  `;
}

function requestError(result) {
  if (!result || typeof result !== "object") {
    return "Request failed.";
  }

  const payload = result.payload || {};
  if (Array.isArray(payload.errors) && payload.errors.length) {
    return payload.errors.join("; ");
  }

  return payload.error || payload.details || payload.message || `Request failed: ${result.status}`;
}

async function requestJson(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (options.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  if (options.requireAdmin) {
    const token = sessionStorage.getItem(STORAGE_KEYS.adminToken);
    if (token) {
      headers.set("X-Blockchain-Admin-Token", token);
    }
  }

  const response = await fetch(path, {
    ...options,
    headers,
  });
  const payload = await response.json().catch(() => ({}));

  return {
    ok: response.ok,
    status: response.status,
    payload,
  };
}

async function apiRequest(path, options = {}) {
  const result = await requestJson(path, options);
  if (!result.ok) {
    throw new Error(requestError(result));
  }
  return result.payload;
}

function canonicalStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalStringify(item)).join(",")}]`;
  }

  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalStringify(value[key])}`)
    .join(",")}}`;
}

function setStatus(message, isError = false) {
  elements.statusStrip.textContent = message;
  elements.statusStrip.classList.toggle("invalid", isError);
}

function getAdminToken() {
  return sessionStorage.getItem(STORAGE_KEYS.adminToken) || "";
}

function adminProtectionEnabled() {
  return state.info?.capabilities?.admin_authentication === true;
}

function requireAdminToken(action) {
  if (adminProtectionEnabled() && !getAdminToken()) {
    throw new Error(`${action} requires the admin token configured for this node.`);
  }
}

function updateOperatorNote() {
  const needsToken = adminProtectionEnabled();
  const hasToken = getAdminToken() !== "";

  if (!state.info) {
    elements.operatorNote.textContent = "Loading node policy...";
    return;
  }

  if (!needsToken) {
    elements.operatorNote.textContent =
      "This node currently allows admin routes without a token, but the token field remains available for stricter environments.";
    return;
  }

  elements.operatorNote.textContent = hasToken
    ? "Admin token loaded for this browser session. Protected actions are enabled."
    : "This node protects admin routes. Save the admin token in this browser session to mine, register peers, resolve consensus, or create backups.";
}

function updateAdminControls() {
  const disabled = adminProtectionEnabled() && !getAdminToken();
  [elements.syncButton, elements.mineButton, elements.peerSubmitButton, elements.backupButton].forEach((button) => {
    if (!button) {
      return;
    }
    button.disabled = disabled;
    button.title = disabled ? "Save an admin token first." : "";
  });
}

function normalizePem(pem) {
  return String(pem || "").trim().replace(/\r\n/g, "\n").replace(/\r/g, "\n") + "\n";
}

function arrayBufferToBase64(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = "";
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });
  return btoa(binary);
}

function base64ToArrayBuffer(base64) {
  const binary = atob(base64.replace(/\s+/g, ""));
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes.buffer;
}

function pemFromArrayBuffer(label, buffer) {
  const base64 = arrayBufferToBase64(buffer);
  const chunks = base64.match(/.{1,64}/g) || [];
  return `-----BEGIN ${label}-----\n${chunks.join("\n")}\n-----END ${label}-----\n`;
}

function arrayBufferFromPem(pem) {
  const base64 = normalizePem(pem)
    .replace(/-----BEGIN [^-]+-----/g, "")
    .replace(/-----END [^-]+-----/g, "")
    .replace(/\s+/g, "");
  return base64ToArrayBuffer(base64);
}

async function sha256Hex(text) {
  const digest = await window.crypto.subtle.digest("SHA-256", textEncoder.encode(text));
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function addressFromPublicKeyPem(publicKeyPem) {
  const hash = await sha256Hex(normalizePem(publicKeyPem));
  return `lbc_${hash.slice(0, 40)}`;
}

async function importWalletCryptoKeys(wallet) {
  const privateKey = await window.crypto.subtle.importKey(
    "pkcs8",
    arrayBufferFromPem(wallet.private_key),
    { name: "ECDSA", namedCurve: "P-256" },
    true,
    ["sign"]
  );
  const publicKey = await window.crypto.subtle.importKey(
    "spki",
    arrayBufferFromPem(wallet.public_key),
    { name: "ECDSA", namedCurve: "P-256" },
    true,
    ["verify"]
  );

  return { privateKey, publicKey };
}

async function generateWallet() {
  const keyPair = await window.crypto.subtle.generateKey(
    { name: "ECDSA", namedCurve: "P-256" },
    true,
    ["sign", "verify"]
  );

  const privateKey = await window.crypto.subtle.exportKey("pkcs8", keyPair.privateKey);
  const publicKey = await window.crypto.subtle.exportKey("spki", keyPair.publicKey);
  const wallet = {
    private_key: pemFromArrayBuffer("PRIVATE KEY", privateKey),
    public_key: pemFromArrayBuffer("PUBLIC KEY", publicKey),
  };
  wallet.address = await addressFromPublicKeyPem(wallet.public_key);
  wallet.keys = keyPair;
  return wallet;
}

async function hydrateWallet(walletLike) {
  const wallet = {
    address: trim(walletLike.address),
    private_key: normalizePem(walletLike.private_key),
    public_key: normalizePem(walletLike.public_key),
  };
  wallet.address = await addressFromPublicKeyPem(wallet.public_key);
  wallet.keys = await importWalletCryptoKeys(wallet);
  return wallet;
}

function serializeWallet(wallet) {
  return JSON.stringify(
    {
      address: wallet.address,
      private_key: wallet.private_key,
      public_key: wallet.public_key,
    },
    null,
    2
  );
}

async function saveWallet(wallet) {
  state.wallet = wallet;
  sessionStorage.setItem(STORAGE_KEYS.wallet, serializeWallet(wallet));
  localStorage.removeItem(LEGACY_WALLET_KEY);
  elements.walletTextarea.value = serializeWallet(wallet);
  elements.senderAddress.value = wallet.address;
  if (!trim(elements.minerAddress.value)) {
    elements.minerAddress.value = wallet.address;
  }
  await refreshWalletAccount().catch(() => {
    state.walletAccount = null;
  });
  renderWallet();
}

async function loadStoredWallet() {
  const serialized = sessionStorage.getItem(STORAGE_KEYS.wallet) || localStorage.getItem(LEGACY_WALLET_KEY);
  if (!serialized) {
    renderWallet();
    return;
  }

  try {
    const wallet = await hydrateWallet(JSON.parse(serialized));
    sessionStorage.setItem(STORAGE_KEYS.wallet, serializeWallet(wallet));
    localStorage.removeItem(LEGACY_WALLET_KEY);
    state.wallet = wallet;
    elements.senderAddress.value = wallet.address;
    elements.minerAddress.value = wallet.address;
    await refreshWalletAccount().catch(() => {
      state.walletAccount = null;
    });
  } catch (_error) {
    sessionStorage.removeItem(STORAGE_KEYS.wallet);
    localStorage.removeItem(LEGACY_WALLET_KEY);
    state.wallet = null;
    state.walletAccount = null;
  }

  renderWallet();
}

function clearWallet() {
  sessionStorage.removeItem(STORAGE_KEYS.wallet);
  localStorage.removeItem(LEGACY_WALLET_KEY);
  state.wallet = null;
  state.walletAccount = null;
  elements.walletTextarea.value = "";
  elements.senderAddress.value = "";
  elements.minerAddress.value = "";
  renderWallet();
}

async function refreshWalletAccount() {
  if (!state.wallet) {
    state.walletAccount = null;
    return null;
  }

  const payload = await apiRequest(`/api/accounts/${state.wallet.address}`);
  state.walletAccount = payload.account;
  return payload.account;
}

function buildSigningPayload(transaction) {
  return {
    amount: normalizeMoney(transaction.amount),
    fee: normalizeMoney(transaction.fee || 0),
    kind: "transfer",
    nonce: Number(transaction.nonce),
    note: String(transaction.note || ""),
    recipient: String(transaction.recipient || ""),
    sender: String(transaction.sender || ""),
    timestamp: String(transaction.timestamp || ""),
  };
}

async function signTransactionPayload(payload) {
  if (!state.wallet?.keys?.privateKey) {
    throw new Error("Load a wallet before signing transactions.");
  }

  const signature = await window.crypto.subtle.sign(
    { name: "ECDSA", hash: "SHA-256" },
    state.wallet.keys.privateKey,
    textEncoder.encode(canonicalStringify(buildSigningPayload(payload)))
  );

  return arrayBufferToBase64(signature);
}

function renderWallet() {
  if (!state.wallet) {
    elements.walletAddress.textContent = "Generate or import a wallet to sign transactions.";
    elements.walletBalance.textContent = "Balance: n/a";
    return;
  }

  elements.walletAddress.textContent = state.wallet.address;
  if (state.walletAccount) {
    elements.walletBalance.textContent = `Balance: ${formatAmount(state.walletAccount.pending_balance)} | Next nonce: ${state.walletAccount.next_nonce}`;
  } else {
    elements.walletBalance.textContent = "Balance: loading...";
  }
}

function renderHero() {
  const info = state.info;
  const snapshot = info.snapshot;
  const stats = snapshot.stats;
  const meta = snapshot.meta;
  const readinessOk = state.readiness?._ok === true;
  const validationOk = snapshot.validation.valid === true;

  elements.heroBadges.innerHTML = [
    capabilityBadge(info.service.mode === "production" ? "Production Mode" : "Development Mode", "badge-warning"),
    capabilityBadge(readinessOk ? "Ready" : "Not Ready", readinessOk ? "badge-positive" : "badge-danger"),
    capabilityBadge(meta.storage_engine || "storage", "badge-positive"),
    capabilityBadge(meta.fork_choice || "consensus", "badge-warning"),
    capabilityBadge(info.capabilities.native_p2p_transport ? "Native P2P" : "HTTP Only", "badge-positive"),
    capabilityBadge(info.capabilities.gossip_transport ? "UDP Gossip" : "Gossip Off", info.capabilities.gossip_transport ? "badge-warning" : "badge-danger"),
  ].join("");

  elements.heroStats.innerHTML = [
    metricCard("Blocks", formatInteger(stats.blocks)),
    metricCard("Pending", formatInteger(stats.pending_transactions)),
    metricCard("Known Peers", formatInteger(stats.known_peers)),
    metricCard("Cumulative Work", formatInteger(stats.cumulative_work)),
    metricCard("Validation", validationOk ? "Valid" : "Invalid"),
    metricCard("Tip Difficulty", stats.tip_difficulty_prefix || meta.difficulty_prefix),
  ].join("");

  elements.heroRuntime.innerHTML = [
    summaryCard("Node", info.service.node_id || "n/a", info.service.node_url || "No node URL advertised"),
    summaryCard("Chain ID", info.service.chain_id || "n/a", info.service.runtime || "runtime unavailable"),
    summaryCard("Storage", meta.storage_engine || "n/a", `schema ${meta.schema_version || "?"}`),
    summaryCard("Updated", formatTimestamp(meta.updated_at), readinessOk ? "ready" : requestError({ payload: state.readiness, status: state.readiness?._status })),
  ].join("");
}

function renderSnapshot() {
  const snapshot = state.info.snapshot;
  const stats = snapshot.stats;
  const meta = snapshot.meta;

  elements.snapshotGrid.innerHTML = [
    metricCard("Node", meta.node_id || "n/a"),
    metricCard("Base Difficulty", meta.difficulty_prefix || "n/a"),
    metricCard("Target Block Time", `${stats.target_block_seconds}s`),
    metricCard("Average Block Time", stats.average_block_time_seconds ? `${stats.average_block_time_seconds}s` : "n/a"),
    metricCard("Accounts", formatInteger(stats.accounts)),
    metricCard("Committed Tx", formatInteger(stats.committed_transactions)),
    metricCard("Circulating Supply", formatAmount(stats.circulating_supply)),
    metricCard("Queued Fees", formatAmount(stats.queued_fees)),
  ].join("");

  const info = state.info;
  const readinessOk = state.readiness?._ok === true;
  elements.securityGrid.innerHTML = [
    metricCard("Admin Auth", info.capabilities.admin_authentication ? "Enabled" : "Open"),
    metricCard("Peer Auth", info.capabilities.peer_authentication ? "Enabled" : "Open"),
    metricCard("Rate Limits", info.capabilities.rate_limiting ? "Enabled" : "Unknown"),
    metricCard("P2P TLS", meta.transports?.p2p?.tls ? "Enabled" : "Off"),
    metricCard("Health", state.health?.status || "unknown", state.health?.config_valid ? "config valid" : "check config"),
    metricCard("Readiness", readinessOk ? "Ready" : "Not Ready", readinessOk ? "" : requestError({ payload: state.readiness, status: state.readiness?._status })),
  ].join("");
}

function renderPeers() {
  const peerRecords = state.info.snapshot.peer_records || [];
  if (!peerRecords.length) {
    elements.peerList.innerHTML = `<div class="list-empty">No peers are registered yet.</div>`;
    return;
  }

  elements.peerList.innerHTML = peerRecords
    .map((peer) => {
      const transport =
        peer.capabilities?.p2p_transport?.endpoint ||
        peer.capabilities?.gossip_transport?.endpoint ||
        peer.node_url ||
        peer.url;

      return `
        <article class="peer-item">
          <div class="peer-row">
            <strong class="mono">${escapeHtml(shorten(peer.url, 18))}</strong>
            <span class="pill">${escapeHtml(peer.state || "active")}</span>
          </div>
          <div class="subtle">
            score ${escapeHtml(peer.score ?? 0)} | successes ${escapeHtml(peer.success_count ?? 0)} | failures ${escapeHtml(peer.failure_count ?? 0)}
          </div>
          <div class="subtle">
            ${escapeHtml(peer.node_id || "unknown node")} | ${escapeHtml(transport || "no transport advertised")}
          </div>
          <div class="subtle">
            height ${escapeHtml(peer.last_advertised_height ?? "n/a")} | work ${escapeHtml(peer.last_cumulative_work ?? "n/a")}
          </div>
          ${peer.last_error ? `<div class="subtle">${escapeHtml(peer.last_error)}</div>` : ""}
        </article>
      `;
    })
    .join("");
}

function renderAccounts() {
  const accounts = [...(state.info.snapshot.accounts || [])].sort(
    (left, right) => Number(right.pending_balance || 0) - Number(left.pending_balance || 0)
  );

  if (!accounts.length) {
    elements.accountList.innerHTML = `<div class="list-empty">No funded accounts yet. Mining creates the first supply.</div>`;
    return;
  }

  elements.accountList.innerHTML = accounts
    .slice(0, 12)
    .map(
      (account) => `
        <article class="account-item">
          <div class="account-row">
            <strong class="mono">${escapeHtml(shorten(account.address, 14))}</strong>
            <span>${escapeHtml(formatAmount(account.pending_balance))}</span>
          </div>
          <div class="subtle">
            confirmed ${escapeHtml(formatAmount(account.confirmed_balance))} | nonce ${escapeHtml(account.next_nonce)}
          </div>
        </article>
      `
    )
    .join("");
}

function renderPending() {
  const pending = state.info.snapshot.pending_transactions || [];
  if (!pending.length) {
    elements.pendingList.innerHTML = `<div class="list-empty">No pending transactions in the mempool.</div>`;
    return;
  }

  elements.pendingList.innerHTML = pending
    .slice(0, 12)
    .map(
      (transaction) => `
        <article class="transaction-item">
          <div class="account-row">
            <strong class="mono">${escapeHtml(shorten(transaction.sender, 12))}</strong>
            <span>${escapeHtml(formatAmount(transaction.amount))}</span>
          </div>
          <div class="subtle">
            to ${escapeHtml(shorten(transaction.recipient, 12))} | fee ${escapeHtml(formatAmount(transaction.fee))} | nonce ${escapeHtml(transaction.nonce)}
          </div>
          <div class="subtle">${escapeHtml(transaction.note || "No note")}</div>
        </article>
      `
    )
    .join("");
}

function renderChain() {
  const chain = state.info.snapshot.chain || [];
  const recentBlocks = [...chain].slice(-CHAIN_RENDER_LIMIT).reverse();
  const intro = chain.length > CHAIN_RENDER_LIMIT
    ? `<article class="summary-card"><strong>Chain window</strong><div class="summary-value">Showing latest ${CHAIN_RENDER_LIMIT} of ${chain.length} blocks</div></article>`
    : "";

  const blocks = recentBlocks
    .map((block) => {
      const transactions = (block.transactions || [])
        .map((transaction) => {
          if (transaction.kind === "reward") {
            return `
              <div class="tx-row reward">
                <strong>Reward</strong>
                <span>${escapeHtml(formatAmount(transaction.amount))} to ${escapeHtml(shorten(transaction.recipient, 12))}</span>
              </div>
            `;
          }

          return `
            <div class="tx-row">
              <strong>${escapeHtml(shorten(transaction.sender, 12))}</strong>
              <span>${escapeHtml(formatAmount(transaction.amount))} + ${escapeHtml(formatAmount(transaction.fee))} fee -> ${escapeHtml(shorten(transaction.recipient, 12))}</span>
            </div>
          `;
        })
        .join("");

      return `
        <article class="block-card">
          <div class="block-head">
            <div>
              <strong>Block ${escapeHtml(block.index)}</strong>
              <div class="subtle">${escapeHtml(formatTimestamp(block.timestamp))}</div>
            </div>
            <span class="pill">${escapeHtml((block.transactions || []).length)} txs</span>
          </div>
          <div class="block-grid">
            ${metricCard("Proof", String(block.proof || "n/a"))}
            ${metricCard("Difficulty", block.difficulty_prefix || "n/a")}
            ${metricCard("Work", formatInteger(block.work || 0))}
            ${metricCard("Cumulative", formatInteger(block.cumulative_work || 0))}
          </div>
          <div class="subtle mono">hash ${escapeHtml(shorten(block.hash, 20))}</div>
          <div class="subtle mono">prev ${escapeHtml(shorten(block.previous_hash, 20))}</div>
          <div class="tx-list">${transactions || '<div class="subtle">No transactions in this block.</div>'}</div>
        </article>
      `;
    })
    .join("");

  elements.chainList.innerHTML = intro + blocks;
}

function renderOverviewCourseCards() {
  const info = state.info;
  const snapshot = info.snapshot;
  const cards = [
    {
      title: "Runtime",
      text: `${info.service.runtime} serves the API and static frontend for node ${info.service.node_id}.`,
    },
    {
      title: "State Engine",
      text: `${snapshot.meta.storage_engine} schema ${snapshot.meta.schema_version} stores blocks, pending transactions, peers, and metadata.`,
    },
    {
      title: "Networking",
      text: `${info.capabilities.native_p2p_transport ? "Native TCP P2P is on" : "Native TCP P2P is off"}${info.capabilities.gossip_transport ? " and UDP gossip discovery is available." : "."}`,
    },
    {
      title: "Fork Choice",
      text: `${snapshot.meta.fork_choice} selects the tip with cumulative work ${formatInteger(snapshot.stats.cumulative_work)}.`,
    },
  ];

  elements.courseReviewGrid.innerHTML = cards
    .map(
      (card) => `
        <article class="study-card">
          <h3>${escapeHtml(card.title)}</h3>
          <p>${escapeHtml(card.text)}</p>
        </article>
      `
    )
    .join("");
}

function renderFactGrid(target, items) {
  target.innerHTML = items
    .map((item) => factCard(item.label, item.value, item.context))
    .join("");
}

function renderCourse() {
  const info = state.info;
  const snapshot = info.snapshot;
  const meta = snapshot.meta;
  const stats = snapshot.stats;

  elements.courseLiveSummary.innerHTML = [
    summaryCard("Mode", info.service.mode, info.service.version),
    summaryCard("Tip", `#${stats.blocks}`, shorten(stats.last_block_hash || "genesis", 12)),
    summaryCard("Consensus", meta.fork_choice || "n/a", `${formatInteger(stats.cumulative_work)} work`),
    summaryCard("Readiness", state.readiness?._ok ? "Ready" : "Attention", state.readiness?._ok ? "validation passed" : requestError({ payload: state.readiness, status: state.readiness?._status })),
  ].join("");

  renderOverviewCourseCards();
  renderFactGrid(elements.architectureFacts, [
    { label: "HTTP Runtime", value: info.service.runtime },
    { label: "Storage Engine", value: meta.storage_engine || "n/a", context: `schema ${meta.schema_version || "?"}` },
    { label: "P2P Endpoint", value: meta.transports?.p2p?.endpoint || "disabled", context: meta.transports?.p2p?.tls ? "TLS enabled" : "TLS off" },
    { label: "Gossip Endpoint", value: meta.transports?.gossip?.endpoint || "disabled", context: meta.transports?.gossip ? `fanout ${meta.transports.gossip.fanout}` : "discovery off" },
  ]);

  renderFactGrid(elements.transactionFacts, [
    { label: "Signature Model", value: "ECDSA P-256", context: "Web Crypto in the browser" },
    { label: "Min Fee", value: formatAmount(meta.limits?.min_transaction_fee || 0) },
    { label: "Note Cap", value: `${meta.limits?.max_transaction_note_bytes || 0} bytes` },
    { label: "Mempool Size", value: formatInteger(stats.pending_transactions), context: `${formatAmount(stats.pending_value)} queued value` },
  ]);

  renderFactGrid(elements.miningFacts, [
    { label: "Base Difficulty", value: meta.difficulty_prefix || "n/a" },
    { label: "Tip Difficulty", value: stats.tip_difficulty_prefix || meta.difficulty_prefix || "n/a" },
    { label: "Target Time", value: `${stats.target_block_seconds}s` },
    { label: "Average Time", value: stats.average_block_time_seconds ? `${stats.average_block_time_seconds}s` : "n/a" },
    { label: "Reward", value: formatAmount(meta.mining_reward || 0) },
    { label: "Queued Fees", value: formatAmount(stats.queued_fees || 0) },
    { label: "Transactions Per Block", value: formatInteger(meta.limits?.max_transactions_per_block || 0) },
    { label: "Circulating Supply", value: formatAmount(stats.circulating_supply || 0) },
  ]);

  renderFactGrid(elements.consensusFacts, [
    { label: "Fork Choice", value: meta.fork_choice || "n/a" },
    { label: "Cumulative Work", value: formatInteger(stats.cumulative_work || 0) },
    { label: "Active Peers", value: formatInteger(stats.peers || 0), context: `${formatInteger(stats.known_peers || 0)} known` },
    { label: "Headers First", value: info.capabilities.headers_first_sync ? "Enabled" : "Disabled" },
    { label: "Native P2P", value: info.capabilities.native_p2p_transport ? "Enabled" : "Disabled" },
    { label: "Gossip Discovery", value: info.capabilities.gossip_transport ? "Enabled" : "Disabled" },
  ]);

  renderFactGrid(elements.securityFacts, [
    { label: "Admin Auth", value: info.capabilities.admin_authentication ? "Enabled" : "Open" },
    { label: "Peer Auth", value: info.capabilities.peer_authentication ? "Enabled" : "Open" },
    { label: "Rate Limiting", value: info.capabilities.rate_limiting ? "Enabled" : "Unknown" },
    { label: "Config State", value: state.health?.config_valid ? "Valid" : "Needs Attention" },
    { label: "P2P TLS", value: meta.transports?.p2p?.tls ? "Enabled" : "Disabled" },
    { label: "Recovery", value: "Backup endpoint available", context: "admin protected" },
  ]);

  elements.implementationFindings.innerHTML = REVIEW_FINDINGS.map(
    (finding) => `
      <article class="review-card">
        <div class="account-row">
          <strong>${escapeHtml(finding.title)}</strong>
          <span class="severity-badge ${escapeHtml(finding.severity)}">${escapeHtml(finding.severity.toUpperCase())}</span>
        </div>
        <p>${escapeHtml(finding.summary)}</p>
        <div class="subtle">${escapeHtml(finding.action)}</div>
        <div class="subtle">${escapeHtml(finding.source)}</div>
      </article>
    `
  ).join("");

  const apiEntries = Object.entries(info.api || {});
  elements.apiMap.innerHTML = apiEntries
    .map(
      ([name, value]) => `
        <article class="api-card">
          <strong>${escapeHtml(name.replace(/_/g, " "))}</strong>
          <div class="mono">${escapeHtml(String(value))}</div>
        </article>
      `
    )
    .join("");
}

function renderStatus() {
  const snapshot = state.info.snapshot;
  const readinessOk = state.readiness?._ok === true;
  const validationOk = snapshot.validation.valid === true;
  const message = validationOk && readinessOk
    ? `Chain valid and node ready. Last update ${formatTimestamp(snapshot.meta.updated_at)}.`
    : validationOk
      ? `Chain valid, but readiness is degraded: ${requestError({ payload: state.readiness, status: state.readiness?._status })}`
      : `Chain invalid: ${snapshot.validation.reason}`;
  setStatus(message, !(validationOk && readinessOk));
}

function renderAll() {
  renderHero();
  renderSnapshot();
  renderPeers();
  renderAccounts();
  renderPending();
  renderChain();
  renderWallet();
  renderCourse();
  renderStatus();
  updateOperatorNote();
  updateAdminControls();
}

function setActiveView(viewId) {
  state.activeView = viewId;
  sessionStorage.setItem(STORAGE_KEYS.activeView, viewId);

  elements.viewSections.forEach((section) => {
    section.classList.toggle("is-active", section.id === viewId);
  });

  elements.viewButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.viewTarget === viewId);
  });
}

function setActiveLesson(sectionId) {
  state.activeLesson = sectionId;
  const index = Math.max(LESSON_ORDER.indexOf(sectionId), 0);
  const progress = ((index + 1) / LESSON_ORDER.length) * 100;

  elements.lessonButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.sectionTarget === sectionId);
  });
  elements.courseProgress.textContent = `Lesson ${index + 1} of ${LESSON_ORDER.length}`;
  elements.courseProgressBar.style.width = `${progress}%`;
}

function initializeRevealObserver() {
  const revealObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("revealed");
        }
      });
    },
    { threshold: 0.12 }
  );

  elements.revealNodes.forEach((node) => revealObserver.observe(node));
}

function initializeLessonObserver() {
  const lessonObserver = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((left, right) => left.boundingClientRect.top - right.boundingClientRect.top);

      if (visible[0]) {
        setActiveLesson(visible[0].target.id);
      }
    },
    {
      rootMargin: "-20% 0px -55% 0px",
      threshold: 0.15,
    }
  );

  elements.lessonSections.forEach((section) => lessonObserver.observe(section));
}

async function refresh() {
  const [infoResult, healthResult, readinessResult] = await Promise.all([
    requestJson("/api/info"),
    requestJson("/api/health"),
    requestJson("/api/ready"),
  ]);

  if (!infoResult.ok) {
    throw new Error(requestError(infoResult));
  }

  state.info = infoResult.payload;
  state.health = healthResult.payload;
  state.readiness = {
    ...(readinessResult.payload || {}),
    _ok: readinessResult.ok,
    _status: readinessResult.status,
  };

  if (state.wallet) {
    await refreshWalletAccount().catch(() => {
      state.walletAccount = null;
    });
  }

  renderAll();
}

async function handleValidate() {
  const result = await apiRequest("/api/validate");
  setStatus(result.valid ? "Chain validation passed." : `Chain invalid: ${result.reason}`, !result.valid);
  await refresh();
}

async function handleConsensusResolve() {
  requireAdminToken("Consensus resolution");
  const result = await apiRequest("/api/consensus/resolve", {
    method: "POST",
    requireAdmin: true,
  });
  const message = result.replaced
    ? `Local chain replaced from ${result.result?.source_peer || "peer network"}.`
    : "Consensus check finished. No replacement was required.";
  setStatus(message);
  if (result.snapshot) {
    state.info.snapshot = result.snapshot;
    renderAll();
  } else {
    await refresh();
  }
}

async function handleBackup() {
  requireAdminToken("Backup creation");
  const payload = await apiRequest("/api/admin/backup", {
    method: "POST",
    requireAdmin: true,
    body: JSON.stringify({
      label: trim(elements.backupLabelInput.value),
    }),
  });
  elements.backupOutput.textContent = JSON.stringify(payload.backup, null, 2);
  setStatus("Backup created.");
}

function loadStoredAdminToken() {
  elements.adminTokenInput.value = getAdminToken();
  updateOperatorNote();
  updateAdminControls();
}

function saveAdminToken() {
  const token = trim(elements.adminTokenInput.value);
  if (!token) {
    sessionStorage.removeItem(STORAGE_KEYS.adminToken);
    updateOperatorNote();
    updateAdminControls();
    setStatus("Admin token cleared from this browser session.");
    return;
  }
  sessionStorage.setItem(STORAGE_KEYS.adminToken, token);
  updateOperatorNote();
  updateAdminControls();
  setStatus("Admin token saved in this browser session.");
}

function clearAdminToken() {
  elements.adminTokenInput.value = "";
  sessionStorage.removeItem(STORAGE_KEYS.adminToken);
  updateOperatorNote();
  updateAdminControls();
  setStatus("Admin token cleared from this browser session.");
}

elements.openConsoleButton.addEventListener("click", () => {
  setActiveView("console-view");
});

elements.openLearnButton.addEventListener("click", () => {
  setActiveView("learn-view");
  byId(state.activeLesson)?.scrollIntoView({ behavior: "smooth", block: "start" });
});

elements.viewButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setActiveView(button.dataset.viewTarget);
  });
});

elements.lessonButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const target = button.dataset.sectionTarget;
    setActiveView("learn-view");
    setActiveLesson(target);
    byId(target)?.scrollIntoView({ behavior: "smooth", block: "start" });
  });
});

elements.refreshButton.addEventListener("click", async () => {
  try {
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.validateButton.addEventListener("click", async () => {
  try {
    await handleValidate();
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.syncButton.addEventListener("click", async () => {
  try {
    await handleConsensusResolve();
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.saveAdminButton.addEventListener("click", saveAdminToken);
elements.clearAdminButton.addEventListener("click", clearAdminToken);
elements.backupButton.addEventListener("click", async () => {
  try {
    await handleBackup();
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.generateWalletButton.addEventListener("click", async () => {
  if (!window.crypto?.subtle) {
    setStatus("This browser does not support Web Crypto.", true);
    return;
  }

  try {
    const wallet = await generateWallet();
    await saveWallet(wallet);
    setStatus("Wallet generated locally in the browser.");
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.exportWalletButton.addEventListener("click", async () => {
  if (!state.wallet) {
    setStatus("Load a wallet before exporting it.", true);
    return;
  }

  const serialized = serializeWallet(state.wallet);
  elements.walletTextarea.value = serialized;

  try {
    await navigator.clipboard.writeText(serialized);
    setStatus("Wallet exported to the textarea and copied to the clipboard.");
  } catch (_error) {
    setStatus("Wallet exported to the textarea.");
  }
});

elements.importWalletButton.addEventListener("click", async () => {
  try {
    const wallet = await hydrateWallet(JSON.parse(elements.walletTextarea.value.trim()));
    await saveWallet(wallet);
    setStatus("Wallet imported and activated.");
  } catch (error) {
    setStatus(error.message || "Unable to import wallet.", true);
  }
});

elements.clearWalletButton.addEventListener("click", () => {
  clearWallet();
  setStatus("Wallet cleared from this browser session.");
});

elements.transactionForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  if (!state.wallet) {
    setStatus("Generate or import a wallet before signing transactions.", true);
    return;
  }

  try {
    const formData = new FormData(elements.transactionForm);
    const recipient = trim(formData.get("recipient")).toLowerCase();
    if (!isAddress(recipient)) {
      throw new Error("Recipient must be a valid address.");
    }

    const accountPayload = await apiRequest(`/api/accounts/${state.wallet.address}`);
    const transactionPayload = {
      amount: normalizeMoney(formData.get("amount")),
      fee: normalizeMoney(formData.get("fee") || 0),
      kind: "transfer",
      nonce: accountPayload.account.next_nonce,
      note: trim(formData.get("note")),
      recipient,
      sender: state.wallet.address,
      timestamp: new Date().toISOString(),
    };

    transactionPayload.public_key = state.wallet.public_key;
    transactionPayload.signature = await signTransactionPayload(transactionPayload);

    await apiRequest("/api/transactions", {
      method: "POST",
      body: JSON.stringify(transactionPayload),
    });

    elements.transactionForm.reset();
    elements.senderAddress.value = state.wallet.address;
    setStatus("Signed transaction queued and broadcast.");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.mineForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  try {
    requireAdminToken("Mining");
    const formData = new FormData(elements.mineForm);
    const miner = trim(formData.get("miner")).toLowerCase() || state.wallet?.address || "";

    if (!isAddress(miner)) {
      throw new Error("Mining requires a valid reward address.");
    }

    await apiRequest("/api/mine", {
      method: "POST",
      requireAdmin: true,
      body: JSON.stringify({ miner }),
    });

    elements.minerAddress.value = miner;
    setStatus("Block mined and propagated.");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.peerForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  try {
    requireAdminToken("Peer registration");
    const formData = new FormData(elements.peerForm);
    await apiRequest("/api/peers", {
      method: "POST",
      requireAdmin: true,
      body: JSON.stringify({
        peer: trim(formData.get("peer")),
      }),
    });
    elements.peerForm.reset();
    setStatus("Peer registered.");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

async function boot() {
  initializeRevealObserver();
  initializeLessonObserver();
  loadStoredAdminToken();
  setActiveView(state.activeView);
  setActiveLesson(state.activeLesson);

  if (!window.crypto?.subtle) {
    elements.generateWalletButton.disabled = true;
    elements.importWalletButton.disabled = true;
    elements.exportWalletButton.disabled = true;
    elements.clearWalletButton.disabled = true;
    elements.walletAddress.textContent = "This browser does not support Web Crypto.";
  }

  await loadStoredWallet();
  await refresh();
}

boot().catch((error) => {
  setStatus(error.message, true);
});

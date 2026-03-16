const STORAGE_KEY = "lua-blockchain-wallet";
const textEncoder = new TextEncoder();

const state = {
  snapshot: null,
  wallet: null,
  walletAccount: null,
};

const elements = {
  heroStats: document.getElementById("hero-stats"),
  snapshotGrid: document.getElementById("snapshot-grid"),
  statusStrip: document.getElementById("status-strip"),
  walletAddress: document.getElementById("wallet-address"),
  walletBalance: document.getElementById("wallet-balance"),
  walletTextarea: document.getElementById("wallet-textarea"),
  senderAddress: document.getElementById("sender-address"),
  minerAddress: document.getElementById("miner-address"),
  peerList: document.getElementById("peer-list"),
  accountList: document.getElementById("account-list"),
  pendingList: document.getElementById("pending-list"),
  chainList: document.getElementById("chain-list"),
  refreshButton: document.getElementById("refresh-button"),
  validateButton: document.getElementById("validate-button"),
  syncButton: document.getElementById("sync-button"),
  generateWalletButton: document.getElementById("generate-wallet-button"),
  exportWalletButton: document.getElementById("export-wallet-button"),
  importWalletButton: document.getElementById("import-wallet-button"),
  clearWalletButton: document.getElementById("clear-wallet-button"),
  transactionForm: document.getElementById("transaction-form"),
  mineForm: document.getElementById("mine-form"),
  peerForm: document.getElementById("peer-form"),
};

async function apiRequest(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || payload.details || `Request failed: ${response.status}`);
  }

  return payload;
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

function formatAmount(value) {
  return Number(value || 0).toFixed(2);
}

function normalizeMoney(value) {
  return Number(Number(value || 0).toFixed(2));
}

function setStatus(message, isError = false) {
  elements.statusStrip.textContent = message;
  elements.statusStrip.classList.toggle("invalid", isError);
}

function metricCard(label, value) {
  return `
    <article class="metric-card">
      <span class="metric-label">${label}</span>
      <div class="metric-value">${value}</div>
    </article>
  `;
}

function shorten(value, keep = 10) {
  if (!value || value.length <= keep * 2) {
    return value || "";
  }

  return `${value.slice(0, keep)}...${value.slice(-keep)}`;
}

function isAddress(value) {
  return /^lbc_[0-9a-f]+$/i.test(String(value || "").trim());
}

function normalizePem(pem) {
  return String(pem || "").trim().replace(/\r\n/g, "\n").replace(/\r/g, "\n") + "\n";
}

function arrayBufferToBase64(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = "";

  for (let index = 0; index < bytes.length; index += 1) {
    binary += String.fromCharCode(bytes[index]);
  }

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
  const normalized = normalizePem(publicKeyPem);
  const hash = await sha256Hex(normalized);
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
    address: walletLike.address,
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
  localStorage.setItem(STORAGE_KEY, serializeWallet(wallet));
  elements.senderAddress.value = wallet.address;
  if (!elements.minerAddress.value) {
    elements.minerAddress.value = wallet.address;
  }
  await refreshWalletAccount();
  renderWallet();
}

async function loadStoredWallet() {
  const serialized = localStorage.getItem(STORAGE_KEY);
  if (!serialized) {
    renderWallet();
    return;
  }

  try {
    const hydrated = await hydrateWallet(JSON.parse(serialized));
    state.wallet = hydrated;
    elements.senderAddress.value = hydrated.address;
    if (!elements.minerAddress.value) {
      elements.minerAddress.value = hydrated.address;
    }
    await refreshWalletAccount();
    renderWallet();
  } catch (error) {
    localStorage.removeItem(STORAGE_KEY);
    state.wallet = null;
    state.walletAccount = null;
    renderWallet();
    setStatus(`Stored wallet could not be restored: ${error.message}`, true);
  }
}

function clearWallet() {
  localStorage.removeItem(STORAGE_KEY);
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
  return state.walletAccount;
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

function renderHero(snapshot) {
  const stats = snapshot.stats;
  const validation = snapshot.validation.valid ? "Valid" : "Invalid";

  elements.heroStats.innerHTML = `
    <article class="hero-mini">
      <span class="metric-label">Blocks</span>
      <strong>${stats.blocks}</strong>
    </article>
    <article class="hero-mini">
      <span class="metric-label">Pending</span>
      <strong>${stats.pending_transactions}</strong>
    </article>
    <article class="hero-mini">
      <span class="metric-label">Peers</span>
      <strong>${stats.peers}</strong>
    </article>
    <article class="hero-mini">
      <span class="metric-label">Chain Status</span>
      <strong>${validation}</strong>
    </article>
  `;
}

function renderSnapshot(snapshot) {
  const stats = snapshot.stats;
  const meta = snapshot.meta;

  elements.snapshotGrid.innerHTML = [
    metricCard("Node", meta.node_id || "n/a"),
    metricCard("Difficulty", meta.difficulty_prefix),
    metricCard("Reward", formatAmount(meta.mining_reward)),
    metricCard("Accounts", stats.accounts),
    metricCard("Supply", formatAmount(stats.circulating_supply)),
    metricCard("Queued Fees", formatAmount(stats.queued_fees)),
  ].join("");
}

function renderPeers(snapshot) {
  if (!snapshot.peers.length) {
    elements.peerList.innerHTML = `<div class="list-empty">No peers registered yet.</div>`;
    return;
  }

  elements.peerList.innerHTML = snapshot.peers
    .map(
      (peer) => `
        <article class="peer-item">
          <div class="mono">${peer}</div>
        </article>
      `
    )
    .join("");
}

function renderAccounts(snapshot) {
  if (!snapshot.accounts.length) {
    elements.accountList.innerHTML = `<div class="list-empty">No funded accounts yet. Mine a block to create supply.</div>`;
    return;
  }

  elements.accountList.innerHTML = snapshot.accounts
    .map(
      (account) => `
        <article class="account-item">
          <div class="account-row">
            <strong class="mono">${shorten(account.address, 12)}</strong>
            <span>${formatAmount(account.pending_balance)}</span>
          </div>
          <div class="subtle">Confirmed: ${formatAmount(account.confirmed_balance)} | Next nonce: ${account.next_nonce}</div>
        </article>
      `
    )
    .join("");
}

function renderPending(snapshot) {
  if (!snapshot.pending_transactions.length) {
    elements.pendingList.innerHTML = `<div class="list-empty">No pending transactions in the mempool.</div>`;
    return;
  }

  elements.pendingList.innerHTML = snapshot.pending_transactions
    .map(
      (transaction) => `
        <article class="transaction-item">
          <div class="account-row">
            <strong>${shorten(transaction.sender, 10)}</strong>
            <span>${formatAmount(transaction.amount)}</span>
          </div>
          <div class="subtle">To ${shorten(transaction.recipient, 10)} | Fee ${formatAmount(transaction.fee)} | Nonce ${transaction.nonce}</div>
          <div class="subtle">${transaction.note || "No note"}</div>
        </article>
      `
    )
    .join("");
}

function renderChain(snapshot) {
  const blocks = [...snapshot.chain].reverse();
  elements.chainList.innerHTML = blocks
    .map((block) => {
      const transactions = (block.transactions || [])
        .map((transaction) => {
          if (transaction.kind === "reward") {
            return `
              <div class="tx-row reward">
                <strong>Reward</strong>
                <span>${formatAmount(transaction.amount)} to ${shorten(transaction.recipient, 10)}</span>
              </div>
            `;
          }

          return `
            <div class="tx-row">
              <strong>${shorten(transaction.sender, 10)}</strong>
              <span>${formatAmount(transaction.amount)} + ${formatAmount(transaction.fee)} fee -> ${shorten(transaction.recipient, 10)} | nonce ${transaction.nonce}</span>
            </div>
          `;
        })
        .join("");

      return `
        <article class="block-card">
          <div class="block-head">
            <div>
              <div class="block-index">Block ${block.index}</div>
              <div class="subtle">${block.timestamp}</div>
            </div>
            <span class="pill">${block.transactions.length} txs</span>
          </div>
          <div class="block-grid">
            <div>
              <div class="metric-label">Proof</div>
              <div class="mono">${block.proof}</div>
            </div>
            <div>
              <div class="metric-label">Mined By</div>
              <div class="mono">${shorten(block.mined_by || "n/a", 12)}</div>
            </div>
            <div>
              <div class="metric-label">Hash</div>
              <div class="mono">${shorten(block.hash, 20)}</div>
            </div>
            <div>
              <div class="metric-label">Previous Hash</div>
              <div class="mono">${shorten(block.previous_hash, 20)}</div>
            </div>
          </div>
          <div class="tx-list">${transactions}</div>
        </article>
      `;
    })
    .join("");
}

function renderAll(snapshot) {
  state.snapshot = snapshot;
  renderHero(snapshot);
  renderSnapshot(snapshot);
  renderPeers(snapshot);
  renderAccounts(snapshot);
  renderPending(snapshot);
  renderChain(snapshot);
  renderWallet();

  const statusText = snapshot.validation.valid
    ? `Chain valid. Last update: ${snapshot.meta.updated_at}`
    : `Chain invalid: ${snapshot.validation.reason}`;

  setStatus(statusText, !snapshot.validation.valid);
}

async function refresh() {
  const snapshot = await apiRequest("/api/chain");
  if (state.wallet) {
    await refreshWalletAccount().catch(() => {
      state.walletAccount = null;
    });
  }
  renderAll(snapshot);
}

elements.refreshButton.addEventListener("click", async () => {
  try {
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.validateButton.addEventListener("click", async () => {
  try {
    const result = await apiRequest("/api/validate");
    setStatus(result.valid ? "Chain validation passed." : `Chain invalid: ${result.reason}`, !result.valid);
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.syncButton.addEventListener("click", async () => {
  try {
    const result = await apiRequest("/api/consensus/resolve", { method: "POST" });
    const message = result.replaced
      ? `Local chain replaced from ${result.result.source_peer}.`
      : "Consensus check finished. No replacement was required.";
    if (state.wallet) {
      await refreshWalletAccount().catch(() => {
        state.walletAccount = null;
      });
    }
    renderAll(result.snapshot);
    setStatus(message);
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
    elements.walletTextarea.value = serializeWallet(wallet);
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
    const raw = elements.walletTextarea.value.trim();
    if (!raw) {
      throw new Error("Paste exported wallet JSON into the textarea first.");
    }

    const wallet = await hydrateWallet(JSON.parse(raw));
    await saveWallet(wallet);
    setStatus("Wallet imported and activated.");
  } catch (error) {
    setStatus(error.message, true);
  }
});

elements.clearWalletButton.addEventListener("click", () => {
  clearWallet();
  setStatus("Local wallet removed from this browser.");
});

elements.transactionForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  if (!state.wallet) {
    setStatus("Generate or import a wallet before signing transactions.", true);
    return;
  }

  try {
    const formData = new FormData(elements.transactionForm);
    const recipient = String(formData.get("recipient") || "").trim().toLowerCase();
    if (!isAddress(recipient)) {
      throw new Error("Recipient must be a valid address.");
    }

    const accountPayload = await apiRequest(`/api/accounts/${state.wallet.address}`);
    const transactionPayload = {
      amount: normalizeMoney(formData.get("amount")),
      fee: normalizeMoney(formData.get("fee") || 0),
      kind: "transfer",
      nonce: accountPayload.account.next_nonce,
      note: String(formData.get("note") || "").trim(),
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
    const formData = new FormData(elements.mineForm);
    const miner = String(formData.get("miner") || "").trim().toLowerCase() || state.wallet?.address || "";
    if (!isAddress(miner)) {
      throw new Error("Mining requires a valid reward address.");
    }

    await apiRequest("/api/mine", {
      method: "POST",
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
    const formData = new FormData(elements.peerForm);
    const payload = Object.fromEntries(formData.entries());
    await apiRequest("/api/peers", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    elements.peerForm.reset();
    setStatus("Peer registered.");
    await refresh();
  } catch (error) {
    setStatus(error.message, true);
  }
});

loadStoredWallet()
  .then(() => refresh())
  .catch((error) => {
    setStatus(error.message, true);
  });

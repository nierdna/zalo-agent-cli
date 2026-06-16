/**
 * `zalo-agent serve` — All-in-one HTTP service for Coolify/Docker deployment.
 *
 * Routes:
 *   GET  /          → QR login page (if not logged in) or status page
 *   GET  /qr.png    → QR image (PNG)
 *   GET  /api/status → JSON status
 *   POST /mcp       → MCP StreamableHTTP endpoint
 *   GET  /health    → health check (no auth)
 */

import express from "express";
import { readFileSync, existsSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { timingSafeEqual } from "crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";

import {
    autoLogin,
    loginWithQR,
    isLoggedIn,
    getApi,
    extractCredentials,
    clearSession,
} from "../core/zalo-client.js";
import { saveCredentials } from "../core/credentials.js";
import { addAccount, setActive, getActive } from "../core/accounts.js";
import { MessageBuffer } from "../mcp/message-buffer.js";
import { ThreadFilter } from "../mcp/thread-filter.js";
import { loadMCPConfig, parseDuration } from "../mcp/mcp-config.js";
import { registerTools } from "../mcp/mcp-tools.js";
import { ThreadNameCache } from "../mcp/thread-name-cache.js";
import { extractMessageText } from "../utils/extract-message-text.js";
import { autoDownloadMedia, isDownloadableMedia } from "../mcp/media-downloader.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const pkg = JSON.parse(readFileSync(join(__dirname, "../../package.json"), "utf8"));

const CLOSE_DUPLICATE = 3000;

function normalizeMessage(msg) {
    const rawContent = msg.data.content;
    const isText = typeof rawContent === "string";
    return {
        id: msg.data.msgId,
        threadId: msg.threadId,
        threadType: msg.type === 0 ? "dm" : "group",
        senderId: msg.data.uidFrom || null,
        senderName: msg.data.dName || null,
        text: isText ? rawContent : extractMessageText(rawContent, msg.data.msgType),
        timestamp: Date.now(),
        type: isText ? "text" : msg.data.msgType || "attachment",
        attachment:
            !isText && rawContent
                ? { type: msg.data.msgType, url: rawContent.href || null, description: rawContent.title || null }
                : null,
        replyTo: null,
    };
}

export function registerServeCommand(program) {
    program
        .command("serve")
        .description("All-in-one HTTP service: QR login UI + MCP endpoint (for Coolify/Docker)")
        .option("--port <port>", "HTTP port", "3000")
        .option("--host <host>", "Bind address", "0.0.0.0")
        .option("--auth <token>", "Bearer token for /mcp endpoint (env: MCP_AUTH_TOKEN)")
        .action(async (opts) => {
            process.env.ZALO_JSON_MODE = "1";

            const port = Number(opts.port) || 3000;
            const host = opts.host || "0.0.0.0";
            const authToken = opts.auth || process.env.MCP_AUTH_TOKEN || null;

            const config = loadMCPConfig();
            const maxAge = parseDuration(config.limits?.bufferMaxAge ?? "2h");
            const maxSize = config.limits?.bufferMaxSize ?? 500;
            const buffer = new MessageBuffer(maxSize, maxAge);
            const filter = new ThreadFilter(config);
            const nameCache = new ThreadNameCache();

            // State
            let qrImagePath = null;
            let loginStatus = "pending"; // pending | logging_in | logged_in | error
            let loginUser = null;
            let reconnectCount = 0;

            const app = express();
            app.use(express.json());

            // --- Auth middleware (skip /health, /api/status, /qr.png, /) ---
            app.use((req, res, next) => {
                const openPaths = ["/health", "/api/status", "/qr.png", "/"];
                if (!authToken || openPaths.includes(req.path)) return next();
                const token = req.headers.authorization?.slice(7) || "";
                const expected = Buffer.from(authToken, "utf8");
                const received = Buffer.from(token, "utf8");
                if (expected.length !== received.length || !timingSafeEqual(expected, received)) {
                    return res.status(401).json({ error: "Unauthorized" });
                }
                next();
            });

            // --- GET / ---
            app.get("/", (_req, res) => {
                const loggedIn = isLoggedIn();
                if (loggedIn) {
                    res.send(statusPage(loginUser, pkg.version, buffer));
                } else {
                    res.send(loginPage(qrImagePath, pkg.version));
                }
            });

            // --- GET /qr.png ---
            app.get("/qr.png", (_req, res) => {
                if (!qrImagePath || !existsSync(qrImagePath)) {
                    return res.status(404).send("QR not ready yet");
                }
                const img = readFileSync(qrImagePath);
                res.set({ "Content-Type": "image/png", "Cache-Control": "no-cache, no-store" });
                res.send(img);
            });

            // --- GET /api/status ---
            app.get("/api/status", (_req, res) => {
                res.json({
                    loggedIn: isLoggedIn(),
                    user: loginUser,
                    status: loginStatus,
                    version: pkg.version,
                    uptime: Math.floor(process.uptime()),
                    bufferedThreads: buffer.getStats().length,
                });
            });

            // --- GET /health ---
            app.get("/health", (_req, res) => {
                res.json({ status: "ok", uptime: Math.floor(process.uptime()) });
            });

            // --- POST /mcp ---
            app.post("/mcp", async (req, res) => {
                if (!isLoggedIn()) {
                    return res.status(503).json({
                        jsonrpc: "2.0",
                        error: { code: -32603, message: "Not logged in. Visit / to scan QR code." },
                        id: null,
                    });
                }
                try {
                    const server = new McpServer({ name: "zalo-agent", version: pkg.version });
                    registerTools(server, getApi(), buffer, filter, config, nameCache);
                    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
                    res.on("close", () => { transport.close(); server.close(); });
                    await server.connect(transport);
                    await transport.handleRequest(req, res, req.body);
                } catch (err) {
                    console.error("[serve] MCP error:", err);
                    if (!res.headersSent) {
                        res.status(500).json({ jsonrpc: "2.0", error: { code: -32603, message: "Internal error" }, id: null });
                    }
                }
            });

            // Start HTTP server
            app.listen(port, host, () => {
                console.error(`[serve] zalo-agent v${pkg.version} listening on ${host}:${port}`);
                console.error(`[serve] Open http://localhost:${port}/ to login`);
            });

            // Try auto-login first
            try {
                await autoLogin(true);
            } catch {}

            if (isLoggedIn()) {
                await startListener();
            } else {
                await startQRLogin();
            }

            async function startQRLogin() {
                loginStatus = "logging_in";
                console.error("[serve] Not logged in — starting QR login...");
                try {
                    const { api, ownId } = await loginWithQR(null, (event) => {
                        qrImagePath = event.data?.qrPath || event.qrPath || null;
                        console.error(`[serve] QR ready at http://localhost:${port}/qr.png`);
                    });

                    // Save credentials
                    const creds = extractCredentials();
                    saveCredentials(ownId, creds);
                    addAccount(ownId, api.getContext?.()?.name || "", null);
                    setActive(ownId);

                    loginUser = api.getContext?.()?.name || ownId;
                    loginStatus = "logged_in";
                    console.error(`[serve] Logged in as: ${loginUser}`);

                    await startListener();
                } catch (e) {
                    loginStatus = "error";
                    console.error("[serve] QR login failed:", e.message);
                }
            }

            async function startListener() {
                loginStatus = "logged_in";
                const active = getActive();
                loginUser = active?.name || loginUser || "Unknown";

                try {
                    await nameCache.init(getApi());
                } catch (e) {
                    console.error("[serve] Thread name cache init failed (non-fatal):", e.message);
                }

                function attachListenerHandlers(api) {
                    api.listener.on("message", (msg) => {
                        if (msg.isSelf) return;
                        const normalized = normalizeMessage(msg);
                        if (!filter.shouldWatch(normalized.threadId, normalized.threadType)) return;
                        if (!filter.shouldKeep(normalized)) return;
                        if (normalized.attachment?.url && isDownloadableMedia(normalized.type)) {
                            const threadName = nameCache?.get(normalized.threadId)?.name || null;
                            autoDownloadMedia(normalized, { downloadDir: config.media?.downloadDir || undefined, threadName });
                        }
                        buffer.push(normalized.threadId, normalized);
                    });

                    api.listener.on("connected", () => {
                        if (reconnectCount > 0) console.error(`[serve] Reconnected (#${reconnectCount})`);
                    });

                    api.listener.on("closed", async (code) => {
                        if (code === CLOSE_DUPLICATE) {
                            console.error("[serve] Duplicate session detected. Exiting.");
                            process.exit(1);
                        }
                        reconnectCount++;
                        if (code === 1000) {
                            await new Promise((r) => setTimeout(r, 2000));
                            try { getApi().listener.start({ retryOnClose: true }); } catch {}
                            return;
                        }
                        console.error(`[serve] WS closed (code: ${code}). Re-login in 5s...`);
                        await new Promise((r) => setTimeout(r, 5000));
                        try {
                            clearSession();
                            await autoLogin(true);
                            const newApi = getApi();
                            attachListenerHandlers(newApi);
                            newApi.listener.start({ retryOnClose: true });
                        } catch (e) {
                            console.error("[serve] Re-login failed:", e.message);
                        }
                    });

                    api.listener.on("error", () => {});
                }

                const api = getApi();
                attachListenerHandlers(api);
                api.listener.start({ retryOnClose: true });
                console.error("[serve] Zalo listener started. MCP ready.");
            }

            await new Promise(() => {});
        });
}

function loginPage(qrImagePath, version) {
    const hasQr = qrImagePath && existsSync(qrImagePath);
    return `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><title>Zalo Agent — Login</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{box-sizing:border-box}
body{display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;
  background:linear-gradient(135deg,#0a1628 0%,#111d33 50%,#0d1f3c 100%);
  font-family:system-ui,-apple-system,sans-serif}
.card{text-align:center;padding:2.5rem 2rem;background:rgba(17,29,51,0.9);
  border-radius:20px;max-width:420px;width:90%;
  border:1px solid rgba(59,130,246,0.2);backdrop-filter:blur(10px);
  box-shadow:0 20px 60px rgba(0,0,0,0.5)}
h1{color:#e2e8f0;font-size:1.2rem;font-weight:600;margin:0 0 0.3rem}
.sub{color:#3b82f6;font-size:0.8rem;margin-bottom:1.5rem;opacity:0.8}
.qr-img{width:260px;height:260px;border-radius:12px;border:3px solid rgba(59,130,246,0.3)}
.hint{color:#94a3b8;font-size:0.85rem;margin-top:1.2rem;line-height:1.5}
.hint strong{color:#60a5fa}
.waiting{color:#94a3b8;font-size:0.9rem;padding:2rem}
.dot{animation:blink 1.2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}
.footer{color:#475569;font-size:0.7rem;margin-top:1.5rem}
.success{color:#4ade80;font-size:1.3rem;font-weight:bold;display:none}
</style></head>
<body><div class="card">
<h1>Zalo Agent</h1>
<p class="sub">v${version} — MCP Service</p>
<div id="qr-section">
${hasQr
    ? `<img src="/qr.png?t=${Date.now()}" class="qr-img" alt="QR Code" id="qrimg"/>
       <p class="hint">Open <strong>Zalo</strong> › <strong>QR Scanner</strong> to scan</p>`
    : `<p class="waiting">Generating QR code<span class="dot">...</span></p>`}
</div>
<div id="success-section" class="success">
  <p>Login Successful!</p>
  <p style="font-size:1rem;color:#94a3b8">You can close this tab.</p>
</div>
<p class="footer">zalo-agent-cli</p>
</div>
<script>
async function poll(){
  try{
    const r=await fetch('/api/status');
    const d=await r.json();
    if(d.loggedIn){
      document.getElementById('qr-section').style.display='none';
      document.getElementById('success-section').style.display='block';
      return;
    }
    // Refresh QR image if not loaded yet
    const img=document.getElementById('qrimg');
    if(!img && d.status==='logging_in'){
      const sec=document.getElementById('qr-section');
      const newImg=document.createElement('img');
      newImg.src='/qr.png?t='+Date.now();
      newImg.className='qr-img';
      newImg.id='qrimg';
      newImg.onerror=()=>setTimeout(()=>{newImg.src='/qr.png?t='+Date.now();},1000);
      sec.innerHTML='';
      sec.appendChild(newImg);
      sec.insertAdjacentHTML('beforeend','<p class="hint">Open <strong>Zalo</strong> › <strong>QR Scanner</strong> to scan</p>');
    }
  }catch{}
  setTimeout(poll, 2000);
}
poll();
</script>
</body></html>`;
}

function statusPage(user, version, buffer) {
    const stats = buffer.getStats();
    return `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><title>Zalo Agent — Running</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{box-sizing:border-box}
body{display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;
  background:linear-gradient(135deg,#0a1628 0%,#111d33 50%,#0d1f3c 100%);
  font-family:system-ui,-apple-system,sans-serif}
.card{text-align:center;padding:2.5rem 2rem;background:rgba(17,29,51,0.9);
  border-radius:20px;max-width:420px;width:90%;
  border:1px solid rgba(74,222,128,0.3);backdrop-filter:blur(10px);
  box-shadow:0 20px 60px rgba(0,0,0,0.5)}
h1{color:#4ade80;font-size:1.4rem;margin:0 0 0.3rem}
.sub{color:#94a3b8;font-size:0.85rem;margin-bottom:1.5rem}
.badge{display:inline-block;background:rgba(74,222,128,0.15);color:#4ade80;
  border:1px solid rgba(74,222,128,0.3);border-radius:20px;padding:0.3rem 1rem;
  font-size:0.8rem;margin-bottom:1.5rem}
.info{color:#94a3b8;font-size:0.85rem;line-height:2}
.info strong{color:#e2e8f0}
.endpoint{background:rgba(0,0,0,0.3);border-radius:8px;padding:0.8rem 1rem;
  margin-top:1.2rem;text-align:left;font-size:0.8rem;color:#60a5fa;word-break:break-all}
.footer{color:#475569;font-size:0.7rem;margin-top:1.5rem}
</style></head>
<body><div class="card">
<h1>Online</h1>
<p class="sub">v${version}</p>
<span class="badge">Logged in as ${user || "Unknown"}</span>
<div class="info">
  <div>Buffered threads: <strong>${stats.length}</strong></div>
  <div>Uptime: <strong id="uptime">...</strong></div>
</div>
<div class="endpoint">MCP endpoint: <strong>/mcp</strong></div>
<p class="footer">zalo-agent-cli</p>
</div>
<script>
let start=Date.now();
async function refresh(){
  try{
    const r=await fetch('/api/status');
    const d=await r.json();
    document.getElementById('uptime').textContent=formatUptime(d.uptime);
  }catch{}
  setTimeout(refresh, 5000);
}
function formatUptime(s){
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sec=s%60;
  return h?h+'h '+m+'m':m?m+'m '+sec+'s':sec+'s';
}
refresh();
</script>
</body></html>`;
}

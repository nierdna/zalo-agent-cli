# Hướng dẫn triển khai MCP cho Zalo SDK (zca-js + WS Listener)

> Tham chiếu từ `zalo-agent-cli`. Áp dụng cho SDK đóng gói riêng dùng zca-js bên dưới.

---

## 1. Kiến trúc tổng quan

```
zca-js WebSocket (Zalo server)
         │ events: message, friend_event, group_event
         ▼
  attachListenerHandlers()
         │ normalize + filter
         ▼
   MessageBuffer (ring buffer, in-memory)
         │
  ┌──────┴──────────────────────────────────┐
  │           MCP Layer                     │
  │  ┌─────────────┐   ┌─────────────────┐  │
  │  │  MCP Tools  │   │  send → zca-js  │  │
  │  │  (read buf) │   │  api.sendMsg()  │  │
  │  └─────────────┘   └─────────────────┘  │
  └──────┬──────────────────────────────────┘
         │ stdio (local) hoặc HTTP (VPS)
         ▼
      Claude
```

**Luồng dữ liệu:**
1. zca-js listener nhận event `message` từ Zalo WebSocket — realtime, ~0ms delay
2. `normalizeMessage()` chuyển raw event → format chuẩn
3. `ThreadFilter` lọc noise (sticker, system msg, emoji ngắn)
4. `MessageBuffer.push()` lưu tin với cursor tăng dần
5. Claude gọi `zalo_get_messages(since=cursor)` → đọc từ buffer
6. Claude gọi `zalo_send_message` → gọi thẳng `api.sendMessage()` của zca-js

---

## 2. Dependencies

```bash
npm install @modelcontextprotocol/sdk zod express
# zca-js đã có trong SDK của bạn rồi — không cài thêm
```

```json
{
  "type": "module",
  "dependencies": {
    "@modelcontextprotocol/sdk": "^1.27.1",
    "express": "^5.0.0",
    "zod": "^4.0.0"
  }
}
```

---

## 3. Cấu trúc thư mục

```
src/mcp/
├── message-buffer.js      // Ring buffer với cursor — copy nguyên
├── thread-filter.js       // Lọc thread/noise — copy nguyên
├── mcp-tools.js           // 7 MCP tools đăng ký với Claude
├── mcp-server.js          // stdio transport (local)
├── mcp-http-transport.js  // HTTP transport (VPS)
├── mcp-config.js          // Config file
└── start.js               // Entry point — wire tất cả
```

---

## 4. MessageBuffer — Copy nguyên, không sửa

Ring buffer lưu tin theo thread, đọc tăng dần bằng cursor số nguyên. Hoàn toàn độc lập với zca-js.

```js
// src/mcp/message-buffer.js
export class MessageBuffer {
    constructor(maxSize = 500, maxAge = 2 * 60 * 60 * 1000) {
        this._threads = new Map();
        this._maxSize = maxSize;
        this._maxAge = maxAge;
        this._globalCursor = 0;
    }

    push(threadId, message) {
        if (!this._threads.has(threadId)) {
            this._threads.set(threadId, { messages: [], lastActivity: Date.now() });
        }
        const thread = this._threads.get(threadId);
        message._cursor = ++this._globalCursor;
        thread.messages.push(message);
        thread.lastActivity = Date.now();
        this._evict(threadId);
    }

    read(threadId, since = 0, maxCount = 20) {
        const sources = threadId
            ? [this._threads.get(threadId)].filter(Boolean)
            : Array.from(this._threads.values());

        const all = [];
        for (const thread of sources) {
            for (const msg of thread.messages) {
                if (msg._cursor > since) all.push(msg);
            }
        }
        all.sort((a, b) => a._cursor - b._cursor);

        const hasMore = all.length > maxCount;
        const messages = all.slice(0, maxCount);
        const cursor = messages.length > 0 ? messages[messages.length - 1]._cursor : since;
        return { messages, cursor, hasMore };
    }

    markRead(cursor) {
        let discarded = 0;
        for (const [, thread] of this._threads) {
            const before = thread.messages.length;
            thread.messages = thread.messages.filter((m) => m._cursor > cursor);
            discarded += before - thread.messages.length;
        }
        return discarded;
    }

    getStats(readCursor = 0) {
        const stats = [];
        for (const [threadId, thread] of this._threads) {
            if (thread.messages.length === 0) continue;
            const unread = thread.messages.filter((m) => m._cursor > readCursor).length;
            stats.push({ threadId, unread, total: thread.messages.length, lastActivity: thread.lastActivity });
        }
        return stats;
    }

    getThreadType(threadId) {
        return this._threads.get(threadId)?.messages?.[0]?.threadType ?? null;
    }

    _evict(threadId) {
        const thread = this._threads.get(threadId);
        if (!thread) return;
        const now = Date.now();
        thread.messages = thread.messages.filter((m) => now - m.timestamp < this._maxAge);
        if (thread.messages.length > this._maxSize) {
            thread.messages = thread.messages.slice(thread.messages.length - this._maxSize);
        }
        if (thread.messages.length === 0) this._threads.delete(threadId);
    }
}
```

**Tại sao cursor?** Claude poll bằng `zalo_get_messages(since=N)` — chỉ lấy tin có `_cursor > N`. Không cần lưu state phía Claude, không bao giờ bỏ sót hay duplicate.

---

## 5. ThreadFilter — Copy nguyên, không sửa

```js
// src/mcp/thread-filter.js
const SYSTEM_MSG_TYPES = new Set(["system", "join", "leave", "pin", "unpin", "rename"]);

export class ThreadFilter {
    constructor(config) {
        this._watchPatterns = config.watchThreads || ["dm:*", "group:*"];
        this._triggerKeywords = (config.triggerKeywords || []).map((k) => k.toLowerCase());
    }

    shouldWatch(threadId, threadType) {
        const key = `${threadType}:${threadId}`;
        for (const pattern of this._watchPatterns) {
            if (pattern === `${threadType}:*`) return true;
            if (pattern === "*" || pattern === "*:*") return true;
            if (pattern === key) return true;
        }
        return false;
    }

    shouldKeep(message) {
        if (message.type && SYSTEM_MSG_TYPES.has(message.type)) return false;
        if (message.type === "sticker") return false;
        if (message.text && message.text.length < 3 && /^[\s\p{Emoji}]*$/u.test(message.text)) return false;
        return true;
    }

    isTrigger(message) {
        if (!message.text || this._triggerKeywords.length === 0) return false;
        const lower = message.text.toLowerCase();
        return this._triggerKeywords.some((kw) => lower.includes(kw));
    }
}
```

**Pattern format:**
- `"dm:*"` — tất cả DM
- `"group:*"` — tất cả nhóm
- `"group:1234567890"` — chỉ nhóm cụ thể
- `"*"` — tất cả

---

## 6. normalizeMessage — Điểm duy nhất cần điều chỉnh

Hàm này map raw zca-js event → format chuẩn cho buffer. Nếu SDK của bạn wrap zca-js và thay đổi shape của event thì điều chỉnh tại đây.

```js
// src/mcp/start.js (hoặc inline trong listener handler)

/**
 * Map raw zca-js message event → buffer message shape.
 * Điều chỉnh nếu SDK của bạn transform event trước khi emit.
 *
 * Raw zca-js event shape (tham khảo):
 *   msg.data.msgId      — message ID
 *   msg.threadId        — thread (userId cho DM, groupId cho group)
 *   msg.type            — 0 = DM, 1 = Group  (ThreadType enum)
 *   msg.data.uidFrom    — sender ID
 *   msg.data.dName      — sender display name
 *   msg.data.content    — string (text) hoặc object (ảnh/file/link)
 *   msg.data.msgType    — "webchat", "photo", "gif", "file", "link", v.v.
 *   msg.isSelf          — true nếu do chính mình gửi
 */
function normalizeMessage(msg) {
    const rawContent = msg.data.content;
    const isText = typeof rawContent === "string";
    return {
        id: msg.data.msgId,
        threadId: msg.threadId,
        threadType: msg.type === 0 ? "dm" : "group",
        senderId: msg.data.uidFrom || null,
        senderName: msg.data.dName || null,
        text: isText ? rawContent : (rawContent?.title || rawContent?.href || null),
        timestamp: Date.now(),
        type: isText ? "text" : (msg.data.msgType || "attachment"),
        attachment: !isText && rawContent
            ? { type: msg.data.msgType, url: rawContent.href || null, description: rawContent.title || null }
            : null,
    };
}
```

---

## 7. attachListenerHandlers — Thay thế PollingLoop

Đây là hàm cốt lõi — gắn event handlers vào zca-js listener. Cần gọi lại sau mỗi lần re-login vì `api` instance mới được tạo.

```js
// src/mcp/start.js

function attachListenerHandlers(api, buffer, filter, config) {
    // ─── Tin nhắn đến ────────────────────────────────────────────────────────
    api.listener.on("message", (msg) => {
        // Bỏ qua tin tự gửi
        if (msg.isSelf) return;

        const normalized = normalizeMessage(msg);

        // Áp dụng thread watch filter
        if (!filter.shouldWatch(normalized.threadId, normalized.threadType)) return;

        // Lọc noise: sticker, system msg, emoji ngắn
        if (!filter.shouldKeep(normalized)) return;

        buffer.push(normalized.threadId, normalized);
        console.error(`[mcp] Buffered ${normalized.threadType} msg from ${normalized.threadId}`);
    });

    // ─── Lifecycle (reconnect) ────────────────────────────────────────────────
    api.listener.on("connected", () => {
        console.error("[mcp] Zalo WS connected");
    });

    api.listener.on("disconnected", (code) => {
        console.error(`[mcp] Disconnected (code: ${code}). Auto-retrying...`);
    });

    // "closed" = mất kết nối hẳn → cần re-login
    api.listener.on("closed", async (code) => {
        // Code 3000 = duplicate session (Zalo Web mở ở nơi khác) → fatal
        if (code === 3000) {
            console.error("[mcp] Duplicate Zalo Web session. Exiting.");
            process.exit(1);
        }

        console.error(`[mcp] Connection closed (${code}). Re-login in 5s...`);
        await sleep(5000);

        try {
            // Gọi hàm re-login của SDK bạn
            await yourSDK.reLogin();
            const newApi = yourSDK.getApi();
            attachListenerHandlers(newApi, buffer, filter, config);
            newApi.listener.start({ retryOnClose: true });
            console.error("[mcp] Re-login OK. Listener restarted.");
        } catch (e) {
            console.error(`[mcp] Re-login failed: ${e.message}. Retry in 30s...`);
            await sleep(30000);
            try {
                await yourSDK.reLogin();
                const retryApi = yourSDK.getApi();
                attachListenerHandlers(retryApi, buffer, filter, config);
                retryApi.listener.start({ retryOnClose: true });
            } catch (e2) {
                console.error(`[mcp] Re-login retry failed: ${e2.message}. Exiting.`);
                process.exit(1);
            }
        }
    });

    api.listener.on("error", () => {
        // WS errors luôn được theo sau bởi close/disconnect → suppress
    });
}

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}
```

**Tại sao phải gọi lại `attachListenerHandlers` sau re-login?**
zca-js tạo ra một `api` instance mới sau khi re-login. Instance cũ đã bị hủy — handlers gắn trên instance cũ không còn hiệu lực. Phải gắn lại trên instance mới.

---

## 8. MCP Tools — 7 tools

```js
// src/mcp/mcp-tools.js
import { z } from "zod";

function ok(result) {
    return { content: [{ type: "text", text: JSON.stringify(result) }] };
}
function err(message) {
    return { content: [{ type: "text", text: `Error: ${message}` }], isError: true };
}

/**
 * @param {McpServer} server
 * @param {object} api        - zca-js api instance (hoặc wrapper của SDK bạn)
 * @param {MessageBuffer} buffer
 * @param {ThreadFilter} filter
 * @param {object} config
 */
export function registerTools(server, api, buffer, filter, config) {
    const maxPerPoll = config.limits?.maxMessagesPerPoll ?? 20;

    // ─── 1. zalo_get_messages ────────────────────────────────────────────────
    server.registerTool(
        "zalo_get_messages",
        {
            title: "Get Zalo Messages",
            description: "Lấy tin nhắn mới từ buffer. Dùng 'since' cursor từ lần trước để không bỏ sót, không duplicate.",
            inputSchema: z.object({
                threadId: z.string().optional().describe("Lọc theo thread cụ thể. Bỏ trống = tất cả."),
                since: z.number().int().min(0).default(0).describe("Cursor từ lần gọi trước"),
                limit: z.number().int().min(1).max(100).default(maxPerPoll),
            }),
        },
        async ({ threadId, since, limit }) => {
            try {
                return ok(buffer.read(threadId, since, limit));
            } catch (e) {
                return err(e.message);
            }
        },
    );

    // ─── 2. zalo_send_message ────────────────────────────────────────────────
    server.registerTool(
        "zalo_send_message",
        {
            title: "Send Zalo Message",
            description: "Gửi tin nhắn text đến DM hoặc nhóm. threadType: 0=DM, 1=Group.",
            inputSchema: z.object({
                threadId: z.string(),
                text: z.string().min(1),
                threadType: z.number().int().min(0).max(1).default(0).describe("0=DM, 1=Group"),
            }),
        },
        async ({ threadId, text, threadType }) => {
            try {
                // Gọi thẳng zca-js — không qua HTTP
                const result = await api.sendMessage(text, threadId, threadType);
                const messageId = result?.message?.msgId ?? result?.msgId ?? null;
                return ok({ success: true, messageId });
            } catch (e) {
                return err(e.message);
            }
        },
    );

    // ─── 3. zalo_list_threads ────────────────────────────────────────────────
    server.registerTool(
        "zalo_list_threads",
        {
            title: "List Zalo Threads",
            description: "Liệt kê threads đang có tin trong buffer, kèm số tin chưa đọc.",
            inputSchema: z.object({
                type: z.enum(["group", "dm", "all"]).default("all"),
            }),
        },
        async ({ type }) => {
            try {
                const stats = buffer.getStats(0).map((t) => ({
                    ...t,
                    threadType: buffer.getThreadType(t.threadId) ?? "unknown",
                }));
                const filtered = type === "all" ? stats : stats.filter((t) => t.threadType === type);
                return ok({ threads: filtered, total: filtered.length });
            } catch (e) {
                return err(e.message);
            }
        },
    );

    // ─── 4. zalo_search_threads ──────────────────────────────────────────────
    server.registerTool(
        "zalo_search_threads",
        {
            title: "Search Zalo Threads",
            description: "Tìm thread theo tên (fuzzy, tiếng Việt). Dùng khi cần tìm threadId của khách theo tên.",
            inputSchema: z.object({
                query: z.string().min(1),
                type: z.enum(["group", "dm", "all"]).default("all"),
                limit: z.number().int().min(1).max(50).default(10),
            }),
        },
        async ({ query, type, limit }) => {
            try {
                // Nếu bạn có ThreadNameCache (xem phần 9), dùng cache.search()
                // Nếu không, gọi API zca-js để tìm:
                const friends = await api.getAllFriends();
                const groups = await api.getAllGroups();

                const normalize = (s) =>
                    s.normalize("NFD").replace(/[̀-ͯ]/g, "").replace(/đ/gi, "d").toLowerCase();

                const q = normalize(query);
                const results = [];

                if (type !== "group") {
                    for (const f of (Array.isArray(friends) ? friends : [])) {
                        const name = f.displayName || f.zaloName || "";
                        if (normalize(name).includes(q)) {
                            results.push({ threadId: f.userId, name, threadType: "dm" });
                        }
                    }
                }

                if (type !== "dm") {
                    const groupMap = groups?.gridInfoMap || {};
                    for (const [gid, g] of Object.entries(groupMap)) {
                        if (normalize(g.name || "").includes(q)) {
                            results.push({ threadId: gid, name: g.name, threadType: "group", memberCount: g.totalMember });
                        }
                    }
                }

                return ok({ results: results.slice(0, limit), total: Math.min(results.length, limit) });
            } catch (e) {
                return err(e.message);
            }
        },
    );

    // ─── 5. zalo_get_history ─────────────────────────────────────────────────
    server.registerTool(
        "zalo_get_history",
        {
            title: "Get Zalo Message History",
            description: "Lấy lịch sử chat từ Zalo server (~2 tuần). Dùng để đọc context trước khi trả lời.",
            inputSchema: z.object({
                threadId: z.string(),
                threadType: z.number().int().min(0).max(1).default(0).describe("0=DM, 1=Group"),
                limit: z.number().int().min(1).max(200).default(50),
                lastMsgId: z.string().optional().nullable().describe("Cursor phân trang"),
            }),
        },
        async ({ threadId, threadType, limit, lastMsgId }) => {
            try {
                // Dùng zca-js requestOldMessages — pattern event-based như repo gốc
                const messages = await new Promise((resolve) => {
                    const collected = [];
                    const handler = (msgs) => {
                        for (const msg of (Array.isArray(msgs) ? msgs : [])) {
                            const target = String(threadId);
                            const msgThread = String(msg.threadId || "");
                            const msgSender = String(msg.data?.uidFrom || "");
                            if (msgThread !== target && msgSender !== target) continue;
                            if (collected.length >= limit) break;
                            const raw = msg.data?.content;
                            const isText = typeof raw === "string";
                            collected.push({
                                msgId: msg.data?.msgId,
                                threadId: msg.threadId,
                                senderId: msg.data?.uidFrom || null,
                                senderName: msg.data?.dName || null,
                                text: isText ? raw : (raw?.title || raw?.href || null),
                                timestamp: msg.data?.ts ? Number(msg.data.ts) : null,
                                type: isText ? "text" : (msg.data?.msgType || "attachment"),
                            });
                        }
                        if (collected.length >= limit) {
                            clearTimeout(timer);
                            api.listener.removeListener("old_messages", handler);
                            resolve(collected);
                        }
                    };
                    const timer = setTimeout(() => {
                        api.listener.removeListener("old_messages", handler);
                        resolve(collected);
                    }, 10000);

                    api.listener.on("old_messages", handler);
                    api.listener.requestOldMessages(threadType, lastMsgId || null);
                });

                messages.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
                return ok({ threadId, count: messages.length, messages });
            } catch (e) {
                return err(e.message);
            }
        },
    );

    // ─── 6. zalo_mark_read ───────────────────────────────────────────────────
    server.registerTool(
        "zalo_mark_read",
        {
            title: "Mark Zalo Messages Read",
            description: "Xoá tin đã xử lý khỏi buffer. Gọi sau khi Claude đã reply xong.",
            inputSchema: z.object({
                cursor: z.number().int().min(0).describe("Cursor từ zalo_get_messages"),
            }),
        },
        async ({ cursor }) => {
            try {
                const discarded = buffer.markRead(cursor);
                return ok({ success: true, discarded });
            } catch (e) {
                return err(e.message);
            }
        },
    );

    // ─── 7. zalo_get_status ──────────────────────────────────────────────────
    server.registerTool(
        "zalo_get_status",
        {
            title: "Get Zalo MCP Status",
            description: "Xem trạng thái buffer và kết nối.",
            inputSchema: z.object({}),
        },
        async () => {
            try {
                const stats = buffer.getStats();
                return ok({
                    threads: stats.length,
                    totalMessages: stats.reduce((s, t) => s + t.total, 0),
                    unread: stats.reduce((s, t) => s + t.unread, 0),
                });
            } catch (e) {
                return err(e.message);
            }
        },
    );
}
```

---

## 9. MCP Server — stdio transport

```js
// src/mcp/mcp-server.js
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { registerTools } from "./mcp-tools.js";

// QUAN TRỌNG: Mọi log PHẢI dùng console.error() — stdout là MCP protocol channel.

export async function createMCPServer(api, buffer, filter, config) {
    const server = new McpServer({ name: "zalo-mcp", version: "1.0.0" });
    registerTools(server, api, buffer, filter, config);

    const transport = new StdioServerTransport();
    await server.connect(transport);

    console.error("[mcp-server] Connected via stdio");
    return server;
}
```

---

## 10. MCP HTTP Transport — dùng khi deploy VPS

```js
// src/mcp/mcp-http-transport.js
import express from "express";
import { timingSafeEqual } from "crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { registerTools } from "./mcp-tools.js";

export function createHTTPServer({ api, buffer, filter, config }, port, token, host = "127.0.0.1") {
    const app = express();
    app.use(express.json());

    if (token) {
        app.use((req, res, next) => {
            if (req.path === "/health") return next();
            const received = req.headers.authorization?.slice(7) || "";
            const expected = Buffer.from(token, "utf8");
            const recv = Buffer.from(received, "utf8");
            if (expected.length !== recv.length || !timingSafeEqual(expected, recv)) {
                return res.status(401).json({ error: "Unauthorized" });
            }
            next();
        });
    }

    // Stateless — mỗi request tạo server+transport mới, share buffer qua closure
    app.post("/mcp", async (req, res) => {
        try {
            const server = new McpServer({ name: "zalo-mcp", version: "1.0.0" });
            registerTools(server, api, buffer, filter, config);

            const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
            res.on("close", () => { transport.close(); server.close(); });

            await server.connect(transport);
            await transport.handleRequest(req, res, req.body);
        } catch (e) {
            console.error("MCP HTTP error:", e.message);
            if (!res.headersSent) {
                res.status(500).json({ jsonrpc: "2.0", error: { code: -32603, message: "Internal error" }, id: null });
            }
        }
    });

    app.get("/health", (_req, res) => {
        res.json({ status: "ok", uptime: Math.floor(process.uptime()), threads: buffer.getStats().length });
    });

    return app.listen(port, host, () => {
        console.error(`[mcp-http] Listening on ${host}:${port}`);
    });
}
```

---

## 11. Entry point — Wiring tất cả

```js
// src/mcp/start.js
import { MessageBuffer } from "./message-buffer.js";
import { ThreadFilter } from "./thread-filter.js";
import { createMCPServer } from "./mcp-server.js";
import { createHTTPServer } from "./mcp-http-transport.js";

// Import api instance từ SDK của bạn
// SDK của bạn đã login qua GUI rồi — chỉ cần getApi()
import { getApi, reLogin } from "your-zalo-sdk";

const config = {
    watchThreads: ["dm:*", "group:*"],
    triggerKeywords: ["@bot"],
    limits: { maxMessagesPerPoll: 20, bufferMaxSize: 500, bufferMaxAge: 2 * 60 * 60 * 1000 },
};

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function normalizeMessage(msg) {
    const raw = msg.data.content;
    const isText = typeof raw === "string";
    return {
        id: msg.data.msgId,
        threadId: msg.threadId,
        threadType: msg.type === 0 ? "dm" : "group",
        senderId: msg.data.uidFrom || null,
        senderName: msg.data.dName || null,
        text: isText ? raw : (raw?.title || raw?.href || null),
        timestamp: Date.now(),
        type: isText ? "text" : (msg.data.msgType || "attachment"),
        attachment: !isText && raw ? { type: msg.data.msgType, url: raw.href || null } : null,
    };
}

function attachListenerHandlers(api, buffer, filter) {
    api.listener.on("message", (msg) => {
        if (msg.isSelf) return;
        const normalized = normalizeMessage(msg);
        if (!filter.shouldWatch(normalized.threadId, normalized.threadType)) return;
        if (!filter.shouldKeep(normalized)) return;
        buffer.push(normalized.threadId, normalized);
        console.error(`[mcp] +msg ${normalized.threadType}:${normalized.threadId}`);
    });

    api.listener.on("connected", () => console.error("[mcp] WS connected"));
    api.listener.on("disconnected", (code) => console.error(`[mcp] WS disconnected (${code})`));

    api.listener.on("closed", async (code) => {
        if (code === 3000) { console.error("[mcp] Duplicate session. Exit."); process.exit(1); }

        console.error(`[mcp] Closed (${code}). Re-login in 5s...`);
        await sleep(5000);
        try {
            await reLogin();
            const newApi = getApi();
            attachListenerHandlers(newApi, buffer, filter);
            newApi.listener.start({ retryOnClose: true });
            console.error("[mcp] Re-login OK");
        } catch (e) {
            console.error(`[mcp] Re-login failed: ${e.message}. Retry 30s...`);
            await sleep(30000);
            try {
                await reLogin();
                const retryApi = getApi();
                attachListenerHandlers(retryApi, buffer, filter);
                retryApi.listener.start({ retryOnClose: true });
            } catch (e2) {
                console.error(`[mcp] Fatal: ${e2.message}`);
                process.exit(1);
            }
        }
    });

    api.listener.on("error", () => {});
}

export async function startMCP({ httpPort = null, httpToken = null, httpHost = "127.0.0.1" } = {}) {
    const buffer = new MessageBuffer(config.limits.bufferMaxSize, config.limits.bufferMaxAge);
    const filter = new ThreadFilter(config);
    const api = getApi();

    // Gắn listener
    attachListenerHandlers(api, buffer, filter);
    api.listener.start({ retryOnClose: true });
    console.error("[mcp] Zalo listener started");

    // Khởi động MCP server
    if (httpPort) {
        createHTTPServer({ api, buffer, filter, config }, httpPort, httpToken, httpHost);
    } else {
        await createMCPServer(api, buffer, filter, config);
    }

    process.on("SIGINT", () => {
        try { api.listener.stop(); } catch {}
        process.exit(0);
    });

    await new Promise(() => {});
}
```

---

## 12. Config cho Claude

### Local — stdio (cùng máy với SDK)

```json
// .claude/settings.json
{
  "mcpServers": {
    "zalo": {
      "command": "node",
      "args": ["/path/to/project/src/mcp/start.js"]
    }
  }
}
```

### VPS — HTTP transport

```json
{
  "mcpServers": {
    "zalo": {
      "url": "http://your-vps-ip:3847/mcp",
      "headers": { "Authorization": "Bearer your-secret" }
    }
  }
}
```

---

## 13. Những điểm quan trọng

### stdout vs stderr — Bắt buộc với stdio transport

```js
// ✅ ĐÚNG
console.error("[mcp] Server started");

// ❌ SAI — phá vỡ JSON-RPC stream
console.log("Server started");
```

### Re-attach sau re-login

```js
// SAI — gắn handlers một lần rồi thôi
const api = getApi();
api.listener.on("message", handler);   // ← mất khi re-login

// ĐÚNG — tạo hàm attachListenerHandlers() và gọi lại với api mới
const newApi = getApi();               // ← instance mới sau re-login
attachListenerHandlers(newApi, ...);   // ← gắn lại
newApi.listener.start(...);
```

### Stateless HTTP — buffer share qua closure

```js
// McpServer/transport được tạo mới mỗi request — đúng theo spec
// Buffer sống xuyên suốt process — share qua closure của registerTools()
// Không cần session management
```

---

## 14. Checklist

- [ ] Copy `MessageBuffer` và `ThreadFilter` nguyên văn
- [ ] Điều chỉnh `normalizeMessage()` nếu SDK wrap lại shape của zca-js event
- [ ] Thay `import { getApi, reLogin } from "your-zalo-sdk"` bằng đúng import
- [ ] Kiểm tra `api.sendMessage(text, threadId, threadType)` có đúng signature với zca-js version đang dùng
- [ ] Toàn bộ MCP layer dùng `console.error()`, không có `console.log()`
- [ ] Test `zalo_get_messages` nhận tin realtime (gửi tin từ điện thoại khác → thấy trong buffer < 1s)
- [ ] Test `zalo_send_message` → tin đến đúng người
- [ ] Test re-login: tắt/bật mạng → listener tự reconnect không crash
- [ ] Nếu VPS: bind `0.0.0.0`, thêm `--auth` token

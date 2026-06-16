/**
 * MCP stdio↔HTTP proxy — connects to a remote MCP HTTP server and re-exposes
 * it locally via stdio transport. Allows Claude Desktop to use a remote
 * zalo-agent instance as if it were a local stdio MCP server.
 */

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const pkg = JSON.parse(readFileSync(join(__dirname, "../../package.json"), "utf8"));

/**
 * Start a local stdio MCP server that proxies all requests to a remote HTTP MCP server.
 * @param {string} url - Remote MCP server URL (e.g. https://zalo-mcp.lynxsolution.vn/mcp)
 * @param {string|null} authToken - Bearer token for Authorization header
 */
export async function startMCPProxy(url, authToken) {
    const headers = authToken ? { Authorization: `Bearer ${authToken}` } : {};

    // Connect to remote HTTP MCP server as a client
    const remoteClient = new Client({ name: "zalo-proxy-client", version: pkg.version });
    const httpTransport = new StreamableHTTPClientTransport(new URL(url), { requestInit: { headers } });

    try {
        await remoteClient.connect(httpTransport);
        console.error(`[mcp-proxy] Connected to remote: ${url}`);
    } catch (e) {
        console.error(`[mcp-proxy] Failed to connect to ${url}: ${e.message}`);
        process.exit(1);
    }

    // Create local stdio MCP server
    const localServer = new Server(
        { name: "zalo-agent", version: pkg.version },
        { capabilities: { tools: {} } },
    );

    // Forward listTools → remote
    localServer.setRequestHandler(ListToolsRequestSchema, async () => {
        return await remoteClient.listTools();
    });

    // Forward callTool → remote
    localServer.setRequestHandler(CallToolRequestSchema, async (req) => {
        return await remoteClient.callTool(req.params);
    });

    // Connect local server to stdio
    const stdioTransport = new StdioServerTransport();
    await localServer.connect(stdioTransport);
    console.error("[mcp-proxy] Stdio proxy ready");

    // Keep alive
    await new Promise(() => {});
}

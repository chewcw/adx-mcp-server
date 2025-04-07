import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js"
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js"
import { Client, KustoConnectionStringBuilder } from "azure-kusto-data"
import { z } from "zod"
import sqlite3 from "sqlite3"
import { promisify } from "util"

class AdxMcpServer {
  private server: any
  private client: Client | null = null
  private dbPath: string = "/workspace/rnd/mcp/adx-mcp-server/store.sqlite"

  constructor() {
    this.server = new McpServer(
      {
        name: "Azure Data Explorer",
        version: "1.0.0",
      },
      {
        capabilities: {
          prompts: {},
          resources: { subscribe: true },
          tools: {},
          logging: {},
        },
      })
  }

  initialize() {
    this.registerSchemaResource()
    this.registerConfigResource()
    this.registerQueryTool()
    return this
  }

  private registerSchemaResource() {
    this.server.resource(
      "schema",
      "schema://main",
      async (uri: { href: string }) => {
        const db = this.getDb()
        try {
          const tables = await db.all(
            "SELECT sql FROM sqlite_master WHERE type='table'",
          )
          return {
            contents: [{
              uri: uri.href,
              text: tables.map((t: { sql: string }) => t.sql).join("\n"),
            }],
          }
        } finally {
          await db.close()
        }
      },
    )
  }

  private registerConfigResource() {
    this.server.resource(
      "config",
      "config://azure-data-explorer-creds",
      async (_: { href: string }) => {
        return {
          contents: [
            {
              uri: "config://azure-data-explorer-creds/cluster-name",
              name: "Cluster Name",
              text: `${process.env.ADX_CLUSTER_NAME}`,
            },
            {
              uri: "config://azure-data-explorer-creds/client-id",
              name: "Client ID",
              text: `${process.env.ADX_CLIENT_ID}`,
            },
            {
              uri: "config://azure-data-explorer-creds/client-secret",
              name: "Client Secret",
              text: `${process.env.ADX_CLIENT_SECRET}`,
            },
            {
              uri: "config://azure-data-explorer-creds/tenant-id",
              name: "Tenant ID",
              text: `${process.env.ADX_TENANT_ID}`,
            },
          ],
        }
      }
    )
  }

  private registerQueryTool() {
    this.server.tool(
      "query",
      { sql: z.string() },
      async ({ sql }: { sql: string }) => {
        const db = this.getDb()
        try {
          const result = await db.all(sql)
          return {
            contents: [{
              type: "text",
              text: JSON.stringify(result, null, 2),
            }],
          }
        } catch (err: unknown) {
          return {
            contents: [{
              type: "text",
              text: `Error: ${err}`,
            }],
            isError: true,
          }
        } finally {
          await db.close()
        }
      }
    )
  }

  private registerNotification() {
    const messages = [
      { level: "debug", data: "Debug-level message" },
      { level: "info", data: "Info-level message" },
      { level: "notice", data: "Notice-level message" },
      { level: "warning", data: "Warning-level message" },
      { level: "error", data: "Error-level message" },
      { level: "critical", data: "Critical-level message" },
      { level: "alert", data: "Alert level-message" },
      { level: "emergency", data: "Emergency-level message" }
    ]
  }

  getDb() {
    const db = new sqlite3.Database(this.dbPath)
    return {
      all: promisify<string, any[]>(db.all.bind(db)),
      close: promisify(db.close.bind(db)),
    }
  }

  async connect() {
    const envSchema = z.object({
      ADX_CLUSTER_NAME: z.string().min(1, "Cluster name is required"),
      ADX_CLIENT_ID: z.string().min(1, "Client ID is required"),
      ADX_CLIENT_SECRET: z.string().min(1, "Client secret is required"),
      ADX_TENANT_ID: z.string().min(1, "Tenant ID is required"),
    });

    const result = envSchema.safeParse(process.env);
    if (result.success) {
      const {
        ADX_CLUSTER_NAME: clusterName,
        ADX_CLIENT_ID: clientId,
        ADX_CLIENT_SECRET: clientSecret,
        ADX_TENANT_ID: tenantId
      } = result.data;

      const kcsb = KustoConnectionStringBuilder.withAadApplicationKeyAuthentication(
        `https://${clusterName}.kusto.windows.net`,
        clientId,
        clientSecret,
        tenantId
      );

      this.client = new Client(kcsb);
    } else {
      console.error("Environment variable validation failed:", result.error.format());
    }
  }

  disconnect() {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }

  async start() {
    const transport = new StdioServerTransport()
    await this.server.connect(transport)
  }
}

export const createServer = async () => {
  const server = new AdxMcpServer()
    .initialize()

  await server.start()
  await server.connect()
}

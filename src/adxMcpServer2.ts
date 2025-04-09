import { Server } from "@modelcontextprotocol/sdk/server/index.js"
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js"
import { Client, KustoConnectionStringBuilder } from "azure-kusto-data"
import { z } from "zod"
import sqlite3 from "sqlite3"
import { promisify } from "util"
import {
  ListResourcesRequestSchema,
  ListResourceTemplatesRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js"
import { config } from "dotenv"

class AdxMcpServer {
  private server: any
  private client: Client | null = null
  private dbPath: string = "/workspace/rnd/mcp/adx-mcp-server/store.sqlite"

  constructor() {
    this.server = new Server(
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
    this.registerResources()
    this.registerResourceTemplates()
    this.registerQueryTool()
    return this
  }

  private registerResources() {
    this.server.setRequestHandler(ListResourcesRequestSchema, async () => {
      return {
        resources: [
          {
            uri: "config://azure-data-explorer-creds",
            name: "Config",
            description: "Config of the Azure Data Explorer",
          },
        ]
      }
    })

    this.server.setRequestHandler(ReadResourceRequestSchema, async (request: z.infer<typeof ReadResourceRequestSchema>) => {
      const uri = request.params.uri

      if (uri.startsWith("config://azure-data-explorer-creds")) {
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
              text: `${process.env.ADX_CLIENT_SECRET ? "******" : "Not set"}`,
            },
            {
              uri: "config://azure-data-explorer-creds/tenant-id",
              name: "Tenant ID",
              text: `${process.env.ADX_TENANT_ID}`,
            },
          ],
        }
      }
    })
  }

  private registerResourceTemplates() {
    this.server.setRequestHandler(ListResourceTemplatesRequestSchema, async () => {
      return {
        resourceTemplates: [
          {
            uriTemplate: "schema://adx/{db}",
            name: "Schema of the db",
            description: "List down the tables in the given db",
          },
          {
            uriTemplate: "schema://adx/{db}/{table}",
            name: "Schema of the table",
            description: "Schema of the given db and table",
          },
          {
            uriTemplate: "schema://adx/{db}/functions",
            name: "Functions of the db",
            description: "List all functions of the given db",
          },
        ]
      }
    })

    this.server.setRequestHandler(ReadResourceRequestSchema, async (request: z.infer<typeof ReadResourceRequestSchema>) => {
      const uri = request.params.uri
      const showTablesRegex = /schema:\/\/adx\/\w+$/
      if (showTablesRegex.test(uri)) {
        const db = uri.split("/")[3]
        if (!this.client) {
          throw new Error("Database client is not initialized")
        }
        if (!db) {
          return {
            contents: [{
              uri: uri,
              name: "Error",
              mimeType: "text/plain",
              text: "Invalid URI",
            }],
          }
        }
        const query = `.show tables`
        const response = await this.client.execute(db, query)
        const result = response.primaryResults[0].toString()
        return {
          contents: [{
            uri: uri,
            name: `Schema of the db ${db}`,
            mimeType: "text/plain",
            text: result,
          }],
        }
      }
      const showTableSchemaRegex = /schema:\/\/adx\/\w+\/\w+$/
      if (showTableSchemaRegex.test(uri)) {
        const db = uri.split("/")[3]
        const table = uri.split("/")[4]
        if (!this.client) {
          throw new Error("Database client is not initialized")
        }
        if (!db || !table) {
          return {
            contents: [{
              uri: uri,
              name: "Error",
              mimeType: "text/plain",
              text: "Invalid URI",
            }],
          }
        }
        const query = `${table} | getschema`
        const response = await this.client.execute(db, query)
        const result = response.primaryResults[0].toString()
        return {
          contents: [{
            uri: uri,
            name: `Schema of the table ${table}`,
            mimeType: "text/plain",
            text: result,
          }],
        }
      }
      const showFunctionsRegex = /schema:\/\/adx\/\w+\/functions$/
      if (showFunctionsRegex.test(uri)) {
        const db = uri.split("/")[3]
        if (!this.client) {
          throw new Error("Database client is not initialized")
        }
        const query = `.show functions`
        const response = await this.client.execute(db, query)
        const result = response.primaryResults[0].toString()
        if (!db) {
          return {
            contents: [{
              uri: uri,
              name: "Error",
              mimeType: "text/plain",
              text: "Invalid URI",
            }],
          }
        }
        return {
          contents: [{
            uri: uri,
            name: `Functions of the db ${db}`,
            mimeType: "text/plain",
            text: result,
          }],
        }
      }
    })
  }

  private registerQueryTool() {
    // this.server.tool(
    //   "query",
    //   { sql: z.string() },
    //   async ({ sql }: { sql: string }) => {
    //     const db = this.getDb()
    //     try {
    //       const result = await db.all(sql)
    //       return {
    //         contents: [{
    //           type: "text",
    //           text: JSON.stringify(result, null, 2),
    //         }],
    //       }
    //     } catch (err: unknown) {
    //       return {
    //         contents: [{
    //           type: "text",
    //           text: `Error: ${err}`,
    //         }],
    //         isError: true,
    //       }
    //     } finally {
    //       await db.close()
    //     }
    //   }
    // )
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
    config()

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

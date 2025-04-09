import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js"
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js"
import { Client, KustoConnectionStringBuilder } from "azure-kusto-data"
import { z } from "zod"
import { config } from "dotenv"

class AdxMcpServer {
  private server: any
  private client: Client | null = null
  private UriSchema = z.object({
    href: z.string(),
  })
  private ResourceTemplateSchema = z.object({
    db: z.string(),
    table: z.string().optional(),
  })
  private QueryToolSchema = z.object({
    db: z.string(),
    query: z.string(),
  })

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
    this.registerSchemaResourceTemplate()
    this.registerConfigResource()
    this.registerQueryTool()
    return this
  }

  private registerSchemaResourceTemplate() {
    this.server.resource(
      "Schema of the db",
      new ResourceTemplate("schema://adx/{db}", { list: undefined }),
      async (uri: z.infer<typeof this.UriSchema>, data: z.infer<typeof this.ResourceTemplateSchema>) => {
        if (!this.client) {
          throw new Error("Client is not initialized")
        }
        if (!data.table) {
          const query = `.show tables`
          const response = await this.client.execute(data.db, query)
          const result = response.primaryResults[0].toString()
          return {
            contents: [{
              uri: uri.href,
              name: `List down all tables in the db ${data.db}`,
              mimeType: "text/plain",
              text: result,
            }],
          }
        }
      }
    )

    this.server.resource(
      "Schema of the table",
      new ResourceTemplate("schema://adx/{db}/{table}", { list: undefined }),
      async (uri: z.infer<typeof this.UriSchema>, data: z.infer<typeof this.ResourceTemplateSchema>) => {
        if (!this.client) {
          throw new Error("Client is not initialized")
        }
        if (data.table) {
          const query = `${data.table} | getschema`
          const response = await this.client.execute(data.db, query)
          const result = response.primaryResults[0].toString()
          return {
            contents: [{
              uri: uri.href,
              name: `Schema of the table ${data.table}`,
              mimeType: "text/plain",
              text: result,
            }],
          }
        }
      }
    )

    this.server.resource(
      "Functions of the db",
      new ResourceTemplate("functions://adx/{db}/functions", { list: undefined }),
      async (uri: z.infer<typeof this.UriSchema>, data: z.infer<typeof this.ResourceTemplateSchema>) => {
        if (!this.client) {
          throw new Error("Client is not initialized")
        }
        const query = `.show functions`
        const response = await this.client.execute(data.db, query)
        const result = response.primaryResults[0].toString()
        return {
          contents: [{
            uri: uri.href,
            name: `List down all functions in the db ${data.db}`,
            mimeType: "text/plain",
            text: result,
          }],
        }
      }
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
    )
  }

  private registerQueryTool() {
    this.server.tool(
      "query",
      { query: z.string(), db: z.string() },
      async (data: z.infer<typeof this.QueryToolSchema>) => {
        if (!this.client) {
          throw new Error("Client is not initialized")
        }
        const query = data.query
        const db = data.db
        const response = await this.client.execute(db, query)
        const result = response.primaryResults[0].toString()
        return {
          contents: [{
            type: "text",
            text: result,
          }],
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

import { createServer } from "./adxMcpServer"
// import { createServer } from "./adxMcpServer2"

createServer().catch(err => {
  console.error('Error starting ADX MCP server:', err)
  process.exit(1)
})

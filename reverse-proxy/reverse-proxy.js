
import 'dotenv/config'
import express from 'express'
import httpProxy from 'http-proxy'
import pg from 'pg'
import { readFileSync } from 'node:fs'

const { Pool } = pg

const app = express()
const PORT = Number(process.env.PORT) || 8000

const S3_BASE_PATH = process.env.AWS_S3_PATH

const proxy = httpProxy.createProxy()

// PostgreSQL connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    ca: readFileSync(process.env.PG_CA_CERT_PATH || './ca.pem', 'utf8'),
    rejectUnauthorized: true,
  },
})

// Reverse proxy middleware
app.use(async (req, res) => {
  try {
    // 1. Extract the requested subdomain
    const hostname = req.hostname
    const subdomain = hostname.split('.')[0]

    // 2. Find the latest successful deployment for this project
    const { rows } = await pool.query(
      `
        SELECT d.id AS "deploymentId"
        FROM "Project" p
        JOIN "Deployment" d ON d.project_id = p.id
        WHERE p.subdomain = $1
          AND d.status = 'READY'
        ORDER BY d.created_at DESC
        LIMIT 1
      `,
      [subdomain]
    )

    if (rows.length === 0) {
      return res.status(404).send('No ready deployment found')
    }

    const deploymentId = rows[0].deploymentId

    // 3. Handle SPA routing
    // /             -> /index.html
    // /about        -> /index.html
    // /assets/app.js -> /assets/app.js

    const url = new URL(req.url, 'http://localhost')
    const pathname = url.pathname

    const isPageRequest = req.method === 'GET' || req.method === 'HEAD'
    const hasExtension = /\.[a-zA-Z0-9]+$/.test(pathname)

    if (isPageRequest && (pathname === '/' || !hasExtension)) {
      url.pathname = '/index.html'
    }

    // Preserve query parameters
    req.url = url.pathname + url.search

    // 4. Forward request to the deployment's S3 directory
    const target = `${S3_BASE_PATH}/${deploymentId}`

    return proxy.web(req, res, {
      target,
      changeOrigin: true,
    })

  } catch (error) {
    console.error('Reverse proxy error:', error)

    if (!res.headersSent) {
      return res.status(500).send('Internal server error')
    }

    res.end()
  }
})

// Handle errors while communicating with S3
proxy.on('error', (error, req, res) => {
  console.error('S3 proxy error:', error)

  if (!res.headersSent) {
    res.writeHead(502, { 'Content-Type': 'text/plain' })
  }

  res.end('Bad gateway')
})

app.listen(PORT, () => {
  console.log(`Reverse Proxy Running on port ${PORT}`)
})
import "dotenv/config"

import express from 'express'
import { readFileSync } from "node:fs"
import { PrismaPg } from '@prisma/adapter-pg'
import { PrismaClient } from "./generated/prisma/client"

const app = express()
const PORT = 8000 || process.env.PORT

const adapter = new PrismaPg({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    ca: readFileSync(new URL("./ca.pem", import.meta.url), "utf-8")
  }
})

export const prisma = new PrismaClient({adapter})

app.listen(PORT, () => console.log(`Listening on ${PORT}`))
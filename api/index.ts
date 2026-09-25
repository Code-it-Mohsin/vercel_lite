import "dotenv/config"

import express from 'express'
import cors from 'cors'

import { createServer } from "node:http"
import { Server } from 'socket.io'

import { readFileSync } from "node:fs"
import { PrismaPg } from '@prisma/adapter-pg'
import { PrismaClient } from "./generated/client/client.js"
import mongoose from 'mongoose'
import dns from "node:dns";

import {Kafka} from 'kafkajs'

// DNS CONFIG TO LOOKUP SRV RECORDS
dns.setServers([
  "1.1.1.1",
  "8.8.8.8",
]);

// EXPRESS
const app = express()
const PORT = process.env.PORT || 8000

// HTTP AND SOCKET.IO

const httpServer = createServer(app)
const io = new Server(httpServer)

io.on("connection", (Socket) => {
  console.log('a user has connected')
  // join the client to the corresponding deployment room
})

// INITIALIZE PRISMA CLIENT
const adapter = new PrismaPg({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    ca: readFileSync(new URL("./ca.pem", import.meta.url), "utf-8")
  }
})
export const prisma = new PrismaClient({ adapter })

// CONNECT TO MONGODB
mongoose.connect(process.env.MONGODB_URI!)
.then(() => console.log(`Connected to Mongodb Atlas... logs db`))
.catch((error) => console.error(error));


const PROJECT_ID = process.env.PROJECT_ID
const DEPLOYMENT_ID = process.env.DEPLOYMENT_ID

// INITIALIZE KAFKA
const kafka = new Kafka({
  clientId: `docker-server-${DEPLOYMENT_ID}`,
  brokers: [process.env.KAFKA_SERVICE_URI!],
  ssl: {
    ca: [readFileSync(new URL ('./kafka.pem', import.meta.url), 'utf-8')],
  },
  sasl: {
    username: process.env.KAFKA_USER!,
    password: process.env.KAFKA_PASSWORD!,
    mechanism: "plain"
  },
})

// INITIALIZE KAFKA CONSUMER
// const kafkaConsumer = kafka.consumer({

// })

// NEW ECS CLIENT 

const config = {
  CLUSTER: '',
  TASK: ''
}

app.use(express.json())
app.use(cors())

// POST ROUTE TO /project

// POST ROUTE TO /deploy

// GET ROUTE TO /logs/:id

// initkafkaConsumer function

httpServer.listen(PORT, () => console.log(`Http server istening on ${PORT}`))


/*
API server
- Depolyment and spinning a docker container
- Container logs being thrown in kafka
- Kafka consumer sends the data to click house
- clickhouse sends the logs to api server and client pulls that data on polling basis

Server
- pull the project using github URL
- Runs a script that build the project
- and pushes the output file to S3
- will have the kafka producer

reverse-proxy
- user visit the URL/domain
- reverse proxy gets the objects (output files) from s3
- Seemless streaming

DB integration
Storing logs - Click house?
Analytics -

*/

// WRITE DOWN SERVICES TO CREATE
// WIRING OF THE SERVICES IN EACH FILE
// THEN THE FUNCTIONS/PROCEDURES FOR EACH FILE
// THEN TAKE HELP OF DOCS ETC TO WRITE THE FUNCTIONS
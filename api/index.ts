import "dotenv/config"

import express from 'express'
import cors from 'cors'
import {z} from 'zod'
import {generateSlug} from 'random-word-slugs'

import { createServer } from "node:http"
import { Server } from 'socket.io'

import { readFileSync } from "node:fs"
import { PrismaPg } from '@prisma/adapter-pg'
import { PrismaClient } from "./generated/client/client.js"
import mongoose from 'mongoose'
import dns from "node:dns";

import { Kafka } from 'kafkajs'
import { ECSClient, RunTaskCommand } from '@aws-sdk/client-ecs'
import { error } from "node:console"

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


// INITIALIZE KAFKA
const kafka = new Kafka({
  clientId: 'api-server',
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
const kafkaConsumer = kafka.consumer({groupId: "vercel-lite-log-consumers"})


// AWS ECS CLIENT 

const ecsClient = new ECSClient({
  region: 'eu-north-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
  }
})

const config = {
  CLUSTER: process.env.AWS_CLUSTER,
  TASK: process.env.AWS_TASK
}

// MIDDLEWARES
app.use(express.json())
app.use(cors())


// POST ROUTE TO /project

app.post('/project', async (req, res) => {

// Validate the data provided with zod, as typescript types can help in development but on server ts is gone..
  const zodSchema = z.object({
    name: z.string(),
    gitURL: z.string()
  })
  const safeParseResult = zodSchema.safeParse(req.body)

  if(safeParseResult.error) return res.status(400).json({error: safeParseResult.error})

  const {name, gitURL} = safeParseResult.data
  const project = await prisma.project.create({
    data: {
      name,
      gitURL,
      subDomain: generateSlug()
    }
  })

// Create a project in the projects column
// generate a subdomain
})


// POST ROUTE TO /deploy

app.post('/deploy', (req, res) => {

  // /project creates a project in Prisma, so on /deploy the project Id is passed
  // find that project in prisma
  // create a deployment row in primsa

  //Spin the container
  // see the AWS doc for RunTaskCommand
   
  // send command via ecsCllient
  // return a json response with status and data conatining deploymentId 

})


// GET ROUTE TO /logs/:id
app.get('/logs/:id', (req, res) => {
  // get deploymentId from params
  // do a mongo query to retrieve the stored logs
  // return those logs

})


// initkafkaConsumer
function initkafkaConsumer(){
  // connect to the consumer
  // subscribe to the topic, fromBeginning

  // run the consumer
    // each 
}
// initkafkaConsumer()


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
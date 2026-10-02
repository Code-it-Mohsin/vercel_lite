import "dotenv/config"

import express from 'express'
import cors from 'cors'
import { z } from 'zod'
import { generateSlug } from 'random-word-slugs'

import { createServer } from "node:http"
import { Server } from 'socket.io'

import { readFileSync } from "node:fs"
import { PrismaPg } from '@prisma/adapter-pg'
import { PrismaClient } from "./generated/client/client.js"
import mongoose from 'mongoose'
import { containerLogs } from './mongo/container.logs.js'
import dns from "node:dns";

import { Kafka } from 'kafkajs'
import { ECSClient, RunTaskCommand } from '@aws-sdk/client-ecs'

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

io.on("connection", (socket) => { // listen to a client's request to establish connection
  console.log(`${socket.id} has connected`)

  socket.on('subscribe', channel => socket.join(channel)) // connect the client to the specific channel
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
    ca: [readFileSync(new URL('./kafka.pem', import.meta.url), 'utf-8')],
  },
  sasl: {
    username: process.env.KAFKA_USER!,
    password: process.env.KAFKA_PASSWORD!,
    mechanism: "plain"
  },
})


// INITIALIZE KAFKA CONSUMER
const kafkaConsumer = kafka.consumer({ groupId: "vercel-lite-log-consumers" })


// AWS ECS CLIENT 

const ecsClient = new ECSClient({
  region: 'eu-north-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
  }
})

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

  if (safeParseResult.error) return res.status(400).json({ error: safeParseResult.error })

  // Create a project in the projects column
  const { name, gitURL } = safeParseResult.data
  const project = await prisma.project.create({
    data: {
      name,
      gitURL,
      subDomain: generateSlug()
    }
  })

  return res.json({ status: 'success', data: { project } })
})


// POST ROUTE TO /deploy

app.post('/deploy', async (req, res) => {

  // /project creates a project in Prisma, so on /deploy the project Id is passed in the body
  const { projectId } = req.body
  if (!projectId) return res.sendStatus(400)

  // find that project in prisma
  const project = await prisma.project.findUnique({ where: { id: projectId } })

  if (!project) return res.sendStatus(400)

  // create a deployment row in primsa
  const deployment = await prisma.deployment.create({
    data: {
      project: { connect: { id: projectId } },
      status: "QUEUED",
    }
  })

  //Spin the container

  const command = new RunTaskCommand({
    cluster: process.env.AWS_CLUSTER,
    taskDefinition: process.env.AWS_TASK,
    launchType: 'FARGATE',
    count: 1,
    networkConfiguration: {
      awsvpcConfiguration: {
        assignPublicIp: 'ENABLED',
        subnets: ['subnet-0edc5d99b88fb5240', 'subnet-0f42439e8110edfb6', 'subnet-076bd49c872843f53'],
        securityGroups: ['sg-01394b824ac1c280b']
      }
    },
    overrides: {
      containerOverrides: [
        {
          name: 'server-img',
          environment: [
            { name: 'GIT_REPO_URL', value: project.gitURL },
            { name: 'PROJECT_ID', value: projectId },
            { name: 'DEPLOYMENT_ID', value: deployment.id }
          ]
        }
      ]
    }
  })
  try {
    const ecsRes = await ecsClient.send(command);

    return res.status(200).json({
      message: ecsRes.tasks?.[0]?.taskArn
    })
  } catch (error) {
    console.log(error)
    return res.status(500).json({
      message: "Failed to start ECS task"
    })
  }
  // return a json response with status and data conatining deploymentId 

})


// GET ROUTE TO /logs/:id
app.get('/logs/:id', async (req, res) => {
  // get deploymentId from params
  const deploymentId = req.params.id

  // do a mongo query to retrieve the stored logs
  const deploymentLogs = await containerLogs.find({ deploymentId })

  // return those logs
  return deploymentLogs
})


// initkafkaConsumer
async function initkafkaConsumer() {
  // connect to the consumer
  await kafkaConsumer.connect()

  // subscribe to the topic, fromBeginning
  await kafkaConsumer.subscribe({ topics: ['container-logs'], fromBeginning: true })

  // run the consumer
  await kafkaConsumer.run({
    eachBatchAutoResolve: true,

    eachBatch: async ({
      batch, resolveOffset, heartbeat, commitOffsetsIfNecessary,
    }) => {
      const messages = batch.messages
      console.log(`Received ${messages.length} messages...`)

      try {
        for (let message of messages) {

          if (!message.value) continue

          const { eventId, projectId, deploymentId, log, timestamp } =
            JSON.parse(message.value.toString())

          await containerLogs.create({
            eventId,
            projectId,
            deploymentId,
            log,
            timestamp,
          })

          // send logs to the frontend via socket.io
          io.to(deploymentId).emit('log', {
            eventId,
            log,
            timestamp
          })

          resolveOffset(message.offset)
          await commitOffsetsIfNecessary()
          await heartbeat()

        }
      } catch (error) {
        console.log(error)
      }
    }
  })
}
// initkafkaConsumer()


httpServer.listen(PORT, () => console.log(`Http server istening on ${PORT}`))

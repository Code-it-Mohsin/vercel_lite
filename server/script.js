import { exec } from 'child_process'
import path from 'path'
import fs from 'fs'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'
import mime from 'mime-types'
import { Kafka, kafka } from 'kafkajs'
import { error } from 'console'

// Sends the logs generated while creating the container to kafka via child process
// build the project
// push the outfiles to S3

// S3 CLIENT CONFIG
const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
})

// ID's injected from ECS run taks command's env
const PROJECT_ID = process.env.PROJECT_ID
const DEPLOYMENT_ID = process.env.DEPLOYMENT_ID

// KAFKA CONFIG AND PRODUCER
const kafka = new Kafka({
  clientId: `docker-server-${DEPLOYMENT_ID}`,
  brokers: [process.env.KAFKA_URI],
  ssl: {
    ca: [fs.readFileSync(new URL('./kafka.pem', import.meta.url), "utf-8")]
  },
  sasl: {
    username: process.env.KAFKA_USER,
    password: process.env.KAFKA_PASSWORD,
    mechanism: 'plain',
  }
})

const kafkaProducer = kafka.producer() // created a kafka producer

// PUBLISH LOG
async function logs(log) {
  await kafkaProducer.send({
    topic: 'container-logs',
    messages: [{
      key: 'log',
      value: JSON.stringify(PROJECT_ID, DEPLOYMENT_ID, log),
    }]
  })
}


// OUT DIRECTORY FOR DIFFERENT PROJECTS
function getOutputDirectory(projectPath) {
  const packageJson = JSON.parse(
    fs.readFileSync(path.join(projectPath, "package.json"), "utf-8")
  );
  const deps = {
    ...packageJson.dependencies,
    ...packageJson.devDependencies
  };

  if (deps.vite) return "dist";
  if (deps["@vue/cli-service"]) return "dist";
  if (deps["react-scripts"]) return "build";
  if (deps.astro) return "dist";
  if (deps["@angular/cli"]) return "dist";

  throw new Error("Unsupported framework or unknown output directory");
}


// INIT FUNCTION ---- REAL EXECUTION OF THIS FILE
async function init() {

  kafkaProducer.connect() // connects to kafka broker with the provided config in this case aiven

  console.log('starting script execution')
  await logs("Build started...")

  const outDirPath = path.join(__dirname, 'output') // current path to output directory in /home/app

  const p = exec(`cd ${outDirPath} && npm install && npm build`) // into the directory, installs dependencies and builds

  p.stdout.on('data', async (data) => {
    console.log(data.toString())
    await logs(data.toString())
  }) // sending logs generated during the process to logs >> kafkaProducer

  p.stdout.on('error', async (error) => {
    console.log('Error occured:', error.toString())
    await logs(`Error occured: ${error.toString()}`)
  }) // Error set as log

  p.on("close", async (code) => {
    if (code != 0) {
      console.log('Build failed... exiting.')
      await logs('Build failed... exiting')
      process.exit(1)
    } // Exit if the build process fails 

    const outputDir = getOutputDirectory(outDirPath)
    const outputFolderPath = path.join(outDirPath, outputDir)
    const outputFolderContents = fs.readFileSync(outputFolderPath, { recursive: true })

    for (const file of outputFolderContents) {
      const filePath = path.join(outputFolderPath, file)
      if (fs.lstatSync(filePath).isDirectory()) continue

      console.log('uploading', filePath)
      await logs(`Uploading ${file}...`)

      const command = new PutObjectCommand({
        Bucket: 'vercel-lite-compiled-projects',
        Key: `__output/${PROJECT_ID}/${file}`,
        Body: fs.createReadStream(filePath),
        ContentType: mime.lookup(filePath)
      })

      await s3Client.send(command)
      console.log(`uploaded ${file}`)
      await logs(`uploaded ${file}`)
    }

    await logs('Done :)')
    console.log("Done :)")
    process.exit(0)
  })
}
init()
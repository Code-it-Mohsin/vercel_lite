import { exec } from 'child_process'
import path from 'path'
import fs from 'fs'
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'
import mime from 'mime-types'
import { Kafka } from 'kafkajs'
import { fileURLToPath } from 'url'
import { randomUUID } from 'crypto'

// Sends the logs generated while creating the container to kafka via child process
// build the project
// push the outfiles to S3

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

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

// SEND LOGS TO KAFKA >> API >> KAFKA CONSUMER
async function logs(log) {
  await kafkaProducer.send({
    topic: 'container-logs',
    messages: [{
      key: 'log',
      value: JSON.stringify({
        eventId: randomUUID(),
        projectId: PROJECT_ID,
        deploymentId: DEPLOYMENT_ID,
        log: log,
        timestamp: new Date().toLocaleString("en-US", {
          month: "short",
          day: "numeric",
          year: "numeric",
          hour: "numeric",
          minute: "2-digit",
          second: "2-digit",
          fractionalSecondDigits: 3
        })
      }),
    }]
  })
}

// OUT DIRECTORY FOR DIFFERENT PROJECTS
function getOutputDirectory(projectPath) {
  const packageJson = JSON.parse(
    fs.readFileSync(path.join(projectPath, "package.json"), "utf-8") // read file /home/app/output/package.json
  );
  const deps = {
    ...packageJson.dependencies, // gets the dependencies obj in package.json
    ...packageJson.devDependencies // similarly devdep obj from package.json
  };

  if (deps.vite) return "dist";
  if (deps["@vue/cli-service"]) return "dist";
  if (deps["react-scripts"]) return "build";
  if (deps.astro) return "dist";
  if (deps["@angular/cli"]) return "dist";

  throw new Error("Unsupported framework or unknown output directory");
}


// INIT FUNCTION - REAL EXECUTION OF THIS FILE
async function init() {

  await kafkaProducer.connect() // connects to kafka broker with the provided config in this case aiven

  console.log('starting script execution')
  await logs("Build started...")

  const outDirPath = path.join(__dirname, 'output') // we are in script.js (that's why it is executing), so here __dirname resolves to /home/app and then the output folder is appened making it, /home/app/output - output folder was created in the shell script.

  const p = exec(`cd ${outDirPath} && npm install && npm run build`) // into the directory, installs dependencies and builds

  p.stdout.on('data', async (data) => {
    console.log(data.toString())
    await logs(data.toString())
  }) // sending logs generated during the process to logs >> kafkaProducer

  p.stderr.on('data', async (error) => {
    console.log('stderr:', error.toString())
    await logs(`Error occured: ${error.toString()}`)
  }) // Error set as log

  p.on('error', async (error) => {
    console.log('Failed to start build process: ', error.toString())
    await logs('Failed to start build process: ', error.toString())
  })

  p.on("close", async (code) => {
    if (code != 0) {
      console.log('Build failed... exiting.')
      await logs('Build failed... exiting')
      process.exit(1)
      return
    } // Exit if the build process fails 

    const outputDir = getOutputDirectory(outDirPath) // returns "build" or "dist"
    const outputFolderPath = path.join(outDirPath, outputDir) // gets appended to /home/app/output/dist

    const outputFolderContents = fs.readdirSync(outputFolderPath, { recursive: true }) // returns an array of every folders and every files path

    for (const file of outputFolderContents) {
      const filePath = path.join(outputFolderPath, file) // Every element in the array gets appended to /home/app/output/dist 

      if (fs.lstatSync(filePath).isDirectory()) continue // fs.lstatSync() asks the OS for metadata about the path, to determine whether it is a directory or a file.

      console.log('uploading', filePath)
      await logs(`Uploading ${file}...`)

      const command = new PutObjectCommand({
        Bucket: 'vercel-lite-compiled-projects',
        Key: `__output/${PROJECT_ID}/${file}`,
        Body: fs.createReadStream(filePath),
        ContentType: mime.lookup(filePath) /*
        index.html   → text/html
        app.js       → application/javascript
        style.css    → text/css
        image.png    → image/png
        returns the relevant type
        */
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
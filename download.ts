import { S3Client, GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3'
import fs from 'fs'
import path from 'path'
import readline from 'readline'
import 'dotenv/config'

const GRAVEYARD_BUCKET = 'carpark-prod-0-graveyard'
const MAX_SOCKETS = parseInt(process.env.STORACHA_DOTSTORAGE_DOWNLOAD_MAX_SOCKETS ?? '100')
const BATCH_SIZE = parseInt(process.env.STORACHA_DOTSTORAGE_DOWNLOAD_BATCH_SIZE ?? '5000')
const DOWNLOAD_DIR = process.env.DOWNLOAD_DIR ?? './downloads'

const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.CLOUDFLARE_R2_ACCESS_KEY_ID!,
    secretAccessKey: process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY!
  },
  requestHandler: {
    httpsAgent: { maxSockets: MAX_SOCKETS },
  }
})

// Parse command line arguments
const args = process.argv.slice(2)
const downloadMode = args.includes('--download')

// Downloaded CSV file path
const downloadedPath = 'data/downloaded.csv'
// Not found CSV file path
const notFoundPath = 'data/download-notfound.csv'

// Ensure download directory exists
if (!fs.existsSync(DOWNLOAD_DIR)) {
  fs.mkdirSync(DOWNLOAD_DIR, { recursive: true })
}

async function downloadFromR2 (cid: string): Promise<boolean> {
  const key = `${cid}/${cid}.car`
  const filePath = path.join(DOWNLOAD_DIR, `${cid}.car`)

  try {
    // First check if object exists
    await s3.send(new HeadObjectCommand({
      Bucket: GRAVEYARD_BUCKET,
      Key: key
    }))
  } catch (error: any) {
    if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
      // Log to notfound.csv
      fs.appendFileSync(notFoundPath, `${cid}\n`)
      console.log(`${cid} not found in ${GRAVEYARD_BUCKET} (logged to download-notfound.csv)`)
      return false
    } else {
      console.error(`error looking for ${cid} in ${GRAVEYARD_BUCKET}: `, error)
      return false
    }
  }

  if (downloadMode) {
    try {
      const response = await s3.send(new GetObjectCommand({
        Bucket: GRAVEYARD_BUCKET,
        Key: key
      }))

      if (response.Body) {
        const chunks: any[] = []
        
        // Handle different body types
        if (response.Body instanceof ReadableStream) {
          const reader = response.Body.getReader()
          let done = false
          while (!done) {
            const { value, done: readerDone } = await reader.read()
            done = readerDone
            if (value) chunks.push(value)
          }
        } else if (typeof response.Body[Symbol.asyncIterator] === 'function') {
          for await (const chunk of response.Body as any) {
            chunks.push(chunk)
          }
        }

        const buffer = Buffer.concat(chunks)
        fs.writeFileSync(filePath, buffer)
        
        fs.appendFileSync(downloadedPath, `${cid}\n`)
        console.log(`✓ Downloaded ${cid} from ${GRAVEYARD_BUCKET} to ${filePath}`)
        return true
      }
    } catch (error: any) {
      console.error(`error downloading ${cid} from ${GRAVEYARD_BUCKET}:`, error)
      return false
    }
  } else {
    console.log("--download not specified, skipping download")
    fs.appendFileSync(downloadedPath, `${cid}\n`)
  }

  return true
}

// thanks, https://stackoverflow.com/posts/66762031/revisions
async function* batches<T> (iterable: AsyncIterable<T>, batchSize: number) {
  let items: T[] = [];
  for await (const item of iterable) {
    items.push(item);
    if (items.length >= batchSize) {
      yield items;
      items = []
    }
  }
  if (items.length !== 0) {
    yield items;
  }
}

// thanks, https://stackoverflow.com/posts/55435856/revisions
function* chunks<T> (arr: T[], n: number): Generator<T[], void> {
  for (let i = 0; i < arr.length; i += n) {
    yield arr.slice(i, i + n);
  }
}

async function processLines (lines: Iterable<string>) {
  for (const line of lines) {
    const cid = line.trim()
    if (!cid) continue // Skip empty lines

    try {
      const downloaded = await downloadFromR2(cid)
      if (downloaded) {
        console.log(`  ✓ ${downloadMode ? 'Downloaded' : 'Found'} ${cid}`)
      } else {
        console.log(`  ✗ ${cid} not found in ${GRAVEYARD_BUCKET}`)
      }
    } catch (error) {
      console.error(`  ✗ Error processing ${cid}:`, error)
    }
  }
}

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity
})

const start = new Date()
console.log(`starting at ${start} with batches of ${BATCH_SIZE} and ${MAX_SOCKETS} sockets`)
console.log(`downloading from bucket: ${GRAVEYARD_BUCKET}`)
console.log(`download directory: ${DOWNLOAD_DIR}`)
console.log(`download mode: ${downloadMode ? 'enabled' : 'disabled (use --download to enable)'}`)

for await (const batch of batches(rl, BATCH_SIZE)) {
  console.log('Processing next batch')
  await Promise.all([...chunks(batch, Math.floor(BATCH_SIZE / MAX_SOCKETS))].map(processLines))
}
const end = new Date()
console.log(`ending at ${end}`)

console.log(`processing took ${end.getTime() - start.getTime()}ms`)
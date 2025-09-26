import * as Link from 'multiformats/link'
import { base58btc } from 'multiformats/bases/base58'
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import fs from 'fs'
import readline from 'readline'
import 'dotenv/config'

const BLOCK_POSITION_TABLE = 'prod-ep-v1-blocks-cars-position'
const MAX_SOCKETS = parseInt(process.env.STORACHA_DOTSTORAGE_LOOKUP_MAX_SOCKETS ?? '100')
const BATCH_SIZE = parseInt(process.env.STORACHA_DOTSTORAGE_LOOKUP_BATCH_SIZE ?? '5000')

const dynamodb = new DynamoDBClient({
  region: 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
  },
  requestHandler: {
    httpsAgent: { maxSockets: MAX_SOCKETS },
  }
})

// Cars CSV file path
const carsPath = 'data/cars.csv'

function parseContentCid (contentCid: string): string {
  const link = Link.parse(contentCid)
  return base58btc.encode(link.multihash.bytes)
}

async function lookupByBlockMultihash (blockmultihash: string): Promise<string[]> {
  try {
    const query = await dynamodb.send(new QueryCommand({
      TableName: BLOCK_POSITION_TABLE,
      KeyConditionExpression: 'blockmultihash = :blockmultihash',
      ExpressionAttributeValues: {
        ':blockmultihash': { S: blockmultihash }
      },
      ProjectionExpression: 'carpath'
    }))
    
    if (query.Items && query.Items.length > 0) {
      return query.Items
        .map(item => item.carpath?.S)
        .filter((carpath): carpath is string => carpath !== undefined)
    }
  } catch (error) {
    console.error(`Error querying ${BLOCK_POSITION_TABLE} for ${blockmultihash}:`, error)
  }
  
  return []
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
    const contentCid = line.trim()
    if (!contentCid) continue // Skip empty lines

    try {
      const blockmultihash = parseContentCid(contentCid)
      const carpaths = await lookupByBlockMultihash(blockmultihash)
      
      if (carpaths.length > 0) {
        for (const carpath of carpaths) {
          const csvLine = `${blockmultihash},${carpath}`
          fs.appendFileSync(carsPath, `${csvLine}\n`)
        }
        console.log(`✓ Found: ${contentCid} -> ${blockmultihash} -> ${carpaths.length} carpath(s)`)
      } else {
        console.log(`✗ Not found: ${contentCid} -> ${blockmultihash}`)
      }
    } catch (error) {
      console.error(`✗ Error processing ${contentCid}:`, error)
    }
  }
}

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity
})

const start = new Date()
console.log(`starting at ${start} with batches of ${BATCH_SIZE} and ${MAX_SOCKETS} sockets`)
console.log(`querying table: ${BLOCK_POSITION_TABLE}`)
console.log(`output file: ${carsPath}`)

for await (const batch of batches(rl, BATCH_SIZE)) {
  console.log('Processing next batch')
  await Promise.all([...chunks(batch, Math.floor(BATCH_SIZE / MAX_SOCKETS))].map(processLines))
}
const end = new Date()
console.log(`ending at ${end}`)

console.log(`processing took ${end.getTime() - start.getTime()}ms`)
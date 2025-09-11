import * as Link from 'multiformats/link'
import { base32 } from 'multiformats/bases/base32'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'
import csv from 'csv-parser'
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import { S3Client, CopyObjectCommand, DeleteObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3'
import fs from 'fs'
import 'dotenv/config'

interface CsvRow {
  car_id: string
}

const CAR_BUCKET = 'carpark-prod-0'
const GRAVEYARD_BUCKET = 'carpark-prod-0-graveyard'

const dynamodb = new DynamoDBClient({
  region: 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
  }
})

const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.CLOUDFLARE_R2_ACCESS_KEY_ID!,
    secretAccessKey: process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY!
  }
})

// Parse command line arguments
const args = process.argv.slice(2)
const deleteMode = args.includes('--delete')

// Initialize not found CSV writer
const notFoundStream = fs.createWriteStream('data/notfound.csv', { flags: 'a' })

async function hasBeenMigrated (carLink: Link.Link<unknown, number, number, Link.Version>): Promise<string | null> {
  const multihashBase58 = base58btc.encode(carLink.multihash.bytes)
  const carLinkStr = carLink.toString()

  // Check prod-w3infra-store table
  const storeQuery = await dynamodb.send(new QueryCommand({
    TableName: 'prod-w3infra-store',
    IndexName: 'cid',
    KeyConditionExpression: 'link = :link',
    ExpressionAttributeValues: {
      ':link': { S: carLinkStr }
    },
    Limit: 1
  }))
  if (storeQuery.Items && storeQuery.Items.length > 0) {
    return 'store'
  }

  // Check prod-w3infra-allocation table
  const allocationQuery = await dynamodb.send(new QueryCommand({
    TableName: 'prod-w3infra-allocation',
    IndexName: 'multihash',
    KeyConditionExpression: 'multihash = :multihash',
    ExpressionAttributeValues: {
      ':multihash': { S: multihashBase58 }
    },
    Limit: 1
  }))
  if (allocationQuery.Items && allocationQuery.Items.length > 0) {
    return 'allocation'
  }

  // Check prod-w3infra-blob-registry table
  const blobQuery = await dynamodb.send(new QueryCommand({
    TableName: 'prod-w3infra-blob-registry',
    IndexName: 'digest',
    KeyConditionExpression: 'digest = :digest',
    ExpressionAttributeValues: {
      ':digest': { S: multihashBase58 }
    },
    Limit: 1
  }))
  if (blobQuery.Items && blobQuery.Items.length > 0) {
    return 'blob-registry'
  }

  return null
}

function parseCarId (carId: string) {
  if (carId.startsWith('ciq')) {
    // parse the "madhat" hash we used for some of this stuff
    const digestBytes = base32.baseDecode(carId)
    const digest = Digest.decode(digestBytes)
    return Link.create(0x0202, digest)
  } else {
    // we'll just assume this is already a CID
    return Link.parse(carId)
  }
}

async function deleteFromR2 (carId: string): Promise<boolean> {
  const key = `${carId}/${carId}.car`

  try {
    // First check if object exists
    await s3.send(new HeadObjectCommand({
      Bucket: CAR_BUCKET,
      Key: key
    }))
  } catch (error: any) {
    if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
      // Log to notfound.csv
      notFoundStream.write(`${carId}\n`)
      return false
    } else {
      console.error(`error looking for ${carId} in ${CAR_BUCKET}: `, error)
    }
  }

  try {
    // Copy to graveyard bucket
    await s3.send(new CopyObjectCommand({
      Bucket: GRAVEYARD_BUCKET,
      Key: key,
      CopySource: `${CAR_BUCKET}/${key}`
    }))

    // Delete from original bucket
    await s3.send(new DeleteObjectCommand({
      Bucket: CAR_BUCKET,
      Key: key
    }))
  } catch (error: any) {
    console.error(`error copying and deleting ${carId} from ${CAR_BUCKET}:`, error)
  }

  return true

}

process.stdin
  .pipe(csv())
  .on('data', async (row: CsvRow) => {
    // Process each row
    const carLink = parseCarId(row.car_id)
    const cid = carLink.toString()
    const migrated = await hasBeenMigrated(carLink)

    if (migrated) {
      console.log(`${cid} has been migrated to the ${migrated} table`)
    } else {
      console.log(`${cid} has not been migrated`)

      if (deleteMode) {
        try {
          const deleted = await deleteFromR2(cid)
          if (deleted) {
            console.log(`  ✓ Deleted ${row.car_id} as ${cid} from ${CAR_BUCKET} and backed up to ${GRAVEYARD_BUCKET}`)
          } else {
            console.log(`  ✗ ${row.car_id} as ${cid} not found in ${CAR_BUCKET} (logged to notfound.csv)`)
          }
        } catch (error) {
          console.error(`  ✗ Error deleting ${row.car_id} as ${cid}:`, error)
        }
      } else {
        console.log("--delete not specified, skipping deletion")
      }
    }
  })
  .on('end', () => {
    console.log('CSV processing complete')
    notFoundStream.end()
  });
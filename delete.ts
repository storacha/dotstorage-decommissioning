import * as Link from 'multiformats/link'
import { base32 } from 'multiformats/bases/base32'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'
import csv from 'csv-parser'
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'

interface CsvRow {
  car_id: string
}

const dynamodb = new DynamoDBClient({ region: 'us-west-2' })

async function hasBeenMigrated(carLink: Link.Link<unknown, number, number, Link.Version>): Promise<string | null> {
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

function parseCarId(carId: string){
  if (carId.startsWith('ciq')){
    // parse the "madhat" hash we used for some of this stuff
    const digestBytes = base32.baseDecode(carId)
    const digest = Digest.decode(digestBytes)
    return Link.create(0x0202, digest)
  } else {
    // we'll just assume this is already a CID
    return Link.parse(carId)
  }
}

process.stdin
  .pipe(csv())
  .on('data', async (row: CsvRow) => {
    // Process each row
    const carLink = parseCarId(row.car_id)
    const migrated = await hasBeenMigrated(carLink)
    console.log(`${carLink.toString()} ${migrated ? `has been migrated to the ${migrated} table` : 'has not been migrated'}`)
  })
  .on('end', () => {
    console.log('CSV processing complete');
  });
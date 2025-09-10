import * as Link from 'multiformats/link'
import { base32 } from 'multiformats/bases/base32'
import { base58btc } from 'multiformats/bases/base58'
import * as Digest from 'multiformats/hashes/digest'
import fs from 'fs'
import csv from 'csv-parser'

process.stdin
  .pipe(csv())
  .on('data', (row) => {
    // Process each row
    //console.log(row);
    const digestBytes = base32.baseDecode(row.car_id)
    const digest = Digest.decode(digestBytes)
    const carLink = Link.create(0x0202, digest)
    
    console.log(carLink.toString())
    console.log(base58btc.encode(carLink.multihash.bytes))
  })
  .on('end', () => {
    console.log('CSV processing complete');
  });
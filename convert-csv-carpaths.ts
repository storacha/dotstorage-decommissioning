import * as Link from 'multiformats/link'
import { base32 } from 'multiformats/bases/base32'
import * as Digest from 'multiformats/hashes/digest'
import readline from 'readline'

function convertToCarCid (input: string): string {
  const trimmed = input.trim()
  
  if (trimmed.startsWith('ciq')) {
    // parse the "madhat" hash we used for some of this stuff
    const digestBytes = base32.baseDecode(trimmed)
    const digest = Digest.decode(digestBytes)
    return Link.create(0x0202, digest).toString()
  } else {
    // we'll just assume this is already a CID or return as-is
    return trimmed
  }
}

function extractAndConvertCarpath (carpath: string): string {
  // Extract everything after the last / and before .car
  const match = carpath.match(/.*\/([^/]+)\.car$/)
  if (match) {
    const extractedId = match[1]
    return convertToCarCid(extractedId)
  }
  return carpath // Return original if no match
}

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

rl.on('line', (line: string) => {
  const trimmed = line.trim()
  if (!trimmed) return
  
  const [firstItem, secondItem] = trimmed.split(',', 2)
  if (firstItem && secondItem) {
    const convertedSecondItem = extractAndConvertCarpath(secondItem)
    console.log(`${firstItem},${convertedSecondItem}`)
  } else {
    // If not proper CSV format, output as-is
    console.log(trimmed)
  }
})

rl.on('close', () => {
  process.exit(0)
})
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

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

rl.on('line', (line: string) => {
  console.log(convertToCarCid(line))
})

rl.on('close', () => {
  process.exit(0)
})
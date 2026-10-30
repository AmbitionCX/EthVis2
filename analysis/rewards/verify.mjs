import { createReadStream } from 'node:fs';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';

export function integer(value, name) {
  if (typeof value !== 'string' || !/^-?\d+$/.test(value)) {
    throw new Error(`${name}: expected an integer string, never a JS Number`);
  }
  return BigInt(value);
}

export function verifyRecord(record) {
  const payload = typeof record.payload === 'string' ? JSON.parse(record.payload) : record.payload;
  if (!payload || typeof payload !== 'object') throw new Error('missing payload');
  if (record.dataset === 'proposer_rewards') {
    const components = ['attestations', 'sync_aggregate', 'proposer_slashings', 'attester_slashings'];
    const total = integer(payload.total, 'total');
    const sum = components.reduce((n, k) => n + integer(payload[k], k), 0n);
    if (sum !== total || total < 0n) throw new Error('proposer components do not equal total');
    return { component: 'proposer_gwei', value: total };
  }
  if (record.dataset === 'attestation_rewards') {
    const required = ['head', 'target', 'source', 'inactivity'];
    let total = required.reduce((n, k) => n + integer(payload[k], k), 0n);
    if (Object.hasOwn(payload, 'inclusion_delay')) total += integer(payload.inclusion_delay, 'inclusion_delay');
    return { component: 'attestation_gwei', value: total };
  }
  if (record.dataset === 'sync_rewards') return { component: 'sync_gwei', value: integer(payload.reward, 'reward') };
  return null;
}

export async function verifyStream(input) {
  const lines = createInterface({ input, crlfDelay: Infinity });
  const totals = { proposer_gwei: 0n, attestation_gwei: 0n, sync_gwei: 0n };
  let records = 0n;
  for await (const line of lines) {
    if (!line.trim()) continue;
    const value = verifyRecord(JSON.parse(line));
    if (value) { totals[value.component] += value.value; records++; }
  }
  return { records: records.toString(), ...Object.fromEntries(Object.entries(totals).map(([k, v]) => [k, v.toString()])) };
}

async function main() {
  if (process.versions.node !== '24.22.1') throw new Error('Use Node.js 24.22.1 (nvm use)');
  const path = process.argv[2];
  if (!path) throw new Error('Usage: node analysis/verify.mjs facts.jsonl[.gz] (published CH export)');
  let input = createReadStream(path);
  if (path.endsWith('.gz')) input = input.pipe(createGunzip());
  console.log(JSON.stringify(await verifyStream(input), null, 2));
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}

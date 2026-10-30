export function hex(value, length = null) {
  if (typeof value !== 'string' || !/^0x(?:[0-9a-f]{2})*$/i.test(value)) throw new Error('invalid hex');
  const bytes = Buffer.from(value.slice(2), 'hex');
  if (length !== null && bytes.length !== length) throw new Error('invalid hex length');
  return bytes;
}
export function integer(value) {
  if (typeof value !== 'string' || !/^\d+$/.test(value)) throw new Error('expected unsigned integer string');
  const n = BigInt(value); if (n > (1n << 64n) - 1n) throw new Error('uint64 overflow'); return n;
}
export function bitlist(value) {
  const bytes = hex(value); if (!bytes.length || bytes.at(-1) === 0) throw new Error('missing bitlist terminator');
  let last = bytes.at(-1), high = -1; while (last) { last >>= 1; high++; }
  const n = (bytes.length - 1) * 8 + high;
  return Array.from({length: n}, (_, i) => Boolean(bytes[i >> 3] & (1 << (i % 8))));
}
export function selected(a, version, max) {
  if (['phase0','altair','bellatrix','capella','deneb'].includes(version)) {
    if (a.committee_bits !== undefined) throw new Error('unexpected committee bits');
    const n = integer(a.data.index); if (n >= BigInt(max)) throw new Error('committee index'); return [n.toString()];
  }
  if (!['electra','fulu'].includes(version)) throw new Error('unsupported fork');
  if (a.data.index !== '0') throw new Error('Electra data index');
  const bytes = hex(a.committee_bits, Math.ceil(max/8)); const out = [];
  for (let i = 0; i < bytes.length * 8; i++) if (bytes[i >> 3] & (1 << (i % 8))) { if (i >= max) throw new Error('padding bit'); out.push(String(i)); }
  if (!out.length) throw new Error('empty committees'); return out;
}

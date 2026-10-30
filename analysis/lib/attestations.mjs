import { createHash } from 'node:crypto';
import { hex, integer, bitlist, selected } from './bits.mjs';
const hash = (left,right) => createHash('sha256').update(left).update(right).digest();
function uint(value) { const b=Buffer.alloc(32); b.writeBigUInt64LE(integer(value)); return b; }
function checkpoint(c) { return hash(uint(c.epoch),hex(c.root,32)); }
export function dataRoot(d) {
  let nodes = [uint(d.slot),uint(d.index),hex(d.beacon_block_root,32),checkpoint(d.source),checkpoint(d.target),...Array.from({length:3},()=>Buffer.alloc(32))];
  while (nodes.length>1) nodes = nodes.filter((_,i)=>!(i%2)).map((_,i)=>hash(nodes[2*i],nodes[2*i+1]));
  return '0x'+nodes[0].toString('hex');
}
export function committees(data, epoch, slots, max) {
  const result=new Map(), seen=new Set();
  for (const c of data) {
    if (integer(c.slot)/slots !== epoch || integer(c.index)>=BigInt(max) || !c.validators.length) throw new Error('invalid committee context');
    const key=c.slot+':'+c.index; if(result.has(key))throw new Error('duplicate committee');
    for(const v of c.validators){integer(v);if(seen.has(v))throw new Error('duplicate epoch validator');seen.add(v);}
    result.set(key,c.validators);
  }
  if(!result.size)throw new Error('empty committees');return result;
}
export function expand(a, version, max, sets, inclusion, slots) {
  hex(a.signature,96);const d=a.data, slot=integer(d.slot);
  if(slot>=inclusion || integer(d.target.epoch)!==slot/slots || integer(d.source.epoch)>integer(d.target.epoch))throw new Error('attestation slot/checkpoint');
  const bits=bitlist(a.aggregation_bits), indices=selected(a,version,max), positions=[];
  for(const index of indices){const members=sets.get(d.slot+':'+index);if(!members)throw new Error('missing committee');members.forEach((validator,position)=>positions.push({validator_index:validator,committee_index:index,committee_position:String(position),aggregation_position:String(positions.length)}));}
  if(bits.length!==positions.length)throw new Error('bit length mismatch');
  const root=dataRoot(d);const rows=positions.filter((_,i)=>bits[i]);
  if(!rows.length || new Set(rows.map(x=>x.validator_index)).size!==rows.length)throw new Error('empty/duplicate aggregate members');
  return rows.map(x=>({...x,attestation_slot:d.slot,head_root:d.beacon_block_root,source_epoch:d.source.epoch,source_root:d.source.root,target_epoch:d.target.epoch,target_root:d.target.root,attestation_data_root:root,bit_length:String(bits.length),participant_count:String(rows.length)}));
}

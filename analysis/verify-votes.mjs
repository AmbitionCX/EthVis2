import { createHash } from 'node:crypto';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { json, lines, local, raw, digest } from './lib/input.mjs';
import { integer, hex } from './lib/bits.mjs';
import { committees, expand } from './lib/attestations.mjs';
const equal=(a,b,message)=>{if(a!==b)throw new Error(`${message}: ${a} != ${b}`);};
export async function verifyVotes(directory){
 const checks=await json(local(directory,'checksums.json'));
 for(const [path,c]of Object.entries(checks)){const got=await digest(local(directory,path));equal(got.sha256,c.sha256,'file checksum '+path);equal(got.bytes,BigInt(c.bytes),'file length '+path);}
 for(const p of ['manifest.json','publication.json','inputs.jsonl'])if(!checks[p])throw new Error('missing checksummed input '+p);
 const m=await json(local(directory,'manifest.json')),pub=await json(local(directory,'publication.json'));
 if(m.version!==3||m.profile!=='blocks_votes'||!m.frozen)throw new Error('incompatible manifest');
 const slots=integer(m.slots_per_epoch),max=Number(integer(m.max_committees_per_slot));if(!slots||max<1||max>1024)throw new Error('invalid preset');
 if(!Array.isArray(m.blocks)||m.blocks.length!==m.target)throw new Error('manifest coverage');
 const sha = value => createHash('sha256').update(value).digest('hex');
 const goJSON = value => JSON.stringify(value).replace(/[<>&\u2028\u2029]/g,c=>'\\u'+c.charCodeAt(0).toString(16).padStart(4,'0'));
 equal(sha(goJSON(m)),pub.manifest_hash,'publication manifest hash');
 equal(sha(goJSON(pub.attempts)),pub.selection_hash,'publication selection hash');
 equal(sha(pub.manifest_hash+pub.selection_hash),pub.publication_id,'publication identity');
 const selectedAttempts=new Map(pub.attempts.map(a=>[a.task_id,a.attempt_id]));if(selectedAttempts.size!==pub.attempts.length)throw new Error('duplicate selected task');
 const roots=new Map(m.blocks.map(b=>[b.root,b]));if(roots.size!==m.target)throw new Error('duplicate frozen roots');
 let previous;for(const b of m.blocks){hex(b.root,32);hex(b.parent,32);hex(b.state,32);integer(b.slot);if(previous){if(integer(b.slot)<=integer(previous.slot)||b.parent!==previous.root)throw new Error('manifest chain');}previous=b;}
 equal(previous.root,m.anchor_root,'anchor');let blockCount=0n,aggregateCount=0n,memberCount=0n;
 const seen=new Set();const checkedPath=p=>{if(!checks[p])throw new Error('input not checksummed '+p);return local(directory,p);};
 for await(const input of lines(local(directory,'inputs.jsonl'))){const ref=roots.get(input.block.root);if(!ref||seen.has(ref.root))throw new Error('extra/duplicate input block');seen.add(ref.root);equal(input.block.slot,ref.slot,'input slot');equal(input.block.state,ref.state,'input state');equal(input.block.parent,ref.parent,'input parent');checkedPath(input.raw.path);
 const b=await raw(directory,input.raw),msg=b.data.message;if(b.execution_optimistic)throw new Error('optimistic block');equal(msg.slot,ref.slot,'block slot');equal(msg.parent_root,ref.parent,'block parent');equal(msg.state_root,ref.state,'block state');hex(b.data.signature,96);
 let expectedFork='phase0',forkEpoch=-1n;for(const name of ['phase0','altair','bellatrix','capella','deneb','electra','fulu'])if(Object.hasOwn(m.forks,name)){const ep=integer(m.forks[name]);if(ep<=integer(ref.slot)/slots&&ep>=forkEpoch){forkEpoch=ep;expectedFork=name;}}equal(b.version,expectedFork,'block fork');
 const sets=new Map(),contexts=new Map();for(const c of input.committees??[]){checkedPath(c.raw.path);const response=await raw(directory,c.raw);if(response.execution_optimistic)throw new Error('optimistic committees');if(contexts.has(c.epoch))throw new Error('duplicate committee epoch');const valid=committees(response.data,integer(c.epoch),slots,max);contexts.set(c.epoch,c);for(const [key,values]of valid){if(sets.has(key))throw new Error('duplicate committee key');sets.set(key,values);}}
 const expected=new Map(),coverage=new Map();for(const [i,a]of msg.body.attestations.entries()){const rows=expand(a,b.version,max,sets,integer(ref.slot),slots);const epoch=(integer(a.data.slot)/slots).toString(),c=contexts.get(epoch);if(!c)throw new Error('missing epoch context');for(const row of rows){const id=`${i}:${row.validator_index}`;expected.set(id,{...row,row_id:id,epoch,attestation_index:String(i),context_id:c.state_root,committee_sha256:c.raw.sha256,input_attempt:selectedAttempts.get(`block_decode:block-v1:${ref.root}`)});}coverage.set(String(i),{participant_count:String(rows.length),bit_length:rows[0].bit_length});aggregateCount++;}
 checkedPath(input.expected);const seenMembers=new Set();for await(const row of lines(local(directory,input.expected))){const want=expected.get(row.row_id);if(!want||seenMembers.has(row.row_id))throw new Error('extra/duplicate member inclusion');seenMembers.add(row.row_id);for(const [key,value]of Object.entries(want))equal(row[key],value,'member '+key);equal(row.run_id,m.id,'member run');equal(row.network,m.network,'member network');equal(row.block_root,ref.root,'member block');equal(row.slot,ref.slot,'member inclusion slot');equal(row.raw_sha256,input.raw.sha256,'member raw');equal(row.attempt_id,selectedAttempts.get(`attestation_map:votes-v1:${ref.root}`),'member selected attempt');memberCount++;}equal(seenMembers.size,expected.size,'member coverage');
 checkedPath(input.coverage);const seenCoverage=new Set();for await(const row of lines(local(directory,input.coverage))){if(seenCoverage.has(row.row_id))throw new Error('duplicate coverage');seenCoverage.add(row.row_id);equal(row.block_root,ref.root,'coverage block');equal(row.attempt_id,selectedAttempts.get(`attestation_map:votes-v1:${ref.root}`),'coverage selected attempt');if(row.row_id==='block')equal(row.participant_count,String(msg.body.attestations.length),'aggregate block count');else{const want=coverage.get(row.row_id);if(!want)throw new Error('extra coverage');for(const [key,value]of Object.entries(want))equal(row[key],value,'coverage '+key);}}equal(seenCoverage.size,coverage.size+1,'aggregate coverage');blockCount++;
 }
 equal(seen.size,m.target,'input block coverage');return {run_id:m.id,publication_id:pub.publication_id,blocks:blockCount.toString(),aggregates:aggregateCount.toString(),member_inclusions:memberCount.toString(),votes_verified:true,bls_verification:'not_performed'};
}
async function main(){if(process.version!=='v24.22.1')throw new Error('Use Node.js v24.22.1');const args=process.argv.slice(2);if(args.length!==2||args[0]!=='--input')throw new Error('usage: node analysis/verify-votes.mjs --input DIRECTORY');console.log(JSON.stringify(await verifyVotes(resolve(args[1])),null,2));}
if(import.meta.url===pathToFileURL(process.argv[1]??'').href)main().catch(e=>{console.error(e.message);process.exitCode=1;});

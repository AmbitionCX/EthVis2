import test from 'node:test';
import assert from 'node:assert/strict';
import { bitlist, selected, integer } from '../lib/bits.mjs';
import { dataRoot, committees, expand } from '../lib/attestations.mjs';
const root=n=>'0x'+n.toString(16).padStart(64,'0');
const att=()=>({aggregation_bits:'0x35',committee_bits:'0x05',signature:'0x'+'11'.repeat(96),data:{slot:'8',index:'0',beacon_block_root:root(8),source:{epoch:'1',root:root(4)},target:{epoch:'2',root:root(8)}}});
test('Electra sparse committees preserve actual positions and large IDs',()=>{
 const cs=committees([{slot:'8',index:'0',validators:['0','9007199254740993']},{slot:'8',index:'2',validators:['12','13','14']}],2n,4n,8);
 const result=expand(att(),'electra',8,cs,10n,4n);
 assert.deepEqual(result.map(r=>[r.validator_index,r.committee_index,r.committee_position,r.aggregation_position]),[['0','0','0','0'],['12','2','0','2'],['14','2','2','4']]);
 const large=att();large.aggregation_bits='0x22';assert.equal(expand(large,'electra',8,cs,10n,4n)[0].validator_index,'9007199254740993');
 const wrong=att();wrong.aggregation_bits='0x03';assert.throws(()=>expand(wrong,'electra',8,cs,10n,4n),/length/);
 cs.delete('8:2');assert.throws(()=>expand(att(),'electra',8,cs,10n,4n),/missing committee/);
});
test('SSZ terminator, LSB order, legacy index and invalid encodings',()=>{
 assert.deepEqual(bitlist('0x0101'),[true,false,false,false,false,false,false,false]);
 for(const bad of ['0x','0x00','0x0100','0x0','zz'])assert.throws(()=>bitlist(bad));
 const a=att();delete a.committee_bits;a.data.index='2';a.aggregation_bits='0x0a';assert.deepEqual(selected(a,'deneb',8),['2']);
 assert.equal(expand(a,'deneb',8,new Map([['8:2',['1','2','3']]]),10n,4n)[0].validator_index,'2');
 assert.throws(()=>integer(9007199254740993));assert.throws(()=>integer('18446744073709551616'));
 assert.throws(()=>selected(att(),'future',8));
 const padding=att();padding.committee_bits='0x80';assert.throws(()=>selected(padding,'electra',7));
});
test('data root commits to all attestation data; signatures are aggregate-only',()=>{
 const a=att(),before=dataRoot(a.data);a.data.index='1';assert.notEqual(dataRoot(a.data),before);
 a.data.beacon_block_root='0x1';assert.throws(()=>dataRoot(a.data));
 assert.throws(()=>committees([{slot:'8',index:'0',validators:['0','0']}],2n,4n,8));
});

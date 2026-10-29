import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { integer, verifyRecord, verifyStream } from './verify.mjs';

test('reject precision-losing JS numbers and retain integers beyond 2^53', () => {
  assert.equal(integer('9007199254740993', 'reward'), 9007199254740993n);
  assert.throws(() => integer(9007199254740992, 'reward'));
});
test('signed inactivity is included once', () => {
  assert.deepEqual(verifyRecord({ dataset: 'attestation_rewards', payload: {head:'10',target:'-20',source:'5',inactivity:'-100'} }), {component:'attestation_gwei', value:-105n});
});
test('wrong proposer total is rejected', () => {
  assert.throws(() => verifyRecord({dataset:'proposer_rewards',payload:{total:'20',attestations:'10',sync_aggregate:'0',proposer_slashings:'0',attester_slashings:'0'}}));
});
test('stream JSONL without loading all validators into memory', async () => {
  const row = JSON.stringify({dataset:'sync_rewards',payload:JSON.stringify({reward:'-9007199254740993'})});
  const result = await verifyStream(Readable.from([`${row}\n${row}\n`]));
  assert.equal(result.sync_gwei, '-18014398509481986');
  assert.equal(result.records,'2');
});

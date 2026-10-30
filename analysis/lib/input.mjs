import { createReadStream } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { resolve, relative, isAbsolute } from 'node:path';
import { createHash } from 'node:crypto';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
export function local(base,path){if(typeof path!=='string'||isAbsolute(path))throw new Error('expected relative input path');const full=resolve(base,path),rel=relative(resolve(base),full);if(rel==='..'||rel.startsWith('../'))throw new Error('input path escapes package');return full;}
export async function* lines(path){const stream=createReadStream(path);let error;stream.on('error',e=>{error=e;});const input=createInterface({input:stream,crlfDelay:Infinity});try{for await(const line of input){if(!line.trim())throw new Error('empty JSONL row');yield JSON.parse(line);}if(error)throw error;}finally{input.close();stream.destroy();}}
export const json = async path => JSON.parse(await readFile(path,'utf8'));
export async function digest(path){const h=createHash('sha256');let bytes=0n;for await(const chunk of createReadStream(path)){h.update(chunk);bytes+=BigInt(chunk.length);}return {sha256:h.digest('hex'),bytes};}
export async function raw(base,ref){const source=createReadStream(local(base,ref.path)),gzip=createGunzip();source.on('error',e=>gzip.destroy(e));source.pipe(gzip);const chunks=[];const h=createHash('sha256');let bytes=0n;try{for await(const chunk of gzip){chunks.push(chunk);h.update(chunk);bytes+=BigInt(chunk.length);}}finally{source.destroy();gzip.destroy();}if(h.digest('hex')!==ref.sha256||bytes!==BigInt(ref.bytes))throw new Error('raw payload hash/length mismatch');return JSON.parse(Buffer.concat(chunks).toString('utf8'));}

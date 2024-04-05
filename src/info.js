import crypto from 'crypto';
import os from 'os';
import { format } from 'util';
import { readFile } from 'node:fs/promises';

const fileUrl = new URL('../package.json', import.meta.url);
const self = JSON.parse(await readFile(fileUrl, 'utf8'));
const host = os.hostname();
const platform = os.platform();
const architecture = os.arch();
const title = process.title;
const pid = process.pid;
const consumerId = format('%s.%s.%s', host, title, pid);
const consistentId = format('%s.%s', host, title);
const toBE = os.endianness() === 'BE';

function createConsumerTag (queueName) {
  if (queueName.indexOf(consumerId) === 0) {
    return queueName;
  } else {
    return format('%s.%s', consumerId, queueName);
  }
}

function hash (id) {
  const bytes = crypto.createHash('md5').update(id).digest();
  const num = toBE ? bytes.readdInt16BE() : bytes.readInt16LE();
  return num < 0 ? Math.abs(num) + 0xffffffff : num;
}

// not great, but good enough for our purposes
function createConsumerHash () {
  return hash(consumerId);
}

function createConsistentHash () {
  return hash(consistentId);
}

function getHostInfo () {
  return format('%s (%s %s)', host, platform, architecture);
}

function getProcessInfo () {
  return format('%s (pid: %d)', title, pid);
}

function getLibInfo () {
  return format('foo-foo-mq - %s', self.version);
}

export default {
  id: consumerId,
  host: getHostInfo,
  lib: getLibInfo,
  process: getProcessInfo,
  createTag: createConsumerTag,
  createHash: createConsumerHash,
  createConsistentHash
};

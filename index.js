#!/usr/bin/env node
import pluginPkg from 'kafkajs-msk-iam-authentication-mechanism';
const { createMechanism: createMskIamMechanism, TYPE: MSK_IAM_TYPE } = pluginPkg;
import kafkaPkg from 'kafkajs';
const { Kafka, logLevel, CompressionCodecs, CompressionTypes } = kafkaPkg;
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

const startTs = Date.now();
function logDebug(enabled, ...args) { if (enabled) console.log('[debug]', ...args); }

// Ensure Snappy codec registered (override internal placeholder) using snappyjs
try {
  const { default: snappy } = await import('snappyjs');
  // KafkaJS exports CompressionCodecs map where value for Snappy currently throws. Override with implementation.
  CompressionCodecs[CompressionTypes.Snappy] = () => ({
    compress: async encoder => snappy.compress(encoder.buffer),
    decompress: async buffer => snappy.uncompress(buffer),
  });
  if (process.env.KAFKA_TOOL_CODEC_LOG) console.log('[debug] Snappy codec (snappyjs) override installed');
} catch (e) {
  if (process.env.KAFKA_TOOL_CODEC_LOG) console.log('[debug] Failed to install snappy codec', e?.message);
}

const baseYargs = yargs(hideBin(process.argv))
  .option('brokers', { type: 'string', demandOption: true, desc: 'Comma separated broker host:port list (Kafka listener)' })
  .option('region', { type: 'string', demandOption: true, desc: 'Kafka cluster AWS region' })
  .option('assumeRoleArn', { type: 'string', desc: 'Optional remote role to assume (cross-account). If omitted uses base creds.' })
  .option('clientId', { type: 'string', default: 'kafka-test-tool' })
  .option('logLevel', { choices: ['info','warn','error','debug','nothing'], default: 'info' })
  .option('ssl', { type: 'boolean', default: true, desc: 'Enable TLS (set false to test PLAINTEXT listener)' })
  .strict();

baseYargs.command('produce', 'Send one or multiple messages', y => y
  .option('topic', { type: 'string', demandOption: true })
  .option('message', { type: 'string', default: 'hello' })
  .option('count', { type: 'number', default: 1 })
  .option('intervalMs', { type: 'number', default: 0, desc: 'Delay between messages' })
  , handlerProduce)
  .command('consume', 'Consume messages (prints value)', y => y
    .option('topic', { type: 'string', demandOption: true })
    .option('groupId', { type: 'string', default: 'kafka-test-tool-group' })
    .option('fromBeginning', { type: 'boolean', default: false })
    .option('limit', { type: 'number', default: 5, desc: 'Stop after N messages (0 = infinite)' })
  , handlerConsume)
  .demandCommand(1)
  .help()
  .parse();

function mapLog(level) {
  switch(level) {
    case 'debug': return logLevel.DEBUG;
    case 'warn': return logLevel.WARN;
    case 'error': return logLevel.ERROR;
    case 'nothing': return logLevel.NOTHING;
    default: return logLevel.INFO;
  }
}

function createAssumeRoleProvider(roleArn, region, sessionName = 'kafkaTestSession', durationSeconds = 3600, refreshBeforeMs = 5*60*1000, dbg=false) {
  if (!roleArn) {
    logDebug(dbg, 'Using base credential provider chain (no AssumeRole)');
    return async () => fromNodeProviderChain()();
  }
  const base = fromNodeProviderChain();
  let current; // {credentials, expiration}
  async function assume() {
    logDebug(dbg, 'Assuming role', roleArn, 'in', region);
    const t0 = Date.now();
    const { STSClient, AssumeRoleCommand } = await import('@aws-sdk/client-sts');
    const baseCreds = await base();
    const sts = new STSClient({ region, credentials: baseCreds });
    const out = await sts.send(new AssumeRoleCommand({ RoleArn: roleArn, RoleSessionName: sessionName, DurationSeconds: durationSeconds }));
    current = {
      credentials: {
        accessKeyId: out.Credentials.AccessKeyId,
        secretAccessKey: out.Credentials.SecretAccessKey,
        sessionToken: out.Credentials.SessionToken
      },
      expiration: out.Credentials.Expiration
    };
    logDebug(dbg, 'AssumeRole success in', (Date.now()-t0)+'ms', 'AKID prefix', current.credentials.accessKeyId.slice(0,4), 'expires', current.expiration.toISOString());
  }
  return async () => {
    if (!current || (current.expiration && Date.now() + refreshBeforeMs > current.expiration.getTime())) {
      await assume();
    }
    return current.credentials;
  };
}

function buildKafka(argv) {
  const dbg = argv.logLevel === 'debug';
  const brokers = argv.brokers.split(',').map(b => b.trim()).filter(Boolean);
  logDebug(dbg, 'Kafka config', { brokers, region: argv.region, assumeRole: !!argv.assumeRoleArn, clientId: argv.clientId, ssl: argv.ssl });
  const credentialsProvider = createAssumeRoleProvider(argv.assumeRoleArn, argv.region, 'kafkaTestSession', 3600, 5*60*1000, dbg);

  const mechanism = createMskIamMechanism({
    region: argv.region,
    credentials: async () => credentialsProvider()
  }, MSK_IAM_TYPE);

  if (!argv.ssl) {
    console.warn('[warn] TLS disabled via --ssl=false. AWS_MSK_IAM normally requires SASL_SSL listener; this may fail if broker does not allow IAM over PLAINTEXT.');
  }

  const kafka = new Kafka({
    clientId: argv.clientId,
    brokers,
    ssl: argv.ssl,
    sasl: mechanism,
    logLevel: mapLog(argv.logLevel),
    connectionTimeout: 8000,
    requestTimeout: 30000,
    retry: { retries: 8, initialRetryTime: 300, maxRetryTime: 30000 }
  });
  logDebug(dbg, 'Kafka instance created');
  return kafka;
}

async function handlerProduce(argv) {
  const dbg = argv.logLevel === 'debug';
  const kafka = buildKafka(argv);
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  logDebug(dbg, 'Connecting producer...');
  await producer.connect();
  logDebug(dbg, 'Producer connected, sending messages');
  for (let i=0;i<argv.count;i++) {
    const payload = { ts: new Date().toISOString(), seq: i+1, msg: argv.message };
    await producer.send({ topic: argv.topic, messages: [{ key: `k-${Date.now()}-${i}`, value: JSON.stringify(payload) }] });
    console.log('Sent', payload);
    if (argv.intervalMs && i < argv.count - 1) await new Promise(r => setTimeout(r, argv.intervalMs));
  }
  logDebug(dbg, 'Disconnecting producer');
  await producer.disconnect();
  logDebug(dbg, 'Producer disconnected');
}

async function handlerConsume(argv) {
  const dbg = argv.logLevel === 'debug';
  const kafka = buildKafka(argv);
  const consumer = kafka.consumer({ groupId: argv.groupId });
  logDebug(dbg, 'Connecting consumer...');
  await consumer.connect();
  logDebug(dbg, 'Consumer connected, subscribing', argv.topic, 'fromBeginning', argv.fromBeginning);
  await consumer.subscribe({ topic: argv.topic, fromBeginning: argv.fromBeginning });
  let seen = 0;
  const startRun = Date.now();
  logDebug(dbg, 'Starting consumer run loop');
  await consumer.run({
    eachMessage: async ({ message, partition, topic }) => {
      console.log('----------------------------------------');
      if (seen === 0) logDebug(dbg, 'First message latency ms', Date.now()-startRun);
      const value = message.value?.toString();
  // Highlight output: topic (cyan), key (yellow), value (green), offset (dim)
  const color = (c, s) => `\x1b[${c}m${s}\x1b[0m`;
  const dim = s => color('2', s);
  const cyan = s => color('36', s);
  const yellow = s => color('33', s);
  const green = s => color('32', s);
  const line = `${dim('[')}${cyan(topic)}:${partition}${dim('#')}${dim(message.offset)}${dim(']')} ${yellow(message.key?.toString() || '-')}: ${green(value)}`;
  console.log(line);
      if (argv.limit && argv.limit > 0 && ++seen >= argv.limit) {
        logDebug(dbg, 'Limit reached, disconnecting');
        await consumer.disconnect();
        logDebug(dbg, 'Consumer disconnected (limit)');
        process.exit(0);
      }
      console.log('----------------------------------------');
    }
  });
  ['SIGINT','SIGTERM'].forEach(sig => process.on(sig, async () => { console.log('Shutting down...'); try { await consumer.disconnect(); } catch(e){} process.exit(0); }));
}


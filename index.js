#!/usr/bin/env node
import kafkaPkg from 'kafkajs';
const { Kafka, logLevel, CompressionCodecs, CompressionTypes } = kafkaPkg;
import { generateAuthTokenFromRole } from 'aws-msk-iam-sasl-signer-js';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

const startTs = Date.now();
function logDebug(enabled, ...args) { if (enabled) console.log('[debug]', ...args); }

const baseYargs = yargs(hideBin(process.argv))
  .option('brokers', { type: 'string', demandOption: true, desc: 'Comma separated broker host:port list (Kafka listener)' })
  .option('region', { type: 'string', demandOption: true, desc: 'Kafka cluster AWS region' })
  .option('assumeRoleArn', { type: 'string', demandOption: true, desc: 'Required role to assume (cross-account).' })
  .option('clientId', { type: 'string', default: 'kafka-test-tool' })
  .option('logLevel', { choices: ['info','warn','error','debug','nothing'], default: 'info' })
  .option('ssl', { type: 'boolean', default: true, desc: 'Enable TLS (should stay true for IAM listener)' })
  .option('awsDebugCreds', { type: 'boolean', default: false, desc: 'Enable signer credential identity debug (extra STS call)' })
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

async function buildOauthBearerProvider(argv, dbg) {
  const { region, assumeRoleArn: roleArn, awsDebugCreds } = argv;
  return async () => {
    const tokenResp = await generateAuthTokenFromRole({
      region,
      awsRoleArn: roleArn,
      awsRoleSessionName: 'kafkaTestSession',
      logger: dbg ? console : undefined
    });
    return { value: tokenResp.token, expiration: tokenResp.expiration ? new Date(tokenResp.expiration).getTime() : undefined };
  };
}

async function buildKafka(argv) {
  const dbg = argv.logLevel === 'debug';
  const brokers = argv.brokers.split(',').map(b => b.trim()).filter(Boolean);
  if (!argv.ssl) console.warn('[warn] TLS disabled but IAM auth requires TLS listener. This may fail.');

  // Ensure snappy registered (idempotent reassign ok)
  try {
    const { default: snappy } = await import('snappyjs');
    CompressionCodecs[CompressionTypes.Snappy] = () => ({
      compress: async encoder => snappy.compress(encoder.buffer),
      decompress: async buffer => snappy.uncompress(buffer),
    });
  } catch {}

  const oauthBearerProvider = await buildOauthBearerProvider(argv, dbg);

  const kafka = new Kafka({
    clientId: argv.clientId,
    brokers,
    logLevel: mapLog(argv.logLevel),
    connectionTimeout: 8000,
    requestTimeout: 30000,
    retry: { retries: 8, initialRetryTime: 300, maxRetryTime: 30000 },
    ssl: argv.ssl,
    sasl: { mechanism: 'oauthbearer', oauthBearerProvider },
  });
  logDebug(dbg, 'Kafka config built', { brokers, hasSasl: true });
  return kafka;
}

async function handlerProduce(argv) {
  const dbg = argv.logLevel === 'debug';
  const kafka = await buildKafka(argv);
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
  const kafka = await buildKafka(argv);
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


# Kafka Test Tool (AWS MSK IAM)

Produce & consume messages against an AWS MSK (IAM-auth) cluster, supporting optional cross-account AssumeRole. Node.js 20+ recommended.

## Install (inside pod or local IRSA env)
```
npm install --omit=dev
```

## Dependencies
- `kafkajs`
- `aws-msk-iam-sasl-signer-js` (generates IAM auth token for MSK via SASL/OAUTHBEARER)
- AWS SDK v3 credential providers + STS (for optional AssumeRole)

## Commands
Two subcommands: `produce` and `consume`.

### New Options
- `--ssl` (default true) Set `--ssl=false` to attempt PLAINTEXT (debug only; IAM auth listener requires TLS).
- `--awsDebugCreds` (default false) Extra STS identity fetch to log which IAM principal is used (adds latency).

### Produce examples
Single message:
```
node index.js produce \
  --brokers "b-1.example.amazonaws.com:9098,b-2.example.amazonaws.com:9098" \
  --topic prism.raw.gap.school \
  --region eu-west-1 \
  --message "probe" \
  --assumeRoleArn $REMOTE_KAFKA_ROLE_ARN
```

Burst 10 messages (100ms interval):
```
node index.js produce \
  --brokers "$BROKERS" \
  --topic prism.raw.gap.school \
  --region $DEST_KAFKA_REGION \
  --count 10 \
  --intervalMs 100
```

### Consume examples
Stream from latest:
```
node index.js consume \
  --brokers "$BROKERS" \
  --topic prism.raw.gap.school \
  --region $DEST_KAFKA_REGION \
  --groupId test-consumer
```

Read first 5 from beginning then exit:
```
node index.js consume \
  --brokers "$BROKERS" \
  --topic prism.raw.gap.school \
  --region $DEST_KAFKA_REGION \
  --fromBeginning \
  --limit 5
```

### Arguments (common)
- `--brokers` Comma list of bootstrap brokers (IAM/SASL_SSL port, usually 9098). Use the `Iam` or `IamTls` line from `aws kafka get-bootstrap-brokers`.
- `--region` Kafka cluster AWS region (SigV4 signing region).
- `--assumeRoleArn` (optional) Remote role to assume. If omitted uses base creds (IRSA / env / shared config).
- `--clientId` Kafka client id (default `kafka-test-tool`).
- `--logLevel` info|warn|error|debug|nothing.

Produce-specific:
- `--message` Body template (string) used for each message.
- `--count` Number of messages (default 1).
- `--intervalMs` Delay between sends.

Consume-specific:
- `--groupId` Consumer group (default `kafka-test-tool-group`).
- `--fromBeginning` Start at earliest offset.
- `--limit` Stop after N messages (0 = keep running).

### Cross-account flow
1. Base credentials (IRSA / env / profile) acquired via AWS default provider chain. 
2. If `--assumeRoleArn` supplied, signer library requests a token using that role (STS AssumeRole under the hood).
3. Tool supplies token to KafkaJS via SASL/OAUTHBEARER `oauthbearer` mechanism.

### Notes
- Ensure MSK cluster has IAM authentication enabled and you are using the IAM listener port (SASL/OAUTHBEARER over TLS).
- Role policy must include necessary `kafka-cluster:*` permissions for topics/consumer groups.
- Clock skew can break SigV4 token generation; container time must be in sync.
- Error `SASL_AUTHENTICATION_FAILED` often means missing permissions or wrong listener.

### Scripts (npm)
```
npm run produce -- --brokers "$BROKERS" --topic prism.raw.gap.school --region $DEST_KAFKA_REGION --message test
npm run consume -- --brokers "$BROKERS" --topic prism.raw.gap.school --region $DEST_KAFKA_REGION
```

Cross-account example:
```
npm run consume -- --brokers b-3-public.kafka-prism-dev.j4p0qp.c3.kafka.eu-west-1.amazonaws.com:9198 \
  --assumeRoleArn "arn:aws:iam::291654376946:role/ef-studio-prism-elive-integration" \
  --topic "prism.raw.gap.school" \
  --region "eu-west-1"
```
npm run consume -- --brokers b-3-public.kafka-prism-dev.j4p0qp.c3.kafka.eu-west-1.amazonaws.com:9198 --assumeRoleArn arn:aws:iam::291654376946:role/ef-studio-prism-elive-integration --topic prism.raw.gap.school --region eu-west-1 --limit 1 --logLevel debug --groupId external.elive.kafka-test-tool-group

node index.js consume \
--brokers "kafka-proxy-1.shared.svc.cluster.local:9093,kafka-proxy-2.shared.svc.cluster.local:9093,kafka-proxy-3.shared.svc.cluster.local:9093" \
--topic prism.raw.catalyst.elive \
--region eu-west-1 \
--groupId external.elive.prism.kafka.proxy \
--clientId el-prism-kafka-proxy \
--assumeRoleArn arn:aws:iam::291654376946:role/ef-studio-prism-elive-integration \
--logLevel debug \
--limit 1




node index.js consume \
--brokers "kafka-proxy-1.shared.svc.cluster.local:9093" \
--topic prism.raw.catalyst.elive \
--region eu-west-1 \
--groupId external.elive.prism.kafka.proxy-test \
--clientId el-prism-kafka-proxy \
--assumeRoleArn arn:aws:iam::291654376946:role/ef-studio-prism-elive-integration \
--logLevel debug \
--limit 1


node index.js consume --brokers "b-2-public.kafka-prism-dev.j4p0qp.c3.kafka.eu-west-1.amazonaws.com:9198,b-1-public.kafka-prism-dev.j4p0qp.c3.kafka.eu-west-1.amazonaws.com:9198,b-3-public.kafka-prism-dev.j4p0qp.c3.kafka.eu-west-1.amazonaws.com:9198" --topic prism.raw.catalyst.elive --region eu-west-1 --groupId external.elive.prism.kafka.proxy.tool --clientId el-prism-kafka-proxy --assumeRoleArn arn:aws:iam::291654376946:role/ef-studio-prism-elive-integration --logLevel debug --limit 1 --ssl=true --iam=true


### Exit
Ctrl+C triggers graceful disconnect.

npm run consume -- --brokers b-3-public.kafka-prism-dev.j4p0qp.c3.kafka.eu-west-1.amazonaws.com:9198 --assumeRoleArn arn:aws:iam::291654376946:role/ef-studio-prism-elive-integration --topic prism.raw.catalyst.elive --region eu-west-1 --limit 1 --logLevel debug
import {
  SQSClient,
  CreateQueueCommand,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
  type Message,
} from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';

export class Queues {
  readonly client: SQSClient;
  readonly endpoint = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';
  private urls = new Map<string, string>();
  readonly names = {
    input: `${process.env.QUEUE_PREFIX ?? ''}wager-transactions.fifo`,
    dlq: `${process.env.QUEUE_PREFIX ?? ''}wager-transactions-dlq.fifo`,
    events: `${process.env.QUEUE_PREFIX ?? ''}wager-events.fifo`,
  };
  constructor() {
    this.client = new SQSClient({
      region: process.env.AWS_REGION ?? 'us-east-1',
      endpoint: this.endpoint,
      maxAttempts: 1,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      },
    });
  }
  private externalize(url: string): string {
    return new URL(new URL(url).pathname, this.endpoint).toString();
  }
  async url(name: string): Promise<string> {
    let url = this.urls.get(name);
    if (!url) {
      const response = await this.client.send(new GetQueueUrlCommand({ QueueName: name }), {
        abortSignal: AbortSignal.timeout(5000),
      });
      url = this.externalize(response.QueueUrl!);
      this.urls.set(name, url);
    }
    return url;
  }
  async bootstrap(): Promise<void> {
    const common = {
      FifoQueue: 'true',
      ContentBasedDeduplication: 'false',
      VisibilityTimeout: process.env.SQS_VISIBILITY_SECONDS ?? '30',
      ReceiveMessageWaitTimeSeconds: '20',
    };
    const dlq = await this.client.send(
      new CreateQueueCommand({
        QueueName: this.names.dlq,
        Attributes: { ...common, MessageRetentionPeriod: '1209600' },
      }),
    );
    const dlqUrl = this.externalize(dlq.QueueUrl!);
    this.urls.set(this.names.dlq, dlqUrl);
    const attrs = await this.client.send(
      new GetQueueAttributesCommand({ QueueUrl: dlqUrl, AttributeNames: ['QueueArn'] }),
    );
    for (const name of [this.names.input, this.names.events]) {
      const response = await this.client.send(
        new CreateQueueCommand({
          QueueName: name,
          Attributes: {
            ...common,
            ...(name === this.names.input
              ? {
                  RedrivePolicy: JSON.stringify({
                    deadLetterTargetArn: attrs.Attributes!.QueueArn,
                    maxReceiveCount: '5',
                  }),
                }
              : {}),
          },
        }),
      );
      this.urls.set(name, this.externalize(response.QueueUrl!));
    }
  }
  async ready(): Promise<void> {
    await Promise.all(
      Object.values(this.names).map(async (name) =>
        this.client.send(
          new GetQueueAttributesCommand({ QueueUrl: await this.url(name), AttributeNames: ['QueueArn'] }),
          { abortSignal: AbortSignal.timeout(5000) },
        ),
      ),
    );
  }
  async send(
    name: string,
    body: string,
    groupId: string,
    dedupId: string = randomUUID(),
    reason?: string,
    sourceMessageId?: string,
  ): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: await this.url(name),
        MessageBody: body,
        MessageGroupId: groupId,
        MessageDeduplicationId: dedupId,
        ...(reason
          ? {
              MessageAttributes: {
                FailureCode: { DataType: 'String', StringValue: reason },
                ...(sourceMessageId
                  ? { SourceMessageId: { DataType: 'String', StringValue: sourceMessageId } }
                  : {}),
              },
            }
          : {}),
      }),
      { abortSignal: AbortSignal.timeout(5000) },
    );
  }
  async receive(name: string, signal?: AbortSignal): Promise<Message[]> {
    const r = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: await this.url(name),
        MaxNumberOfMessages: 5,
        WaitTimeSeconds: Number(process.env.SQS_WAIT_SECONDS ?? 20),
        MessageSystemAttributeNames: ['ApproximateReceiveCount'],
        MessageAttributeNames: ['All'],
      }),
      signal ? { abortSignal: signal } : {},
    );
    return r.Messages ?? [];
  }
  async ack(name: string, receipt: string): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({ QueueUrl: await this.url(name), ReceiptHandle: receipt }),
      { abortSignal: AbortSignal.timeout(5000) },
    );
  }
  async visibility(name: string, receipt: string, seconds: number): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: await this.url(name),
        ReceiptHandle: receipt,
        VisibilityTimeout: seconds,
      }),
      { abortSignal: AbortSignal.timeout(5000) },
    );
  }
  close(): void {
    this.client.destroy();
  }
}

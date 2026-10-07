/** Test barriers are not exposed over HTTP and are disabled outside NODE_ENV=test. */
export async function fault(
  point: string,
  identifiers: { messageId?: string | undefined; transactionId?: string } = {},
): Promise<void> {
  if (process.env.NODE_ENV !== 'test' || process.env.TEST_FAULT_POINT !== point) return;
  if (process.env.TEST_FAULT_MESSAGE_ID && process.env.TEST_FAULT_MESSAGE_ID !== identifiers.messageId)
    return;
  if (process.send) process.send({ type: 'barrier', point, ...identifiers });
  if (process.env.TEST_FAULT_ACTION === 'throw') throw new Error(`Injected failure: ${point}`);
  await new Promise<void>((resolve) => {
    const release = (message: unknown) => {
      if (message && typeof message === 'object' && 'type' in message && message.type === 'release-fault') {
        process.off('message', release);
        resolve();
      }
    };
    process.on('message', release);
  });
}

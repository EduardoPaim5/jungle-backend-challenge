import { Queues } from '../src/infrastructure/sqs.js';
const queues = new Queues();
try {
  const deadline = Date.now() + 60000;
  while (true) {
    try {
      await queues.bootstrap();
      break;
    } catch (e) {
      if (Date.now() > deadline) throw e;
      await Bun.sleep(500);
    }
  }
  console.log(JSON.stringify({ queues: Object.values(queues.names), success: true }));
} finally {
  queues.close();
}

import express from 'express';
import { connect, StringCodec } from 'nats';

const app = express();
app.use(express.json());

const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
const PORT = process.env.PORT || 3000;

const sc = StringCodec();
const STREAM_NAME = 'unified_stream';
let js;
let jsm;

// Connect to NATS and JetStream
const setup = async () => {
  const nc = await connect({ servers: NATS_URL });
  js = nc.jetstream();
  jsm = await nc.jetstreamManager();

  // Try to create the stream if it doesn't exist
  try {
    await jsm.streams.add({
      name: STREAM_NAME,
      subjects: ['*'],
      retention: 'limits',
      storage: 'memory',
      max_msgs: -1,
      max_bytes: -1,
      discard: 'old',
      num_replicas: 1
    });
    console.log(`✅ Created JetStream stream "${STREAM_NAME}"`);
  } catch (err) {
    if (!err.message.includes('stream name already in use')) {
      console.error(`❌ Failed to create stream:`, err.message);
    }
  }
};
await setup();

// PUT /?subject=my.test.subject
app.put('/', async (req, res) => {
  const { subject } = req.query;
  const body = req.body;

  if (!subject || !body) {
    return res.status(400).json({ error: 'Missing subject or body' });
  }

  try {
    await js.publish(subject, sc.encode(typeof body === 'string' ? body : JSON.stringify(body)));
    return res.status(200).json({ status: 'ok' });
  } catch (err) {
    return res.status(500).json({ error: 'Publish failed', detail: err.message });
  }
});

// GET /?subject=my.test.subject
app.get('/', async (req, res) => {
  const { subject } = req.query;
  if (!subject) {
    return res.status(400).json({ error: 'Missing subject' });
  }

  const durable = `durable_${subject.replace(/[^\w]/g, '_')}`;

  try {
    // Ensure consumer exists
    try {
      await jsm.consumers.info(STREAM_NAME, durable);
    } catch {
      await jsm.consumers.add(STREAM_NAME, {
        durable_name: durable,
        ack_policy: 'explicit',
        deliver_policy: 'last',
        max_deliver: -1,
        filter_subject: subject
      });
      console.log(`✅ Created consumer "${durable}" for "${subject}"`);
    }

    const sub = await js.pullSubscribe(subject, {
      stream: STREAM_NAME,
      durable
    });

    const messages = [];
    const done = (async () => {
      for await (const m of sub) {
        messages.push(sc.decode(m.data));
        m.ack();
        break;
      }
    })();

    await js.pull(sub, { batch: 1, expires: 1000 });
    await done;

    return res.status(200).json({ message: messages[0] || null });
  } catch (err) {
    return res.status(500).json({ error: 'Fetch failed', detail: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 NATS HTTP frontend listening on port ${PORT}`);
});

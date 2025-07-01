import express from 'express';
import { connect, StringCodec } from 'nats';

const app = express();
app.use(express.json());

const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
const PORT = process.env.PORT || 8080;

const sc = StringCodec();
let js, jsm;
const STREAM_NAME = 'universal_stream';

// Connect to NATS and JetStream
const setup = async () => {
  const nc = await connect({ servers: NATS_URL });
  js = nc.jetstream();
  jsm = await nc.jetstreamManager();

  // Ensure universal stream exists
  try {
    await jsm.streams.add({
      name: STREAM_NAME,
      subjects: ['>'],
      retention: 'limits',
      storage: 'memory',
      max_msgs: -1,
      max_bytes: -1,
      discard: 'old',
      num_replicas: 1,
      no_ack: true // required for wildcard subjects
    });
    console.log(`✅ Created stream '${STREAM_NAME}'`);
  } catch (err) {
    if (err.message.includes('stream name already in use')) {
      console.log(`ℹ️ Stream '${STREAM_NAME}' already exists`);
    } else {
      console.error(`❌ Stream creation failed: ${err.message}`);
    }
  }
};

await setup();

// PUT /?subject=foo.bar
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

// GET /?subject=foo.bar
app.get('/', async (req, res) => {
  const { subject } = req.query;

  if (!subject) {
    return res.status(400).json({ error: 'Missing subject' });
  }

  try {
    const durableName = `durable_${subject.replace(/[.*>]/g, '_')}`;
    const sub = await js.pullSubscribe(subject, {
      durable: durableName
    });

    const messages = [];
    const consume = (async () => {
      for await (const m of sub) {
        messages.push(sc.decode(m.data));
        m.ack();
        break; // only one message
      }
    })();

    await js.pull(sub, { batch: 1, expires: 2000 });
    await consume;

    return res.status(200).json({ message: messages[0] || null });
  } catch (err) {
    return res.status(500).json({ error: 'Fetch failed', detail: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 NATS HTTP frontend listening on port ${PORT}`);
});

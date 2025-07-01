import express from 'express';
import { connect, StringCodec } from 'nats';

const app = express();
app.use(express.json());

const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
const PORT = process.env.PORT || 8080;

const sc = StringCodec();
const STREAM_NAME = 'mystream';
let js;
let jsm;

const setup = async () => {
  const nc = await connect({ servers: NATS_URL });
  js = nc.jetstream();
  jsm = await nc.jetstreamManager();

  // Try to create the stream with wildcard subject
  try {
await jsm.streams.add({
  name: STREAM_NAME,
  subjects: ['>'],
  retention: 'limits',
  storage: 'memory',
  discard: 'old',
  num_replicas: 1,
  no_ack: true // <-- Required when using wildcard subjects like '>'
});
    console.log(`✅ Created stream "${STREAM_NAME}"`);
  } catch (err) {
    if (!err.message.includes('stream name already in use')) {
      console.error(`❌ Stream creation failed: ${err.message}`);
    } else {
      console.log(`ℹ️ Stream "${STREAM_NAME}" already exists`);
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
  if (!subject) return res.status(400).json({ error: 'Missing subject' });

  try {
    const sub = await js.pullSubscribe(subject, {
      stream: STREAM_NAME,
      durable: `durable_${subject.replace(/\./g, '_')}`,
      config: {
        ack_policy: 'explicit',
        deliver_policy: 'last'
      }
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

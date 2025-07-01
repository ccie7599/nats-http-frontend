import express from 'express';
import { connect, StringCodec, consumerOpts } from 'nats';

const NATS_SERVER = process.env.NATS_URL || 'nats://localhost:4222';
const PORT = process.env.PORT || 8080;
const STREAM_NAME = 'http-stream';
const SUBJECT_WILDCARD = 'http.>';

const sc = StringCodec();
let js, jsm;

const app = express();
app.use(express.text({ type: '*/*' }));

// Ensure stream exists (creates one for all subjects starting with "http.")
async function ensureStreamExists() {
  try {
    await jsm.streams.info(STREAM_NAME);
    console.log(`✅ Stream "${STREAM_NAME}" already exists`);
  } catch (err) {
    if (err.code === '404' || err.message.includes('stream not found')) {
      console.log(`ℹ️ Stream "${STREAM_NAME}" not found. Creating...`);
      await jsm.streams.add({
        name: STREAM_NAME,
        subjects: [SUBJECT_WILDCARD],
        retention: 'limits',
        max_msgs: 100_000,
        max_bytes: 100 * 1024 * 1024,
        storage: 'memory',
        num_replicas: 1
      });
      console.log(`✅ Stream "${STREAM_NAME}" created`);
    } else {
      console.error('❌ Failed to verify or create stream:', err.message);
      throw err;
    }
  }
}

// Setup NATS connection
async function setupNATS() {
  const nc = await connect({ servers: NATS_SERVER });
  js = nc.jetstream();
  jsm = await nc.jetstreamManager();
  await ensureStreamExists();
}
await setupNATS();

// PUT /?subject=http.foo
app.put('/', async (req, res) => {
  const { subject } = req.query;
  if (!subject) return res.status(400).json({ error: 'Missing subject' });

  try {
    await js.publish(subject, sc.encode(req.body));
    res.status(200).json({ status: 'ok' });
  } catch (err) {
    res.status(500).json({ error: 'Publish failed', detail: err.message });
  }
});

// GET /?subject=http.foo
app.get('/', async (req, res) => {
  const { subject } = req.query;
  if (!subject) return res.status(400).json({ error: 'Missing subject' });

  try {
    const durable = `durable-${subject.replace(/\W/g, '-')}`;
    const opts = consumerOpts();
    opts.durable(durable);
    opts.manualAck();
    opts.ackExplicit();
    opts.deliverTo(`${durable}-inbox`);

    const sub = await js.pullSubscribe(subject, opts);
    const done = (async () => {
      for await (const m of sub) {
        const msg = sc.decode(m.data);
        m.ack();
        res.status(200).json({ message: msg });
        return;
      }
    })();

    await js.pull(sub, { batch: 1, expires: 1000 });
    await done;
  } catch (err) {
    res.status(500).json({ error: 'Fetch failed', detail: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Listening on http://0.0.0.0:${PORT}`);
});

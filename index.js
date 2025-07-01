import express from 'express';
import { connect, StringCodec, consumerOpts } from 'nats';

const app = express();
app.use(express.json());

const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
const PORT = process.env.PORT || 3000;

const sc = StringCodec();
const subjectToStreamMap = {}; // cache stream creation to avoid re-creating

let js;

// Connect to NATS and JetStream
const setup = async () => {
  const nc = await connect({ servers: NATS_URL });
  js = nc.jetstream();

  // Store reference to JetStream Manager
  app.locals.jsm = await nc.jetstreamManager();
};
await setup();

// Helper to create a stream for the given subject
async function ensureStream(subject, jsm) {
  if (subjectToStreamMap[subject]) return;

  const streamName = `stream_${subject.replace(/[.*>]/g, '_')}`;
  try {
    await jsm.streams.add({
      name: streamName,
      subjects: [subject],
      retention: 'limits',
      storage: 'memory',
      max_msgs: -1,
      max_bytes: -1,
      discard: 'old',
      num_replicas: 1
    });
    console.log(`✅ Created JetStream stream "${streamName}" for subject "${subject}"`);
    subjectToStreamMap[subject] = true;
  } catch (err) {
    if (!err.message.includes('stream name already in use')) {
      console.error(`❌ Failed to create stream for subject "${subject}":`, err.message);
    }
  }
}

// PUT /?subject=my.test.subject
app.put('/', async (req, res) => {
  const { subject } = req.query;
  const body = req.body;

  if (!subject || !body) {
    return res.status(400).json({ error: 'Missing subject or body' });
  }

  try {
    await js.publish(subject, sc.encode(JSON.stringify(body)));
    return res.status(200).json({ status: 'ok' });
  } catch (err) {
    if (err.code === '503') {
      console.warn(`⚠️ No stream found for "${subject}", attempting to create it...`);
      await ensureStream(subject, app.locals.jsm);

      try {
        await js.publish(subject, sc.encode(JSON.stringify(body)));
        return res.status(200).json({ status: 'ok (after stream creation)' });
      } catch (err2) {
        return res.status(500).json({ error: 'Publish failed after stream creation', detail: err2.message });
      }
    }
    return res.status(500).json({ error: 'Publish failed', detail: err.message });
  }
});

// GET /?subject=my.test.subject
app.get('/', async (req, res) => {
  const { subject } = req.query;

  if (!subject) {
    return res.status(400).json({ error: 'Missing subject' });
  }

  try {
    const sub = await js.pullSubscribe(subject, {
      durable: 'durable',
    });

    const messages = [];
    const done = (async () => {
      for await (const m of sub) {
        messages.push(JSON.parse(sc.decode(m.data)));
        m.ack();
        break; // only one for simplicity
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

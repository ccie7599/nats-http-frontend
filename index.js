import express from 'express';
import { connect, StringCodec } from 'nats';

const app = express();
app.use(express.json());

const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
const PORT = process.env.PORT || 3000;
const STREAM_NAME = 'messages';
const SUBJECT_PREFIX = 'messages.';
const DURABLE_NAME = 'durable';

const sc = StringCodec();
let js;
let jsm;

// Connect to NATS and JetStream
const setup = async () => {
  const nc = await connect({ servers: NATS_URL });
  js = nc.jetstream();
  jsm = await nc.jetstreamManager();

  try {
    await jsm.streams.add({
      name: STREAM_NAME,
      subjects: [`${SUBJECT_PREFIX}>`],
      retention: 'limits',
      storage: 'memory',
      max_msgs: -1,
      max_bytes: -1,
      discard: 'old',
      num_replicas: 1
    });
    console.log(`✅ Created stream "${STREAM_NAME}"`);
  } catch (err) {
    if (!err.message.includes('stream name already in use')) {
      console.error(`❌ Failed to create stream "${STREAM_NAME}":`, err.message);
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

  const fullSubject = `${SUBJECT_PREFIX}${subject}`;

  try {
    await js.publish(fullSubject, sc.encode(JSON.stringify(body)));
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

  const fullSubject = `${SUBJECT_PREFIX}${subject}`;

  // Ensure durable consumer for this subject exists
  try {
    await jsm.consumers.info(STREAM_NAME, DURABLE_NAME);
  } catch {
    try {
      await jsm.consumers.add(STREAM_NAME, {
        durable_name: DURABLE_NAME,
        ack_policy: 'explicit',
        deliver_policy: 'all',
        max_deliver: -1,
        filter_subject: fullSubject
      });
      console.log(`✅ Created consumer "${DURABLE_NAME}" with filter "${fullSubject}"`);
    } catch (err) {
      return res.status(500).json({ error: 'Consumer creation failed', detail: err.message });
    }
  }

  try {
    const sub = await js.pullSubscribe(fullSubject, {
      stream: STREAM_NAME,
      durable: DURABLE_NAME
    });

    const messages = [];
    const done = (async () => {
      for await (const m of sub) {
        messages.push(JSON.parse(sc.decode(m.data)));
        m.ack();
        break; // fetch one message only
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

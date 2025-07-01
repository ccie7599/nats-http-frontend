import express from 'express';
import { connect, StringCodec, consumerOpts } from 'nats';

const app = express();
const PORT = process.env.PORT || 8080;
const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
const sc = StringCodec();

app.use(express.text({ type: "*/*" }));

let nc, js;

async function initNATS() {
  nc = await connect({ servers: NATS_URL });
  js = nc.jetstream();
  console.log(`Connected to NATS at ${NATS_URL}`);
}

app.put('/', async (req, res) => {
  const subject = req.query.subject;
  const body = req.body;

  if (!subject || !body) {
    return res.status(400).send('Missing subject or body');
  }

  try {
    await js.publish(subject, sc.encode(body));
    res.status(202).send(`Message published to ${subject}`);
  } catch (err) {
    console.error(err);
    res.status(500).send('Failed to publish message');
  }
});

app.get('/', async (req, res) => {
  const subject = req.query.subject;

  if (!subject) {
    return res.status(400).send('Missing subject');
  }

  try {
    const sub = await js.pullSubscribe(subject, {
      config: { durable_name: `reader-${subject}` },
    });

    const msgs = [];
    const done = (async () => {
      for await (const m of sub) {
        msgs.push(sc.decode(m.data));
        m.ack();
        if (msgs.length >= 1) break;
      }
    })();

    await sub.pull({ batch: 1, expires: 1000 });
    await done;

    res.status(200).json({ messages: msgs });
  } catch (err) {
    console.error(err);
    res.status(500).send('Failed to pull message');
  }
});

initNATS().then(() => {
  app.listen(PORT, () => {
    console.log(`HTTP frontend listening on port ${PORT}`);
  });
}).catch(err => {
  console.error('Failed to connect to NATS:', err);
  process.exit(1);
});

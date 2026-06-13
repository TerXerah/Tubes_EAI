const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

const PORT = process.env.PORT || 8001;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';

const pool = new Pool({
  host: process.env.DB_HOST || 'postgres-pos',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'pos_db',
  user: process.env.DB_USER || 'pos_user',
  password: process.env.DB_PASS || 'pos_pass',
});

let channel = null;

//connection rabbitmq
async function connectRabbitMQ(retries = 10, delay = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL);
      channel = await conn.createChannel();
      await channel.assertExchange('sales.broadcast', 'fanout', { durable: true });
      console.log('[POS] Connected to RabbitMQ, exchange sales.broadcast ready');
      return;
    } catch (err) {
      console.log(`[POS] RabbitMQ not ready, retrying in ${delay}ms... (${i + 1}/${retries})`);
      await new Promise((r) => setTimeout(r, delay));
      delay *= 2;
    }
  }
  throw new Error('Could not connect to RabbitMQ after retries');
}

//databse
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS products (
      product_id VARCHAR(50) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      unit_price INTEGER NOT NULL
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS transactions (
      transaction_id VARCHAR(50) PRIMARY KEY,
      product_id VARCHAR(50) NOT NULL,
      quantity INTEGER NOT NULL,
      unit_price INTEGER NOT NULL,
      customer_id VARCHAR(50) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  //sample
  await pool.query(`
    INSERT INTO products (product_id, name, unit_price) VALUES
      ('PRD-001', 'Cumi Hitam Pak Kris', 75000),
      ('PRD-002', 'Oseng Oseng Biawak', 45000),
      ('PRD-003', 'Tiket Whoosh', 300000)
    ON CONFLICT (product_id) DO NOTHING;
  `);
}

//routes
app.get('/health', (req, res) => res.json({ status: 'ok', service: 'pos-service' }));

app.get('/products', async (req, res) => {
  const result = await pool.query('SELECT * FROM products ORDER BY product_id');
  res.json(result.rows);
});

app.get('/transactions', async (req, res) => {
  const result = await pool.query('SELECT * FROM transactions ORDER BY created_at DESC');
  res.json(result.rows);
});

app.post('/transactions', async (req, res) => {
  try {
    const { product_id, quantity, customer_id } = req.body;

    if (!product_id || !quantity || !customer_id) {
      return res.status(400).json({ error: 'product_id, quantity, and customer_id are required' });
    }

    const productResult = await pool.query('SELECT * FROM products WHERE product_id = $1', [product_id]);
    if (productResult.rows.length === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    const product = productResult.rows[0];

    const transaction_id = `TXN-${uuidv4().slice(0, 8).toUpperCase()}`;

    await pool.query(
      'INSERT INTO transactions (transaction_id, product_id, quantity, unit_price, customer_id) VALUES ($1, $2, $3, $4, $5)',
      [transaction_id, product_id, quantity, product.unit_price, customer_id]
    );

    const cdmEvent = {
      event_type: 'SALE_COMPLETED',
      event_id: uuidv4(),
      product_id,
      quantity,
      unit_price: product.unit_price,
      customer_id,
      transaction_id,
      timestamp: new Date().toISOString(),
    };

    if (channel) {
      channel.publish('sales.broadcast', '', Buffer.from(JSON.stringify(cdmEvent)), {
        persistent: true,
        contentType: 'application/json',
      });
      console.log('[POS] Published SALE_COMPLETED event:', cdmEvent.event_id);
    } else {
      console.warn('[POS] RabbitMQ channel not ready, event not published');
    }

    res.status(201).json({
      message: 'Transaction created and event published',
      transaction: { transaction_id, product_id, quantity, unit_price: product.unit_price, customer_id },
      event: cdmEvent,
    });
  } catch (err) {
    console.error('[POS] Error creating transaction:', err);
    res.status(500).json({ error: err.message });
  }
});

//start
async function start() {
  await initDB();
  await connectRabbitMQ();
  app.listen(PORT, () => console.log(`[POS] POS Service running on port ${PORT}`));
}

start().catch((err) => {
  console.error('[POS] Failed to start:', err);
  process.exit(1);
});
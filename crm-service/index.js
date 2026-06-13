const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

const PORT = process.env.PORT || 8003;

const pool = new Pool({
  host: process.env.DB_HOST || 'postgres-crm',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'crm_db',
  user: process.env.DB_USER || 'crm_user',
  password: process.env.DB_PASS || 'crm_pass',
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS customers (
      customer_id VARCHAR(50) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      loyalty_points INTEGER NOT NULL DEFAULT 0,
      total_purchases INTEGER NOT NULL DEFAULT 0
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS purchase_history (
      id SERIAL PRIMARY KEY,
      customer_id VARCHAR(50) NOT NULL,
      transaction_id VARCHAR(50) NOT NULL,
      product_id VARCHAR(50) NOT NULL,
      quantity INTEGER NOT NULL,
      amount INTEGER NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  //sample customer
  await pool.query(`
    INSERT INTO customers (customer_id, name, loyalty_points, total_purchases) VALUES
      ('CUST-042', 'Topek', 0, 0),
      ('CUST-043', 'Cahyo', 0, 0),
      ('CUST-044', 'Adi Hidayat', 0, 0)
    ON CONFLICT (customer_id) DO NOTHING;
  `);
}

app.get('/health', (req, res) => res.json({ status: 'ok', service: 'crm-service' }));

app.get('/customers', async (req, res) => {
  const result = await pool.query('SELECT * FROM customers ORDER BY customer_id');
  res.json(result.rows);
});

app.get('/customers/:id', async (req, res) => {
  const result = await pool.query('SELECT * FROM customers WHERE customer_id = $1', [req.params.id]);
  if (result.rows.length === 0) return res.status(404).json({ error: 'Customer not found' });
  res.json(result.rows[0]);
});

app.get('/purchase-history', async (req, res) => {
  const result = await pool.query('SELECT * FROM purchase_history ORDER BY created_at DESC');
  res.json(result.rows);
});

app.post('/customers/update-from-sale', async (req, res) => {
  try {
    const { item_ref, member_code, tx_ref, quantity, unit_price } = req.body;

    if (!member_code || !tx_ref || !quantity || !unit_price) {
      return res.status(400).json({ error: 'member_code, tx_ref, quantity, unit_price are required' });
    }

    const amount = quantity * unit_price;
    const pointsEarned = Math.floor(amount / 10000);

    const customerCheck = await pool.query('SELECT * FROM customers WHERE customer_id = $1', [member_code]);
    if (customerCheck.rows.length === 0) {
      //otomatis membuat customer jika belum ada
      await pool.query(
        'INSERT INTO customers (customer_id, name, loyalty_points, total_purchases) VALUES ($1, $2, 0, 0)',
        [member_code, `Customer ${member_code}`]
      );
    }

    await pool.query(
      `UPDATE customers
       SET loyalty_points = loyalty_points + $1,
           total_purchases = total_purchases + $2
       WHERE customer_id = $3`,
      [pointsEarned, amount, member_code]
    );

    await pool.query(
      'INSERT INTO purchase_history (customer_id, transaction_id, product_id, quantity, amount) VALUES ($1, $2, $3, $4, $5)',
      [member_code, tx_ref, item_ref, quantity, amount]
    );

    const updated = await pool.query('SELECT * FROM customers WHERE customer_id = $1', [member_code]);

    console.log(`[CRM] Updated customer ${member_code}: +${pointsEarned} points (ref=${tx_ref})`);

    res.json({
      message: 'Customer profile updated',
      customer: updated.rows[0],
      points_earned: pointsEarned,
    });
  } catch (err) {
    console.error('[CRM] Error updating customer:', err);
    res.status(500).json({ error: err.message });
  }
});

async function start() {
  await initDB();
  app.listen(PORT, () => console.log(`[CRM] CRM Service running on port ${PORT}`));
}

start().catch((err) => {
  console.error('[CRM] Failed to start:', err);
  process.exit(1);
});
const amqp = require('amqplib');
const axios = require('axios');
const { create } = require('xmlbuilder2');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const INVENTORY_SOAP_URL = process.env.INVENTORY_SOAP_URL || 'http://inventory-service:8002/soap/inventory';
const CRM_API_URL = process.env.CRM_API_URL || 'http://crm-service:8003/customers/update-from-sale';

const MAX_RETRIES = 3;

const processedEvents = {
  inventory: new Set(),
  crm: new Set(),
};

async function connectRabbitMQ(retries = 10, delay = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL);
      const channel = await conn.createChannel();

      await channel.assertExchange('sales.broadcast', 'fanout', { durable: true });

      await channel.assertExchange('dlx', 'direct', { durable: true });
      await channel.assertQueue('dlq.failed', { durable: true });
      await channel.bindQueue('dlq.failed', 'dlx', '');

      const queueOptions = {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': 'dlx',
        },
      };
      await channel.assertQueue('inventory.updates', queueOptions);
      await channel.assertQueue('crm.updates', queueOptions);

      await channel.bindQueue('inventory.updates', 'sales.broadcast', '');
      await channel.bindQueue('crm.updates', 'sales.broadcast', '');

      await channel.prefetch(1);

      console.log('[Integration] Connected to RabbitMQ. Topology ready:');
      console.log('  - sales.broadcast (fanout exchange)');
      console.log('  - inventory.updates, crm.updates (queues)');
      console.log('  - dlq.failed (dead letter queue)');

      return channel;
    } catch (err) {
      console.log(`[Integration] RabbitMQ not ready, retrying in ${delay}ms... (${i + 1}/${retries})`);
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, 16000);
    }
  }
  throw new Error('Could not connect to RabbitMQ after retries');
}

function translateToSoapXml(cdmEvent) {
  const doc = create({ version: '1.0', encoding: 'UTF-8' })
    .ele('soap:Envelope', { 'xmlns:soap': 'http://schemas.xmlsoap.org/soap/envelope/' })
      .ele('soap:Body')
        .ele('DeductStock', { xmlns: 'http://inventory.example.com/soap' })
          .ele('sku_code').txt(cdmEvent.product_id).up()
          .ele('quantity_deducted').txt(String(cdmEvent.quantity)).up()
          .ele('reference_id').txt(cdmEvent.transaction_id).up()
        .up()
      .up()
    .up();

  return doc.end({ prettyPrint: false });
}

//call inventory
async function callInventoryService(cdmEvent) {
  const xmlPayload = translateToSoapXml(cdmEvent);
  console.log('[Integration][Translator] JSON CDM -> SOAP XML:');
  console.log('  ' + xmlPayload);

  const response = await axios.post(INVENTORY_SOAP_URL, xmlPayload, {
    headers: { 'Content-Type': 'text/xml' },
    timeout: 5000,
  });
  console.log('[Integration][Adapter] Inventory SOAP response status:', response.status);
  return response.data;
}

//call crm service
async function callCrmService(cdmEvent) {
  const crmPayload = {
    item_ref: cdmEvent.product_id,
    member_code: cdmEvent.customer_id,
    tx_ref: cdmEvent.transaction_id,
    quantity: cdmEvent.quantity,
    unit_price: cdmEvent.unit_price,
  };

  console.log('[Integration][Translator] CDM -> CRM payload:', JSON.stringify(crmPayload));

  const response = await axios.post(CRM_API_URL, crmPayload, { timeout: 5000 });
  console.log('[Integration][Adapter] CRM REST response status:', response.status);
  return response.data;
}

function shouldRouteToInventory(eventType) {
  return eventType === 'SALE_COMPLETED' || eventType === 'RESTOCK';
}

function shouldRouteToCrm(eventType) {
  return eventType === 'SALE_COMPLETED';
}

function consumeQueue(channel, queueName, idempotencyKey, handler) {
  channel.consume(queueName, async (msg) => {
    if (!msg) return;

    let event;
    try {
      event = JSON.parse(msg.content.toString());
    } catch (e) {
      console.error(`[Integration][${queueName}] Invalid JSON, sending to DLQ`);
      channel.nack(msg, false, false);
      return;
    }

    const retryCount = (msg.properties.headers && msg.properties.headers['x-retry-count']) || 0;

    if (processedEvents[idempotencyKey].has(event.event_id)) {
      console.log(`[Integration][${queueName}] Duplicate event ${event.event_id}, skipping (idempotent)`);
      channel.ack(msg);
      return;
    }

    const route =
      idempotencyKey === 'inventory'
        ? shouldRouteToInventory(event.event_type)
        : shouldRouteToCrm(event.event_type);

    if (!route) {
      console.log(`[Integration][${queueName}] event_type=${event.event_type} not relevant, skipping`);
      processedEvents[idempotencyKey].add(event.event_id);
      channel.ack(msg);
      return;
    }

    try {
      await handler(event);
      processedEvents[idempotencyKey].add(event.event_id);
      channel.ack(msg);
      console.log(`[Integration][${queueName}] Successfully processed event ${event.event_id}`);
    } catch (err) {
      console.error(`[Integration][${queueName}] Error processing event ${event.event_id}: ${err.message}`);

      if (retryCount < MAX_RETRIES) {
        // Republish with incremented retry count (manual retry, simple backoff)
        console.log(`[Integration][${queueName}] Retrying (${retryCount + 1}/${MAX_RETRIES})...`);
        setTimeout(() => {
          channel.sendToQueue(queueName, msg.content, {
            persistent: true,
            headers: { 'x-retry-count': retryCount + 1 },
          });
        }, 1000 * (retryCount + 1));
        channel.ack(msg);
      } else {
        console.error(`[Integration][${queueName}] Max retries exceeded, sending to DLQ`);
        channel.nack(msg, false, false);
      }
    }
  });
}

//startup
async function start() {
  const channel = await connectRabbitMQ();

  consumeQueue(channel, 'inventory.updates', 'inventory', callInventoryService);
  consumeQueue(channel, 'crm.updates', 'crm', callCrmService);

  console.log('[Integration] Integration Service is running and consuming queues...');
}

start().catch((err) => {
  console.error('[Integration] Failed to start:', err);
  process.exit(1);
});
exports.handler = async function (event) {
  console.log('expire-reservations lambda invoked');
  const { Client } = require('pg');
  const AWS = require('aws-sdk');

  const dbUrl = process.env.DATABASE_URL || 'postgres://postgres:postgres@postgres:5432/app';
  const localstackUrl = process.env.LOCALSTACK_URL || 'http://localstack:4566';

  const client = new Client({ connectionString: dbUrl });
  await client.connect();

  try {
    // Find all active reservations that have expired
    const res = await client.query(
      `SELECT sr.id, sr.order_id, sr.product_id, sr.quantity
       FROM inventory_service.stock_reservations sr
       WHERE sr.status = 'ACTIVE' AND sr.expires_at < now()`
    );

    if (res.rows.length === 0) {
      console.log('No expired reservations found');
      return { status: 'ok', expired: 0 };
    }

    console.log(`Found ${res.rows.length} expired reservation(s)`);

    const sqs = new AWS.SQS({
      endpoint: localstackUrl,
      region: 'us-east-1',
      accessKeyId: 'test',
      secretAccessKey: 'test'
    });

    let qUrl;
    try {
      qUrl = await sqs.getQueueUrl({ QueueName: 'reservation-expiry-queue' }).promise().then(r => r.QueueUrl);
    } catch (err) {
      console.error('Failed to get reservation-expiry-queue URL', err.message);
      qUrl = null;
    }

    let expiredCount = 0;
    for (const row of res.rows) {
      try {
        await client.query('BEGIN');

        // Mark reservation as EXPIRED
        await client.query(
          "UPDATE inventory_service.stock_reservations SET status='EXPIRED' WHERE id=$1 AND status='ACTIVE'",
          [row.id]
        );

        // Decrement reserved_stock and increment version on the product
        const updateRes = await client.query(
          `UPDATE inventory_service.products
           SET reserved_stock = GREATEST(0, reserved_stock - $1),
               version = version + 1,
               updated_at = now()
           WHERE id = $2`,
          [row.quantity, row.product_id]
        );

        if (updateRes.rowCount === 0) {
          console.warn('Product not found for reservation expiry:', row.product_id);
          await client.query('ROLLBACK');
          continue;
        }

        // Insert stock_movements record
        await client.query(
          `INSERT INTO inventory_service.stock_movements
             (product_id, movement_type, quantity_delta, reference_id, reference_type, created_at)
           VALUES ($1, 'EXPIRY', $2, $3, 'ORDER', now())`,
          [row.product_id, -row.quantity, row.order_id]
        );

        await client.query('COMMIT');
        expiredCount++;

        // Send message to reservation-expiry-queue so order-service can cancel the order
        if (qUrl) {
          try {
            await sqs.sendMessage({
              QueueUrl: qUrl,
              MessageBody: JSON.stringify({ orderId: row.order_id, reservationId: row.id })
            }).promise();
          } catch (sqsErr) {
            console.error('Failed to send SQS message for order', row.order_id, sqsErr.message);
          }
        }
      } catch (rowErr) {
        try { await client.query('ROLLBACK'); } catch (e) { /* ignore */ }
        console.error('Error processing expired reservation', row.id, rowErr.message);
      }
    }

    console.log(`Expired ${expiredCount} reservation(s)`);
    return { status: 'ok', expired: expiredCount };
  } catch (err) {
    console.error('expire-reservations lambda failed', err);
    throw err;
  } finally {
    try { await client.end(); } catch (e) { /* ignore */ }
  }
};

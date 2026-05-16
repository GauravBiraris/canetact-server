// cutclock-backend/services/maintenanceService.js
const cron = require('node-cron');
const pool = require('../db');

const startDataPruningCron = () => {
  // Runs at 02:00 on day-of-month 1 and 15.
  cron.schedule('0 2 1,15 * *', async () => {
    console.log('Running bi-monthly Basic tier data pruning...');
    const client = await pool.connect();

    try {
      // Postgres allows deleting records based on a subquery of another table
      // We target lots older than 15 days belonging to tenants explicitly marked as 'basic'
      const pruneQuery = `
        DELETE FROM lots 
        WHERE crushed_time < NOW() - INTERVAL '15 days'
        AND tenant_id IN (
          SELECT id FROM tenants WHERE LOWER(subscription_tier) = 'basic'
        );
      `;
      
      const result = await client.query(pruneQuery);
      console.log(`Pruning complete: Deleted ${result.rowCount} old records from Basic tenants.`);
    } catch (error) {
      console.error('Error during data pruning:', error);
    } finally {
      client.release();
    }
  });
};

module.exports = { startDataPruningCron };
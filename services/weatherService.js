// cutclock-backend/services/weatherService.js
const cron = require('node-cron');
const axios = require('axios');
const pool = require('../db');

// Run this cron job at the top of every hour (e.g., 1:00, 2:00)
const startWeatherCron = () => {
  cron.schedule('0 * * * *', async () => {
    console.log('Running hourly weather fetch...');
    const client = await pool.connect();

    try {
      // 1. Get all active tenants and their coordinates
      const res = await client.query('SELECT id, lat, lng FROM tenants WHERE lat IS NOT NULL');
      const tenants = res.rows;

      for (const tenant of tenants) {
        // 2. Fetch current temperature from OpenWeatherMap
        const url = `https://api.openweathermap.org/data/2.5/weather?lat=${tenant.lat}&lon=${tenant.lng}&units=metric&appid=${process.env.OPENWEATHER_API_KEY}`;
        
        try {
          const weatherRes = await axios.get(url);
          const tempC = weatherRes.data.main.temp;

          // 3. Save to weather_logs table
          await client.query(
            'INSERT INTO weather_logs (tenant_id, recorded_at, temp_celsius) VALUES ($1, CURRENT_TIMESTAMP, $2)',
            [tenant.id, tempC]
          );
          console.log(`Logged ${tempC}°C for tenant ${tenant.id}`);
        } catch (apiError) {
          console.error(`Failed to fetch weather for tenant ${tenant.id}:`, apiError.message);
        }
      }
    } catch (dbError) {
      console.error('Database error in weather cron:', dbError);
    } finally {
      client.release();
    }
  });
};

// Runs at 02:00 AM every day
const startCleanupCron = () => {
  cron.schedule('0 2 * * *', async () => {
    console.log('Running routine database cleanup...');
    const client = await pool.connect();

    try {
      // Deletes any weather logs older than 7 days
      const deleteQuery = `
        DELETE FROM weather_logs 
        WHERE recorded_at < NOW() - INTERVAL '7 days';
      `;
      
      const result = await client.query(deleteQuery);
      console.log(`Cleanup complete: Purged ${result.rowCount} old weather records.`);
      
    } catch (dbError) {
      console.error('Database error during cleanup:', dbError);
    } finally {
      client.release();
    }
  });
};

// Export both cron jobs
module.exports = { startWeatherCron, startCleanupCron };
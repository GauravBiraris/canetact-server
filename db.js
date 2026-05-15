// db.js
const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    // Allows local connection to Neon without strict cert validation
    rejectUnauthorized: false 
  }
});

module.exports = pool;
require('dotenv').config();
const express = require('express');
const stream = require('stream');
const pool = require('./db');
const { auth } = require('./firebase');
const { verifyToken, requireRole, requireActiveTenant } = require('./middleware/authMiddleware');
const { startWeatherCron, startCleanupCron } = require('./services/weatherService');
const { calculateDI } = require('./utils/diCalculator');
const QueryStream = require('pg-query-stream');
const fastcsv = require('fast-csv');
const { startDataPruningCron } = require('./services/maintenanceService');

startWeatherCron();
startCleanupCron();
startDataPruningCron()

const app = express();
app.use(cors());
app.use(express.json());

// Middleware for internal App Team operations
const verifySuperAdmin = (req, res, next) => {
  const apiKey = req.headers['x-api-key'];
  if (!apiKey || apiKey !== process.env.SUPER_ADMIN_API_KEY) {
    return res.status(401).json({ error: 'Unauthorized: Invalid API Key' });
  }
  next();
};

 
// --- ENDPOINT: BULK UPLOAD PARCHAS (Accepts Clean JSON array) ---
app.post('/api/lots/upload', verifyToken, requireRole(['admin', 'manager']), async (req, res) => {
  const { lots } = req.body; // Expecting an array of perfectly mapped objects
  const tenant_id = req.user.tenant_id;

  if (!lots || !Array.isArray(lots) || lots.length === 0) {
    return res.status(400).json({ error: 'No data provided' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. UPDATED: Added is_ratoon and $7 to the query
    const insertQuery = `
      INSERT INTO lots (tenant_id, parcha_no, farmer_name, variety_code, harvest_method, burn_status, is_ratoon)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT DO NOTHING; 
    `;

    let insertedCount = 0;

    for (const row of lots) {
      if (!row.parcha_no) continue; // Skip invalid rows

      // Normalize string/boolean inputs from whatever the CSV had
      const method = row.harvest_method ? String(row.harvest_method).toLowerCase() : 'manual';
      const isBurnt = row.burn_status === 'true' || row.burn_status === true || String(row.burn_status).toUpperCase() === 'Y';
      
      // 2. UPDATED: Parse the is_ratoon value safely
      const isRatoon = row.is_ratoon === 'true' || row.is_ratoon === true || String(row.is_ratoon).toUpperCase() === 'Y';

      // 3. UPDATED: Added isRatoon to the end of the data array
      await client.query(insertQuery, [
        tenant_id,
        String(row.parcha_no).trim(),
        row.farmer_name || null,
        row.variety_code || 'Tier 2',
        method,
        isBurnt,
        isRatoon
      ]);
      
      insertedCount++;
    }

    await client.query('COMMIT');
    res.status(200).json({ message: `Successfully imported ${insertedCount} parchas.` });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Import Error:', error);
    res.status(500).json({ error: 'Database error during import' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: REGISTER NEW TENANT & ADMIN ---
// This is a public or securely-keyed route for SaaS onboarding
app.post('/api/tenants/register', verifySuperAdmin, async (req, res) => {
  const { tenantName, lat, lng, adminEmail, adminPassword, adminName } = req.body;

  let firebaseUid = null;
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // 1. Create the Tenant in Neon
    const tenantQuery = `
      INSERT INTO tenants (name, lat, lng)
      VALUES ($1, $2, $3) RETURNING id;
    `;
    const tenantResult = await client.query(tenantQuery, [tenantName, lat, lng]);
    const tenant_id = tenantResult.rows[0].id;

    // 2. Create the Admin User in Firebase
    const userRecord = await auth.createUser({
      email: adminEmail,
      password: adminPassword,
      displayName: adminName,
    });
    firebaseUid = userRecord.uid;

    // 3. Set Custom Claims in Firebase (Crucial for RBAC)
    await auth.setCustomUserClaims(firebaseUid, { 
      tenant_id: tenant_id, 
      role: 'admin' 
    });

    // 4. Insert the Admin User into Neon
    const userQuery = `
      INSERT INTO users (firebase_uid, tenant_id, role, name)
      VALUES ($1, $2, $3, $4) RETURNING *;
    `;
    await client.query(userQuery, [firebaseUid, tenant_id, 'admin', adminName]);

    await client.query('COMMIT');
    
    res.status(201).json({ 
      message: 'Tenant and Admin created successfully.',
      tenant_id: tenant_id 
    });

  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Registration Error:', error);

    // Cleanup: If the database transaction failed but Firebase user was created, delete the orphaned Firebase user
    if (firebaseUid) {
      try {
        await auth.deleteUser(firebaseUid);
        console.log(`Rolled back Firebase user creation for ${firebaseUid}`);
      } catch (fbError) {
        console.error('Failed to rollback Firebase user:', fbError);
      }
    }

    res.status(500).json({ error: 'Failed to register tenant and admin.' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: TOGGLE TENANT SUBSCRIPTION STATUS (App Team Only) ---
app.patch('/api/tenants/:id/status', verifySuperAdmin, async (req, res) => {
  const { status } = req.body; // e.g., 'active' or 'paused'
  const tenant_id = req.params.id;
  const client = await pool.connect();

  try {
    const query = `UPDATE tenants SET subscription_status = $1 WHERE id = $2 RETURNING *`;
    const result = await client.query(query, [status, tenant_id]);
    res.status(200).json({ message: `Tenant status changed to ${status}` });
  } catch (error) {
    res.status(500).json({ error: 'Failed to update tenant status' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: SYNC OFFLINE CUT LOGS (Field User) ---
app.post('/api/lots/sync', verifyToken, requireRole(['field_user', 'admin', 'manager']), async (req, res) => {
  const { logs } = req.body;
  const tenant_id = req.user.tenant_id;

  if (!logs || !Array.isArray(logs) || logs.length === 0) {
    return res.status(400).json({ error: 'No logs provided' });
  }

  const client = await pool.connect();
  const syncedIds = [];

  try {
    await client.query('BEGIN');

    // MOVED OUTSIDE THE LOOP: Fetch user defaults exactly once
    const userQuery = await client.query('SELECT default_harvest_method, default_burn_status FROM users WHERE firebase_uid = $1', [req.user.uid]);
    const defaults = userQuery.rows[0] || { default_harvest_method: 'manual', default_burn_status: false };

    for (const log of logs) {
      const updateQuery = `
        UPDATE lots
        SET cut_start_time = $1
        WHERE tenant_id = $2 AND parcha_no = $3
        RETURNING id;
      `;
      const updateRes = await client.query(updateQuery, [log.cut_time, tenant_id, log.parcha_no]);

      if (updateRes.rowCount > 0) {
        syncedIds.push(log.id); 
      } else {
        const insertQuery = `
          INSERT INTO lots (tenant_id, parcha_no, cut_start_time, harvest_method, burn_status)
          VALUES ($1, $2, $3, $4, $5)
          RETURNING id;
        `;
        await client.query(insertQuery, [
            tenant_id, 
            log.parcha_no, 
            log.cut_time, 
            defaults.default_harvest_method, 
            defaults.default_burn_status
        ]);
        syncedIds.push(log.id);
      }
    }

    await client.query('COMMIT');
    res.status(200).json({ syncedIds }); 

  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Sync Error:', error);
    res.status(500).json({ error: 'Database error during sync' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: GET PENDING PARCHAS FOR GATE ---
app.get('/api/lots/pending-arrival', verifyToken, requireRole(['gateman', 'admin', 'manager']), async (req, res) => {
  const tenant_id = req.user.tenant_id;
  const client = await pool.connect();
  
  try {
    // Fetch parchas that exist but haven't arrived at the gate yet
    const query = `
      SELECT parcha_no 
      FROM lots 
      WHERE tenant_id = $1 AND gate_arrival_time IS NULL
      ORDER BY created_at DESC;
    `;
    const result = await client.query(query, [tenant_id]);
    
    // Return a simple array of strings (e.g., ["45821", "45822"])
    res.status(200).json(result.rows.map(row => row.parcha_no));
  } catch (error) {
    console.error('Fetch Pending Error:', error);
    res.status(500).json({ error: 'Database error fetching pending parchas' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: LOG GATE ARRIVAL (Updated for Multi-Truck Splitting) ---
app.post('/api/lots/arrive', verifyToken, requireActiveTenant, requireRole(['gateman', 'admin', 'manager']), async (req, res) => {
  const { parcha_no, net_weight, zone } = req.body;
  const tenant_id = req.user.tenant_id;
  
  // Ensure parcha is clean and uppercase for consistent matching
  const cleanParcha = String(parcha_no).trim().toUpperCase();

  if (!cleanParcha || !net_weight || !zone) {
    return res.status(400).json({ error: 'Parcha No, Weight, and Zone are required.' });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // 1. Fetch the BASE parcha record exactly
    const baseQuery = `SELECT * FROM lots WHERE tenant_id = $1 AND parcha_no = $2`;
    const baseResult = await client.query(baseQuery, [tenant_id, cleanParcha]);

    if (baseResult.rowCount > 0) {
      const baseLot = baseResult.rows[0];

      // 2. Has the base parcha already arrived?
      if (baseLot.gate_arrival_time !== null) {
        // --- SCENARIO A: Second/Third Truck Arrival (Clone and Suffix) ---
        
        // Count existing trucks for this base parcha. 
        // We look for the exact base '45821' OR any suffixes like '45821-%'
        const countQuery = `
          SELECT COUNT(*) FROM lots 
          WHERE tenant_id = $1 
          AND (parcha_no = $2 OR parcha_no LIKE $3)
        `;
        const countResult = await client.query(countQuery, [tenant_id, cleanParcha, `${cleanParcha}-%`]);
        
        // If 1 truck exists, the next one is truck #2
        const truckNumber = parseInt(countResult.rows[0].count) + 1; 
        const newParchaNo = `${cleanParcha}-${truckNumber}`;

        // Insert the cloned row with the new weight, zone, and current time, carrying over the field data
        const insertCloneQuery = `
          INSERT INTO lots (
            tenant_id, parcha_no, farmer_name, variety_code, is_ratoon, 
            harvest_method, burn_status, cut_start_time, 
            gate_arrival_time, net_weight, zone
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, $9, $10)
          RETURNING id, parcha_no;
        `;
        const clonedResult = await client.query(insertCloneQuery, [
          tenant_id, newParchaNo, baseLot.farmer_name, baseLot.variety_code, 
          baseLot.is_ratoon, baseLot.harvest_method, baseLot.burn_status, 
          baseLot.cut_start_time, net_weight, zone
        ]);

        await client.query('COMMIT');
        return res.status(201).json({ 
          message: `Arrival logged as Truck #${truckNumber} (${newParchaNo})`, 
          lot: clonedResult.rows[0] 
        });

      } else {
        // --- SCENARIO B: First Truck Arrival (Base parcha exists but hasn't arrived) ---
        const updateQuery = `
          UPDATE lots 
          SET gate_arrival_time = CURRENT_TIMESTAMP, net_weight = $1, zone = $2
          WHERE id = $3 
          RETURNING id, parcha_no;
        `;
        const updateResult = await client.query(updateQuery, [net_weight, zone, baseLot.id]);
        
        await client.query('COMMIT');
        return res.status(200).json({ 
          message: `Arrival logged for ${cleanParcha}`, 
          lot: updateResult.rows[0] 
        });
      }

    } else {
      // --- SCENARIO C: Stub Creation (Parcha not in DB at all) ---
      const insertStubQuery = `
        INSERT INTO lots (tenant_id, parcha_no, gate_arrival_time, net_weight, zone)
        VALUES ($1, $2, CURRENT_TIMESTAMP, $3, $4) 
        RETURNING id, parcha_no;
      `;
      const stubResult = await client.query(insertStubQuery, [tenant_id, cleanParcha, net_weight, zone]);
      
      await client.query('COMMIT');
      return res.status(201).json({ 
        message: 'Arrival logged (Field data pending)', 
        lot: stubResult.rows[0] 
      });
    }

  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Gate Arrival Error:', error);
    res.status(500).json({ error: 'Database error logging arrival' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: GET YARD QUEUE (Dashboard Touchpoint 3) ---
app.get('/api/lots/queue', verifyToken, requireActiveTenant, requireRole(['manager', 'admin']), async (req, res) => {
  const tenant_id = req.user.tenant_id;
  const client = await pool.connect();

  try {
    // Get all lots that have arrived at the gate but haven't been crushed yet
    const query = `
      SELECT * FROM lots 
      WHERE tenant_id = $1 
        AND gate_arrival_time IS NOT NULL 
        AND crushed_time IS NULL;
    `;
    const result = await client.query(query, [tenant_id]);
    let lots = result.rows;

    // Fetch the weather logs for this tenant (last 72 hours should be enough)
    const weatherQuery = `
      SELECT recorded_at, temp_celsius 
      FROM weather_logs 
      WHERE tenant_id = $1 
        AND recorded_at >= NOW() - INTERVAL '72 hours'
      ORDER BY recorded_at ASC;
    `;
    const weatherResult = await client.query(weatherQuery, [tenant_id]);
    const allWeather = weatherResult.rows;

    // Calculate live DI for each lot
    lots = lots.map(lot => {
      // Filter weather logs that occurred AFTER the cut time
      const relevantWeather = allWeather.filter(w => 
        new Date(w.recorded_at) >= new Date(lot.cut_start_time)
      );
      
      const di = calculateDI(lot, relevantWeather);
      
      // Calculate simple hours elapsed for display
      const hoursSinceCut = lot.cut_start_time 
        ? ((new Date() - new Date(lot.cut_start_time)) / (1000 * 60 * 60)).toFixed(1)
        : 'N/A';

      return { ...lot, live_di: di, hours_since_cut: hoursSinceCut };
    });

    // Sort by DI descending (highest priority first)
    lots.sort((a, b) => b.live_di - a.live_di);

    res.status(200).json(lots);
  } catch (error) {
    console.error('Queue Error:', error);
    res.status(500).json({ error: 'Failed to fetch queue' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: MARK LOT AS CRUSHED (Touchpoint 3 Action) ---
app.post('/api/lots/crush', verifyToken, requireRole(['manager', 'admin']), async (req, res) => {
  const { lot_id, final_di } = req.body;
  const tenant_id = req.user.tenant_id;

  const client = await pool.connect();

  try {
    const updateQuery = `
      UPDATE lots
      SET 
        crushed_time = CURRENT_TIMESTAMP,
        final_di = $1
      WHERE id = $2 AND tenant_id = $3
      RETURNING id, parcha_no, crushed_time, final_di;
    `;

    const result = await client.query(updateQuery, [final_di, lot_id, tenant_id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Lot not found or not authorized.' });
    }

    res.status(200).json({ message: 'Lot crushed successfully', lot: result.rows[0] });
  } catch (error) {
    console.error('Crush Error:', error);
    res.status(500).json({ error: 'Database error marking lot as crushed' });
  } finally {
    client.release();
  }
});

// --- ADMIN: GET TENANT SETTINGS ---
app.get('/api/tenants/settings', verifyToken, requireRole(['admin']), async (req, res) => {
  const tenant_id = req.user.tenant_id;
  const client = await pool.connect();
  
  try {
    const query = `
      SELECT name, address, lat, lng, identifier_label, default_language 
      FROM tenants 
      WHERE id = $1
    `;
    const result = await client.query(query, [tenant_id]);
    res.status(200).json(result.rows[0] || {});
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch settings' });
  } finally {
    client.release();
  }
});

// --- ADMIN: UPDATE TENANT SETTINGS (UPDATED) ---
app.put('/api/tenants/settings', verifyToken, requireRole(['admin']), async (req, res) => {
  const { name, address, lat, lng, identifier_label, default_language } = req.body;
  const tenant_id = req.user.tenant_id;
  const client = await pool.connect();

  try {
    const updateQuery = `
      UPDATE tenants 
      SET name = COALESCE($1, name), 
          address = COALESCE($2, address), 
          lat = COALESCE($3, lat), 
          lng = COALESCE($4, lng),
          identifier_label = COALESCE($5, identifier_label),
          default_language = COALESCE($6, default_language)
      WHERE id = $7 RETURNING *;
    `;
    const result = await client.query(updateQuery, [
      name, address, lat, lng, identifier_label, default_language, tenant_id
    ]);
    
    res.status(200).json({ message: 'Settings updated', tenant: result.rows[0] });
  } catch (error) {
    console.error('Tenant Update Error:', error);
    res.status(500).json({ error: 'Failed to update tenant settings' });
  } finally {
    client.release();
  }
});

// --- ADMIN: GET ALL USERS ---
app.get('/api/users', verifyToken, requireRole(['admin']), async (req, res) => {
  const tenant_id = req.user.tenant_id;
  const client = await pool.connect();
  try {
    const result = await client.query('SELECT firebase_uid, name, role, default_harvest_method, default_burn_status FROM users WHERE tenant_id = $1', [tenant_id]);
    res.status(200).json(result.rows);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch users' });
  } finally {
    client.release();
  }
});

// --- ADMIN: CREATE USER (Updated with Defaults) ---
// Overwrite your previous /api/users POST route with this one
app.post('/api/users', verifyToken, requireRole(['admin']), async (req, res) => {
  const { email, password, name, role, harvest_method, burn_status } = req.body;
  const tenant_id = req.user.tenant_id;
  
  try {
    const userRecord = await auth.createUser({ email, password, displayName: name });
    await auth.setCustomUserClaims(userRecord.uid, { 
      tenant_id, 
      role,
      tenant_status: 'active' // Inject the default active status
    });

    const query = `
      INSERT INTO users (firebase_uid, tenant_id, role, name, default_harvest_method, default_burn_status)
      VALUES ($1, $2, $3, $4, $5, $6) RETURNING *;
    `;
    const defaults = role === 'field_user' ? [harvest_method || 'manual', burn_status || false] : [null, null];
    
    const result = await pool.query(query, [userRecord.uid, tenant_id, role, name, ...defaults]);
    res.status(201).json({ message: 'User created', user: result.rows[0] });
  } catch (error) {
    res.status(500).json({ error: 'Failed to create user' });
  }
});

// --- ENDPOINT: TOGGLE TENANT STATUS (Super Admin Only) ---
app.patch('/api/tenants/:id/status', verifySuperAdmin, async (req, res) => {
  const { status } = req.body; // 'active' or 'paused'
  const tenant_id = req.params.id;
  const client = await pool.connect();
  
  try {
    // 1. Update the PostgreSQL database
    await client.query('UPDATE tenants SET subscription_status = $1 WHERE id = $2', [status, tenant_id]);

    // 2. Fetch all users belonging to this tenant
    const usersResult = await client.query('SELECT firebase_uid, role FROM users WHERE tenant_id = $1', [tenant_id]);
    
    // 3. Update Firebase Claims for every user
    const updatePromises = usersResult.rows.map(async (user) => {
      // Re-apply their tenant_id and role, but overwrite the status
      await auth.setCustomUserClaims(user.firebase_uid, {
        tenant_id: tenant_id,
        role: user.role,
        tenant_status: status
      });

      // 4. Force immediate logout! 
      // If paused, revoke their current active session so they can't keep using the app
      if (status === 'paused') {
        await auth.revokeRefreshTokens(user.firebase_uid);
      }
    });

    await Promise.all(updatePromises);

    res.status(200).json({ 
      message: `Tenant ${tenant_id} changed to ${status}. ${usersResult.rowCount} users updated.` 
    });

  } catch (error) {
    console.error('Pause Error:', error);
    res.status(500).json({ error: 'Failed to update tenant status across DB and Firebase' });
  } finally {
    client.release();
  }
});

// --- ADMIN: DELETE USER ---
app.delete('/api/users/:uid', verifyToken, requireRole(['admin']), async (req, res) => {
  const targetUid = req.params.uid;
  const tenant_id = req.user.tenant_id;

  // Prevent self-deletion (The Safety Rail)
  if (targetUid === req.user.uid) {
    return res.status(400).json({ error: 'Action denied: You cannot delete your own admin account.' });
  }

  const client = await pool.connect();

  try {
    // 1. Verify user belongs to this tenant before deleting
    const checkQuery = await client.query('SELECT firebase_uid FROM users WHERE firebase_uid = $1 AND tenant_id = $2', [targetUid, tenant_id]);
    if (checkQuery.rowCount === 0) return res.status(403).json({ error: 'Unauthorized to delete this user' });

    // 2. Delete from Firebase
    await auth.deleteUser(targetUid);

    // 3. Delete from Neon
    await client.query('DELETE FROM users WHERE firebase_uid = $1', [targetUid]);
    res.status(200).json({ message: 'User deleted successfully' });
  } catch (error) {
    console.error('Delete Error:', error);
    res.status(500).json({ error: 'Failed to delete user' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: GET CURRENT TENANT INFO ---
app.get('/api/tenants/me', verifyToken, async (req, res) => {
  const tenant_id = req.user.tenant_id;
  const client = await pool.connect();
  try {
    const result = await client.query('SELECT name FROM tenants WHERE id = $1', [tenant_id]);
    if (result.rows.length > 0) {
      res.status(200).json(result.rows[0]);
    } else {
      res.status(404).json({ error: 'Tenant not found' });
    }
  } catch (error) {
    console.error('Tenant fetch error:', error);
    res.status(500).json({ error: 'Failed to fetch tenant details' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: GET TENANT CSV MAPPING (Admin & Manager) ---
app.get('/api/tenants/mapping', verifyToken, requireRole(['admin', 'manager']), async (req, res) => {
  const tenant_id = req.user.tenant_id;
  const client = await pool.connect();
  try {
    const result = await client.query('SELECT csv_mapping FROM tenants WHERE id = $1', [tenant_id]);
    res.status(200).json(result.rows[0]?.csv_mapping || {});
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch mapping' });
  } finally {
    client.release();
  }
});

// --- ENDPOINT: UPDATE TENANT CSV MAPPING (Admin Only) ---
app.put('/api/tenants/mapping', verifyToken, requireRole(['admin']), async (req, res) => {
  const { mapping } = req.body;
  const tenant_id = req.user.tenant_id;
  const client = await pool.connect();
  try {
    const updateQuery = `
      UPDATE tenants SET csv_mapping = $1 WHERE id = $2 RETURNING csv_mapping;
    `;
    const result = await client.query(updateQuery, [mapping, tenant_id]);
    res.status(200).json({ message: 'Mapping updated', mapping: result.rows[0].csv_mapping });
  } catch (error) {
    res.status(500).json({ error: 'Failed to update mapping' });
  } finally {
    client.release();
  }
});

// --- EXPORT: DAILY CRUSHING LOG ---
app.get('/api/exports/crushing-log', verifyToken, requireRole(['admin', 'manager']), async (req, res) => {
  const tenant_id = req.user.tenant_id;
  const { start_date, end_date } = req.query;

  const start = new Date(start_date);
  const end = new Date(end_date);
  const diffDays = (end - start) / (1000 * 60 * 60 * 24);

  if (diffDays > 31 || diffDays < 0) {
    return res.status(400).json({ error: 'Date range must be between 0 and 31 days.' });
  }

  const client = await pool.connect();
  try {
    // Set headers to force file download
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="Crushing_Log_${start_date}_to_${end_date}.csv"`);

    const query = new QueryStream(`
      SELECT 
        parcha_no as "Parcha Number",
        farmer_name as "Farmer Name",
        variety_code as "Variety",
        harvest_method as "Harvest Method",
        net_weight as "Net Weight (MT)",
        zone as "Yard Zone",
        cut_start_time as "Cut Time",
        gate_arrival_time as "Gate Arrival",
        crushed_time as "Crushed Time",
        ROUND(EXTRACT(EPOCH FROM (crushed_time - cut_start_time))/3600) as "Total Age (Hours)",
        final_di as "Final Deterioration Index"
      FROM lots
      WHERE tenant_id = $1 
        AND crushed_time IS NOT NULL
        AND crushed_time >= $2 
        AND crushed_time <= $3::date + interval '1 day'
      ORDER BY crushed_time DESC;
    `, [tenant_id, start_date, end_date]);

    const stream = client.query(query);
    
    // Pipe DB stream -> CSV Formatter -> HTTP Response
    stream.pipe(fastcsv.format({ headers: true })).pipe(res);

    stream.on('end', () => client.release());
    stream.on('error', (err) => {
      console.error('Stream error:', err);
      client.release();
      res.status(500).end();
    });

  } catch (error) {
    client.release();
    console.error('Export Error:', error);
    res.status(500).json({ error: 'Failed to generate export' });
  }
});

// --- EXPORT: FRESHNESS LEADERBOARD ---
app.get('/api/exports/leaderboard', verifyToken, requireRole(['admin', 'manager']), async (req, res) => {
  const tenant_id = req.user.tenant_id;
  const { start_date, end_date } = req.query;

  const start = new Date(start_date);
  const end = new Date(end_date);
  const diffDays = (end - start) / (1000 * 60 * 60 * 24);

  if (diffDays > 31 || diffDays < 0) {
    return res.status(400).json({ error: 'Date range must be between 0 and 31 days.' });
  }

  const client = await pool.connect();
  try {
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="Leaderboard_${start_date}_to_${end_date}.csv"`);

    // Aggregate query grouping by Harvest Method to show performance
    const query = new QueryStream(`
      SELECT 
        harvest_method as "Harvest Method",
        COUNT(id) as "Total Lots Delivered",
        SUM(net_weight) as "Total Weight (MT)",
        ROUND(AVG(EXTRACT(EPOCH FROM (gate_arrival_time - cut_start_time))/3600)::numeric, 1) as "Avg Transit Time (Hrs)",
        ROUND(AVG(EXTRACT(EPOCH FROM (crushed_time - gate_arrival_time))/3600)::numeric, 1) as "Avg Yard Wait (Hrs)",
        ROUND(AVG(final_di)::numeric, 2) as "Average Final DI"
      FROM lots
      WHERE tenant_id = $1 
        AND crushed_time IS NOT NULL
        AND crushed_time >= $2 
        AND crushed_time <= $3::date + interval '1 day'
      GROUP BY harvest_method
      ORDER BY "Average Final DI" ASC;
    `, [tenant_id, start_date, end_date]);

    const stream = client.query(query);
    stream.pipe(fastcsv.format({ headers: true })).pipe(res);
    stream.on('end', () => client.release());

  } catch (error) {
    client.release();
    res.status(500).json({ error: 'Failed to generate leaderboard' });
  }
});

// --- ENDPOINT: GET TENANT PROFILE ---
app.get('/api/tenants/profile', verifyToken, async (req, res) => {
  const tenant_id = req.user.tenant_id;
  const client = await pool.connect();
  
  try {
    // Added identifier_label and default_language to the SELECT statement
    const query = 'SELECT name, subscription_tier, identifier_label, default_language FROM tenants WHERE id = $1';
    const result = await client.query(query, [tenant_id]);
    
    if (result.rowCount === 0) return res.status(404).json({ error: 'Tenant not found' });
    
    res.status(200).json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch tenant profile' });
  } finally {
    client.release();
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`CutClock backend running on port ${PORT}`));

// A simplified mapping of varieties to their baseline deterioration factors
const getVarietyFactor = (varietyCode) => {
  const code = varietyCode.toUpperCase();
  // Tier 1 (Low deterioration)
  if (['COC-671', 'CO-86032', 'CO-94012'].includes(code)) return 0.85;
  // Tier 3 (High deterioration)
  if (['COM-0265'].includes(code)) return 1.25;
  // Tier 2 (Baseline)
  return 1.0; 
};

/**
 * Calculates the Deterioration Index for a single lot.
 * @param {Object} lot - The lot database record
 * @param {Array} weatherLogs - Array of hourly temperature records since cut_time
 */
const calculateDI = (lot, weatherLogs) => {
  // --- GAP 1 FIX: The 6:00 AM Fallback ---
  // We no longer immediately return 0 if cut_start_time is missing.
  let activeStartTime = lot.cut_start_time;
  
  if (!activeStartTime) {
    // If no field log exists, assume it was cut at 6:00 AM on the day the CSV was uploaded
    const fallbackDate = new Date(lot.created_at || Date.now());
    fallbackDate.setHours(6, 0, 0, 0); // Set to 6:00 AM local time
    
    // If the fallback time is somehow in the future (e.g., CSV uploaded at 5:00 AM), cap it to "now"
    activeStartTime = fallbackDate > new Date() ? new Date() : fallbackDate;
  }

  // 1. Calculate EXACT hours elapsed using the active start time
  const hoursElapsed = (new Date() - new Date(activeStartTime)) / (1000 * 60 * 60);
  
  // Safety check: if time is mathematically negative or zero, DI is 0
  if (hoursElapsed <= 0) return 0;

  let t_thermal = 0;

  if (weatherLogs.length === 0) {
     // Fallback: If no weather data exists yet, assume baseline 30°C (rate = 1.0)
     t_thermal = hoursElapsed; 
  } else {
    // 2. Calculate the average thermal rate based on KNOWN weather logs
    let knownThermalSum = 0;
    for (const log of weatherLogs) {
      knownThermalSum += Math.exp(0.069 * (log.temp_celsius - 30));
    }
    const averageThermalRate = knownThermalSum / weatherLogs.length;

    // 3. Apply the average rate to the TOTAL hours elapsed
    // This scales the DI accurately regardless of how many logs exist in the DB
    t_thermal = averageThermalRate * hoursElapsed;
  }

  // 4. Apply Base Factors
  const harvest_factor = lot.harvest_method?.toLowerCase() === 'mechanical' ? 1.9 : 1.0;
  const burn_factor = lot.burn_status === true ? 1.4 : 1.0;
  let variety_factor = getVarietyFactor(lot.variety_code || '');
  
  // --- GAP 3 FIX: The Ratoon Penalty ---
  // If the lot is marked as a ratoon crop, add the 0.15 penalty to its base variety factor
  if (lot.is_ratoon) {
    variety_factor += 0.15;
  }

  // Note: Skipping rain_factor for MVP simplicity unless you want it hardcoded

  // 5. Final DI Calculation
  const rawDI = t_thermal * harvest_factor * burn_factor * variety_factor;
  
  // Normalize or cap if desired, returning 2 decimal places
  return parseFloat(rawDI.toFixed(2));
};

module.exports = { calculateDI };

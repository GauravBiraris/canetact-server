// cutclock-backend/utils/diCalculator.js

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
  // If we don't have a cut time yet, DI is effectively 0 (or unknown)
  if (!lot.cut_start_time) return 0;

// 1. Calculate EXACT hours elapsed
  const hoursElapsed = (new Date() - new Date(lot.cut_start_time)) / (1000 * 60 * 60);
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

  // 2. Apply Factors
  const harvest_factor = lot.harvest_method?.toLowerCase() === 'mechanical' ? 1.9 : 1.0;
  const burn_factor = lot.burn_status === true ? 1.4 : 1.0;
  const variety_factor = getVarietyFactor(lot.variety_code || '');
  
  // Note: Skipping rain_factor for MVP simplicity unless you want it hardcoded

  // 3. Final DI Calculation
  const rawDI = t_thermal * harvest_factor * burn_factor * variety_factor;
  
  // Normalize or cap if desired, returning 2 decimal places
  return parseFloat(rawDI.toFixed(2));
};

module.exports = { calculateDI };
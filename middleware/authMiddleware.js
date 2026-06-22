const { auth } = require('../firebase');

const verifyToken = async (req, res, next) => {
  const token = req.headers.authorization?.split('Bearer ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Unauthorized: No token provided' });
  }

  try {
    const decodedToken = await auth.verifyIdToken(token);
    // Attach the decoded user object (including custom claims like tenant_id and role) to the request
    req.user = decodedToken; 
    next();
  } catch (error) {
    console.error('Error verifying token:', error);
    return res.status(403).json({ error: 'Unauthorized: Invalid token' });
  }
};

// Optional helper to restrict routes by role
const requireRole = (allowedRoles) => {
  return (req, res, next) => {
    if (!req.user || !allowedRoles.includes(req.user.role)) {
      return res.status(403).json({ error: 'Forbidden: Insufficient permissions' });
    }
    next();
  };
};

const requireActiveTenant = (req, res, next) => {
  // We read the status directly from the Firebase token in memory
  if (req.user.tenant_status === 'paused') {
    return res.status(403).json({ 
      error: 'Account Suspended', 
      message: 'Your mill\'s subscription is currently inactive. Please contact CutClock support.' 
    });
  }
  next();
};

module.exports = { verifyToken, requireRole, requireActiveTenant };

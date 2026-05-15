const admin = require('firebase-admin');

// Load the service account key
// Ensure this file is in your .gitignore!
const serviceAccount = require('./firebase-service-account.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const auth = admin.auth();

module.exports = { admin, auth };
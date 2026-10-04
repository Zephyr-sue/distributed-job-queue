const mongoose = require('mongoose');
const config = require('../config');

let isConnected = false;
let isUsingFallback = false;

async function connectDB() {
  if (isConnected) return;

  try {
    mongoose.set('strictQuery', false);
    await mongoose.connect(config.mongo.uri, {
      serverSelectionTimeoutMS: 2000,
    });
    isConnected = true;
    isUsingFallback = false;
    console.log(`[MongoDB] Connected successfully to ${config.mongo.uri}`);
  } catch (err) {
    if (config.enableInMemoryFallback) {
      console.log(`[MongoDB] External MongoDB unavailable (${err.message}). Using In-Memory Document Store fallback.`);
      isConnected = true;
      isUsingFallback = true;
    } else {
      console.error('[MongoDB] Connection error:', err.message);
      throw err;
    }
  }
}

function isFallbackMode() {
  return mongoose.connection.readyState !== 1;
}

module.exports = {
  connectDB,
  isFallbackMode,
  mongoose,
};

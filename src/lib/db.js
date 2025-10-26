/**
 * src/lib/db.js
 * Factory za MongoClient.
 * Čita MONGO_URI i DB_NAME iz .env.
 * Koristi se za sve konekcije u projektu.
 */
import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';
dotenv.config();

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME;

if (!MONGO_URI || !DB_NAME) {
  throw new Error('MONGO_URI i DB_NAME moraju biti definisani u .env fajlu');
}

let client;

export async function getDb() {
  if (!client) {
    client = new MongoClient(MONGO_URI, { useUnifiedTopology: true });
    await client.connect();
  }
  return client.db(DB_NAME);
}

export async function closeDb() {
  if (client) {
    await client.close();
    client = null;
  }
}

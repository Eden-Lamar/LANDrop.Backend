import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

// Ensure a data directory exists to hold the database file
const dataDir = path.join(process.cwd(), 'data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir);
}

// Initialize the database (creates landrop.db file if it doesn't exist)
const db = new Database(path.join(dataDir, 'landrop.db'));

// Enable Write-Ahead Logging for better performance
db.pragma('journal_mode = WAL');
// Enable foreign key constraints to maintain referential integrity
db.pragma('foreign_keys = ON');

// Create our tables
db.exec(`
  CREATE TABLE IF NOT EXISTS devices (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    ip_address TEXT NOT NULL,
    last_seen DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS transfers (
    id TEXT PRIMARY KEY,
    sender_id TEXT NOT NULL,
    receiver_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    size INTEGER NOT NULL,
    status TEXT NOT NULL,
    expected_hash TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sender_id) REFERENCES devices(id),
    FOREIGN KEY (receiver_id) REFERENCES devices(id)
  );
`);

console.log('✅ SQLite Database initialized');

export default db;
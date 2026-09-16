const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");
const { pipeline } = require("node:stream/promises");
const { Readable } = require("node:stream");
function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

async function main() {
  const databaseUrl = required("FIREBASE_DATABASE_URL").replace(/\/$/, "");
  const response = await fetch(`${databaseUrl}/.json`, {
    headers: { Accept: "application/json" },
  });

  if (!response.ok || !response.body) {
    const detail = await response.text().catch(() => "");
    throw new Error(`Firebase export failed with HTTP ${response.status}: ${detail.slice(0, 1000)}`);
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const filename = path.resolve(`realtime-db-backup-${timestamp}.json`);
  const output = fs.createWriteStream(filename, { flags: "wx" });

  try {
    await pipeline(Readable.fromWeb(response.body), output);
  } catch (error) {
    await fsp.rm(filename, { force: true });
    throw error;
  }

  const stat = await fsp.stat(filename);
  if (stat.size === 0) throw new Error("Firebase export produced an empty file");
  console.log(`BACKUP_FILE=${filename}`);
  console.log(`BACKUP_BYTES=${stat.size}`);
}

main().catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});

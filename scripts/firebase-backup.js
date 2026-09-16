const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");
const { once } = require("node:events");

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

function firebasePath(pathParts) {
  const encoded = pathParts.map((part) => encodeURIComponent(String(part))).join("/");
  return `/${encoded}.json`;
}

async function request(databaseUrl, pathParts, shallow = false) {
  const suffix = shallow ? "?shallow=true" : "";
  const url = `${databaseUrl}${firebasePath(pathParts)}${suffix}`;
  let lastError;

  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      return await fetch(url, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(60_000),
      });
    } catch (error) {
      lastError = error;
      if (attempt === 4) break;
      await new Promise((resolve) => setTimeout(resolve, 2 ** attempt * 1000));
    }
  }

  const cause = lastError?.cause?.message || lastError?.message || String(lastError);
  throw new Error(`Network request failed for ${url}: ${cause}`, { cause: lastError });
}

async function writeChunk(output, chunk) {
  if (output.write(chunk)) return;
  await once(output, "drain");
}

async function streamResponse(output, response) {
  if (!response.body) throw new Error("Firebase returned an empty response body");
  for await (const chunk of response.body) {
    await writeChunk(output, chunk);
  }
}

async function responseError(response) {
  const detail = await response.text().catch(() => "");
  return `Firebase export failed with HTTP ${response.status}: ${detail.slice(0, 1000)}`;
}

function isArrayKeys(keys) {
  return keys.length > 0 && keys.every((key, index) => String(index) === key);
}

async function streamNode(databaseUrl, pathParts, output) {
  const full = await request(databaseUrl, pathParts);

  if (full.ok) {
    await streamResponse(output, full);
    return;
  }

  if (full.status !== 413) throw new Error(await responseError(full));
  if (full.body) await full.body.cancel().catch(() => {});

  // The node is too large for one REST response. Shallow-read its keys and
  // recursively stream each child into the same valid JSON document.
  const shallow = await request(databaseUrl, pathParts, true);
  if (!shallow.ok) throw new Error(await responseError(shallow));
  const children = await shallow.json();
  if (!children || typeof children !== "object" || Array.isArray(children)) {
    throw new Error(`Node ${pathParts.join("/") || "/"} exceeded the response limit but cannot be split`);
  }

  const keys = Object.keys(children);
  const array = isArrayKeys(keys);
  await writeChunk(output, array ? "[" : "{");

  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (index > 0) await writeChunk(output, ",");
    if (!array) await writeChunk(output, JSON.stringify(key) + ":");
    await streamNode(databaseUrl, [...pathParts, key], output);
  }

  await writeChunk(output, array ? "]" : "}");
}

async function main() {
  const databaseUrl = required("FIREBASE_DATABASE_URL").replace(/\/$/, "");
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const filename = path.resolve(`realtime-db-backup-${timestamp}.json`);
  const output = fs.createWriteStream(filename, { flags: "wx" });

  try {
    await streamNode(databaseUrl, [], output);
    output.end();
    await once(output, "close");
  } catch (error) {
    output.destroy();
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

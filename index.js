require('dotenv').config();
const mysql = require('mysql2/promise');
const axios = require('axios');
const cron = require('node-cron');

const {
  THINGER_USER,
  THINGER_TOKEN,
  THINGER_BUCKET,
  MYSQL_HOST,
  MYSQL_PORT,
  MYSQL_DATABASE,
  MYSQL_USER,
  MYSQL_PASSWORD,
  SYNC_INTERVAL_CRON
} = process.env;

// Valida que existan las variables de entorno mínimas
const required = [
  'THINGER_USER', 'THINGER_TOKEN', 'THINGER_BUCKET',
  'MYSQL_HOST', 'MYSQL_DATABASE', 'MYSQL_USER', 'MYSQL_PASSWORD'
];
for (const key of required) {
  if (!process.env[key]) {
    console.error(`[startup] Falta la variable de entorno ${key}`);
    process.exit(1);
  }
}

const THINGER_API_URL = `https://backend.thinger.io/v1/users/${THINGER_USER}/buckets/${THINGER_BUCKET}/data`;

let pool;

async function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: MYSQL_HOST,
      port: MYSQL_PORT || 3306,
      database: MYSQL_DATABASE,
      user: MYSQL_USER,
      password: MYSQL_PASSWORD,
      waitForConnections: true,
      connectionLimit: 5
    });
  }
  return pool;
}

async function ensureTables(conn) {
  await conn.query(`
    CREATE TABLE IF NOT EXISTS variables_meteorologicas (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      ts BIGINT NOT NULL,
      fecha DATETIME NOT NULL,
      direccion DOUBLE,
      humedad DOUBLE,
      lluvia DOUBLE,
      luz DOUBLE,
      presion DOUBLE,
      temperatura DOUBLE,
      velocidad DOUBLE,
      UNIQUE KEY uniq_ts (ts)
    )
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS sync_state (
      bucket_name VARCHAR(255) PRIMARY KEY,
      last_ts BIGINT NOT NULL
    )
  `);
}

async function getLastTs(conn) {
  const [rows] = await conn.query(
    'SELECT last_ts FROM sync_state WHERE bucket_name = ?',
    [THINGER_BUCKET]
  );
  return rows.length ? Number(rows[0].last_ts) : 0;
}

async function setLastTs(conn, ts) {
  await conn.query(
    `INSERT INTO sync_state (bucket_name, last_ts) VALUES (?, ?)
     ON DUPLICATE KEY UPDATE last_ts = VALUES(last_ts)`,
    [THINGER_BUCKET, ts]
  );
}

async function fetchNewData(sinceTs) {
  const params = { items: 1000 };
  if (sinceTs) {
    params.date_start = new Date(sinceTs + 1).toISOString();
  }
  const { data } = await axios.get(THINGER_API_URL, {
    headers: { Authorization: `Bearer ${THINGER_TOKEN}` },
    params
  });
  return Array.isArray(data) ? data : [];
}

// Thinger a veces entrega los valores anidados dentro de "val" en lugar de
// planos junto al "ts". Esta función busca el campo en ambos lugares, y sin
// importar mayúsculas/minúsculas.
function getField(record, fieldName) {
  const payload = record.val || record;
  if (payload[fieldName] !== undefined) return payload[fieldName];
  const lower = fieldName.toLowerCase();
  for (const key of Object.keys(payload)) {
    if (key.toLowerCase() === lower) return payload[key];
  }
  return null;
}

// Convierte un timestamp (ms, UTC) a un string "YYYY-MM-DD HH:MM:SS" en hora
// del centro de México (UTC-6, sin horario de verano desde 2022).
function toMexicoDateTime(tsMillis) {
  const mx = new Date(tsMillis - 6 * 60 * 60 * 1000);
  const pad = n => String(n).padStart(2, '0');
  return `${mx.getUTCFullYear()}-${pad(mx.getUTCMonth() + 1)}-${pad(mx.getUTCDate())} ${pad(mx.getUTCHours())}:${pad(mx.getUTCMinutes())}:${pad(mx.getUTCSeconds())}`;
}

async function syncOnce() {
  const conn = await getPool();
  await ensureTables(conn);
  const lastTs = await getLastTs(conn);

  console.log(`[sync] Buscando datos nuevos desde ts=${lastTs}...`);
  const records = await fetchNewData(lastTs);

  if (!records.length) {
    console.log('[sync] Sin datos nuevos.');
    return;
  }

  // Muestra el primer registro crudo la primera vez, útil para verificar
  // que los nombres de campo coinciden con los de tu bucket en Thinger.
  if (lastTs === 0) {
    console.log('[sync] Ejemplo de registro recibido:', JSON.stringify(records[0]));
  }

  let maxTs = lastTs;
  let inserted = 0;

  for (const r of records) {
    const ts = r.ts;
    if (!ts) continue;

    // 1. Usamos toMexicoDateTime para formatear la fecha a hora de México (UTC-6)
    const fechaMX = toMexicoDateTime(ts);

    // 2. Usamos getField para extraer los valores sin importar si vienen dentro de 'val'
    // o si vienen en mayúsculas/minúsculas en el bucket de Thinger
    await conn.query(
      `INSERT IGNORE INTO variables_meteorologicas
       (ts, fecha, direccion, humedad, lluvia, luz, presion, temperatura, velocidad)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        ts,
        fechaMX,
        getField(r, 'direccion'),
        getField(r, 'humedad'),
        getField(r, 'lluvia'),
        getField(r, 'luz'),
        getField(r, 'presion'),
        getField(r, 'temperatura'),
        getField(r, 'velocidad')
      ]
    );

    inserted++;
    if (ts > maxTs) maxTs = ts;
  }

  await setLastTs(conn, maxTs);
  console.log(`[sync] ${inserted} registro(s) procesado(s). Nuevo last_ts=${maxTs}`);
}

const scheduleExpr = SYNC_INTERVAL_CRON || '*/5 * * * *'; // cada 5 minutos por defecto

console.log(`[startup] Servicio iniciado. Sincronización programada: "${scheduleExpr}"`);

cron.schedule(scheduleExpr, () => {
  syncOnce().catch(err => console.error('[sync] Error:', err.message));
});

// Corre una vez de inmediato al arrancar, sin esperar al primer disparo del cron
syncOnce().catch(err => console.error('[sync] Error inicial:', err.message));
// Servidor HTTP simple para mantener la instancia activa en Clever Cloud
const http = require('http');
const PORT = process.env.PORT || 8080;

http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Servicio de sincronizacion Thinger-MySQL activo');
}).listen(PORT, () => {
  console.log(`[startup] Servidor HTTP escuchando en el puerto ${PORT}`);
});

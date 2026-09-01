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

function getField(record, fieldName) {
  const payload = record.val || record;
  if (payload[fieldName] !== undefined) return payload[fieldName];
  const lower = fieldName.toLowerCase();
  for (const key of Object.keys(payload)) {
    if (key.toLowerCase() === lower) return payload[key];
  }
  return null;
}

function toMexicoDateTime(tsMillis) {
  const date = new Date(Number(tsMillis));
  const formatter = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'America/Mexico_City',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
  return formatter.format(date).replace('T', ' ');
}

async function syncOnce() {
  const conn = await getPool();
  await ensureTables(conn);
  let lastTs = await getLastTs(conn);

  console.log(`[sync] Iniciando sincronización desde ts=${lastTs}...`);

  let keepFetching = true;
  let totalInserted = 0;

  while (keepFetching) {
    const params = { items: 1000, sort: 'asc' };
    
    // Se envía min_ts en milisegundos y date_start en formato ISO para máxima compatibilidad con Thinger
    if (lastTs > 0) {
      params.min_ts = lastTs + 1;
      params.date_start = new Date(lastTs + 1).toISOString();
    }

    let data;
    try {
      const res = await axios.get(THINGER_API_URL, {
        headers: { Authorization: `Bearer ${THINGER_TOKEN}` },
        params
      });
      data = res.data;
    } catch (err) {
      console.error('[sync] Error al consultar Thinger.io API:', err.message);
      break;
    }

    const chunk = Array.isArray(data) ? data : [];

    if (chunk.length === 0) {
      console.log('[sync] No hay más registros nuevos por recuperar.');
      keepFetching = false;
      break;
    }

    let batchMaxTs = lastTs;

   // En lugar de hacer conn.query dentro del for uno por uno,
// abre una transacción para procesar el lote completo:
await conn.beginTransaction();
try {
  for (const r of chunk) {
    const rawTs = r.ts || r.timestamp;
    if (!rawTs) continue;

    const ts = typeof rawTs === 'string' ? new Date(rawTs).getTime() : Number(rawTs);
    if (isNaN(ts)) continue;

    const fechaMX = toMexicoDateTime(ts);

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

    if (ts > batchMaxTs) batchMaxTs = ts;
  }
  await conn.commit();
} catch (err) {
  await conn.rollback();
  throw err;
}

    // Si el timestamp no avanzó, detenemos para evitar un bucle infinito en caso de respuesta repetida
    if (batchMaxTs <= lastTs) {
      console.log(`[sync] El timestamp final (${batchMaxTs}) no superó el anterior (${lastTs}). Finalizando bucle.`);
      keepFetching = false;
      break;
    }

    lastTs = batchMaxTs;
    await setLastTs(conn, lastTs);
    console.log(`[sync] Lote procesado. Nuevos registros en este bloque: ${chunk.length}. Total cargados en esta sesión: ${totalInserted}. Nuevo last_ts=${lastTs}`);

    if (chunk.length < 1000) {
      keepFetching = false;
    }
  }
}

const scheduleExpr = SYNC_INTERVAL_CRON || '*/5 * * * *';

console.log(`[startup] Servicio iniciado. Sincronización programada: "${scheduleExpr}"`);

cron.schedule(scheduleExpr, () => {
  syncOnce().catch(err => console.error('[sync] Error en cron:', err.message));
});

syncOnce().catch(err => console.error('[sync] Error inicial:', err.message));

const http = require('http');
const PORT = process.env.PORT || 8080;

http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Servicio de sincronizacion Thinger-MySQL activo');
}).listen(PORT, () => {
  console.log(`[startup] Servidor HTTP escuchando en el puerto ${PORT}`);
});

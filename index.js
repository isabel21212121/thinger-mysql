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

function getField(record, fieldName) {
  const payload = record.val || record;
  if (payload[fieldName] !== undefined) return payload[fieldName];
  const lower = fieldName.toLowerCase();
  for (const key of Object.keys(payload)) {
    if (key.toLowerCase() === lower) return payload[key];
  }
  return null;
}

// Formatea la fecha a hora de México respetando cambios de horario históricos
function toMexicoDateTime(tsMillis) {
  const date = new Date(tsMillis);
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

// Bucle continuo para paginar de 1000 en 1000 hasta descargar todo el historial
async function syncOnce() {
  const conn = await getPool();
  await ensureTables(conn);
  let lastTs = await getLastTs(conn);

  console.log(`[sync] Iniciando sincronización desde ts=${lastTs}...`);

  let keepFetching = true;
  let totalInserted = 0;

  while (keepFetching) {
    const params = { items: 1000, sort: 'asc' };
    if (lastTs > 0) {
      params.date_start = new Date(lastTs + 1).toISOString();
    }

    const { data } = await axios.get(THINGER_API_URL, {
      headers: { Authorization: `Bearer ${THINGER_TOKEN}` },
      params
    });

    const chunk = Array.isArray(data) ? data : [];

    if (chunk.length === 0) {
      console.log('[sync] No hay más registros históricos por recuperar.');
      keepFetching = false;
      break;
    }

    let batchMaxTs = lastTs;

    for (const r of chunk) {
      const ts = r.ts || r.timestamp;
      if (!ts) continue;

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

      totalInserted++;
      if (ts > batchMaxTs) batchMaxTs = ts;
    }

    // Detener el bucle si el lote no avanzó en marcas de tiempo o si el bloque es menor a 1000
    if (batchMaxTs === lastTs || chunk.length < 1000) {
      keepFetching = false;
    }

    lastTs = batchMaxTs;
    await setLastTs(conn, lastTs);
    console.log(`[sync] Lote procesado. Acumulado: ${totalInserted} registros. Último ts=${lastTs}`);
  }
}

const scheduleExpr = SYNC_INTERVAL_CRON || '*/5 * * * *';

console.log(`[startup] Servicio iniciado. Sincronización programada: "${scheduleExpr}"`);

cron.schedule(scheduleExpr, () => {
  syncOnce().catch(err => console.error('[sync] Error en cron:', err.message));
});

// Ejecución inmediata al arrancar el servidor
syncOnce().catch(err => console.error('[sync] Error inicial:', err.message));

// Servidor HTTP de soporte para Clever Cloud
const http = require('http');
const PORT = process.env.PORT || 8080;

http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Servicio de sincronizacion Thinger-MySQL activo');
}).listen(PORT, () => {
  console.log(`[startup] Servidor HTTP escuchando en el puerto ${PORT}`);
});

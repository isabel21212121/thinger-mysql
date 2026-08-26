# Thinger → MySQL Sync

Servicio que corre de forma continua y automática: cada cierto intervalo (5
minutos por defecto) consulta el bucket `Variables_Meteorologicas` en
Thinger.io y guarda los registros nuevos en tu base MySQL de Clever Cloud.
No requiere supervisión manual una vez desplegado.

## Qué hace

1. Al arrancar, crea (si no existen) dos tablas en tu MySQL:
   - `variables_meteorologicas`: los datos de tus sensores.
   - `sync_state`: guarda internamente hasta qué momento ya se sincronizó,
     para no duplicar ni perder registros aunque el servicio se reinicie.
2. Cada cierto intervalo, pide a la API de Thinger los datos nuevos desde
   la última sincronización y los inserta en MySQL.

## Configuración local (para probar antes de desplegar)

```bash
npm install
cp .env.example .env
# Edita .env y coloca tu THINGER_TOKEN y tu MYSQL_PASSWORD reales
npm start
```

Revisa la consola: al primer ciclo imprime un ejemplo del registro recibido
de Thinger (`[sync] Ejemplo de registro recibido: ...`). Verifica que los
nombres de campo (DIRECCION, HUMEDAD, etc.) coincidan con los que usa el
script — si tu bucket usa otros nombres o la respuesta viene en otro
formato, avísame para ajustar `index.js`.

## Desplegar en Clever Cloud (recomendado, todo en un solo lugar)

1. Sube este proyecto a un repositorio de Git (GitHub, GitLab, o el propio
   Git de Clever Cloud).
2. En el panel de Clever Cloud, crea una nueva aplicación → **Node.js**.
3. Conéctala a tu repositorio (o usa `git push clever main` si usas el Git
   de Clever Cloud directamente).
4. En **Environment variables** de esa aplicación, agrega las mismas
   variables que están en `.env.example`, con tus valores reales:
   - `THINGER_USER`, `THINGER_TOKEN`, `THINGER_BUCKET`
   - `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`,
     `MYSQL_PASSWORD` (puedes copiarlas del addon MySQL con el botón
     "Export Environment Variables" y solo ajustar los nombres si
     Clever Cloud las expone con otro prefijo, ej. `MYSQL_ADDON_HOST`).
   - `SYNC_INTERVAL_CRON` (opcional).
5. Vincula el addon MySQL a esta aplicación desde la sección de addons de
   Clever Cloud (así viven en el mismo entorno y la conexión es más
   directa/segura).
6. Despliega. El servicio queda corriendo indefinidamente, reiniciándose
   solo si Clever Cloud reinicia la app, y retomando la sincronización
   automáticamente gracias a la tabla `sync_state`.

## Notas de seguridad

- Nunca subas el archivo `.env` con tus credenciales reales a un
  repositorio público. Usa siempre las variables de entorno del panel de
  Clever Cloud para los valores reales.
- El token de Thinger que compartiste tiene permisos amplios (`Bucket: *`,
  `Device: *`). Si en el futuro quieres reducir el riesgo, puedes crear un
  token nuevo en Thinger con permisos limitados solo a
  `Bucket: Variables_Meteorologicas` y acción de solo lectura.

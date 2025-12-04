## Arquitectura general

- Sistema de análisis distribuido con colas y exchange en RabbitMQ, que procesa CSV del cliente y produce resultados en NDJSON.
- Componentes principales:
  - Cliente (`client/common/client.py`): envía CSV por tipo, recibe resultados y guarda `client-<id>.ndjson`.
  - Gateway (`gateway/common`): encola datos por tipo, enruta resultados al cliente, agrega EOF de consultas (controla réplicas).
  - Workers:
    - Filters/Aggregators/Joiners (`worker/*`): transforman, agregan y unen flujos por consulta (Q1–Q4).
    - Health monitor (`health_monitor`): escucha heartbeats.
  - Persistencia de estado por consulta (`pkg/storage/state_storage/*`): append-only, idempotente, recuperable.

## Flujo de datos (alto nivel)

1. El cliente obtiene un `request_id` del Gateway y envía CSV por tipo con `msg_num` monótono.
2. El Gateway publica en un exchange; las colas de cada worker se enlazan por tipo de mensaje.
3. Cada worker consume, acumula en memoria, y persiste snapshots periódicos.
4. Al completar entradas (EOF por tipo/replicas), el worker emite resultados y un EOF de consulta.
5. El Gateway reenvía al cliente un único EOF por consulta cuando corresponde (e.g. Q1 y Q4 con réplicas), y cierra la sesión.

## Foco en resiliencia a fallas

### Persistencia y recuperación

- Estado en disco por `request_id`:
  - Formatos específicos por consulta (TR/UB/SE para top-3, I/S para Q2, ST/SE/PC para Q4, etc.).
  - Append-only e idempotente: al cargar, se “suma” sobre estructuras (conteos, acumulados).
- Snapshots periódicos:
  - Cada N mensajes (`SNAPSHOT_INTERVAL`), se guarda y se “limpia” memoria de claves no críticas.
- Carga selectiva:
  - Al iniciar o en momentos clave (EOF), se carga estado persistido completo o parcial (p.ej., solo `last_by_sender`).
- Recuperación ante fallas:
  - Workers al arrancar llaman `load_state_all`/`load_specific_state_all` y continúan desde disco.
  - Diseño permite at-least-once: si llegan mensajes duplicados o reordenados, el modelo de acumulación lo tolera.

### Sharding y escalabilidad

- Réplicas por rol (competing consumers):
  - Q4 `top_three_clients` usa múltiples réplicas para paralelizar join y top-3, con `NODE_ID` y `msg_num` local.
  - Gateway agrega EOFs de réplicas (e.g., no forwardea EOF de Q1/Q4 hasta recibir todos).
- Enrutamiento por tipo:
  - Exchange de RabbitMQ enruta por tipo de mensaje a colas específicas (users, stores, transactions, etc.).
- Balanceo natural:
  - Varias instancias consumen de la misma cola; RabbitMQ distribuye mensajes.

### Chequeo de duplicados y orden

- Deduplicación por emisor y stream:
  - Clave: `stream:node_id.request_id` → `last_by_sender[key] = last_msg_num`.
  - Exact duplicates: se descartan (mismo `msg_num`).
  - Out-of-order: se aceptan para no perder contenido (evita divergencias entre clientes).
- Persistencia de dedup:
  - `last_by_sender` se persiste y se rehidrata al iniciar, previniendo reprocesos tras reinicios.
- Idempotencia de acumulación:
  - Acumuladores por clave (e.g., año-mes, tienda, usuario) se suman; al cargar, se agregan en lugar de sustituir.

### Coordinación de EOF

- Por worker (`Joiner`/agregadores):
  - `expected_eofs` por consulta: cada worker cuenta EOFs y sólo emite resultados/EOF al completar.
- Por Gateway (hacia el cliente):
  - Envía un único EOF de consulta al cliente tras recibir todos los EOFs de sus réplicas (p.ej., Q1/Q4).
- Limpieza de estado:
  - Al enviar EOF de consulta, se borra estado en memoria y en disco del `request_id`.

### Joins tolerantes a asincronía

- Q4: dos etapas robustas a reordenamientos:
  - `top_three_clients`: computa top-3 por tienda y usa `users_birthdates`; ante snapshots desfasados, mergea en EOF el estado persistido + el estado en RAM previo a emitir resultados intermedios.
  - `join_stores_q4`: si faltan `stores` para resolver nombres, guarda `pending_results`; al llegar EOF de `stores` o al cierre, “flushea” pendientes.
- Resultado: mismo contenido entre clientes, aunque varíe el orden de llegada de mensajes.

### Observabilidad y robustez operacional

- Heartbeats (`utils/heartbeat.py`) enviados por workers; `health_monitor` puede detectar caídas.
- Chaos Monkey (`chaos_monkey.py`): inyecta fallas/latencias para validar recuperación.
- Logging detallado contextual (request_id, msg_num, sender).

## Diseño por consulta (resumen)

- Q1: Pipeline distribuido con réplicas y agregación final coordinada por EOFs.
- Q2 (Max Quantity / Max Profit):
  - Agregador `QuantityAndProfit` acumula por `year_month_created_at` y `item_id`.
  - Persistencia incremental (`I;...` y `S;...`), idempotente al cargar.
  - En EOF (tras `expected_eofs`), calcula dos máximos y emite resultados.
- Q3:
  - Agregaciones por semestre y tienda; formatos de estado específicos para rehidratación.
- Q4:
  - `TopThreeClientsJoiner`: cuenta transacciones por usuario y tienda; top-3 por tienda con tie-break determinista local y merge de birthdates persistidas + en memoria.
  - `Q4StoresJoiner`: resuelve `store_id` a `store_name`, acumula pendientes hasta tener el diccionario de tiendas, y emite resultados finales.

## Patrones de resistencia usados

- Persistencia append-only por `request_id` con carga idempotente.
- Snapshots periódicos y “limpieza” selectiva de memoria.
- Deduplicación por emisor y stream, persistida; tolerancia a out-of-order.
- Coordinación de EOF por etapa y gating final en el Gateway.
- Competing consumers para escalado horizontal (sharding a nivel de cola).
- Joins con buffering de pendientes y merge de estados (persistido + memoria) para evitar pérdidas.
- Heartbeats y Chaos Monkey para monitoreo y pruebas de resiliencia.

## Trade-offs y límites conocidos

- Orden no determinista: el contenido es consistente entre clientes, pero el orden de líneas puede diferir (by design).
- At-least-once: pueden aparecer reentregas; la dedup exacta y la idempotencia de agregación mitigan.
- Ventana de memoria acotada: snapshot interval demasiado grande puede aumentar recuperación; demasiado pequeño aumenta I/O.

## Controles y parámetros

- `SNAPSHOT_INTERVAL` por worker (e.g., top-3).
- `TOP_THREE_CLIENTS_REPLICAS`, `NODE_ID` para Q4.
- `EXPECTED_EOFS` por agregador/joiner.
- Rutas y colas definidas por variables de entorno en `docker-compose-dev.yaml`.

## Validación

- Prueba de consistencia entre clientes (`test/test_reduced_results.py`):
  - Verifica que el conjunto de resultados esperado sea subconjunto del actual por consulta.
  - Luego de los ajustes (aceptar out-of-order y merge de estado en Q4), “missing” para Q4 debe ser 0 en ambos clientes.

## Guía rápida de lectura (para la demo)

- “Cómo nos recuperamos de fallas”: persistencia + carga idempotente + EOF coordinado.
- “Cómo evitamos datos perdidos/duplicados”: dedup exacto, aceptación de out-of-order, acumulación idempotente.
- “Cómo escalamos”: múltiples réplicas con competing consumers; Gateway coordina EOFs.
- “Cómo manejamos asincronía en joins”: pendientes + merge de estado (persistido y RAM) en el cierre.
- “Cómo monitoreamos y probamos”: heartbeats, health monitor, chaos monkey.
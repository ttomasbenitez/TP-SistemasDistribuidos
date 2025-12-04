## Evaluación contra la rúbrica

- Sistema funciona correctamente: Sí, con fixes recientes (Q4 determinismo de contenido, Q2 EOF counter). Orden de salida puede variar; el contenido debe coincidir.
- Resultados consistentes entre caídas (con y sin fallos): Sí, por persistencia append-only, carga idempotente y dedup persistido. Validar con Chaos Monkey.
- Resultados consistentes para clientes con mismos datos: Sí (post-fix Q4). Nota: orden no determinista, contenido igual.
- Correcto con cliente sin datos: Probable Sí (EOF fluye, workers emiten nada y EOF). Recomiendo test rápido dedicado.
- Demo preparada: Parcial Sí.
  - Datasets razonables: Sí (reducidos + Kaggle de prueba).
  - Estimación tiempos: Falta dejar explícita por corrida (agregar al README).
  - Herramientas validación: Sí (`test/test_reduced_results.py`).
  - Mostrar resultado en error: Sí (reporte de “missing” y métricas).
  - Simular fallas: Sí (`chaos_monkey.py`).
  - Configurable (nodos/frecuencia/whitelist): Parcial Sí (via env/flags). Dejar ejemplos de config.

### Escalabilidad y Tolerancia

- Escalable: Sí.
  - Competing consumers, réplicas (p.ej. Q4), enrutamiento por tipo, cliente/gateway multiproceso.
- Healthcheckers: Sí (heartbeats + health_monitor).
- Filtros/Acumuladores/Gateway/Cliente: Sí, desacoplados por colas.
- Tolera fallos:
  - Caídas simultáneas/continuas/intensivas: Parcial Sí (persistencia + rehidratación + at-least-once). Demostrable con Chaos Monkey ajustando ritmo de fallas.
- Todos los nodos toleran fallos: Parcial Sí.
  - Workers (filters/accumulators/joiners): Sí, estado idempotente.
  - Gateway: Sí (reintentos y gating de EOF por réplicas).
  - Cliente: Sí (id de request, recibe EOF final).
  - Healthcheckers: Sí (detección por heartbeat).
- Persiste y limpia estado por petición: Sí (append-only por request_id; `delete_state` tras EOF final).

### Diseño

- Arquitectura modificable: Sí (env vars, docker-compose, colas y exchanges configurables).
- Estado almacena correctamente:
  - Atómico: Parcial Sí (append + flush + fsync por snapshot; lock en memoria).
  - Eficiente por mensaje: Parcial Sí (batching + snapshots por intervalo).
  - Levanta eficientemente: Sí (parsers por línea y carga selectiva: `load_state_all`/`load_specific_state_all`).
- Recuperación de nodos:
  - Healthcheck/heartbeat: Sí (UDP/TCP via socket; se usa un sender periódico y monitor).
  - Implementación genérica de nodos: Sí (Worker base, Joiner base).
  - Soporta caídas sucesivas: Sí (persistencia y dedup rehidratado).
  - Exclusión mutua en sección crítica: Sí (locks en `StateStorage`).
  - Elección de líder/ring: No (no aplica; la coordinación se delega a RabbitMQ y al gating de EOF).

### Documentación

- Índice + Informe 4+1: Parcial (README presente; sugerido agregar 4+1 con vistas).
  - Escenarios (casos de uso): Incluir demo de caídas/recuperación.
  - Vista lógica: Diagramar flujos Q1–Q4 y colas.
  - Vista física: Topología de contenedores/colas y robustez.
  - Vista de desarrollo: Estructura de paquetes y responsabilidades.
- Explica tolerancia a fallos: Parcial Sí (README + este resumen).
  - Healthcheckers: Sí, documentar puerto/frecuencia.
  - Filtro de duplicados: Sí, explicar “exact duplicate drop + accept out-of-order”.
  - Persistencia: Sí, formatos por consulta y carga idempotente.

### Coding Best Practices

- Legible: Sí (logs contextuales, separación de responsabilidades).
- Bien abstraído: Sí (Worker/Joiner, StateStorage, Message).
- DRY: Parcial Sí (algunos patrones repetidos podrían factorizarse más, p.ej., helpers de merge/EOF gating).

## Mecanismos de resiliencia implementados

- Persistencia append-only por `request_id` (formatos TR/UB/SE, I/S, ST/SE/PC).
- Snapshots periódicos y limpieza selectiva de memoria.
- Deduplicación por emisor y stream persistida; se aceptan mensajes fuera de orden para no perder contenido.
- Coordinación de EOF por etapa y gating en Gateway para réplicas.
- Joins robustos a asincronía: buffering de pendientes y merge de estado (persistido + RAM) en EOF.
- Heartbeats y Chaos Monkey para monitoreo y pruebas de fallas.

## Gaps menores y recomendaciones

- Documentar tiempos de ejecución por dataset (pequeño/mediano) y parámetros que impactan (batch size, snapshot interval, réplicas).
- Añadir test explícito “cliente sin datos” y “fallas intensivas” con Chaos Monkey parametrizado.
- Incluir ejemplo de configuración de Chaos Monkey (nodos/frecuencia/whitelist) en README.
- Agregar sección 4+1 con diagramas y escenarios de falla/recuperación.
- Opcional: métrica/contador de reentregas por nodo para observabilidad.

## Checklist de demo (rápido)

- Dataset chico + estimación de duración.
- Ejecución normal: ambos clientes con mismos datos → contenido igual (orden puede variar).
- Cliente sin datos → 0 resultados y EOF final.
- Corrida con Chaos Monkey (frecuencia moderada) → consistencia tras caídas.
- Validación post-run con `test_reduced_results.py`.
- Mostrar logs de health monitor y detección de fallas.
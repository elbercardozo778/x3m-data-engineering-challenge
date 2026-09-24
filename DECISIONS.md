# DECISIONS

## 1. Decisiones técnicas y trade-offs

### Semántica de los datos

| Campo | Definición | Por qué |
|---|---|---|
| `date` | Fecha del snapshot (fecha lógica de la corrida, UTC; en runs manuales, `run_after`) | Los carts no traen fecha. Supuesto: los carts de un snapshot son las ventas de ese día |
| `revenue` | Suma de `discountedTotal` | Es lo cobrado. `gross_revenue` (precio × cantidad) queda como columna extra |
| `product_title` | Título del catálogo del mismo día | Si un producto se renombra, refleja el catálogo de esa fecha |
| Productos sin ventas | No tienen fila | Es una tabla de hechos de ventas |

Alternativa descartada: contar cada `cart_id` solo el día en que aparece. Depende de que los ids sean
estables entre días, algo que no se puede verificar con una API estática.

### Hallazgos de la fuente

- 12 carts repiten un producto en dos líneas: la clave de `stg_cart_lines` es
  `(snapshot_date, cart_id, line_number)`, no `(cart_id, product_id)`.
- Los montos son consistentes entre sí (línea, descuento, total del cart): quedaron como tests de dbt.

### Decisiones

| Decisión | Por qué | Trade-off |
|---|---|---|
| Postgres (bases `airflow` y `warehouse` en una instancia) | Airflow ya lo necesita; `jsonb` para la capa cruda; adapter de dbt maduro | No es un motor analítico; a esta escala no importa |
| Capas `raw` → `staging` → `consume` | Nombres de la convención dbt, alineados con el prefijo `stg_` | Equivale a bronze / silver / gold |
| `raw` guarda el JSON completo por `(snapshot_date, id)` | La API no tiene historia: se reprocesa desde `raw` | Ocupa más que columnas tipadas |
| `staging` como vistas, `consume` incremental | Staging no duplica datos; consume es lo que se consulta y agrega | Las vistas re-parsean el JSON al leerse; con volumen pasarían a incrementales |
| Carga idempotente: `DELETE` + `INSERT` por `snapshot_date` en una transacción | Re-ejecutar un día reemplaza, no duplica | — |
| `consume` incremental `delete+insert` por `date` | Cada corrida recalcula solo su día | — |
| `catchup=False`, schedule `15 0 * * *` UTC | Un backfill guardaría datos de hoy con fecha vieja; 15 min de margen tras la actualización | El pasado se reprocesa desde `raw` |
| `extract` rechaza fechas con más de 1 día de atraso | Un trigger manual con fecha pasada guardaría los datos de hoy con esa fecha; falla sin reintentar y dice qué hacer | El día de margen cubre arrancar el stack entre 00:00 y 00:15 UTC |
| Cliente HTTP propio en vez de dlt | Dos endpoints fijos, ~60 líneas testeables | Con muchas fuentes, dlt conviene |
| Control de calidad propio antes de `raw` | Liviano y configurable; Great Expectations o Soda son grandes para esto | Menos reglas disponibles |
| Reglas en JSON | Sin dependencias, sintaxis estricta | No admite comentarios |
| Airflow 3 + Cosmos | Una tarea `run` y otra `test` por modelo: el fallo se ve en el modelo exacto | Más tareas que un `dbt build` |
| Un solo DAG con grupos extract / control / load / transform | Una fuente, un horario: la fecha del snapshot es la misma en todas las etapas | Con varias fuentes, un DAG por fuente disparado por assets |
| Datos entre tareas por XCom (~300 KB por recurso) | Si falla `load`, se reintenta sin volver a llamar a la API | Con volumen, pasaría solo la ruta a S3 |
| dbt en un venv propio | Sus dependencias chocan con las constraints de Airflow | — |
| `dbt_utils` + 4 tests propios | Estándar para claves compuestas y rangos; los propios cubren lo que no existe (integridad dentro del mismo día, conciliación) | Una dependencia más, fijada con lock |
| LocalExecutor, código copiado en la imagen | Menos contenedores; sin problemas de permisos en Windows/Linux | Hay que reconstruir tras cada cambio |

### Calidad de datos

| Dónde | Qué valida |
|---|---|
| `control` (`data_quality/rules.json`) | Antes de `raw`: conteo contra la API, ids únicos, tipos, rangos, valores aceptados. `rule_accuracy` = registros que cumplen / chequeados; `file_accuracy` = reglas que cumplen / totales. Si el archivo no llega a su `file_accuracy`, no se carga. Ambos valen 1.0 por defecto |
| dbt, staging | Claves compuestas únicas, not null, rangos |
| dbt, cruzados | Montos de línea, total del cart = suma de líneas, productos vendidos existen en el catálogo del día |
| dbt, consume | Unicidad `(date, product_id)` y conciliación: revenue del día = suma de `discountedTotal` |

Única regla con `rule_accuracy` propio: `brand` en 0.4 (47 % del catálogo no tiene marca; mide 52,6 %).

### Observabilidad

- Logs con eventos de nombre fijo y campos `key=value`, en secciones plegables de la UI: partición procesada,
  páginas leídas, cada regla de calidad y filas reemplazadas.
- `raw.load_audit` (filas por carga) y `raw.dq_results` (accuracy por regla y archivo) por corrida.
- `on_failure_callback` en todas las tareas; hoy solo loguea.

### Desarrollo

24 tests de pytest y pre-commit con ruff, sqlfluff y pytest.

## 2. Flujo de trabajo con IA

Usé **Claude Code** para el analisis y la implementacion.

- **Decisiones mías** sobre sus propuestas: semántica de `date` y `revenue`, capas y sus nombres, un solo DAG
  con grupos ELT, no usar dlt, control de calidad con accuracy configurable en JSON para antes de iniciar el modelado, `dbt_utils` en vez de
  tests hechos a mano.
- **Validación en el entorno real**, que detectó bugs que el código solo no mostraba: build en paralelo de
  Compose, psycopg 3 en el provider de Postgres, runs manuales sin `logical_date` en Airflow 3.
- **Validación final** desde una copia limpia del repo siguiendo el README: 17/17 tareas, conciliación 0.00,
  sin duplicados al re-ejecutar, y dos pruebas negativas (una regla más estricta en la config corta la carga;
  un dato inválido en `raw` lo frenan los tests de dbt).

## 3. Alcance

### Afuera a propósito

- **CI:** los chequeos corren con pre-commit; llevarlos a GitHub Actions es un workflow.
- **Alertas:** el callback existe; falta el destino.
- **SCD2 del catálogo y deduplicación de carts entre días.**

### En producción (AWS + Databricks)

- **Raw en S3:** `extract` escribe el JSON en S3 particionado por `snapshot_date` y por XCom pasa solo la
  ruta. Auto Loader lo carga a tablas Delta bronze.
- **Unity Catalog:** capas `raw` / `staging` / `consume` como schemas de un catálogo, con permisos y linaje.
- **Transformación:** `dbt-databricks` sobre Delta, con Cosmos igual que hoy. `consume` incremental con
  `replace_where` por fecha.
- **Orquestación:** Airflow en MWAA con el provider de Databricks, o Databricks Workflows si todo vive ahí.
- **Calidad:** las reglas de `rules.json` como gate previo; resultados en una tabla Delta y chequeos de
  volumen contra el histórico con Lakehouse Monitoring.
- **Secretos e identidad:** AWS Secrets Manager como secrets backend de Airflow y roles IAM; nada de
  credenciales en archivos.
- **Monitoreo y alertas:** CloudWatch para métricas y logs, y el `on_failure_callback` publicando en
  SNS/Slack.
- **CI/CD:** GitHub Actions con pre-commit y `dbt build` contra un schema efímero; deploy con Databricks
  Asset Bundles y sync de DAGs al bucket de MWAA.

# DummyJSON → Postgres → `product_daily_revenue`

Pipeline batch con Airflow 3 que toma un snapshot diario de Products y Carts de
[DummyJSON](https://dummyjson.com), valida su calidad, lo carga crudo en Postgres y lo modela con dbt hasta
`consume.product_daily_revenue`. Decisiones y trade-offs en [DECISIONS.md](DECISIONS.md).

## Requisitos

- Docker con Compose v2 y al menos 4 GB de RAM.
- Puertos libres: `8080` (Airflow) y `5433` (Postgres).
- Internet (build de la imagen y API).

Funciona en macOS, Linux y Windows.

## Cómo ejecutarlo

```bash
# 1. Levantar todo. El DAG corre solo al iniciar.
docker compose up -d --build --wait

# 2. (Opcional) Disparar otra corrida
docker compose exec airflow-scheduler airflow dags trigger src_product_daily_revenue

# 3. Validar el resultado (~2 minutos después del paso 1)
docker compose exec postgres psql -U warehouse -d warehouse -f /sql/validate_product_daily_revenue.sql
```

UI de Airflow: <http://localhost:8080> (sin login). Para borrar todo: `docker compose down -v`.

## Query de validación

[`sql/validate_product_daily_revenue.sql`](sql/validate_product_daily_revenue.sql) muestra el resultado final:
todos los productos del último snapshot, ordenados por revenue.

```sql
select
    pdr.product_id,
    pdr.product_title,
    pdr.date,
    pdr.units_sold,
    pdr.revenue,
    pdr.gross_revenue
from consume.product_daily_revenue as pdr
where pdr.date = (select max(latest.date) from consume.product_daily_revenue as latest)
order by pdr.revenue desc, pdr.product_id asc;
```

Se ejecuta con el paso 3. También se puede correr desde cualquier cliente SQL (DBeaver, psql, etc.) con
`postgresql://warehouse:warehouse@localhost:5433/warehouse`.

Chequeos adicionales del pipeline (conciliación contra los carts crudos, auditoría de cargas y resultado de
las reglas de calidad):

```bash
docker compose exec postgres psql -U warehouse -d warehouse -f /sql/pipeline_checks.sql
```

## DAG `src_product_daily_revenue`

```
extract            control               load              transform (Cosmos)
extract_products ─▶ validate_products ─▶ load_products ─┐  ├─ stg_products          run → test
extract_carts ────▶ validate_carts ────▶ load_carts ────┴▶ ├─ stg_carts             run → test
                                                           ├─ stg_cart_lines        run → test
                                                           ├─ product_daily_revenue run → test
                                                           └─ tests que cruzan modelos
```

| Capa | Tablas | Contenido |
|---|---|---|
| raw | `products_snapshot`, `carts_snapshot` | JSON completo por `(snapshot_date, id)` |
| raw | `load_audit`, `dq_results` | Filas por carga; accuracy por regla y archivo |
| staging | `stg_products`, `stg_carts`, `stg_cart_lines` | Tablas tipadas, una fila por línea de cart |
| consume | `product_daily_revenue` | `product_id`, `product_title`, `date`, `units_sold`, `revenue`, `gross_revenue`, `carts_count` |

`revenue` es neto de descuentos; `date` es la fecha del snapshot (UTC).

## Reglas de calidad

Se configuran en [`data_quality/rules.json`](data_quality/rules.json) y se evalúan antes de cargar en `raw`.

| Nivel | Cálculo | Clave | Por defecto |
|---|---|---|---|
| Archivo | reglas que cumplen / reglas totales | `file_accuracy` | 1.0 |
| Regla | registros que cumplen / registros chequeados | `rule_accuracy` | 1.0 |

Si un archivo no llega a su `file_accuracy`, `validate_*` falla y no se carga nada.

```json
"products": {
  "file_accuracy": 1.0,
  "rules": [
    {"rule": "range", "field": "price", "min": 0},
    {"rule": "not_null", "field": "brand", "rule_accuracy": 0.4}
  ]
}
```

Reglas: `not_empty`, `count_matches_total`, `unique`, `not_null`, `type`, `range`, `accepted_values`,
`not_empty_list`. `field` admite campos anidados (`products[].quantity`).

## Estructura

```
dags/           DAG de Airflow
src/pipeline/   Cliente HTTP, control de calidad, carga y logging
data_quality/   Reglas de calidad
database/init/  DDL inicial del warehouse
dbt/            Modelos staging y consume, y tests
sql/            Query de validación y chequeos del pipeline
tests/          Tests unitarios
```

## Desarrollo

```bash
uv venv -p 3.12 && uv pip install -r requirements-dev.txt
uv run pre-commit install
uv run pre-commit run --all-files   # ruff, sqlfluff, pytest y chequeos de archivos
```

El código va dentro de la imagen: después de un cambio, `docker compose up -d --build --wait`.

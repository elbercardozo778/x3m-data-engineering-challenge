from __future__ import annotations

import datetime as dt

# Un día de margen: si el stack arranca entre las 00:00 y las 00:15 UTC, la corrida programada
# pendiente es la del día anterior y se ejecuta al día siguiente.
MAX_SNAPSHOT_LAG_DAYS = 1


class StaleSnapshotError(Exception):
    pass


def check_snapshot_date(
    snapshot_date: dt.date, today: dt.date, max_lag_days: int = MAX_SNAPSHOT_LAG_DAYS
) -> None:
    """Rechaza extraer para una fecha vieja: la API solo devuelve los datos de hoy."""
    lag = (today - snapshot_date).days
    if lag > max_lag_days:
        raise StaleSnapshotError(
            f"snapshot_date={snapshot_date} tiene {lag} días de atraso (máximo {max_lag_days}). La API no "
            "guarda historia: extraer ahora guardaría los datos de hoy con esa fecha. Para reprocesar un día "
            "pasado, relanzá solo el grupo `transform` de esa corrida."
        )

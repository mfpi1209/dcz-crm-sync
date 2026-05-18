"""Cálculo de janela para sync incremental Kommo."""

from datetime import datetime, timezone, timedelta

from config import KOMMO_DELTA_LOOKBACK_DAYS


def delta_from_ts(last_sync_at: str) -> int:
    """Timestamp mínimo (updated_at) para delta: desde o último sync, no máximo N dias atrás."""
    dt = datetime.fromisoformat(last_sync_at.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    from_ts = int(dt.timestamp()) - 300
    if KOMMO_DELTA_LOOKBACK_DAYS > 0:
        floor = int(
            (datetime.now(timezone.utc) - timedelta(days=KOMMO_DELTA_LOOKBACK_DAYS)).timestamp()
        )
        from_ts = max(from_ts, floor)
    return from_ts

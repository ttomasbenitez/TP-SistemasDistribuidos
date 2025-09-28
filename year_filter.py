# filter_years_node.py
import json
from datetime import datetime
from typing import Any, Dict, Optional, Set

from Middleware.middleware import (
    MessageMiddleware,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)

class FilterYearsNode:
    """
    Nodo filtro:
      - Consume mensajes (JSON o dict) desde un middleware de entrada.
      - Si year(created_at) ∈ allowed_years, reenvía a out_rq.
      - Finaliza ÚNICAMENTE cuando llega EOF (ver _is_eof).
    """

    def __init__(
        self,
        in_rq,                      # Queue de entrada de RabbitMQ
        out_rq,                     # Queue de salida de RabbitMQ
        allowed_years: Set[int],    # {2024, 2025}
        ts_field: str = "created_at"
    ):
        self.in_rq = in_rq
        self.out_rq = out_rq
        self.allowed_years = allowed_years
        self.ts_field = ts_field
        self._running = False

    def start(self):
        self._running = True
        try:
            self.in_rq.start_consuming(self._on_message)
        except Exception as e:
            print(f"[FilterYearsNode] Error al consumir: {type(e).__name__}: {e}")
            self._safe_close()

    def stop(self):
        if not self._running:
            return
        self._running = False
        try:
            self.in_rq.stop_consuming()
        finally:
            self._safe_close()

    def _on_message(self, raw: Any):
        # EOF: único modo de corte
        if self._is_eof(raw):
            print("[FilterYearsNode] EOF recibido → deteniendo.")
            self.stop()
            return

        # Datos
        try:
            payload = self._coerce_to_dict(raw)
        except ValueError as e:
            print(f"[FilterYearsNode] Mensaje inválido. raw={raw!r} err={e}")
            return

        year = self._extract_year(payload)
        if year is None:
            # Por si falla alguna extracción
            print(f"[FilterYearsNode] No se pudo extraer año. payload={payload}")
            return

        if year in self.allowed_years:
            try:
                self.out_rq.send(payload)
            except Exception as e:
                print(f"[FilterYearsNode] Error enviando a salida: {type(e).__name__}: {e}")

    def _extract_year(self, payload: Dict[str, Any]) -> Optional[int]:
        """
        Extrae año desde payload[created_at].
        Formato objetivo: 'YYYY-MM-DD HH:MM:SS'
        """
        val = payload.get(self.ts_field)
        if val is None:
            return None

        if isinstance(val, str):
            s = val.strip()
            # Formato principal del TP
            try:
                return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").year
            except Exception:
                # Fallbacks comunes
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                    try:
                        return datetime.strptime(s, fmt).year
                    except Exception:
                        pass
                return None

        return None

    def _coerce_to_dict(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="strict")
        if isinstance(raw, str):
            return json.loads(raw)  # se espera un JSON con los campos del payload
        if isinstance(raw, dict):
            return raw
        raise ValueError(f"Tipo de mensaje no soportado: {type(raw).__name__}")

    def _is_eof(self, raw: Any) -> bool:
        """
        Detecta end-of-stream sin usar mensajes de control.
        Acepta EOF en varias formas por robustez:
          - None
          - b'EOF' / 'EOF' (case-insensitive)
          - {'eof': true} o {'type': 'eof'} (por si el middleware lo emite así)
        """
        if raw is None:
            return True
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode("utf-8", errors="ignore")
            except Exception:
                return False
        if isinstance(raw, str):
            return raw.strip().upper() == "EOF"
        if isinstance(raw, dict):
            return raw.get("eof") is True or str(raw.get("type", "")).lower() == "eof"
        return False

    def _safe_close(self):
        for mw, name in ((self.in_rq, "in"), (self.out_rq, "out")):
            try:
                mw.close()
            except Exception as e:
                print(f"[FilterYearsNode] Error cerrando mw {name}: {type(e).__name__}: {e}")
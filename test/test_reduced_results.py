# subset_compare_ndjson.py
import os, json, math, argparse
from typing import Any, Iterable, Dict, Tuple, List, Optional, Set
import datetime

# --------- carga NDJSON ---------
def load_ndjson(path: str) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                obj = json.loads(ln)
                if isinstance(obj, dict):
                    yield obj
            except json.JSONDecodeError:
                # línea rota -> ignorar
                continue

# --------- normalización ---------
def _qid(rec: dict) -> str:
    q = rec.get("qId")
    if q is None:
        # heurística mínima por columnas (por si falta qId)
        if "transaction_id" in rec: return "q1"
        if "period" in rec and "tpv" in rec: return "q3"
        if "purchases_qty" in rec and "store_name" in rec: return "q4"
        if "year_month_created_at" in rec and "item_name" in rec: return "q2"
        return "unknown"
    return str(q).lower()

def _to_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).strip())
        except Exception:
            return None

def _to_int(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(str(x).strip())
        except Exception:
            return None

def _quantize(v: Optional[float], atol: float) -> Optional[str]:
    """cuantiza floats según atol para comparaciones robustas"""
    if v is None:
        return None
    if atol <= 0:
        return f"{float(v):.12g}"
    dec = max(0, int(round(-math.log10(atol))))
    return f"{float(v):.{dec}f}"

def canon_tuple(rec: dict, atol: float) -> Tuple:
    """
    Representación hashable por qId (elige solo columnas relevantes).
    Cada fila de q2 puede tener qty o profit (o ambas): se codifican separadas.
    """
    q = _qid(rec)
    if q == "q1":
        return ("q1",
                str(rec.get("transaction_id")),
                _quantize(_to_float(rec.get("final_amount")), atol))
    elif q == "q2":
        ym   = str(rec.get("year_month_created_at"))
        item = str(rec.get("item_name"))
        qty  = _to_int(rec.get("sellings_qty"))
        prof = _to_float(rec.get("profit_sum"))
        return ("q2", ym, item,
                qty if qty is not None else None,
                _quantize(prof, atol) if prof is not None else None)
    elif q == "q3":
        return ("q3",
                str(rec.get("period")),
                str(rec.get("store_name")),
                _quantize(_to_float(rec.get("tpv")), atol))
    elif q == "q4":
        return ("q4",
                str(rec.get("store_name")),
                str(rec.get("birthdate")),
                _to_int(rec.get("purchases_qty")))
    else:
        return ("unknown",)
    
def pretty_missing_examples(missing):
    formatted = []
    for tpl in missing:
        if tpl[0] == "q4":
            qid, store, birth_ts, qty = tpl
            try:
                ts = int(birth_ts)
                if abs(ts) > 10**10:
                    ts //= 1000
                birth_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            except Exception:
                birth_str = birth_ts
            formatted.append((qid, store, birth_str, qty))
        else:
            formatted.append(tpl)
    return formatted

# --------- comparación A ⊆ B ---------
def subset_check_ndjson(expected_path: str, actual_path: str, atol: float = 1e-6) -> dict:
    """
    Verifica que todo lo de expected esté en actual. Devuelve dict con resumen.
    """
    exp_set: Set[Tuple] = set(canon_tuple(r, atol) for r in load_ndjson(expected_path) if _qid(r) != "unknown")
    act_set: Set[Tuple] = set(canon_tuple(r, atol) for r in load_ndjson(actual_path) if _qid(r) != "unknown")
    missing = sorted(exp_set - act_set)
    return {
        "ok": len(missing) == 0,
        "expected_count": len(exp_set),
        "actual_count": len(act_set),
        "same_length": len(exp_set) == len(act_set),
        "missing_count": len(missing),
        "missing_examples": missing[:50],
        "queries_missing": sorted(set(t[0] for t in missing)),
    }

# --------- CLI opcional ---------
def main():
    res = subset_check_ndjson("data/kaggle/results.ndjson", "../client/storage/client-0.ndjson", atol=1e-6)
    res["missing_examples"] = pretty_missing_examples(res["missing_examples"])
    print(json.dumps(res, ensure_ascii=False, indent=2))


    raise SystemExit(0 if res["ok"] else 1)

if __name__ == "__main__":
    main()

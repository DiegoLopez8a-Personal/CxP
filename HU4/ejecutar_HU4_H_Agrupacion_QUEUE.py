async def ejecutar_HU4_H_Agrupacion_QUEUE():
    """
    Ejecuta [CxP].[HU4_H_Agrupacion] en modo QUEUE.

    Guarda:
    - vLocStrBatchIdPuntoH: BatchId (GUID)
    - vLocJsonFileOpsPuntoH: JSON con filas para mover (1 fila por archivo)
    - vLocStrResultadoSP: True/False
    - vLocStrResumenSP: resumen corto
    """
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback
    import re
    import unicodedata

    # ----------------------------
    # Helpers Rocketbot-safe
    # ----------------------------
    def safe_str(v):
        try:
            if v is None:
                return ""
            if isinstance(v, (bytes, bytearray)):
                try:
                    return bytes(v).decode("utf-8", errors="replace")
                except Exception:
                    return bytes(v).decode("cp1252", errors="replace")
            return str(v)
        except Exception:
            return ""

    def to_ascii(s):
        try:
            s = "" if s is None else str(s)
            s = unicodedata.normalize("NFKD", s)
            s = s.encode("ascii", "ignore").decode("ascii", "ignore")
            s = "".join(ch if 32 <= ord(ch) <= 126 else " " for ch in s)
            return " ".join(s.split())
        except Exception:
            return ""

    def reset_vars():
        try:
            SetVar("vGblStrMensajeError", "")
            SetVar("vGblStrSystemError", "")
            SetVar("vLocStrResultadoSP", "")
            SetVar("vLocStrResumenSP", "")
            SetVar("vLocStrBatchIdPuntoH", "")
            SetVar("vLocJsonFileOpsPuntoH", "")
            SetVar("vLocJsonResultadosFileOpsPuntoH", "")
        except Exception:
            pass

    def set_error_vars(user_msg, exc=None):
        try:
            SetVar("vGblStrMensajeError", to_ascii(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else to_ascii(traceback.format_exc()))
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", to_ascii(user_msg))
            SetVar("vLocStrBatchIdPuntoH", "")
            SetVar("vLocJsonFileOpsPuntoH", "[]")
        except Exception:
            pass

    def parse_config(raw):
        if isinstance(raw, dict):
            return raw
        t = safe_str(raw).strip()
        if not t:
            raise ValueError("vLocDicConfig vacio")
        try:
            return json.loads(t)
        except Exception:
            return ast.literal_eval(t)

    def is_missing(v):
        return v in ("", None, "ERROR_NOT_VAR")

    def to_int(v, default):
        if is_missing(v):
            return int(default)
        try:
            s = safe_str(v).strip()
            if s.upper() == "ERROR_NOT_VAR":
                return int(default)
            return int(float(s))
        except Exception:
            return int(default)

    def to_int_or_none(v):
        if is_missing(v):
            return None
        try:
            s = safe_str(v).strip()
            if s.upper() == "ERROR_NOT_VAR":
                return None
            return int(float(s))
        except Exception:
            return None

    def normalize_guid_text(x):
        t = safe_str(x).strip()
        if not t:
            return ""
        t = t.replace("{", "").replace("}", "").strip()
        m = re.search(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", t)
        return m.group(1) if m else ""

    reset_vars()

    # ----------------------------
    # Configuracion
    # ----------------------------
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))
        servidor = safe_str(cfg["ServidorBaseDatos"]).replace("\\\\", "\\")
        db = safe_str(cfg["NombreBaseDatos"])

        execution_num = to_int_or_none(GetVar("vGblIntExecutionNum"))
        if execution_num is None:
            execution_num = to_int_or_none(cfg.get("executionNum"))

        dias_max = to_int(cfg.get("DiasMaximos", cfg.get("PlazoMaximo", 120)), 120)
        batch = to_int(cfg.get("BatchSize", cfg.get("Lote", 500)), 500)

    except Exception as e:
        set_error_vars("ERROR Punto H QUEUE | configuracion", e)
        return False, None

    # ----------------------------
    # Ejecucion SP (sync)
    # ----------------------------
    def run_sp_sync():
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={servidor};"
            f"DATABASE={db};"
            "Trusted_Connection=yes;"
        )

        filas = []
        batch_id = ""

        with pyodbc.connect(conn_str) as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(
                "EXEC [CxP].[HU4_H_Agrupacion] "
                "@Modo=?, @executionNum=?, @BatchId=?, @DiasMaximos=?, @UseBogotaTime=?, @BatchSize=?, @ResultadosJson=?;",
                "QUEUE",
                execution_num,
                None,
                dias_max,
                0,
                batch,
                None
            )

            while True:
                if cur.description:
                    cols = [c[0] for c in cur.description]
                    cols_lower = [safe_str(c[0]).lower() for c in cur.description]
                    try:
                        rows = cur.fetchall()
                    except Exception:
                        rows = []

                    if rows:
                        if (not batch_id) and ("batchid" in cols_lower):
                            idx = cols_lower.index("batchid")
                            batch_id = normalize_guid_text(rows[0][idx])

                        for r in rows:
                            d = {}
                            for i, name in enumerate(cols):
                                d[safe_str(name)] = safe_str(r[i])
                            filas.append(d)

                if not cur.nextset():
                    break

        if not batch_id:
            for d in filas:
                if "BatchId" in d:
                    batch_id = normalize_guid_text(d.get("BatchId"))
                    if batch_id:
                        break

        try:
            SetVar("vLocStrBatchIdPuntoH", batch_id)
            SetVar("vLocJsonFileOpsPuntoH", json.dumps(filas, ensure_ascii=True))
        except Exception:
            pass

        resumen = (
            f"Punto H QUEUE | SP=CxP.HU4_H_Agrupacion | "
            f"BatchId={batch_id if batch_id else 'NO_DISPONIBLE'} | "
            f"ArchivosEnCola={len(filas)} | executionNum={execution_num if execution_num is not None else 'NULL'} | "
            f"DiasMaximos={dias_max} | BatchSize={batch}"
        )

        return True, filas, resumen

    # ----------------------------
    # Wrapper async
    # ----------------------------
    try:
        loop = asyncio.get_running_loop()
        ok, filas, resumen = await loop.run_in_executor(None, run_sp_sync)

        try:
            SetVar("vLocStrResultadoSP", bool(ok))
            SetVar("vLocStrResumenSP", to_ascii(resumen))
        except Exception:
            pass

        return bool(ok), filas

    except Exception as e:
        set_error_vars("ERROR Punto H QUEUE | ejecucion", e)
        return False, None

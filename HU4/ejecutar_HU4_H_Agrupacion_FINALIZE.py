async def ejecutar_HU4_H_Agrupacion_FINALIZE():
    """
    Ejecuta [CxP].[HU4_Punto_H_Agrupacion] en modo FINALIZE.

    Requiere:
      - vLocStrBatchIdPuntoH (BatchId del QUEUE)
      - vLocJsonResultadosFileOpsPuntoH (puede ser '[]')

    Guarda:
      - vLocStrResultadoSP (True/False)
      - vLocStrResumenSP (resumen)
      - vGblStrMensajeError / vGblStrSystemError (si falla)
    """
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback
    import unicodedata
    import re

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

    def normalize_guid_text(x):
        t = safe_str(x).strip()
        if not t:
            return ""
        t = t.replace("{", "").replace("}", "").strip()
        m = re.search(
            r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
            t
        )
        return m.group(1) if m else ""

    def set_error(user_msg, exc=None):
        try:
            SetVar("vGblStrMensajeError", to_ascii(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else to_ascii(traceback.format_exc()))
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", to_ascii(user_msg))
        except Exception:
            pass

    # ----------------------------
    # Config
    # ----------------------------
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))
        servidor = safe_str(cfg["ServidorBaseDatos"]).replace("\\\\", "\\")
        db = safe_str(cfg["NombreBaseDatos"])

        dias_max = int(cfg.get("DiasMaximos", cfg.get("PlazoMaximo", 120)))
        batch_size = int(cfg.get("BatchSize", cfg.get("Lote", 500)))

        exec_num = cfg.get("executionNum")
        try:
            exec_num = int(exec_num) if exec_num not in ("", None, "ERROR_NOT_VAR") else None
        except Exception:
            exec_num = None

    except Exception as e:
        set_error("ERROR configuracion Punto H (FINALIZE)", e)
        return False, None

    batch_id = normalize_guid_text(GetVar("vLocStrBatchIdPuntoH"))
    resultados_json = safe_str(GetVar("vLocJsonResultadosFileOpsPuntoH")).strip()
    if not resultados_json:
        resultados_json = "[]"

    if not batch_id:
        set_error("ERROR Punto H FINALIZE | BatchId vacio o invalido (vLocStrBatchIdPuntoH).")
        return False, None

    # ----------------------------
    # Ejecucion SP (sync)
    # ----------------------------
    def run_finalize_sync():
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={servidor};"
            f"DATABASE={db};"
            "Trusted_Connection=yes;"
        )

        with pyodbc.connect(conn_str) as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(
                "EXEC [CxP].[HU4_H_Agrupacion] "
                "@Modo=?, @executionNum=?, @BatchId=?, @DiasMaximos=?, @UseBogotaTime=?, @BatchSize=?, @ResultadosJson=?;",
                "FINALIZE",
                exec_num,
                batch_id,
                dias_max,
                0,
                batch_size,
                resultados_json
            )

            rs1 = None
            if cur.description:
                row = cur.fetchone()
                if row:
                    cols = [c[0] for c in cur.description]
                    rs1 = {cols[i]: row[i] for i in range(len(cols))}

            ok_cnt = safe_str(rs1.get("OK") if rs1 else "")
            fail_cnt = safe_str(rs1.get("FAIL") if rs1 else "")

            resumen = f"Punto H FINALIZE OK | BatchId={batch_id} | OK={ok_cnt or '0'} | FAIL={fail_cnt or '0'}"
            return True, resumen

    # ----------------------------
    # Wrapper async
    # ----------------------------
    try:
        loop = asyncio.get_running_loop()
        ok, resumen = await loop.run_in_executor(None, run_finalize_sync)

        try:
            SetVar("vLocStrResultadoSP", bool(ok))
            SetVar("vLocStrResumenSP", to_ascii(resumen))
        except Exception:
            pass

        return bool(ok), resumen

    except Exception as e:
        set_error("ERROR ejecucion Punto H (FINALIZE)", e)
        return False, None

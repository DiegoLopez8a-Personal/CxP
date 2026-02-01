async def ejecutar_HU4_ABCD_CamposObligatorios():
    """
    Ejecuta el SP [CxP].[HU4_ABCD_CamposObligatorios] (NO modificar SP).

    - Lee configuración desde `vLocDicConfig` (GetVar/SetVar).
    - Ejecuta el SP en un thread usando run_in_executor.
    - Consume 2 ResultSets:
        1) RESUMEN (1 fila)
        2) DETALLE (N filas)

    SetVar:
      - vLocStrResultadoSP: True si ejecutó OK, False si falló.
      - vLocStrResumenSP: resumen legible.
      - vGblStrMensajeError / vGblStrSystemError: detalle de error si falla.

    Nota técnica:
      - Evita error pyodbc "ODBC SQL type -155 is not yet supported" (DATETIMEOFFSET)
        ejecutando el SP con WITH RESULT SETS para convertir fechas a NVARCHAR.
    """
    # IMPORTS dentro de la función (requisito)
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback
    from collections import Counter

    # ----------------------------
    # Helpers internos (anidados)
    # ----------------------------
    def safe_str(v):
        try:
            return "" if v is None else str(v)
        except Exception:
            return ""

    def as_int(x, default=0):
        try:
            if x is None:
                return default
            return int(x)
        except Exception:
            return default

    def reset_vars():
        # Estado conocido desde el inicio
        try:
            SetVar("vGblStrMensajeError", "")
            SetVar("vGblStrSystemError", "")
            SetVar("vLocStrResultadoSP", False)  # default: False hasta terminar OK
            SetVar("vLocStrResumenSP", "")
        except Exception:
            pass

    def set_error_vars(user_msg, exc=None):
        try:
            SetVar("vGblStrMensajeError", safe_str(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else traceback.format_exc())
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", "")
        except Exception:
            pass

    def parse_config(raw):
        if isinstance(raw, dict):
            return raw
        text = safe_str(raw).strip()
        if not text:
            raise ValueError("vLocDicConfig vacío")
        try:
            return json.loads(text)
        except Exception:
            return ast.literal_eval(text)

    def row_to_dict(cursor, row):
        if row is None:
            return None
        cols = [c[0] for c in (cursor.description or [])]
        return {cols[i]: row[i] for i in range(len(cols))}

    def rows_to_dicts(cursor, rows):
        cols = [c[0] for c in (cursor.description or [])]
        out = []
        for r in rows or []:
            d = {}
            for i, name in enumerate(cols):
                d[name] = r[i]
            out.append(d)
        return out

    def build_summary(resumen_dict, detalle_list):
        if not resumen_dict:
            return "INFO | Sin datos de RESUMEN"

        fecha = safe_str(resumen_dict.get("FechaEjecucion"))
        dias = as_int(resumen_dict.get("DiasMaximos"))
        batch = as_int(resumen_dict.get("BatchSize"))
        proc = as_int(resumen_dict.get("RegistrosProcesados"))
        retoma = as_int(resumen_dict.get("RetomaSetDesdeNull"))
        noex = as_int(resumen_dict.get("MarcadosNoExitoso"))
        rech = as_int(resumen_dict.get("MarcadosRechazado"))
        ok = as_int(resumen_dict.get("OKDentroDiasMaximos"))
        filas_comp = as_int(resumen_dict.get("FilasInsertadasComparativa"))

        c_estados = Counter()
        top_obs = Counter()
        for r in (detalle_list or []):
            est = safe_str(r.get("ResultadoFinalAntesEventos")).strip() or "SIN_ESTADO"
            c_estados[est] += 1
            obs = safe_str(r.get("ObservacionesFase_4")).strip()
            if obs:
                top_obs[obs] += 1

        extra = ""
        if c_estados:
            extra += " | TopEstados=" + ", ".join([f"{k}={v}" for k, v in c_estados.most_common(3)])
        if top_obs:
            top2 = []
            for k, v in top_obs.most_common(2):
                kk = (k[:60] + "...") if len(k) > 60 else k
                top2.append(f"{kk}={v}")
            extra += " | TopObs=" + ", ".join(top2)

        return (
            f"OK | Fecha={fecha} | DiasMax={dias} | Batch={batch} | Procesados={proc} | "
            f"RetomaSet={retoma} | NoExitoso={noex} | Rechazados={rech} | OKDentroDias={ok} | "
            f"FilasComparativa={filas_comp}{extra}"
        )

    # ----------------------------
    # Inicio
    # ----------------------------
    reset_vars()

    # ----------------------------
    # Configuración
    # ----------------------------
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))

        # Params SP (SOLO 2)
        dias = int(cfg.get("PlazoMaximo", cfg.get("DiasMaximos", 120)))
        batch = int(cfg.get("Lote", cfg.get("BatchSize", 500)))

        servidor = safe_str(cfg["ServidorBaseDatos"]).replace("\\\\", "\\")
        db = safe_str(cfg["NombreBaseDatos"])

        # Opcional: timeout por comando (0 = sin timeout)
        cmd_timeout = int(cfg.get("CommandTimeout", 0) or 0)

    except Exception as e:
        set_error_vars("Error configuración HU4_ABCD_CamposObligatorios", e)
        try:
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", "ERROR | configuracion | HU4_ABCD | Ver vGblStrSystemError")
        except Exception:
            pass
        return False, None

    # ----------------------------
    # Ejecución SP (sync en thread)
    # ----------------------------
    def run_sp_sync():
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={servidor};"
            f"DATABASE={db};"
            "Trusted_Connection=yes;"
        )

        # SQL wrapper: convierte DATETIMEOFFSET -> NVARCHAR para evitar type -155 en pyodbc
        sql_exec = """
        EXEC [CxP].[HU4_ABCD_CamposObligatorios] ?, ?
        WITH RESULT SETS
        (
            (
                FechaEjecucion NVARCHAR(40),
                DiasMaximos INT,
                BatchSize INT,
                RegistrosProcesados INT,
                RetomaSetDesdeNull INT,
                MarcadosNoExitoso INT,
                MarcadosRechazado INT,
                OKDentroDiasMaximos INT,
                FilasInsertadasComparativa INT
            ),
            (
                ID BIGINT,
                numero_de_factura NVARCHAR(200),
                nit_emisor_o_nit_del_proveedor NVARCHAR(200),
                documenttype NVARCHAR(50),
                Fecha_de_retoma_antes_de_contabilizacion NVARCHAR(40),
                DiasTranscurridosDesdeRetoma INT,
                ResultadoFinalAntesEventos NVARCHAR(200),
                EstadoFinalFase_4 NVARCHAR(4000),
                ObservacionesFase_4 NVARCHAR(MAX)
            )
        );
        """

        try:
            with pyodbc.connect(conn_str) as c:
                c.autocommit = True
                cur = c.cursor()

                if cmd_timeout and cmd_timeout > 0:
                    try:
                        cur.timeout = cmd_timeout
                    except Exception:
                        pass

                cur.execute(sql_exec, dias, batch)

                # ResultSet 1: resumen
                r1 = cur.fetchone()
                resumen = row_to_dict(cur, r1) if r1 else None

                # ResultSet 2: detalle
                detalle = []
                if cur.nextset():
                    try:
                        rows = cur.fetchall()
                        detalle = rows_to_dicts(cur, rows)
                    except Exception:
                        detalle = []

                payload = {
                    "resumen_general": resumen,
                    "detalle_registros": detalle,
                    "detalle_errores": detalle,  # alias por compatibilidad
                }
                return json.dumps(payload, ensure_ascii=False, default=str)

        except pyodbc.Error as e:
            raise RuntimeError(f"pyodbc.Error ejecutando HU4_ABCD: {safe_str(e)}") from e

    # ----------------------------
    # Wrapper async + setvars finales
    # ----------------------------
    try:
        loop = asyncio.get_running_loop()
        payload_json = await loop.run_in_executor(None, run_sp_sync)

        # Si llegó aquí, ejecutó OK
        try:
            SetVar("vLocStrResultadoSP", True)
        except Exception:
            pass

        try:
            payload = json.loads(payload_json) if payload_json else {}
        except Exception:
            payload = {}

        resumen_txt = build_summary(
            payload.get("resumen_general"),
            payload.get("detalle_registros") or payload.get("detalle_errores")
        )

        try:
            SetVar("vLocStrResumenSP", resumen_txt)
        except Exception:
            pass

        return True, payload_json

    except Exception as e:
        set_error_vars("Error ejecución HU4_ABCD_CamposObligatorios", e)
        try:
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", "ERROR | ejecucion | HU4_ABCD | Ver vGblStrSystemError")
        except Exception:
            pass
        return False, None

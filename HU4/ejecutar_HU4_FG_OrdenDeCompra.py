# Author: Diego Ivan Lopez Ochoa
"""
Validar la consistencia de la Orden de Compra (Puntos F, G).

LOGICA:
Cruza la información del documento con el historial de Órdenes de Compra para validar montos, items y vigencia.

VARIABLES ROCKETBOT:
- vLocDicConfig: Configuracion BD
- vLocStrResultadoSP: Resultado ejecucion
"""
async def ejecutar_HU4_FG_OrdenDeCompra():
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback

    # ----------------------------
    # Helpers
    # ----------------------------
    def safe_str(v):
        try:
            return "" if v is None else str(v)
        except Exception:
            return ""

    def reset_vars():
        try:
            SetVar("vGblStrMensajeError", "")
            SetVar("vGblStrSystemError", "")
            SetVar("vLocStrResultadoSP", "")
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
            raise ValueError("vLocDicConfig vacio")
        try:
            return json.loads(text)
        except Exception:
            return ast.literal_eval(text)

    def is_missing(v):
        return v in ("", None, "ERROR_NOT_VAR")

    def to_int(v, default):
        if is_missing(v):
            return int(default)
        try:
            return int(float(safe_str(v).strip()))
        except Exception:
            return int(default)

    def read_one_row_as_dict(cur):
        try:
            if not cur.description:
                return None
            row = cur.fetchone()
            if not row:
                return None
            cols = [c[0] for c in cur.description]
            d = {}
            for i in range(len(cols)):
                d[cols[i]] = row[i]
            return d
        except Exception:
            return None

    def consume_all_rows(cur):
        try:
            if cur.description:
                _ = cur.fetchall()
        except Exception:
            pass

    def build_summary(rs1):
        # Resumen corto, estable y legible
        if not rs1:
            return "Estado=ERROR | SP no retorno resumen (ResultSet 1)."

        fecha = safe_str(rs1.get("FechaEjecucion"))
        dias = safe_str(rs1.get("DiasMaximos"))
        batch = safe_str(rs1.get("BatchSize"))

        procesados = safe_str(rs1.get("RegistrosProcesados", "0"))
        retoma = safe_str(rs1.get("RetomaSetDesdeNull", "0"))
        imp = safe_str(rs1.get("ExcluidosImportaciones", "0"))
        fletes = safe_str(rs1.get("ExcluidosCostoIndirectoFletes", "0"))
        obs = safe_str(rs1.get("ComparativaObservacionesActualizadas", "0"))
        est = safe_str(rs1.get("ComparativaEstadosActualizados", "0"))

        estado = "OK"
        # Si no proceso nada, sigue siendo OK pero informativo
        # Si quieres marcarlo como INFO, se puede.
        if procesados == "0":
            estado = "OK"

        return (
            "Estado=" + estado
            + " | SP=CxP.HU4_FG_OrdenDeCompra"
            + " | FechaEjecucion=" + fecha
            + " | DiasMaximos=" + dias
            + " | BatchSize=" + batch
            + " | Procesados=" + procesados
            + " | RetomaSetDesdeNull=" + retoma
            + " | ExcluidosImportaciones=" + imp
            + " | ExcluidosCostoIndirectoFletes=" + fletes
            + " | ComparativaObsActualizadas=" + obs
            + " | ComparativaEstadosActualizados=" + est
        )

    reset_vars()

    # ----------------------------
    # Configuracion
    # ----------------------------
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))
        servidor = safe_str(cfg["ServidorBaseDatos"]).replace("\\\\", "\\")
        db = safe_str(cfg["NombreBaseDatos"])

        dias = to_int(GetVar("vGblIntDiasMaximos"), cfg.get("DiasMaximos", cfg.get("PlazoMaximo", 120)))
        batch = to_int(GetVar("vGblIntBatchSize"), cfg.get("BatchSize", cfg.get("Lote", 500)))
    except Exception as e:
        set_error_vars("Error configuracion SP HU4_FG_OrdenDeCompra", e)
        return False, None

    # ----------------------------
    # Ejecucion SP (sync en thread)
    # ----------------------------
    def run_sp_sync():
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={servidor};"
            f"DATABASE={db};"
            "Trusted_Connection=yes;"
        )

        with pyodbc.connect(conn_str) as c:
            c.autocommit = True
            cur = c.cursor()

            # Verificar existencia del SP para evitar error 2812
            sql_check = (
                "SELECT 1 "
                "FROM sys.procedures p "
                "INNER JOIN sys.schemas s ON s.schema_id = p.schema_id "
                "WHERE s.name = ? AND p.name = ?"
            )
            cur.execute(sql_check, ("CxP", "HU4_FG_OrdenDeCompra"))
            if cur.fetchone() is None:
                raise RuntimeError("SP no existe: [CxP].[HU4_FG_OrdenDeCompra]")

            # Ejecutar SP
            cur.execute("EXEC [CxP].[HU4_FG_OrdenDeCompra] ?, ?", dias, batch)

            # RS1: Resumen
            rs1 = read_one_row_as_dict(cur)

            # RS2: Detalle (solo consumir)
            if cur.nextset():
                consume_all_rows(cur)

            resumen = build_summary(rs1)
            return True, resumen

    # ----------------------------
    # Wrapper ASYNC
    # ----------------------------
    try:
        loop = asyncio.get_running_loop()
        ok, resumen = await loop.run_in_executor(None, run_sp_sync)

        try:
            SetVar("vLocStrResultadoSP", bool(ok))
            SetVar("vLocStrResumenSP", safe_str(resumen))
        except Exception:
            pass

        return bool(ok), safe_str(resumen)

    except Exception as e:
        set_error_vars("Error ejecucion SP HU4_FG_OrdenDeCompra", e)
        return False, None
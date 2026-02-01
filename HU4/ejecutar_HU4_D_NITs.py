# Author: Diego Ivan Lopez Ochoa
async def ejecutar_HU4_D_NITs():
    """Ejecuta Punto D – Validación NITs contra el SP [CxP].[HU4_D_NITs]."""
    import asyncio
    import os
    import pyodbc
    import openpyxl
    import json
    import ast
    import traceback
    import struct
    from datetime import datetime, timedelta, timezone

    def safe_str(v):
        try:
            return "" if v is None else str(v)
        except Exception:
            return ""

    def safe_int(v, default=0):
        try:
            if v is None:
                return default
            return int(v)
        except Exception:
            return default

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
            raise ValueError("vLocDicConfig vacío")
        try:
            return json.loads(text)
        except Exception:
            return ast.literal_eval(text)

    def fetch_resultset_as_dict(cursor):
        cols = [c[0] for c in cursor.description] if cursor.description else []
        r = cursor.fetchone()
        if not r:
            return {}
        return {cols[i]: r[i] for i in range(len(cols))}

    def build_sp_summary_from_row(row_dict, total_nits, detalle_rows=0, detalle_matched=0):
        candidatos = safe_int(row_dict.get("TotalCandidatos"))
        procesados = safe_int(row_dict.get("TotalIDsProcesados"))
        coincidencias = safe_int(row_dict.get("TotalIDsConNITEnListado"))
        actualizaciones_dp = safe_int(row_dict.get("TotalUpdatesDocumentsProcessing"))
        actualizaciones_comparativa_observaciones = safe_int(row_dict.get("TotalUpdatesComparativa_Observaciones"))
        actualizaciones_comparativa_estado = safe_int(row_dict.get("TotalUpdatesComparativa_EstadoValidacion"))
        grupos_nit_factura = safe_int(row_dict.get("TotalGruposNITFactura"))
        sin_factura = safe_int(row_dict.get("TotalIDsSinFactura"))

        return (
            f"OK | CantidadNITsLeidos={int(total_nits)} | "
            f"TotalCandidatos={candidatos} | TotalProcesados={procesados} | "
            f"TotalCoincidenciasDeNIT={coincidencias} | "
            f"TotalActualizacionesEnDocumentsProcessing={actualizaciones_dp} | "
            f"TotalActualizacionesEnComparativaObservaciones={actualizaciones_comparativa_observaciones} | "
            f"TotalActualizacionesEnComparativaEstadoValidacion={actualizaciones_comparativa_estado} | "
            f"TotalGruposPorNITYFactura={grupos_nit_factura} | "
            f"TotalRegistrosSinFactura={sin_factura} | "
            f"TotalFilasEnDetalle={int(detalle_rows)} | "
            f"TotalFilasCoincidentesEnDetalle={int(detalle_matched)}"
        )

    reset_vars()

    # ----------------------------
    # Configuración
    # ----------------------------
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))
        servidor = safe_str(cfg["ServidorBaseDatos"]).replace("\\\\", "\\")
        db = safe_str(cfg["NombreBaseDatos"])
        ruta = os.path.join(
            safe_str(cfg["RutaInsumoNitMaestros"]),
            safe_str(cfg["NombreArchivoNitsMaestros"])
        )
        dias_maximos = int(cfg.get("DiasMaximosPuntoD", 120))
        batch_size = int(cfg.get("BatchSizePuntoD", 500))
    except Exception as e:
        set_error_vars("Error configuración Punto D NITs", e)
        try:
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", "ERROR | configuracion | Punto D NITs | Ver vGblStrSystemError")
        except Exception:
            pass
        return False, None

    # ----------------------------
    # Ejecución SP (sync en thread)
    # ----------------------------
    def run_sp_sync():
        if not os.path.exists(ruta):
            raise FileNotFoundError(f"Archivo NITs no existe: {ruta}")

        wb = openpyxl.load_workbook(ruta, data_only=True)
        if "SIN MANDATORIOS" not in wb.sheetnames:
            raise ValueError("Hoja 'SIN MANDATORIOS' no existe en el Excel")

        ws = wb["SIN MANDATORIOS"]
        nits = []
        for r in ws.iter_rows(min_row=2, max_col=1, values_only=True):
            if r and r[0]:
                nit_txt = str(r[0]).strip()
                if nit_txt:
                    nits.append(nit_txt)

        nits_unicos = sorted(set(nits))
        lista = ",".join(nits_unicos)

        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={servidor};"
            f"DATABASE={db};"
            "Trusted_Connection=yes;"
        )

        # ---- FIX para DATETIMEOFFSET (ODBC type -155) ----
        def handle_datetimeoffset(dto_value):
            # ref: pyodbc wiki (datetimeoffset -> SQL type -155)
            # tupla: (year, month, day, hour, minute, second, nanoseconds, offset_hours, offset_minutes)
            tup = struct.unpack("<6hI2h", dto_value)
            return datetime(
                tup[0], tup[1], tup[2], tup[3], tup[4], tup[5],
                tup[6] // 1000,
                timezone(timedelta(hours=tup[7], minutes=tup[8]))
            )

        with pyodbc.connect(conn_str) as c:
            c.autocommit = True

            # Registrar converter ANTES de ejecutar el SP
            c.add_output_converter(-155, handle_datetimeoffset)

            cur = c.cursor()
            cur.execute(
                "EXEC [CxP].[HU4_D_NITs] @DiasMaximos=?, @BatchSize=?, @ListadoNITS=?",
                dias_maximos, batch_size, lista
            )

            # RS1: resumen (1 fila)
            resumen_row = fetch_resultset_as_dict(cur)

            # RS2: detalle (opcional: solo contamos)
            detalle_count = 0
            detalle_matched = 0

            if cur.nextset() and cur.description:
                cols = [c[0] for c in cur.description]
                idx_matched = None
                for i, name in enumerate(cols):
                    if str(name).lower() == "matched":
                        idx_matched = i
                        break

                for row in cur:
                    detalle_count += 1
                    if idx_matched is not None:
                        try:
                            if row[idx_matched] in (1, True, "1"):
                                detalle_matched += 1
                        except Exception:
                            pass

            payload = {
                "resumen": resumen_row,
                "detalle_rows": detalle_count,
                "detalle_matched_rows": detalle_matched
            }
            return True, payload, len(nits_unicos)

    # ----------------------------
    # Wrapper async + SetVars
    # ----------------------------
    try:
        loop = asyncio.get_running_loop()
        ok, payload, total_nits = await loop.run_in_executor(None, run_sp_sync)

        resumen_row = payload.get("resumen", {}) if isinstance(payload, dict) else {}
        resumen_txt = build_sp_summary_from_row(
            resumen_row,
            total_nits,
            detalle_rows=payload.get("detalle_rows", 0),
            detalle_matched=payload.get("detalle_matched_rows", 0),
        )


        try:
            SetVar("vLocStrResultadoSP", ok)
            SetVar("vLocStrResumenSP", resumen_txt)
        except Exception:
            pass

        return ok, payload

    except Exception as e:
        set_error_vars("Error ejecución Punto D NITs", e)
        try:
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", "ERROR | ejecucion | Punto D NITs | Ver vGblStrSystemError")
        except Exception:
            pass
        return False, None

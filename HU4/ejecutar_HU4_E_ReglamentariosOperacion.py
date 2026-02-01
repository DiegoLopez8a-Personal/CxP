async def ejecutar_HU4_E_ReglamentariosOperacion():
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback
    import unicodedata
    from datetime import datetime

    def safe_str(v):
        try:
            return "" if v is None else str(v)
        except Exception:
            return ""

    def to_ascii(v):
        try:
            if v is None:
                s = ""
            elif isinstance(v, (bytes, bytearray)):
                try:
                    s = v.decode("cp1252", errors="replace")
                except Exception:
                    s = v.decode("latin-1", errors="replace")
            else:
                s = str(v)

            s = unicodedata.normalize("NFKD", s)
            s = s.encode("ascii", "ignore").decode("ascii", "ignore")
            s = "".join(ch if 32 <= ord(ch) <= 126 else " " for ch in s)
            s = " ".join(s.split())
            return s
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
            SetVar("vGblStrMensajeError", to_ascii(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else to_ascii(traceback.format_exc()))
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

    def normalize_int(v, default):
        try:
            if v in ("", None, "ERROR_NOT_VAR"):
                return int(default)
            return int(v)
        except Exception:
            return int(default)

    def fmt_dt(v):
        try:
            if v is None:
                return ""
            if isinstance(v, datetime):
                return v.strftime("%Y-%m-%d %H:%M:%S")
            return to_ascii(v)
        except Exception:
            return ""

    def read_resultset_one(cur):
        try:
            if not cur.description:
                return None
            r = cur.fetchone()
            if not r:
                return None
            cols = [c[0] for c in cur.description]
            return {cols[i]: r[i] for i in range(len(cols))}
        except Exception:
            return None

    def consume_resultset_all(cur):
        # Solo para consumir el siguiente resultset sin fallar
        try:
            if not cur.description:
                return
            _ = cur.fetchall()
        except Exception:
            return

    def build_report_ascii(rs1, params):
        if not rs1:
            return to_ascii("ERROR | SP no retorno ResultSet 1 (Resumen).")

        fecha = fmt_dt(rs1.get("FechaEjecucion"))
        proc = to_ascii(rs1.get("RegistrosProcesados", "0"))
        nov = to_ascii(rs1.get("RegistrosConNovedad", "0"))
        ano = to_ascii(rs1.get("RegistrosAnoCerrado", "0"))

        has_doc = to_ascii(rs1.get("HasDocumentCurrencyCode", "0"))
        has_calc = to_ascii(rs1.get("HasCalculationRate", "0"))
        has_vlr = to_ascii(rs1.get("HasVlrPagarCop", "0"))
        has_pay = to_ascii(rs1.get("HasPaymentMeans", "0"))

        estado = "OK"
        if nov != "0" or ano != "0":
            estado = "CON_CAMBIOS"

        line0 = "Estado=" + estado
        line1 = "PUNTO E | SP=CxP.HU4_E_CamposReglamentarios | Fecha=" + fecha
        line2 = "Procesados=" + proc + " | ConNovedad=" + nov + " | AnoCerrado=" + ano
        line3 = (
            "DiasMaximos=" + to_ascii(params["dias"]) +
            " | BatchSize=" + to_ascii(params["batch"]) +
            " | RangoMaxValor=" + to_ascii(params["rango"])
        )
        line4 = (
            "HasDocCurrency=" + has_doc +
            " | HasCalcRate=" + has_calc +
            " | HasVlrPagarCop=" + has_vlr +
            " | HasPaymentMeans=" + has_pay
        )

        return to_ascii("\n".join([line0, line1, line2, line3, line4])).strip()

    reset_vars()

    # Configuracion
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))

        servidor = safe_str(cfg["ServidorBaseDatos"]).replace("\\\\", "\\")
        db = safe_str(cfg["NombreBaseDatos"])

        dias = normalize_int(cfg.get("DiasMaximos", cfg.get("PlazoMaximo", 120)), 120)
        batch = normalize_int(cfg.get("BatchSize", cfg.get("Lote", 500)), 500)
        rango = normalize_int(cfg.get("RangoMaxValor", 500), 500)

        tax_codes = safe_str(cfg.get("TaxLevelCodes", cfg.get("TaxLevelCode", "O-13,O-15,O-23,O-47,R-99-PN"))).strip()
        if not tax_codes:
            tax_codes = "O-13,O-15,O-23,O-47,R-99-PN"

        invoice_codes = safe_str(cfg.get("InvoiceTypecodes", cfg.get("InvoiceTypecode", "01,02,03,04,91,92,96"))).strip()
        if not invoice_codes:
            invoice_codes = "01,02,03,04,91,92,96"

        estados_omitidos = safe_str(cfg.get("ListadoEstados", cfg.get("EstadosOmitir", ""))).strip()
        if not estados_omitidos:
            estados_omitidos = "APROBADO,APROBADO CONTADO Y/O EVENTO MANUAL,APROBADO SIN CONTABILIZACION,RECHAZADO,RECLASIFICAR,RECHAZADO - RETORNADO,CON NOVEDAD - RETORNADO,EN ESPERA DE POSICIONES,NO EXITOSO"

        params = {"dias": dias, "batch": batch, "rango": rango}

    except Exception as e:
        set_error_vars("Error configuracion Punto E (Campos Reglamentarios)", e)
        return False, None

    # Ejecucion SP
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

            # Verifica existencia sin SQL multilinea
            sql_check = "SELECT 1 FROM sys.procedures p INNER JOIN sys.schemas s ON s.schema_id = p.schema_id WHERE s.name = ? AND p.name = ?"
            cur.execute(sql_check, ("CxP", "HU4_E_CamposReglamentarios"))
            if cur.fetchone() is None:
                raise RuntimeError("SP no existe: [CxP].[HU4_E_CamposReglamentarios]")

            cur.execute(
                "EXEC [CxP].[HU4_E_CamposReglamentarios] ?, ?, ?, ?, ?, ?",
                dias, batch, rango, tax_codes, invoice_codes, estados_omitidos
            )

            rs1 = read_resultset_one(cur)

            # Consume RS2 para no dejar cursor en estado raro
            if cur.nextset():
                consume_resultset_all(cur)

            return True, build_report_ascii(rs1, params)

    try:
        loop = asyncio.get_running_loop()
        ok, reporte = await loop.run_in_executor(None, run_sp_sync)

        try:
            SetVar("vLocStrResultadoSP", bool(ok))
            SetVar("vLocStrResumenSP", to_ascii(reporte))
        except Exception:
            pass

        return bool(ok), to_ascii(reporte)

    except Exception as e:
        set_error_vars("Error ejecucion Punto E (Campos Reglamentarios)", e)
        return False, None

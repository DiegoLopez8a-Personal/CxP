#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: ejecutar_HU4_E_ReglamentariosOperacion.py
================================================================================

Descripcion General:
--------------------
    Este modulo ejecuta el Punto E del proceso HU4 - Validacion de Campos
    Reglamentarios de Operacion. Verifica que los documentos cumplan con
    los requisitos reglamentarios definidos (codigos de impuestos, tipos
    de factura, etc.).

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Fecha de Creacion: 2025

Stored Procedure:
-----------------
    [CxP].[HU4_E_CamposReglamentarios]
    
    Parametros:
        @DiasMaximos INT - Dias maximos para filtrar
        @BatchSize INT - Tamano del lote
        @RangoMaxValor INT - Rango maximo de valor
        @TaxLevelCodes NVARCHAR - Codigos de nivel de impuesto (CSV)
        @InvoiceTypecodes NVARCHAR - Codigos de tipo de factura (CSV)
        @EstadosOmitir NVARCHAR - Estados a omitir (CSV)

Dependencias:
-------------
    - asyncio: Ejecucion asincrona
    - pyodbc: Conexion a SQL Server
    - json, ast: Parseo de configuracion
    - traceback: Captura de errores
    - unicodedata: Normalizacion de texto
    - datetime: Formateo de fechas

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |        ejecutar_HU4_E_ReglamentariosOperacion()             |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  reset_vars() - Inicializar variables                       |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Parsear configuracion:                                     |
    |  - ServidorBaseDatos, NombreBaseDatos                       |
    |  - DiasMaximos, BatchSize, RangoMaxValor                    |
    |  - TaxLevelCodes (default: "O-13,O-15,O-23,O-47,R-99-PN")   |
    |  - InvoiceTypecodes (default: "01,02,03,04,91,92,96")       |
    |  - ListadoEstados (estados a omitir)                        |
    +-------------------------+-----------------------------------+
                              |
              +---------------+---------------+
              |    ¿Error en configuracion?   |
              +---------------+---------------+
                     |                |
                     | SI             | NO
                     v                v
    +------------------------+   +--------------------------------+
    |  set_error_vars()      |   |  Conectar a SQL Server         |
    |  return False, None    |   |  ODBC Driver 17                |
    +------------------------+   +-----------------+--------------+
                                                   |
                                                   v
    +-------------------------------------------------------------+
    |  Verificar existencia del SP:                               |
    |  SELECT 1 FROM sys.procedures WHERE name = ?                |
    +-------------------------+-----------------------------------+
                              |
              +---------------+---------------+
              |      ¿SP existe?              |
              +---------------+---------------+
                     |                |
                     | NO             | SI
                     v                v
    +------------------------+   +--------------------------------+
    |  RuntimeError          |   |  EXEC SP con 6 parametros      |
    |  "SP no existe"        |   |  @dias, @batch, @rango,        |
    +------------------------+   |  @taxCodes, @invoiceCodes,     |
                                 |  @estadosOmitidos              |
                                 +-----------------+--------------+
                                                   |
                                                   v
    +-------------------------------------------------------------+
    |  ResultSet 1: RESUMEN                                       |
    |  - FechaEjecucion                                           |
    |  - RegistrosProcesados, RegistrosConNovedad                 |
    |  - RegistrosAnoCerrado                                      |
    |  - HasDocumentCurrencyCode, HasCalculationRate              |
    |  - HasVlrPagarCop, HasPaymentMeans                          |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Consumir ResultSet 2 (si existe) - evitar cursor sucio     |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  build_report_ascii()                                       |
    |  - Determinar Estado: "OK" o "CON_CAMBIOS"                  |
    |  - Formatear metricas en lineas ASCII                       |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  SetVar("vLocStrResultadoSP", True)                         |
    |  SetVar("vLocStrResumenSP", reporte)                        |
    |  return True, reporte                                       |
    |                          FIN                                |
    +-------------------------------------------------------------+

================================================================================
VARIABLES DE ENTRADA/SALIDA
================================================================================

Variables de Entrada (GetVar):
------------------------------
    vLocDicConfig : dict o str
        {
            "ServidorBaseDatos": "SERVIDOR\\INSTANCIA",
            "NombreBaseDatos": "CxP_Database",
            "DiasMaximos": 120,              # o "PlazoMaximo"
            "BatchSize": 500,                # o "Lote"
            "RangoMaxValor": 500,
            "TaxLevelCodes": "O-13,O-15,O-23,O-47,R-99-PN",
            "InvoiceTypecodes": "01,02,03,04,91,92,96",
            "ListadoEstados": "APROBADO,RECHAZADO,..."
        }

Variables de Salida (SetVar):
-----------------------------
    vLocStrResultadoSP : bool
        True si exito, False si error.
        
    vLocStrResumenSP : str
        Reporte multilinea en ASCII:
        
        "Estado=OK
         PUNTO E | SP=CxP.HU4_E_CamposReglamentarios | Fecha=2025-01-15
         Procesados=1500 | ConNovedad=50 | AnoCerrado=10
         DiasMaximos=120 | BatchSize=500 | RangoMaxValor=500
         HasDocCurrency=1 | HasCalcRate=1 | HasVlrPagarCop=1 | HasPaymentMeans=1"
         
    vGblStrMensajeError : str
        Mensaje de error (vacio si exito).
        
    vGblStrSystemError : str
        Stack trace (vacio si exito).

================================================================================
CODIGOS POR DEFECTO
================================================================================

TaxLevelCodes (Codigos de nivel de impuesto):
    - O-13: IVA Responsable
    - O-15: Gran Contribuyente
    - O-23: Agente de Retencion
    - O-47: Regimen Simple
    - R-99-PN: Persona Natural

InvoiceTypecodes (Codigos de tipo de factura):
    - 01: Factura de Venta Nacional
    - 02: Factura de Exportacion
    - 03: Documento equivalente
    - 04: Nota Credito
    - 91: Nota Credito sin referencia
    - 92: Nota Debito sin referencia
    - 96: Nota de Ajuste

================================================================================
"""



async def ejecutar_HU4_E_ReglamentariosOperacion():
    """
    Ejecuta Punto E - Validacion de Campos Reglamentarios de Operacion.
    
    Verifica que los documentos cumplan con requisitos reglamentarios
    como codigos de impuestos, tipos de factura y otros campos obligatorios
    segun la normativa colombiana.
    
    Returns:
        tuple: (bool, str|None)
            - bool: True si exito, False si error
            - str: Reporte ASCII o None si error
    
    Side Effects:
        Lee: vLocDicConfig
        Escribe: vLocStrResultadoSP, vLocStrResumenSP,
                 vGblStrMensajeError, vGblStrSystemError
    
    Example:
        Configuracion::
        
            SetVar("vLocDicConfig", {
                "ServidorBaseDatos": "SQLPROD\\CXP",
                "NombreBaseDatos": "CuentasPorPagar",
                "DiasMaximos": 120,
                "BatchSize": 500,
                "RangoMaxValor": 500
            })
        
        Ejecucion::
        
            ok, reporte = await ejecutar_HU4_E_ReglamentariosOperacion()
            
            if ok:
                print(reporte)
                # Estado=OK
                # PUNTO E | SP=CxP.HU4_E_CamposReglamentarios | ...
    
    Note:
        - El SP debe existir en el schema [CxP]
        - Se verifica existencia antes de ejecutar
        - Los parametros de codigos tienen valores por defecto
    """
    # ==========================================================================
    # IMPORTS
    # ==========================================================================
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback
    import unicodedata
    from datetime import datetime

    # ==========================================================================
    # FUNCIONES AUXILIARES
    # ==========================================================================
    
    def safe_str(v):
        """Convierte valor a string de forma segura."""
        try:
            return "" if v is None else str(v)
        except Exception:
            return ""

    def to_ascii(v):
        """
        Convierte texto a ASCII puro, eliminando caracteres especiales.
        
        Util para evitar problemas de encoding en variables de Rocketbot.
        
        Args:
            v: Valor a convertir.
        
        Returns:
            str: Texto ASCII normalizado.
        
        Example:
            >>> to_ascii("Cafe anos")
            "Cafe anos"
            >>> to_ascii(b'\\xc3\\xa9')
            "e"
        """
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

            # Normalizar y eliminar acentos
            s = unicodedata.normalize("NFKD", s)
            s = s.encode("ascii", "ignore").decode("ascii", "ignore")
            
            # Solo caracteres imprimibles
            s = "".join(ch if 32 <= ord(ch) <= 126 else " " for ch in s)
            
            # Normalizar espacios
            s = " ".join(s.split())
            return s
        except Exception:
            return ""

    def reset_vars():
        """Inicializa variables de salida."""
        try:
            SetVar("vGblStrMensajeError", "")
            SetVar("vGblStrSystemError", "")
            SetVar("vLocStrResultadoSP", "")
            SetVar("vLocStrResumenSP", "")
        except Exception:
            pass

    def set_error_vars(user_msg, exc=None):
        """Establece variables de error."""
        try:
            SetVar("vGblStrMensajeError", to_ascii(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else to_ascii(traceback.format_exc()))
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", "")
        except Exception:
            pass

    def parse_config(raw):
        """Parsea configuracion desde JSON o literal."""
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
        """
        Normaliza valor a entero con manejo de ERROR_NOT_VAR.
        
        Args:
            v: Valor a convertir.
            default: Valor por defecto.
        
        Returns:
            int: Valor entero.
        """
        try:
            if v in ("", None, "ERROR_NOT_VAR"):
                return int(default)
            return int(v)
        except Exception:
            return int(default)

    def fmt_dt(v):
        """
        Formatea valor de fecha a string.
        
        Args:
            v: datetime o string.
        
        Returns:
            str: Fecha formateada o string ASCII.
        """
        try:
            if v is None:
                return ""
            if isinstance(v, datetime):
                return v.strftime("%Y-%m-%d %H:%M:%S")
            return to_ascii(v)
        except Exception:
            return ""

    def read_resultset_one(cur):
        """Lee una fila del cursor como diccionario."""
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
        """
        Consume todas las filas del ResultSet actual.
        
        Necesario para evitar dejar el cursor en estado inconsistente
        cuando hay multiples ResultSets.
        """
        try:
            if not cur.description:
                return
            _ = cur.fetchall()
        except Exception:
            return

    def build_report_ascii(rs1, params):
        """
        Construye reporte ASCII multilinea.
        
        Args:
            rs1: Diccionario con datos del ResultSet 1.
            params: Diccionario con parametros de ejecucion.
        
        Returns:
            str: Reporte formateado en ASCII.
        
        Example:
            >>> build_report_ascii(
            ...     {"FechaEjecucion": "2025-01-15", ...},
            ...     {"dias": 120, "batch": 500, "rango": 500}
            ... )
            "Estado=OK
             PUNTO E | SP=CxP.HU4_E_CamposReglamentarios | Fecha=2025-01-15
             ..."
        """
        if not rs1:
            return to_ascii("ERROR | SP no retorno ResultSet 1 (Resumen).")

        # Extraer valores
        fecha = fmt_dt(rs1.get("FechaEjecucion"))
        proc = to_ascii(rs1.get("RegistrosProcesados", "0"))
        nov = to_ascii(rs1.get("RegistrosConNovedad", "0"))
        ano = to_ascii(rs1.get("RegistrosAnoCerrado", "0"))

        # Flags de campos presentes
        has_doc = to_ascii(rs1.get("HasDocumentCurrencyCode", "0"))
        has_calc = to_ascii(rs1.get("HasCalculationRate", "0"))
        has_vlr = to_ascii(rs1.get("HasVlrPagarCop", "0"))
        has_pay = to_ascii(rs1.get("HasPaymentMeans", "0"))

        # Determinar estado
        estado = "OK"
        if nov != "0" or ano != "0":
            estado = "CON_CAMBIOS"

        # Construir lineas del reporte
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

    # ==========================================================================
    # INICIO
    # ==========================================================================
    reset_vars()

    # ==========================================================================
    # CONFIGURACION
    # ==========================================================================
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))

        servidor = safe_str(cfg["ServidorBaseDatos"]).replace("\\\\", "\\")
        db = safe_str(cfg["NombreBaseDatos"])

        # Parametros numericos con defaults
        dias = normalize_int(cfg.get("DiasMaximos", cfg.get("PlazoMaximo", 120)), 120)
        batch = normalize_int(cfg.get("BatchSize", cfg.get("Lote", 500)), 500)
        rango = normalize_int(cfg.get("RangoMaxValor", 500), 500)

        # Codigos de impuestos (con default)
        tax_codes = safe_str(
            cfg.get("TaxLevelCodes", cfg.get("TaxLevelCode", "O-13,O-15,O-23,O-47,R-99-PN"))
        ).strip()
        if not tax_codes:
            tax_codes = "O-13,O-15,O-23,O-47,R-99-PN"

        # Codigos de tipo de factura (con default)
        invoice_codes = safe_str(
            cfg.get("InvoiceTypecodes", cfg.get("InvoiceTypecode", "01,02,03,04,91,92,96"))
        ).strip()
        if not invoice_codes:
            invoice_codes = "01,02,03,04,91,92,96"

        # Estados a omitir (con default extenso)
        estados_omitidos = safe_str(
            cfg.get("ListadoEstados", cfg.get("EstadosOmitir", ""))
        ).strip()
        if not estados_omitidos:
            estados_omitidos = (
                "APROBADO,APROBADO CONTADO Y/O EVENTO MANUAL,"
                "APROBADO SIN CONTABILIZACION,RECHAZADO,RECLASIFICAR,"
                "RECHAZADO - RETORNADO,CON NOVEDAD - RETORNADO,"
                "EN ESPERA DE POSICIONES,NO EXITOSO"
            )

        # Guardar parametros para el reporte
        params = {"dias": dias, "batch": batch, "rango": rango}

    except Exception as e:
        set_error_vars("Error configuracion Punto E (Campos Reglamentarios)", e)
        return False, None

    # ==========================================================================
    # EJECUCION SP
    # ==========================================================================
    def run_sp_sync():
        """Ejecuta el SP de forma sincrona."""
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={servidor};"
            f"DATABASE={db};"
            "Trusted_Connection=yes;"
        )

        with pyodbc.connect(conn_str) as c:
            c.autocommit = True
            cur = c.cursor()

            # Verificar existencia del SP antes de ejecutar
            sql_check = (
                "SELECT 1 FROM sys.procedures p "
                "INNER JOIN sys.schemas s ON s.schema_id = p.schema_id "
                "WHERE s.name = ? AND p.name = ?"
            )
            cur.execute(sql_check, ("CxP", "HU4_E_CamposReglamentarios"))
            if cur.fetchone() is None:
                raise RuntimeError("SP no existe: [CxP].[HU4_E_CamposReglamentarios]")

            # Ejecutar SP con 6 parametros
            cur.execute(
                "EXEC [CxP].[HU4_E_CamposReglamentarios] ?, ?, ?, ?, ?, ?",
                dias, batch, rango, tax_codes, invoice_codes, estados_omitidos
            )

            # ResultSet 1: Resumen
            rs1 = read_resultset_one(cur)

            # Consumir ResultSet 2 para limpiar cursor
            if cur.nextset():
                consume_resultset_all(cur)

            return True, build_report_ascii(rs1, params)

    # ==========================================================================
    # WRAPPER ASYNC
    # ==========================================================================
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


# ==============================================================================
# EJEMPLOS DE USO
# ==============================================================================
"""
EJEMPLO 1: Ejecucion con parametros por defecto
-----------------------------------------------
    SetVar("vLocDicConfig", {
        "ServidorBaseDatos": "SQLPROD\\CXP",
        "NombreBaseDatos": "CuentasPorPagar",
        "DiasMaximos": 120,
        "BatchSize": 500,
        "RangoMaxValor": 500
        # TaxLevelCodes, InvoiceTypecodes, ListadoEstados usan defaults
    })
    
    ok, reporte = await ejecutar_HU4_E_ReglamentariosOperacion()
    
    if ok:
        print(reporte)
        # Estado=OK
        # PUNTO E | SP=CxP.HU4_E_CamposReglamentarios | Fecha=2025-01-15 10:30:00
        # Procesados=1500 | ConNovedad=50 | AnoCerrado=10
        # DiasMaximos=120 | BatchSize=500 | RangoMaxValor=500
        # HasDocCurrency=1 | HasCalcRate=1 | HasVlrPagarCop=1 | HasPaymentMeans=1

EJEMPLO 2: Con codigos personalizados
-------------------------------------
    SetVar("vLocDicConfig", {
        "ServidorBaseDatos": "SQLPROD\\CXP",
        "NombreBaseDatos": "CuentasPorPagar",
        "DiasMaximos": 90,
        "BatchSize": 1000,
        "RangoMaxValor": 1000,
        "TaxLevelCodes": "O-13,O-15",  # Solo estos codigos
        "InvoiceTypecodes": "01,04",    # Solo facturas y notas credito
        "ListadoEstados": "RECHAZADO,NO EXITOSO"  # Solo omitir estos
    })
    
    ok, reporte = await ejecutar_HU4_E_ReglamentariosOperacion()

EJEMPLO 3: Interpretacion del resultado
---------------------------------------
    ok, reporte = await ejecutar_HU4_E_ReglamentariosOperacion()
    
    if ok:
        resumen = GetVar("vLocStrResumenSP")
        
        # Parsear estado
        if "Estado=CON_CAMBIOS" in resumen:
            print("⚠️ Se detectaron novedades o anos cerrados")
        else:
            print("✓ Procesamiento sin novedades")
        
        # Verificar campos
        if "HasDocCurrency=0" in resumen:
            print("⚠️ Falta DocumentCurrencyCode en algunos registros")
        if "HasPaymentMeans=0" in resumen:
            print("⚠️ Falta PaymentMeans en algunos registros")

EJEMPLO 4: Manejo de errores
----------------------------
    # Error: SP no existe
    ok, _ = await ejecutar_HU4_E_ReglamentariosOperacion()
    # ok = False
    # GetVar("vGblStrSystemError") contiene "RuntimeError: SP no existe: ..."
"""
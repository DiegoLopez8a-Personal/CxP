#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: ejecutar_HU4_I_NumLiquidacion_50_QUEUE.py
================================================================================

Descripcion General:
--------------------
    Este modulo ejecuta la fase QUEUE del Punto I del proceso HU4 - Numero de
    Liquidacion 50. Es la primera parte de un proceso de dos fases (QUEUE/FINALIZE)
    para el manejo de documentos con numero de liquidacion especifico.
    
    QUEUE: Obtiene la lista de IDs candidatos para procesar y genera un BatchId
    unico para el seguimiento.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0

Stored Procedure:
-----------------
    [CxP].[HU4_I_NumLiquidacion_50]
    
    Parametros (modo QUEUE):
        @executionNum INT - Numero de ejecucion (opcional)
        @DiasMaximos INT - Dias maximos para filtrar
        @UseBogotaTime BIT - Usar hora de Bogota (0/1)
        @BatchSize INT - Tamanio del lote
        @Modo = 'QUEUE'
        @BatchId = NULL - Se genera automaticamente
        @ResultadosJson = NULL - No aplica en QUEUE

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |       ejecutar_HU4_I_NumLiquidacion_50_QUEUE()              |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  reset_vars() - Inicializar TODAS las variables:            |
    |  - vGblStrMensajeError = ""                                 |
    |  - vGblStrSystemError = ""                                  |
    |  - vLocStrResultadoSP = ""                                  |
    |  - vLocStrResumenSP = ""                                    |
    |  - vLocStrBatchIdPuntoI = ""                                |
    |  - vLocJsonFileOpsPuntoI = "[]"                             |
    |  - vLocJsonResultadosFileOpsPuntoI = "[]"                   |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Parsear configuracion:                                     |
    |  - ServidorBaseDatos, NombreBaseDatos                       |
    |  - vGblIntExecutionNum (o config executionNum)              |
    |  - DiasMaximos, BatchSize                                   |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Conectar a SQL Server y ejecutar SP:                       |
    |  EXEC [CxP].[HU4_I_NumLiquidacion_50] @Modo='QUEUE'...      |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Iterar sobre TODOS los ResultSets:                         |
    |  - Buscar columna "BatchId" para extraer GUID               |
    |  - Agregar filas a lista de operaciones                     |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Guardar resultados:                                        |
    |  - SetVar("vLocStrBatchIdPuntoI", batch_id)                 |
    |  - SetVar("vLocJsonFileOpsPuntoI", json.dumps(filas))       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  return True, filas (lista de dicts)                        |
    +-------------------------------------------------------------+

================================================================================
VARIABLES DE ENTRADA/SALIDA
================================================================================

Variables de Entrada (GetVar):
------------------------------
    vLocDicConfig : dict o str
        Configuracion con ServidorBaseDatos, NombreBaseDatos, DiasMaximos, etc.
    
    vGblIntExecutionNum : int (opcional)
        Numero de ejecucion. Tiene prioridad sobre config.

Variables de Salida (SetVar):
-----------------------------
    vLocStrBatchIdPuntoI : str - GUID unico del batch.
    vLocJsonFileOpsPuntoI : str (JSON) - Lista de registros a procesar.
    vLocJsonResultadosFileOpsPuntoI : str - Inicializado a "[]".
    vLocStrResultadoSP : bool - True si exito.
    vLocStrResumenSP : str - Resumen de ejecucion.

================================================================================
DIFERENCIAS CON PUNTO H
================================================================================

    Punto H (Agrupacion):
        - Trabaja con archivos fisicos individuales
        - Operacion: MOVER (shutil.move)
        - Campos: RutaOrigenFull, NombreArchivo
        
    Punto I (NumLiquidacion_50):
        - Trabaja con IDs de registros
        - Puede tener multiples archivos por registro (separados por ;)
        - Operacion: COPIAR (shutil.copy2)
        - Campos: RutaOrigen, NombresArchivos (multiples)

================================================================================
"""


async def ejecutar_HU4_I_NumLiquidacion_50_QUEUE():
    """
    Ejecuta [CxP].[HU4_I_NumLiquidacion_50] en modo QUEUE.
    
    Fase inicial del proceso de dos fases para Punto I. Obtiene la lista
    de IDs candidatos y genera un BatchId unico para seguimiento.
    
    Returns:
        tuple: (bool, list|None)
            - bool: True si exito, False si error
            - list: Lista de dicts con operaciones, o None si error
    
    Example:
        SetVar("vLocDicConfig", {
            "ServidorBaseDatos": "SQLPROD\\CXP",
            "NombreBaseDatos": "CuentasPorPagar",
            "DiasMaximos": 120,
            "BatchSize": 500
        })
        
        ok, registros = await ejecutar_HU4_I_NumLiquidacion_50_QUEUE()
        
        if ok and registros:
            batch_id = GetVar("vLocStrBatchIdPuntoI")
            print(f"BatchId: {batch_id}")
            print(f"IDs a procesar: {len(registros)}")
    """
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback
    import re
    import unicodedata

    def safe_str(v):
        """Convierte valor a string de forma segura."""
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
        """Convierte texto a ASCII puro."""
        try:
            s = "" if s is None else str(s)
            s = unicodedata.normalize("NFKD", s)
            s = s.encode("ascii", "ignore").decode("ascii", "ignore")
            s = "".join(ch if 32 <= ord(ch) <= 126 else " " for ch in s)
            return " ".join(s.split())
        except Exception:
            return ""

    def reset_vars():
        """Inicializa TODAS las variables de salida para Punto I."""
        try:
            SetVar("vGblStrMensajeError", "")
            SetVar("vGblStrSystemError", "")
            SetVar("vLocStrResultadoSP", "")
            SetVar("vLocStrResumenSP", "")
            SetVar("vLocStrBatchIdPuntoI", "")
            SetVar("vLocJsonFileOpsPuntoI", "[]")
            SetVar("vLocJsonResultadosFileOpsPuntoI", "[]")
        except Exception:
            pass

    def set_error(user_msg, exc=None):
        """Establece variables de error."""
        try:
            SetVar("vGblStrMensajeError", to_ascii(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else to_ascii(traceback.format_exc()))
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", to_ascii(user_msg))
            SetVar("vLocStrBatchIdPuntoI", "")
            SetVar("vLocJsonFileOpsPuntoI", "[]")
        except Exception:
            pass

    def parse_config(raw):
        """Parsea configuracion desde JSON o literal."""
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
        """Verifica si valor esta ausente."""
        return v in ("", None, "ERROR_NOT_VAR")

    def to_int(v, default):
        """Convierte a entero con default."""
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
        """Convierte a entero o None si no valido."""
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
        """Extrae y normaliza un GUID de un texto."""
        t = safe_str(x).strip().replace("{", "").replace("}", "")
        m = re.search(
            r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
            t
        )
        return m.group(1) if m else ""

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

        execution_num = to_int_or_none(GetVar("vGblIntExecutionNum"))
        if execution_num is None:
            execution_num = to_int_or_none(cfg.get("executionNum"))

        dias_max = to_int(cfg.get("DiasMaximos", cfg.get("PlazoMaximo", 120)), 120)
        batch = to_int(cfg.get("BatchSize", cfg.get("Lote", 500)), 500)

    except Exception as e:
        set_error("ERROR Punto I QUEUE | configuracion", e)
        return False, None

    # ==========================================================================
    # EJECUCION SP
    # ==========================================================================
    def run_sp_sync():
        """Ejecuta SP de forma sincrona."""
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={servidor};"
            f"DATABASE={db};"
            "Trusted_Connection=yes;"
        )

        filas = []
        batch_id = ""

        with pyodbc.connect(conn_str, unicode_results=False) as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(
                "EXEC [CxP].[HU4_I_NumLiquidacion_50] "
                "@executionNum=?, @DiasMaximos=?, @UseBogotaTime=?, @BatchSize=?, "
                "@Modo=?, @BatchId=?, @ResultadosJson=?;",
                execution_num, dias_max, 0, batch, "QUEUE", None, None
            )

            while True:
                if cur.description:
                    cols = [safe_str(c[0]) for c in cur.description]
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

        SetVar("vLocStrBatchIdPuntoI", batch_id)
        SetVar("vLocJsonFileOpsPuntoI", json.dumps(filas, ensure_ascii=True))

        resumen = (
            f"Punto I QUEUE | SP=CxP.HU4_I_NumLiquidacion_50 | "
            f"BatchId={batch_id if batch_id else 'NO_DISPONIBLE'} | "
            f"IDsEnCola={len(filas)} | "
            f"executionNum={execution_num if execution_num is not None else 'NULL'} | "
            f"DiasMaximos={dias_max} | BatchSize={batch}"
        )
        return True, filas, resumen

    # ==========================================================================
    # WRAPPER ASYNC
    # ==========================================================================
    try:
        loop = asyncio.get_running_loop()
        ok, filas, resumen = await loop.run_in_executor(None, run_sp_sync)
        
        SetVar("vLocStrResultadoSP", bool(ok))
        SetVar("vLocStrResumenSP", to_ascii(resumen))
        return bool(ok), filas
        
    except Exception as e:
        set_error("ERROR Punto I QUEUE | ejecucion", e)
        return False, None


# ==============================================================================
# EJEMPLOS DE USO
# ==============================================================================
"""
EJEMPLO 1: Ejecucion basica
---------------------------
    SetVar("vLocDicConfig", {
        "ServidorBaseDatos": "SQLPROD\\CXP",
        "NombreBaseDatos": "CuentasPorPagar",
        "DiasMaximos": 120,
        "BatchSize": 500
    })
    
    ok, registros = await ejecutar_HU4_I_NumLiquidacion_50_QUEUE()
    
    if ok:
        batch_id = GetVar("vLocStrBatchIdPuntoI")
        print(f"BatchId: {batch_id}")
        print(f"Registros: {len(registros)}")

EJEMPLO 2: Procesar registros con multiples archivos
----------------------------------------------------
    ok, registros = await ejecutar_HU4_I_NumLiquidacion_50_QUEUE()
    
    if ok:
        for reg in registros:
            id_reg = reg.get("ID_registro")
            nombres = reg.get("NombresArchivos", "")
            # NombresArchivos puede ser "doc1.pdf;doc2.pdf;doc3.pdf"
            archivos = [f.strip() for f in nombres.split(";") if f.strip()]
            print(f"ID {id_reg}: {len(archivos)} archivos")

EJEMPLO 3: Flujo completo QUEUE -> COPY -> FINALIZE
---------------------------------------------------
    # === PASO 1: QUEUE ===
    ok, registros = await ejecutar_HU4_I_NumLiquidacion_50_QUEUE()
    
    # === PASO 2: COPIAR ARCHIVOS ===
    ok, resultados = await ejecutar_FileOps_PuntoI_COPIAR()
    
    # === PASO 3: FINALIZE ===
    ok, resumen = await ejecutar_HU4_I_NumLiquidacion_50_FINALIZE()
"""
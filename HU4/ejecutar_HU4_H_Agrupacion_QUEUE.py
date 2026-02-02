#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: ejecutar_HU4_H_Agrupacion_QUEUE.py
================================================================================

Descripcion General:
--------------------
    Este modulo ejecuta la fase QUEUE del Punto H del proceso HU4 - Agrupacion
    de documentos. Es la primera parte de un proceso de dos fases (QUEUE/FINALIZE)
    que permite mover archivos de forma controlada.
    
    QUEUE: Obtiene la lista de archivos candidatos para mover y genera un BatchId
    unico para el seguimiento del proceso.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Fecha de Creacion: 2025

Patron QUEUE/FINALIZE:
----------------------
    1. QUEUE: Obtiene candidatos del SP, genera BatchId, retorna lista de archivos
    2. [Proceso externo]: Mueve archivos fisicamente usando la lista
    3. FINALIZE: Reporta resultados al SP para actualizar estado en BD

Stored Procedure:
-----------------
    [CxP].[HU4_H_Agrupacion]
    
    Parametros (modo QUEUE):
        @Modo = 'QUEUE'
        @executionNum INT - Numero de ejecucion (opcional)
        @BatchId = NULL - Se genera automaticamente
        @DiasMaximos INT - Dias maximos para filtrar
        @UseBogotaTime BIT - Usar hora de Bogota (0/1)
        @BatchSize INT - Tamano del lote
        @ResultadosJson = NULL - No aplica en QUEUE

Dependencias:
-------------
    - asyncio: Ejecucion asincrona
    - pyodbc: Conexion a SQL Server
    - json, ast: Parseo de configuracion
    - traceback: Captura de errores
    - re: Extraccion de GUID
    - unicodedata: Normalizacion de texto

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |           ejecutar_HU4_H_Agrupacion_QUEUE()                 |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  reset_vars() - Inicializar TODAS las variables:            |
    |  - vGblStrMensajeError = ""                                 |
    |  - vGblStrSystemError = ""                                  |
    |  - vLocStrResultadoSP = ""                                  |
    |  - vLocStrResumenSP = ""                                    |
    |  - vLocStrBatchIdPuntoH = ""           <- BatchId           |
    |  - vLocJsonFileOpsPuntoH = ""          <- Lista archivos    |
    |  - vLocJsonResultadosFileOpsPuntoH = "" <- Para FINALIZE    |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Parsear configuracion:                                     |
    |  - ServidorBaseDatos, NombreBaseDatos                       |
    |  - vGblIntExecutionNum (o config executionNum)              |
    |  - DiasMaximos, BatchSize                                   |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Conectar a SQL Server y ejecutar SP:                       |
    |  EXEC [CxP].[HU4_H_Agrupacion]                              |
    |    @Modo='QUEUE', @executionNum=?, @BatchId=NULL,           |
    |    @DiasMaximos=?, @UseBogotaTime=0, @BatchSize=?,          |
    |    @ResultadosJson=NULL                                     |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Iterar sobre TODOS los ResultSets:                         |
    |  while True:                                                |
    |    - Leer columnas y filas                                  |
    |    - Buscar columna "BatchId" para extraer GUID             |
    |    - Agregar filas a lista de operaciones                   |
    |    - if not cur.nextset(): break                            |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  ¿Se encontro BatchId?                                      |
    |  - Si no, buscar en las filas como fallback                 |
    |  - normalize_guid_text() para limpiar GUID                  |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Guardar resultados:                                        |
    |  - SetVar("vLocStrBatchIdPuntoH", batch_id)                 |
    |  - SetVar("vLocJsonFileOpsPuntoH", json.dumps(filas))       |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Generar resumen:                                           |
    |  "Punto H QUEUE | SP=CxP.HU4_H_Agrupacion |                 |
    |   BatchId=XXXXXXXX-... | ArchivosEnCola=N | ..."            |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  return True, filas (lista de dicts)                        |
    |                        FIN                                  |
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
            "DiasMaximos": 120,
            "BatchSize": 500,
            "executionNum": 1
        }
    
    vGblIntExecutionNum : int (opcional)
        Numero de ejecucion. Si esta presente, tiene prioridad sobre config.

Variables de Salida (SetVar):
-----------------------------
    vLocStrBatchIdPuntoH : str
        GUID unico del batch para tracking.
        Ejemplo: "A1B2C3D4-E5F6-7890-ABCD-EF1234567890"
        
    vLocJsonFileOpsPuntoH : str (JSON)
        Lista de archivos a mover en formato JSON:
        [
            {
                "ID_registro": "12345",
                "BatchId": "A1B2C3D4-...",
                "RutaOrigenFull": "C:\\Origen\\archivo.pdf",
                "CarpetaDestino": "C:\\Destino\\",
                "NombreArchivo": "archivo.pdf"
            },
            ...
        ]
        
    vLocStrResultadoSP : bool
        True si exito, False si error.
        
    vLocStrResumenSP : str
        Resumen legible del proceso.

================================================================================
ESTRUCTURA DE FILAS (FileOps)
================================================================================

Cada fila en vLocJsonFileOpsPuntoH tiene la estructura:

    {
        "ID_registro": "12345",           # ID unico del documento
        "BatchId": "A1B2C3D4-...",        # GUID del batch
        "RutaOrigenFull": "C:\\...",      # Ruta completa del archivo origen
        "CarpetaDestino": "C:\\...",      # Carpeta destino (sin nombre archivo)
        "NombreArchivo": "documento.pdf"  # Nombre del archivo a mover
    }

================================================================================
"""


async def ejecutar_HU4_H_Agrupacion_QUEUE():
    """
    Ejecuta [CxP].[HU4_H_Agrupacion] en modo QUEUE.
    
    Fase inicial del proceso de dos fases. Obtiene la lista de archivos
    candidatos para mover y genera un BatchId unico para seguimiento.
    
    Returns:
        tuple: (bool, list|None)
            - bool: True si exito, False si error
            - list: Lista de dicts con operaciones de archivo, o None si error
    
    Side Effects:
        Lee:
            - vLocDicConfig
            - vGblIntExecutionNum (opcional)
        Escribe:
            - vLocStrBatchIdPuntoH (GUID del batch)
            - vLocJsonFileOpsPuntoH (JSON con lista de archivos)
            - vLocStrResultadoSP
            - vLocStrResumenSP
            - vGblStrMensajeError
            - vGblStrSystemError
    
    Example:
        Configuracion::
        
            SetVar("vLocDicConfig", {
                "ServidorBaseDatos": "SQLPROD\\CXP",
                "NombreBaseDatos": "CuentasPorPagar",
                "DiasMaximos": 120,
                "BatchSize": 500
            })
        
        Ejecucion::
        
            ok, archivos = await ejecutar_HU4_H_Agrupacion_QUEUE()
            
            if ok and archivos:
                batch_id = GetVar("vLocStrBatchIdPuntoH")
                print(f"BatchId: {batch_id}")
                print(f"Archivos a mover: {len(archivos)}")
                
                # Mover archivos fisicamente...
                for op in archivos:
                    src = op["RutaOrigenFull"]
                    dst = op["CarpetaDestino"]
                    # shutil.move(src, dst + op["NombreArchivo"])
    
    Note:
        - El BatchId es esencial para el FINALIZE posterior
        - Si no hay archivos, la lista estara vacia pero es exito
        - El proceso externo debe mover archivos antes de FINALIZE
    """
    # ==========================================================================
    # IMPORTS
    # ==========================================================================
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback
    import re
    import unicodedata

    # ==========================================================================
    # FUNCIONES AUXILIARES
    # ==========================================================================
    
    def safe_str(v):
        """
        Convierte valor a string de forma segura.
        Maneja bytes con multiples encodings.
        """
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
        """Inicializa TODAS las variables de salida incluyendo especificas de Punto H."""
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
        """Establece variables de error."""
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
        """
        Extrae y normaliza un GUID de un texto.
        
        Args:
            x: Texto que puede contener un GUID.
        
        Returns:
            str: GUID normalizado (sin llaves) o string vacio.
        
        Example:
            >>> normalize_guid_text("{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}")
            "A1B2C3D4-E5F6-7890-ABCD-EF1234567890"
            >>> normalize_guid_text("texto sin guid")
            ""
        """
        t = safe_str(x).strip()
        if not t:
            return ""
        t = t.replace("{", "").replace("}", "").strip()
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

        # executionNum: variable global tiene prioridad
        execution_num = to_int_or_none(GetVar("vGblIntExecutionNum"))
        if execution_num is None:
            execution_num = to_int_or_none(cfg.get("executionNum"))

        dias_max = to_int(cfg.get("DiasMaximos", cfg.get("PlazoMaximo", 120)), 120)
        batch = to_int(cfg.get("BatchSize", cfg.get("Lote", 500)), 500)

    except Exception as e:
        set_error_vars("ERROR Punto H QUEUE | configuracion", e)
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

        with pyodbc.connect(conn_str) as conn:
            conn.autocommit = True
            cur = conn.cursor()

            # Ejecutar SP en modo QUEUE
            cur.execute(
                "EXEC [CxP].[HU4_H_Agrupacion] "
                "@Modo=?, @executionNum=?, @BatchId=?, @DiasMaximos=?, "
                "@UseBogotaTime=?, @BatchSize=?, @ResultadosJson=?;",
                "QUEUE",        # Modo
                execution_num,  # executionNum (puede ser None)
                None,           # BatchId (NULL, se genera en SP)
                dias_max,       # DiasMaximos
                0,              # UseBogotaTime
                batch,          # BatchSize
                None            # ResultadosJson (NULL en QUEUE)
            )

            # Iterar sobre TODOS los ResultSets
            while True:
                if cur.description:
                    cols = [c[0] for c in cur.description]
                    cols_lower = [safe_str(c[0]).lower() for c in cur.description]
                    
                    try:
                        rows = cur.fetchall()
                    except Exception:
                        rows = []

                    if rows:
                        # Buscar BatchId en primera fila
                        if (not batch_id) and ("batchid" in cols_lower):
                            idx = cols_lower.index("batchid")
                            batch_id = normalize_guid_text(rows[0][idx])

                        # Convertir filas a diccionarios
                        for r in rows:
                            d = {}
                            for i, name in enumerate(cols):
                                d[safe_str(name)] = safe_str(r[i])
                            filas.append(d)

                if not cur.nextset():
                    break

        # Fallback: buscar BatchId en filas si no se encontro
        if not batch_id:
            for d in filas:
                if "BatchId" in d:
                    batch_id = normalize_guid_text(d.get("BatchId"))
                    if batch_id:
                        break

        # Guardar resultados
        try:
            SetVar("vLocStrBatchIdPuntoH", batch_id)
            SetVar("vLocJsonFileOpsPuntoH", json.dumps(filas, ensure_ascii=True))
        except Exception:
            pass

        resumen = (
            f"Punto H QUEUE | SP=CxP.HU4_H_Agrupacion | "
            f"BatchId={batch_id if batch_id else 'NO_DISPONIBLE'} | "
            f"ArchivosEnCola={len(filas)} | "
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

        try:
            SetVar("vLocStrResultadoSP", bool(ok))
            SetVar("vLocStrResumenSP", to_ascii(resumen))
        except Exception:
            pass

        return bool(ok), filas

    except Exception as e:
        set_error_vars("ERROR Punto H QUEUE | ejecucion", e)
        return False, None


# ==============================================================================
# EJEMPLOS DE USO
# ==============================================================================
"""
EJEMPLO 1: Ejecucion completa del flujo QUEUE -> MOVE -> FINALIZE
---------------------------------------------------------------
    # === PASO 1: QUEUE ===
    SetVar("vLocDicConfig", {
        "ServidorBaseDatos": "SQLPROD\\CXP",
        "NombreBaseDatos": "CuentasPorPagar",
        "DiasMaximos": 120,
        "BatchSize": 500
    })
    
    ok, archivos = await ejecutar_HU4_H_Agrupacion_QUEUE()
    
    if not ok:
        print(f"Error en QUEUE: {GetVar('vGblStrMensajeError')}")
        return
    
    batch_id = GetVar("vLocStrBatchIdPuntoH")
    print(f"BatchId: {batch_id}")
    print(f"Archivos a mover: {len(archivos)}")
    
    # === PASO 2: MOVER ARCHIVOS (proceso externo) ===
    # Tipicamente se usa ejecutar_FileOps_PuntoH_MOVER()
    # o se mueven manualmente
    
    resultados = []
    for op in archivos:
        try:
            src = op["RutaOrigenFull"]
            dst_dir = op["CarpetaDestino"]
            nombre = op["NombreArchivo"]
            
            import shutil, os
            os.makedirs(dst_dir, exist_ok=True)
            shutil.move(src, os.path.join(dst_dir, nombre))
            
            resultados.append({
                "ID_registro": op["ID_registro"],
                "MovimientoExitoso": True,
                "NuevaRutaArchivo": dst_dir,
                "ErrorMsg": ""
            })
        except Exception as e:
            resultados.append({
                "ID_registro": op["ID_registro"],
                "MovimientoExitoso": False,
                "NuevaRutaArchivo": "",
                "ErrorMsg": str(e)
            })
    
    # Guardar resultados para FINALIZE
    SetVar("vLocJsonResultadosFileOpsPuntoH", json.dumps(resultados))
    
    # === PASO 3: FINALIZE ===
    ok, resumen = await ejecutar_HU4_H_Agrupacion_FINALIZE()
    print(f"FINALIZE: {resumen}")

EJEMPLO 2: Verificar si hay archivos antes de procesar
------------------------------------------------------
    ok, archivos = await ejecutar_HU4_H_Agrupacion_QUEUE()
    
    if ok:
        if not archivos or len(archivos) == 0:
            print("No hay archivos para mover en este batch")
        else:
            print(f"Procesando {len(archivos)} archivos...")
            # Continuar con el movimiento

EJEMPLO 3: Acceso al JSON de operaciones desde Rocketbot
--------------------------------------------------------
    ok, _ = await ejecutar_HU4_H_Agrupacion_QUEUE()
    
    # En Rocketbot, obtener el JSON
    json_ops = GetVar("vLocJsonFileOpsPuntoH")
    
    # Parsear
    import json
    operaciones = json.loads(json_ops)
    
    for op in operaciones:
        print(f"ID: {op['ID_registro']}")
        print(f"  Origen: {op['RutaOrigenFull']}")
        print(f"  Destino: {op['CarpetaDestino']}")
        print(f"  Archivo: {op['NombreArchivo']}")
"""
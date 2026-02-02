#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: ejecutar_HU4_H_Agrupacion_FINALIZE.py
================================================================================

Descripcion General:
--------------------
    Este modulo ejecuta la fase FINALIZE del Punto H del proceso HU4 - Agrupacion
    de documentos. Es la segunda parte de un proceso de dos fases (QUEUE/FINALIZE)
    que reporta los resultados del movimiento de archivos al SP.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0

Stored Procedure:
-----------------
    [CxP].[HU4_H_Agrupacion]
    
    Parametros (modo FINALIZE):
        @Modo = 'FINALIZE'
        @executionNum INT - Numero de ejecucion (opcional)
        @BatchId UNIQUEIDENTIFIER - BatchId del QUEUE (REQUERIDO)
        @DiasMaximos INT - Dias maximos
        @UseBogotaTime BIT - Usar hora de Bogota (0/1)
        @BatchSize INT - Tamanio del lote
        @ResultadosJson NVARCHAR(MAX) - JSON con resultados del movimiento

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |         ejecutar_HU4_H_Agrupacion_FINALIZE()                |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Parsear configuracion:                                     |
    |  - ServidorBaseDatos, NombreBaseDatos                       |
    |  - DiasMaximos, BatchSize, executionNum                     |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Obtener BatchId y ResultadosJson:                          |
    |  - batch_id = GetVar("vLocStrBatchIdPuntoH")                |
    |  - resultados = GetVar("vLocJsonResultadosFileOpsPuntoH")   |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Validar BatchId con normalize_guid_text()                  |
    |  Si vacio -> ERROR, return False                            |
    +-----------------------------+-------------------------------+
                                  |
                  +---------------+---------------+
                  |      Es BatchId valido?       |
                  +---------------+---------------+
                         |                |
                         | NO             | SI
                         v                v
    +------------------------+   +--------------------------------+
    |  set_error()           |   |  Conectar a SQL Server         |
    |  return False, None    |   +----------------+---------------+
    +------------------------+                    |
                                                  v
    +-------------------------------------------------------------+
    |  EXEC [CxP].[HU4_H_Agrupacion] @Modo='FINALIZE'...          |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  ResultSet 1: RESUMEN (OK, FAIL)                            |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Generar resumen y retornar                                 |
    |  return True, resumen                                       |
    +-------------------------------------------------------------+

================================================================================
VARIABLES DE ENTRADA/SALIDA
================================================================================

Variables de Entrada (GetVar):
------------------------------
    vLocDicConfig : dict o str
        Configuracion con ServidorBaseDatos, NombreBaseDatos, etc.
    
    vLocStrBatchIdPuntoH : str (REQUERIDO)
        GUID del batch generado en la fase QUEUE.
        
    vLocJsonResultadosFileOpsPuntoH : str (JSON)
        Resultados del movimiento de archivos.

Variables de Salida (SetVar):
-----------------------------
    vLocStrResultadoSP : bool - True si exito, False si error.
    vLocStrResumenSP : str - Resumen de ejecucion.
    vGblStrMensajeError : str - Mensaje de error.
    vGblStrSystemError : str - Stack trace.

================================================================================
"""


async def ejecutar_HU4_H_Agrupacion_FINALIZE():
    """
    Ejecuta [CxP].[HU4_H_Agrupacion] en modo FINALIZE.
    
    Fase final del proceso de dos fases. Reporta los resultados del
    movimiento de archivos al SP para actualizar estados en BD.
    
    Returns:
        tuple: (bool, str|None)
            - bool: True si exito, False si error
            - str: Resumen de ejecucion o None si error
    
    Requires:
        - vLocStrBatchIdPuntoH: BatchId del QUEUE (OBLIGATORIO)
        - vLocJsonResultadosFileOpsPuntoH: JSON con resultados
    
    Example:
        # Despues de ejecutar QUEUE y mover archivos
        ok, resumen = await ejecutar_HU4_H_Agrupacion_FINALIZE()
        
        if ok:
            print(f"FINALIZE exitoso: {resumen}")
            # Punto H FINALIZE OK | BatchId=A1B2C3D4-... | OK=145 | FAIL=5
    """
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback
    import unicodedata
    import re

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

    def normalize_guid_text(x):
        """Extrae y normaliza un GUID de un texto."""
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
        """Establece variables de error."""
        try:
            SetVar("vGblStrMensajeError", to_ascii(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else to_ascii(traceback.format_exc()))
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", to_ascii(user_msg))
        except Exception:
            pass

    # ==========================================================================
    # CONFIGURACION
    # ==========================================================================
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

    # ==========================================================================
    # OBTENER BATCH ID Y RESULTADOS
    # ==========================================================================
    batch_id = normalize_guid_text(GetVar("vLocStrBatchIdPuntoH"))
    resultados_json = safe_str(GetVar("vLocJsonResultadosFileOpsPuntoH")).strip()
    if not resultados_json:
        resultados_json = "[]"

    if not batch_id:
        set_error("ERROR Punto H FINALIZE | BatchId vacio o invalido (vLocStrBatchIdPuntoH).")
        return False, None

    # ==========================================================================
    # EJECUCION SP
    # ==========================================================================
    def run_finalize_sync():
        """Ejecuta SP FINALIZE de forma sincrona."""
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
                "@Modo=?, @executionNum=?, @BatchId=?, @DiasMaximos=?, "
                "@UseBogotaTime=?, @BatchSize=?, @ResultadosJson=?;",
                "FINALIZE", exec_num, batch_id, dias_max, 0, batch_size, resultados_json
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

    # ==========================================================================
    # WRAPPER ASYNC
    # ==========================================================================
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


# ==============================================================================
# EJEMPLOS DE USO
# ==============================================================================
"""
EJEMPLO 1: Flujo completo QUEUE -> MOVE -> FINALIZE
---------------------------------------------------
    # === PASO 1: QUEUE ===
    ok, archivos = await ejecutar_HU4_H_Agrupacion_QUEUE()
    batch_id = GetVar("vLocStrBatchIdPuntoH")
    
    # === PASO 2: MOVER ARCHIVOS ===
    ok, resultados = await ejecutar_FileOps_PuntoH_MOVER()
    
    # === PASO 3: FINALIZE ===
    ok, resumen = await ejecutar_HU4_H_Agrupacion_FINALIZE()
    print(f"Proceso completado: {resumen}")

EJEMPLO 2: FINALIZE manual con resultados propios
-------------------------------------------------
    resultados = [
        {"ID_registro": "123", "MovimientoExitoso": True, 
         "NuevaRutaArchivo": "C:\\Destino", "ErrorMsg": ""},
        {"ID_registro": "124", "MovimientoExitoso": False, 
         "NuevaRutaArchivo": "", "ErrorMsg": "Permiso denegado"},
    ]
    SetVar("vLocJsonResultadosFileOpsPuntoH", json.dumps(resultados))
    ok, resumen = await ejecutar_HU4_H_Agrupacion_FINALIZE()

EJEMPLO 3: Error por BatchId faltante
-------------------------------------
    SetVar("vLocStrBatchIdPuntoH", "")
    ok, resumen = await ejecutar_HU4_H_Agrupacion_FINALIZE()
    # ok = False, GetVar("vGblStrMensajeError") contiene error
"""
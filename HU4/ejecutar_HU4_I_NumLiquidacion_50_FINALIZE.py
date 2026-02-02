#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: ejecutar_HU4_I_NumLiquidacion_50_FINALIZE.py
================================================================================

Descripcion General:
--------------------
    Este modulo ejecuta la fase FINALIZE del Punto I del proceso HU4 - Numero
    de Liquidacion 50. Es la segunda parte de un proceso de dos fases
    (QUEUE/FINALIZE) que reporta los resultados de la copia de archivos.
    
    CARACTERISTICA ESPECIAL: Si el QUEUE no devolvio candidatos (lista vacia),
    este FINALIZE retorna exito sin ejecutar el SP.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0

Stored Procedure:
-----------------
    [CxP].[HU4_I_NumLiquidacion_50]
    
    Parametros (modo FINALIZE):
        @executionNum INT - Numero de ejecucion (opcional)
        @DiasMaximos INT - Dias maximos
        @UseBogotaTime BIT - Usar hora de Bogota (0/1)
        @BatchSize INT - Tamanio del lote
        @Modo = 'FINALIZE'
        @BatchId UNIQUEIDENTIFIER - BatchId del QUEUE (REQUERIDO si hay datos)
        @ResultadosJson NVARCHAR(MAX) - JSON con resultados

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |      ejecutar_HU4_I_NumLiquidacion_50_FINALIZE()            |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Verificar si QUEUE devolvio candidatos:                    |
    |  raw_queue = GetVar("vLocJsonFileOpsPuntoI")                |
    +-----------------------------+-------------------------------+
                                  |
                  +---------------+---------------+
                  | raw_queue vacio o "[]"?       |
                  +---------------+---------------+
                         |                |
                         | SI             | NO
                         v                v
    +------------------------+   +--------------------------------+
    |  Retorno temprano:     |   |  Continuar con FINALIZE normal |
    |  return True, resumen  |   +----------------+---------------+
    |  "Sin candidatos"      |                    |
    +------------------------+                    v
    +-------------------------------------------------------------+
    |  Parsear configuracion y obtener BatchId                    |
    +-----------------------------+-------------------------------+
                                  |
                  +---------------+---------------+
                  |      Es BatchId valido?       |
                  +---------------+---------------+
                         |                |
                         | NO             | SI
                         v                v
    +------------------------+   +--------------------------------+
    |  set_error()           |   |  Ejecutar SP @Modo='FINALIZE'  |
    |  return False, None    |   +----------------+---------------+
    +------------------------+                    |
                                                  v
    +-------------------------------------------------------------+
    |  ResultSet 1: RESUMEN (OK, FAIL, IDsFinalizados)            |
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
    
    vLocStrBatchIdPuntoI : str (REQUERIDO si hay datos)
        GUID del batch generado en la fase QUEUE.
        
    vLocJsonFileOpsPuntoI : str (JSON)
        Lista de operaciones del QUEUE. Se usa para detectar si hay datos.
        
    vLocJsonResultadosFileOpsPuntoI : str (JSON)
        Resultados de la copia de archivos.

Variables de Salida (SetVar):
-----------------------------
    vLocStrResultadoSP : bool - True si exito, False si error.
    vLocStrResumenSP : str - Resumen de ejecucion.
    vGblStrMensajeError : str - Mensaje de error.
    vGblStrSystemError : str - Stack trace.

================================================================================
COMPORTAMIENTO ESPECIAL: QUEUE VACIO
================================================================================

Si el QUEUE previo no devolvio candidatos (vLocJsonFileOpsPuntoI esta vacio
o es "[]"), este FINALIZE:

    1. NO ejecuta el SP
    2. Retorna exito (True)
    3. Establece resumen informativo
    
Esto permite que el flujo continue sin errores cuando no hay trabajo.

================================================================================
"""


async def ejecutar_HU4_I_NumLiquidacion_50_FINALIZE():
    """
    Ejecuta [CxP].[HU4_I_NumLiquidacion_50] en modo FINALIZE.
    
    Fase final del proceso de dos fases para Punto I. Reporta los resultados
    de la copia de archivos al SP para actualizar estados en BD.
    
    NOTA: Si el QUEUE no devolvio candidatos, retorna exito sin ejecutar SP.
    
    Returns:
        tuple: (bool, str|None)
            - bool: True si exito, False si error
            - str: Resumen de ejecucion o None si error
    
    Example:
        # Caso con datos
        ok, resumen = await ejecutar_HU4_I_NumLiquidacion_50_FINALIZE()
        # "Punto I FINALIZE OK | BatchId=... | IDsFinalizados=75 | OK=70 | FAIL=5"
        
        # Caso sin datos (QUEUE vacio)
        ok, resumen = await ejecutar_HU4_I_NumLiquidacion_50_FINALIZE()
        # ok = True, resumen = "Punto I FINALIZE | Sin candidatos en QUEUE..."
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
        t = safe_str(x).strip().replace("{", "").replace("}", "")
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
    # VERIFICAR SI QUEUE DEVOLVIO CANDIDATOS
    # ==========================================================================
    try:
        raw_queue = safe_str(GetVar("vLocJsonFileOpsPuntoI")).strip()
        hay_queue = False
        
        if raw_queue and raw_queue not in ("[]", "null", "None"):
            try:
                arr = json.loads(raw_queue)
                hay_queue = isinstance(arr, list) and len(arr) > 0
            except Exception:
                hay_queue = False

        # Si no hay datos del QUEUE, retornar exito sin ejecutar SP
        if not hay_queue:
            resumen = "Punto I FINALIZE | Sin candidatos en QUEUE (no hay nada para procesar)."
            SetVar("vLocStrResultadoSP", True)
            SetVar("vLocStrResumenSP", to_ascii(resumen))
            return True, resumen
            
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
        set_error("ERROR configuracion Punto I (FINALIZE)", e)
        return False, None

    # ==========================================================================
    # OBTENER BATCH ID Y RESULTADOS
    # ==========================================================================
    batch_id = normalize_guid_text(GetVar("vLocStrBatchIdPuntoI"))
    resultados_json = safe_str(GetVar("vLocJsonResultadosFileOpsPuntoI")).strip() or "[]"

    if not batch_id:
        set_error("ERROR Punto I FINALIZE | BatchId vacio o invalido (vLocStrBatchIdPuntoI).")
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

        with pyodbc.connect(conn_str, unicode_results=False) as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(
                "EXEC [CxP].[HU4_I_NumLiquidacion_50] "
                "@executionNum=?, @DiasMaximos=?, @UseBogotaTime=?, @BatchSize=?, "
                "@Modo=?, @BatchId=?, @ResultadosJson=?;",
                exec_num, dias_max, 0, batch_size, "FINALIZE", batch_id, resultados_json
            )

            rs1 = None
            if cur.description:
                row = cur.fetchone()
                if row:
                    cols = [safe_str(c[0]) for c in cur.description]
                    rs1 = {cols[i]: row[i] for i in range(len(cols))}

            ok_cnt = safe_str(rs1.get("OK") if rs1 else "")
            fail_cnt = safe_str(rs1.get("FAIL") if rs1 else "")
            ids_fin = safe_str(rs1.get("IDsFinalizados") if rs1 else "")

            resumen = (
                f"Punto I FINALIZE OK | BatchId={batch_id} | "
                f"IDsFinalizados={ids_fin or '0'} | "
                f"OK={ok_cnt or '0'} | FAIL={fail_cnt or '0'}"
            )
            return True, resumen

    # ==========================================================================
    # WRAPPER ASYNC
    # ==========================================================================
    try:
        loop = asyncio.get_running_loop()
        ok, resumen = await loop.run_in_executor(None, run_finalize_sync)

        SetVar("vLocStrResultadoSP", bool(ok))
        SetVar("vLocStrResumenSP", to_ascii(resumen))
        return bool(ok), resumen

    except Exception as e:
        set_error("ERROR ejecucion Punto I (FINALIZE)", e)
        return False, None


# ==============================================================================
# EJEMPLOS DE USO
# ==============================================================================
"""
EJEMPLO 1: Flujo completo QUEUE -> COPY -> FINALIZE
---------------------------------------------------
    # === PASO 1: QUEUE ===
    ok, registros = await ejecutar_HU4_I_NumLiquidacion_50_QUEUE()
    
    # === PASO 2: COPIAR ARCHIVOS ===
    ok, resultados = await ejecutar_FileOps_PuntoI_COPIAR()
    
    # === PASO 3: FINALIZE ===
    ok, resumen = await ejecutar_HU4_I_NumLiquidacion_50_FINALIZE()
    print(f"Resultado: {resumen}")

EJEMPLO 2: QUEUE sin candidatos (comportamiento especial)
---------------------------------------------------------
    # Si QUEUE no devolvio nada
    # vLocJsonFileOpsPuntoI = "[]"
    
    ok, resumen = await ejecutar_HU4_I_NumLiquidacion_50_FINALIZE()
    # ok = True (NO es error)
    # resumen = "Punto I FINALIZE | Sin candidatos en QUEUE..."
    # El SP NO se ejecuta

EJEMPLO 3: Error por BatchId faltante
-------------------------------------
    # Si hay datos pero no BatchId
    SetVar("vLocJsonFileOpsPuntoI", '[{"ID_registro": "123"}]')
    SetVar("vLocStrBatchIdPuntoI", "")  # Vacio!
    
    ok, resumen = await ejecutar_HU4_I_NumLiquidacion_50_FINALIZE()
    # ok = False
    # GetVar("vGblStrMensajeError") = "ERROR Punto I FINALIZE | BatchId vacio..."
"""
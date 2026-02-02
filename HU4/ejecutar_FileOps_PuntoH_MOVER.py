#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: ejecutar_FileOps_PuntoH_MOVER.py
================================================================================

Descripcion General:
--------------------
    Este modulo ejecuta las operaciones de movimiento de archivos para el
    Punto H del proceso HU4. Lee la lista de archivos desde vLocJsonFileOpsPuntoH
    (generada por QUEUE) y los mueve fisicamente a sus destinos.
    
    Los resultados se guardan en vLocJsonResultadosFileOpsPuntoH para ser
    consumidos por la fase FINALIZE.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0

Caracteristicas Principales:
----------------------------
    - Operacion: MOVER archivos (shutil.move)
    - Idempotencia: Si origen no existe pero destino si -> OK (ya movido)
    - Re-ejecucion: Si ambos existen, elimina destino y mueve de nuevo
    - Agrupacion: Resultados agrupados por ID_registro (1 resultado por registro)

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |            ejecutar_FileOps_PuntoH_MOVER()                  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Leer JSON de operaciones:                                  |
    |  raw = GetVar("vLocJsonFileOpsPuntoH")                      |
    |  file_ops = json.loads(raw)                                 |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Agrupar operaciones por ID_registro:                       |
    |  por_id = defaultdict(list)                                 |
    |  for op in file_ops:                                        |
    |      por_id[ID_registro].append((src, dest, filename))      |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Para cada ID_registro:                                     |
    |  +-------------------------------------------------------+  |
    |  |  Para cada archivo del registro:                      |  |
    |  |  +---------------------------------------------------+|  |
    |  |  |  Validar: ID, src, dest_dir, filename             ||  |
    |  |  +---------------------------------------------------+|  |
    |  |                        |                              |  |
    |  |                        v                              |  |
    |  |  +---------------------------------------------------+|  |
    |  |  |  Crear directorio destino (makedirs)              ||  |
    |  |  +---------------------------------------------------+|  |
    |  |                        |                              |  |
    |  |                        v                              |  |
    |  |  +---------------------------------------------------+|  |
    |  |  |  Verificar idempotencia:                          ||  |
    |  |  |  - Si !src y dst existe -> OK (ya movido)         ||  |
    |  |  |  - Si src y dst existen -> eliminar dst, mover    ||  |
    |  |  |  - Si !src y !dst -> ERROR                        ||  |
    |  |  |  - Si src existe -> shutil.move(src, dst)         ||  |
    |  |  +---------------------------------------------------+|  |
    |  +-------------------------------------------------------+  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Construir resultado por ID_registro:                       |
    |  {                                                          |
    |      "ID_registro": "12345",                                |
    |      "MovimientoExitoso": True/False,                       |
    |      "NuevaRutaArchivo": "C:\\Destino\\" o "",              |
    |      "ErrorMsg": "" o "mensaje de error"                    |
    |  }                                                          |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Guardar resultados:                                        |
    |  SetVar("vLocJsonResultadosFileOpsPuntoH", json.dumps(...)) |
    |  SetVar("vLocStrResultadoSP", True)                         |
    |  SetVar("vLocStrResumenSP", resumen)                        |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  return True, resultados                                    |
    +-------------------------------------------------------------+

================================================================================
VARIABLES DE ENTRADA/SALIDA
================================================================================

Variables de Entrada (GetVar):
------------------------------
    vLocJsonFileOpsPuntoH : str (JSON)
        Lista de operaciones generada por QUEUE:
        [
            {
                "ID_registro": "12345",
                "RutaOrigenFull": "C:\\Origen\\archivo.pdf",
                "CarpetaDestino": "C:\\Destino\\",
                "NombreArchivo": "archivo.pdf"
            },
            ...
        ]

Variables de Salida (SetVar):
-----------------------------
    vLocJsonResultadosFileOpsPuntoH : str (JSON)
        Resultados agrupados por ID_registro:
        [
            {
                "ID_registro": "12345",
                "MovimientoExitoso": true,
                "NuevaRutaArchivo": "C:\\Destino\\",
                "ErrorMsg": ""
            },
            ...
        ]
        
    vLocStrResultadoSP : bool - True si exito.
    vLocStrResumenSP : str - Resumen de ejecucion.
    vGblStrMensajeError : str - Mensaje de error.
    vGblStrSystemError : str - Stack trace.

================================================================================
LOGICA DE IDEMPOTENCIA
================================================================================

La funcion maneja los siguientes casos para garantizar idempotencia:

    Caso 1: Origen existe, Destino NO existe
        -> Mover normalmente (shutil.move)
        -> Resultado: OK
        
    Caso 2: Origen NO existe, Destino existe
        -> Ya fue movido anteriormente
        -> Resultado: OK (idempotente)
        
    Caso 3: Origen existe, Destino existe
        -> Re-ejecucion: eliminar destino, mover de nuevo
        -> Resultado: OK
        
    Caso 4: Origen NO existe, Destino NO existe
        -> Error: archivo perdido
        -> Resultado: FAIL

================================================================================
"""


async def ejecutar_FileOps_PuntoH_MOVER():
    """
    Lee vLocJsonFileOpsPuntoH, mueve los archivos y genera ResultadosJson.
    
    Procesa la lista de archivos generada por QUEUE, mueve cada archivo
    a su destino, y guarda los resultados para FINALIZE.
    
    Returns:
        tuple: (bool, list|None)
            - bool: True si exito, False si error
            - list: Lista de dicts con resultados por ID_registro
    
    Side Effects:
        Lee: vLocJsonFileOpsPuntoH
        Escribe:
            - vLocJsonResultadosFileOpsPuntoH (resultados para FINALIZE)
            - vLocStrResultadoSP
            - vLocStrResumenSP
            - vGblStrMensajeError
            - vGblStrSystemError
    
    Example:
        # Despues de ejecutar QUEUE
        ok, resultados = await ejecutar_FileOps_PuntoH_MOVER()
        
        if ok:
            print(f"Resultados: {len(resultados)} registros procesados")
            for r in resultados:
                if r["MovimientoExitoso"]:
                    print(f"  ID {r['ID_registro']}: OK -> {r['NuevaRutaArchivo']}")
                else:
                    print(f"  ID {r['ID_registro']}: FAIL -> {r['ErrorMsg']}")
    """
    import asyncio
    import json
    import os
    import shutil
    import traceback
    import unicodedata
    from collections import defaultdict

    def safe_str(v):
        """Convierte valor a string de forma segura."""
        try:
            if v is None:
                return ""
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

    def set_error(user_msg, exc=None):
        """Establece variables de error."""
        try:
            SetVar("vGblStrMensajeError", to_ascii(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else to_ascii(traceback.format_exc()))
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", to_ascii(user_msg))
            SetVar("vLocJsonResultadosFileOpsPuntoH", "[]")
        except Exception:
            pass

    # ==========================================================================
    # LEER JSON DE OPERACIONES
    # ==========================================================================
    try:
        raw = safe_str(GetVar("vLocJsonFileOpsPuntoH")).strip()
        file_ops = json.loads(raw) if raw else []
    except Exception as e:
        set_error("ERROR Punto H FileOps | JSON invalido en vLocJsonFileOpsPuntoH", e)
        return False, None

    # ==========================================================================
    # EJECUTAR MOVIMIENTOS
    # ==========================================================================
    def run_move_sync():
        """Ejecuta movimientos de forma sincrona."""
        
        # Agrupar por ID_registro para devolver 1 resultado por registro
        por_id = defaultdict(list)
        for op in file_ops:
            _id = safe_str(op.get("ID_registro")).strip()
            src = safe_str(op.get("RutaOrigenFull")).strip()
            dest_dir = safe_str(op.get("CarpetaDestino")).strip()
            filename = safe_str(op.get("NombreArchivo")).strip()

            # Si no hay filename, extraerlo del src
            if not filename and src:
                filename = os.path.basename(src)

            por_id[_id].append((src, dest_dir, filename))

        resultados = []
        ok_ids = 0
        fail_ids = 0

        for _id, items in por_id.items():
            ok = True
            err = ""
            nueva_ruta = ""

            for (src, dest_dir, filename) in items:
                try:
                    # Validaciones
                    if not _id:
                        raise ValueError("ID_registro vacio")
                    if not src:
                        raise ValueError("RutaOrigenFull vacia")
                    if not dest_dir:
                        raise ValueError("CarpetaDestino vacia")
                    if not filename:
                        raise ValueError("NombreArchivo vacio")

                    # Crear directorio destino
                    os.makedirs(dest_dir, exist_ok=True)
                    dst_full = os.path.join(dest_dir, filename)

                    # IDEMPOTENCIA: si destino existe y origen no -> OK (ya movido)
                    if (not os.path.exists(src)) and os.path.exists(dst_full):
                        nueva_ruta = dest_dir
                        continue

                    # RE-EJECUCION: si ambos existen, eliminar destino y mover
                    if os.path.exists(src) and os.path.exists(dst_full):
                        try:
                            os.remove(dst_full)
                        except Exception as ex_rm:
                            raise RuntimeError(
                                f"Destino ya existe y no se pudo borrar: {dst_full} | {safe_str(ex_rm)}"
                            )

                    # ERROR: origen no existe
                    if not os.path.exists(src):
                        raise FileNotFoundError(f"No existe origen: {src}")

                    # MOVER archivo
                    shutil.move(src, dst_full)
                    nueva_ruta = dest_dir

                except Exception as e:
                    ok = False
                    err = safe_str(e)
                    break

            # Agregar resultado para este ID_registro
            resultados.append({
                "ID_registro": _id,
                "MovimientoExitoso": bool(ok),
                "NuevaRutaArchivo": nueva_ruta if ok else "",
                "ErrorMsg": "" if ok else err
            })

            if ok:
                ok_ids += 1
            else:
                fail_ids += 1

        resumen = (
            f"Punto H FileOps MOVE OK | IDs_OK={ok_ids} | IDs_FAIL={fail_ids} | "
            f"TotalIDs={len(por_id)} | TotalArchivos={len(file_ops)}"
        )
        return True, resultados, resumen

    # ==========================================================================
    # WRAPPER ASYNC
    # ==========================================================================
    try:
        loop = asyncio.get_running_loop()
        ok, resultados, resumen = await loop.run_in_executor(None, run_move_sync)

        try:
            SetVar("vLocStrResultadoSP", bool(ok))
            SetVar("vLocStrResumenSP", to_ascii(resumen))
            SetVar("vLocJsonResultadosFileOpsPuntoH", json.dumps(resultados, ensure_ascii=True))
        except Exception:
            pass

        return bool(ok), resultados

    except Exception as e:
        set_error("ERROR Punto H FileOps | fallo ejecucion MOVE", e)
        return False, None


# ==============================================================================
# EJEMPLOS DE USO
# ==============================================================================
"""
EJEMPLO 1: Flujo completo QUEUE -> MOVE -> FINALIZE
---------------------------------------------------
    # === PASO 1: QUEUE ===
    ok, archivos = await ejecutar_HU4_H_Agrupacion_QUEUE()
    
    # === PASO 2: MOVER ARCHIVOS ===
    ok, resultados = await ejecutar_FileOps_PuntoH_MOVER()
    
    if ok:
        for r in resultados:
            status = "OK" if r["MovimientoExitoso"] else "FAIL"
            print(f"ID {r['ID_registro']}: {status}")
    
    # === PASO 3: FINALIZE ===
    ok, resumen = await ejecutar_HU4_H_Agrupacion_FINALIZE()

EJEMPLO 2: Verificar resultados detallados
------------------------------------------
    ok, resultados = await ejecutar_FileOps_PuntoH_MOVER()
    
    if ok:
        exitosos = [r for r in resultados if r["MovimientoExitoso"]]
        fallidos = [r for r in resultados if not r["MovimientoExitoso"]]
        
        print(f"Exitosos: {len(exitosos)}")
        print(f"Fallidos: {len(fallidos)}")
        
        for f in fallidos:
            print(f"  ID {f['ID_registro']}: {f['ErrorMsg']}")

EJEMPLO 3: Idempotencia - Re-ejecucion segura
---------------------------------------------
    # Primera ejecucion
    ok, resultados = await ejecutar_FileOps_PuntoH_MOVER()
    # Archivos movidos de C:\\Origen a C:\\Destino
    
    # Segunda ejecucion (re-intento)
    ok, resultados = await ejecutar_FileOps_PuntoH_MOVER()
    # OK! Los archivos ya estan en destino, no hay error
    # MovimientoExitoso = True para todos

EJEMPLO 4: Estructura del JSON de entrada
-----------------------------------------
    # vLocJsonFileOpsPuntoH debe tener esta estructura:
    [
        {
            "ID_registro": "12345",
            "RutaOrigenFull": "C:\\Documentos\\Facturas\\factura_001.pdf",
            "CarpetaDestino": "C:\\Archivo\\2025\\01\\",
            "NombreArchivo": "factura_001.pdf"
        },
        {
            "ID_registro": "12345",  # Mismo ID, otro archivo
            "RutaOrigenFull": "C:\\Documentos\\Facturas\\anexo_001.pdf",
            "CarpetaDestino": "C:\\Archivo\\2025\\01\\",
            "NombreArchivo": "anexo_001.pdf"
        },
        {
            "ID_registro": "12346",  # Diferente ID
            "RutaOrigenFull": "C:\\Documentos\\Facturas\\factura_002.pdf",
            "CarpetaDestino": "C:\\Archivo\\2025\\01\\",
            "NombreArchivo": "factura_002.pdf"
        }
    ]
    
    # Resultado: 2 registros (12345 y 12346), 3 archivos movidos
"""
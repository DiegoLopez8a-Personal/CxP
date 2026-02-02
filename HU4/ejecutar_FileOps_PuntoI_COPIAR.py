#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: ejecutar_FileOps_PuntoI_COPIAR.py
================================================================================

Descripcion General:
--------------------
    Este modulo ejecuta las operaciones de copia de archivos para el
    Punto I del proceso HU4. Lee la lista de registros desde vLocJsonFileOpsPuntoI
    (generada por QUEUE) y copia los archivos a sus destinos.
    
    Los resultados se guardan en vLocJsonResultadosFileOpsPuntoI para ser
    consumidos por la fase FINALIZE.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0

Caracteristicas Principales:
----------------------------
    - Operacion: COPIAR archivos (shutil.copy2 - preserva metadata)
    - Multiples archivos por registro: NombresArchivos separados por ;
    - Idempotencia: Si origen no existe pero destino si -> OK (ya copiado)
    - Re-ejecucion: Si destino existe, lo elimina y copia de nuevo
    - Agrupacion: Resultados agrupados por ID_registro

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |            ejecutar_FileOps_PuntoI_COPIAR()                 |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Leer JSON de operaciones:                                  |
    |  raw = GetVar("vLocJsonFileOpsPuntoI")                      |
    |  file_ops = json.loads(raw)                                 |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Agrupar operaciones por ID_registro:                       |
    |  por_id = defaultdict(list)                                 |
    |  for op in file_ops:                                        |
    |      por_id[ID_registro].append((ruta, nombres, destino))   |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Para cada ID_registro:                                     |
    |  +-------------------------------------------------------+  |
    |  |  Validar insumos: RutaOrigen, NombresArchivos, Destino|  |
    |  +-------------------------------------------------------+  |
    |  |  Separar NombresArchivos por ";"                      |  |
    |  |  Ej: "doc1.pdf;doc2.pdf" -> ["doc1.pdf", "doc2.pdf"]  |  |
    |  +-------------------------------------------------------+  |
    |  |  Para cada archivo:                                   |  |
    |  |  +---------------------------------------------------+|  |
    |  |  |  Construir ruta origen completa                   ||  |
    |  |  |  Crear directorio destino (makedirs)              ||  |
    |  |  |  Verificar idempotencia                           ||  |
    |  |  |  shutil.copy2(src, dst) si procede                ||  |
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
    |  SetVar("vLocJsonResultadosFileOpsPuntoI", json.dumps(...)) |
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
    vLocJsonFileOpsPuntoI : str (JSON)
        Lista de operaciones generada por QUEUE:
        [
            {
                "ID_registro": "12345",
                "RutaOrigen": "C:\\Origen\\",
                "NombresArchivos": "doc1.pdf;doc2.pdf;anexo.pdf",
                "CarpetaDestino": "C:\\Destino\\"
            },
            ...
        ]
        
        NOTA: NombresArchivos puede contener multiples archivos separados por ;

Variables de Salida (SetVar):
-----------------------------
    vLocJsonResultadosFileOpsPuntoI : str (JSON)
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
DIFERENCIAS CON PUNTO H (MOVER)
================================================================================

    Punto H (MOVER):
        - Usa shutil.move() - el archivo se ELIMINA del origen
        - Un archivo por fila
        - Campo: RutaOrigenFull (ruta completa con nombre)
        - Campo: NombreArchivo (un solo archivo)
        
    Punto I (COPIAR):
        - Usa shutil.copy2() - el archivo PERMANECE en origen
        - Multiples archivos por fila (separados por ;)
        - Campo: RutaOrigen (solo carpeta, sin nombre)
        - Campo: NombresArchivos (lista separada por ;)

================================================================================
LOGICA DE IDEMPOTENCIA
================================================================================

    Caso 1: Origen existe, Destino NO existe
        -> Copiar normalmente (shutil.copy2)
        -> Resultado: OK
        
    Caso 2: Origen NO existe, Destino existe
        -> Ya fue copiado anteriormente
        -> Resultado: OK (idempotente)
        
    Caso 3: Origen existe, Destino existe
        -> Re-ejecucion: eliminar destino, copiar de nuevo
        -> Resultado: OK
        
    Caso 4: Origen NO existe, Destino NO existe
        -> Error: archivo no encontrado
        -> Resultado: FAIL

================================================================================
"""


async def ejecutar_FileOps_PuntoI_COPIAR():
    """
    Lee vLocJsonFileOpsPuntoI, copia los archivos y genera ResultadosJson.
    
    Procesa la lista de registros generada por QUEUE, copia cada archivo
    a su destino (puede haber multiples archivos por registro), y guarda
    los resultados para FINALIZE.
    
    Returns:
        tuple: (bool, list|None)
            - bool: True si exito, False si error
            - list: Lista de dicts con resultados por ID_registro
    
    Side Effects:
        Lee: vLocJsonFileOpsPuntoI
        Escribe:
            - vLocJsonResultadosFileOpsPuntoI (resultados para FINALIZE)
            - vLocStrResultadoSP
            - vLocStrResumenSP
            - vGblStrMensajeError
            - vGblStrSystemError
    
    Example:
        # Despues de ejecutar QUEUE
        ok, resultados = await ejecutar_FileOps_PuntoI_COPIAR()
        
        if ok:
            for r in resultados:
                if r["MovimientoExitoso"]:
                    print(f"ID {r['ID_registro']}: OK -> {r['NuevaRutaArchivo']}")
                else:
                    print(f"ID {r['ID_registro']}: FAIL -> {r['ErrorMsg']}")
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
            return "" if v is None else str(v)
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
            SetVar("vLocJsonResultadosFileOpsPuntoI", "[]")
        except Exception:
            pass

    def build_src_full(ruta_origen, filename):
        """
        Construye la ruta completa del archivo origen.
        
        Maneja casos donde ruta_origen ya incluye el nombre del archivo
        o donde solo es el directorio.
        
        Args:
            ruta_origen: Ruta del directorio origen.
            filename: Nombre del archivo.
        
        Returns:
            str: Ruta completa del archivo origen.
        
        Example:
            >>> build_src_full("C:\\Docs", "file.pdf")
            "C:\\Docs\\file.pdf"
            >>> build_src_full("C:\\Docs\\file.pdf", "file.pdf")
            "C:\\Docs\\file.pdf"
        """
        ruta_origen = safe_str(ruta_origen).strip()
        filename = safe_str(filename).strip()
        
        if not ruta_origen or not filename:
            return ""
        
        r_low = ruta_origen.lower()
        f_low = filename.lower()
        
        # Si la ruta ya termina con el nombre del archivo
        if r_low.endswith(f_low):
            return ruta_origen
        
        # Agregar separador si es necesario
        if ruta_origen.endswith("\\") or ruta_origen.endswith("/"):
            return ruta_origen + filename
        
        return ruta_origen + "\\" + filename

    # ==========================================================================
    # LEER JSON DE OPERACIONES
    # ==========================================================================
    try:
        raw = safe_str(GetVar("vLocJsonFileOpsPuntoI")).strip()
        file_ops = json.loads(raw) if raw else []
    except Exception as e:
        set_error("ERROR Punto I FileOps | JSON invalido en vLocJsonFileOpsPuntoI", e)
        return False, None

    # ==========================================================================
    # EJECUTAR COPIAS
    # ==========================================================================
    def run_copy_sync():
        """Ejecuta copias de forma sincrona."""
        
        # Agrupar por ID_registro
        por_id = defaultdict(list)

        for op in file_ops:
            _id = safe_str(op.get("ID_registro")).strip()
            ruta_origen = safe_str(op.get("RutaOrigen")).strip()
            nombres = safe_str(op.get("NombresArchivos")).strip()
            dest_dir = safe_str(op.get("CarpetaDestino")).strip()
            por_id[_id].append((ruta_origen, nombres, dest_dir))

        resultados = []
        ok_ids = 0
        fail_ids = 0
        total_archivos = 0

        for _id, packs in por_id.items():
            ok = True
            err = ""
            nueva_ruta = ""

            # Validar insumos: si no hay ruta o nombres => FAIL
            archivos = []
            for (ruta_origen, nombres, dest_dir) in packs:
                if not dest_dir:
                    ok = False
                    err = "CarpetaDestino vacia"
                    break
                if (not ruta_origen) or (not nombres):
                    ok = False
                    err = "No se logran identificar insumos (RutaOrigen o NombresArchivos vacio)"
                    break

                # Separar multiples archivos por ;
                for nombre in [x.strip() for x in nombres.split(";") if x.strip()]:
                    archivos.append((build_src_full(ruta_origen, nombre), dest_dir, nombre))

            if ok:
                try:
                    for (src_full, dest_dir, nombre) in archivos:
                        total_archivos += 1
                        
                        # Crear directorio destino
                        os.makedirs(dest_dir, exist_ok=True)
                        dst_full = os.path.join(dest_dir, nombre)

                        # IDEMPOTENCIA: si destino existe y origen no -> OK
                        if (not os.path.exists(src_full)) and os.path.exists(dst_full):
                            nueva_ruta = dest_dir
                            continue

                        # ERROR: origen no existe
                        if not os.path.exists(src_full):
                            raise FileNotFoundError(f"No existe origen: {src_full}")

                        # RE-EJECUCION: si destino existe, eliminarlo
                        if os.path.exists(dst_full):
                            try:
                                os.remove(dst_full)
                            except Exception as ex_rm:
                                raise RuntimeError(
                                    f"Destino existe y no se pudo borrar: {dst_full} | {safe_str(ex_rm)}"
                                )

                        # COPIAR archivo (preserva metadata)
                        shutil.copy2(src_full, dst_full)
                        nueva_ruta = dest_dir

                except Exception as e:
                    ok = False
                    err = safe_str(e)

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
            f"Punto I FileOps COPY OK | IDs_OK={ok_ids} | IDs_FAIL={fail_ids} | "
            f"TotalIDs={len(por_id)} | TotalArchivos={total_archivos}"
        )
        return True, resultados, resumen

    # ==========================================================================
    # WRAPPER ASYNC
    # ==========================================================================
    try:
        loop = asyncio.get_running_loop()
        ok, resultados, resumen = await loop.run_in_executor(None, run_copy_sync)

        SetVar("vLocStrResultadoSP", bool(ok))
        SetVar("vLocStrResumenSP", to_ascii(resumen))
        SetVar("vLocJsonResultadosFileOpsPuntoI", json.dumps(resultados, ensure_ascii=True))
        return bool(ok), resultados

    except Exception as e:
        set_error("ERROR Punto I FileOps | fallo ejecucion COPY", e)
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
    
    if ok:
        for r in resultados:
            status = "OK" if r["MovimientoExitoso"] else "FAIL"
            print(f"ID {r['ID_registro']}: {status}")
    
    # === PASO 3: FINALIZE ===
    ok, resumen = await ejecutar_HU4_I_NumLiquidacion_50_FINALIZE()

EJEMPLO 2: Estructura del JSON de entrada (multiples archivos)
--------------------------------------------------------------
    # vLocJsonFileOpsPuntoI debe tener esta estructura:
    [
        {
            "ID_registro": "12345",
            "RutaOrigen": "C:\\Documentos\\Facturas\\",
            "NombresArchivos": "factura_001.pdf;anexo_001.pdf;soporte.xml",
            "CarpetaDestino": "C:\\Archivo\\2025\\01\\"
        },
        {
            "ID_registro": "12346",
            "RutaOrigen": "C:\\Documentos\\Facturas\\",
            "NombresArchivos": "factura_002.pdf",
            "CarpetaDestino": "C:\\Archivo\\2025\\01\\"
        }
    ]
    
    # Resultado:
    # - ID 12345: 3 archivos copiados
    # - ID 12346: 1 archivo copiado
    # Total: 4 archivos, 2 registros

EJEMPLO 3: Verificar resultados detallados
------------------------------------------
    ok, resultados = await ejecutar_FileOps_PuntoI_COPIAR()
    
    if ok:
        exitosos = [r for r in resultados if r["MovimientoExitoso"]]
        fallidos = [r for r in resultados if not r["MovimientoExitoso"]]
        
        print(f"Exitosos: {len(exitosos)}")
        print(f"Fallidos: {len(fallidos)}")
        
        for f in fallidos:
            print(f"  ID {f['ID_registro']}: {f['ErrorMsg']}")

EJEMPLO 4: Idempotencia - Re-ejecucion segura
---------------------------------------------
    # Primera ejecucion
    ok, resultados = await ejecutar_FileOps_PuntoI_COPIAR()
    # Archivos copiados de C:\\Origen a C:\\Destino
    
    # Segunda ejecucion (re-intento)
    ok, resultados = await ejecutar_FileOps_PuntoI_COPIAR()
    # OK! Archivos ya copiados, se sobrescriben
    # MovimientoExitoso = True para todos

EJEMPLO 5: Manejo de errores
----------------------------
    # Si un archivo no existe
    # vLocJsonFileOpsPuntoI = [
    #     {"ID_registro": "123", "RutaOrigen": "C:\\NoExiste\\", 
    #      "NombresArchivos": "archivo.pdf", "CarpetaDestino": "C:\\Destino\\"}
    # ]
    
    ok, resultados = await ejecutar_FileOps_PuntoI_COPIAR()
    # ok = True (la funcion no falla)
    # resultados[0]["MovimientoExitoso"] = False
    # resultados[0]["ErrorMsg"] = "No existe origen: C:\\NoExiste\\archivo.pdf"
"""
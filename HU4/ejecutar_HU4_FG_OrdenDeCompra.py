#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: ejecutar_HU4_FG_OrdenDeCompra.py
================================================================================

Descripcion General:
--------------------
    Este modulo ejecuta los Puntos F y G del proceso HU4 - Validacion de
    Ordenes de Compra. Procesa documentos para verificar asociaciones con
    ordenes de compra, excluyendo importaciones y costos indirectos de fletes.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Fecha de Creacion: 2025

Stored Procedure:
-----------------
    [CxP].[HU4_FG_OrdenDeCompra]
    
    Parametros:
        @DiasMaximos INT - Dias maximos para filtrar registros
        @BatchSize INT - Tamano del lote de procesamiento

Dependencias:
-------------
    - asyncio: Ejecucion asincrona
    - pyodbc: Conexion a SQL Server
    - json, ast: Parseo de configuracion
    - traceback: Captura de errores

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |            ejecutar_HU4_FG_OrdenDeCompra()                  |
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
    |  - vGblIntDiasMaximos (o config DiasMaximos)                |
    |  - vGblIntBatchSize (o config BatchSize)                    |
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
    |  return False, None    |   +-----------------+--------------+
    +------------------------+                     |
                                                   |
                                                   v
    +-------------------------------------------------------------+
    |  Verificar existencia del SP:                               |
    |  SELECT 1 FROM sys.procedures WHERE name = ?                |
    +-------------------------+-----------------------------------+
                              |
              +---------------+---------------+
              |       ¿SP existe?             |
              +---------------+---------------+
                     |                |
                     | NO             | SI
                     v                v
    +------------------------+   +--------------------------------+
    |  RuntimeError          |   |  EXEC [CxP].[HU4_FG_OrdenDe    |
    |  "SP no existe"        |   |  Compra] @dias, @batch         |
    +------------------------+   +-----------------+--------------+
                                                   |
                                                   v
    +-------------------------------------------------------------+
    |  ResultSet 1: RESUMEN                                       |
    |  - FechaEjecucion, DiasMaximos, BatchSize                   |
    |  - RegistrosProcesados, RetomaSetDesdeNull                  |
    |  - ExcluidosImportaciones, ExcluidosCostoIndirectoFletes    |
    |  - ComparativaObservacionesActualizadas                     |
    |  - ComparativaEstadosActualizados                           |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Consumir ResultSet 2 (DETALLE) - solo para limpiar cursor  |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  build_summary()                                            |
    |  - Estado=OK (siempre, incluso si procesados=0)             |
    |  - Construir resumen con todas las metricas                 |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  SetVar("vLocStrResultadoSP", True)                         |
    |  SetVar("vLocStrResumenSP", resumen)                        |
    |  return True, resumen                                       |
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
            "DiasMaximos": 120,    # o "PlazoMaximo"
            "BatchSize": 500       # o "Lote"
        }
    
    vGblIntDiasMaximos : int (opcional)
        Override global para DiasMaximos.
        Si esta presente y valido, tiene prioridad sobre config.
        
    vGblIntBatchSize : int (opcional)
        Override global para BatchSize.
        Si esta presente y valido, tiene prioridad sobre config.

Variables de Salida (SetVar):
-----------------------------
    vLocStrResultadoSP : bool
        True si exito, False si error.
        
    vLocStrResumenSP : str
        Ejemplo:
        "Estado=OK | SP=CxP.HU4_FG_OrdenDeCompra | FechaEjecucion=2025-01-15 |
         DiasMaximos=120 | BatchSize=500 | Procesados=1500 | RetomaSetDesdeNull=50 |
         ExcluidosImportaciones=100 | ExcluidosCostoIndirectoFletes=25 |
         ComparativaObsActualizadas=1400 | ComparativaEstadosActualizados=1350"
         
    vGblStrMensajeError : str
        Mensaje de error (vacio si exito).
        
    vGblStrSystemError : str
        Stack trace (vacio si exito).

================================================================================
METRICAS DEL RESUMEN
================================================================================

    RegistrosProcesados:
        Total de documentos procesados en el lote.
        
    RetomaSetDesdeNull:
        Registros cuyo estado de retoma era NULL y fue establecido.
        
    ExcluidosImportaciones:
        Documentos excluidos por ser importaciones.
        
    ExcluidosCostoIndirectoFletes:
        Documentos excluidos por ser costos indirectos de fletes.
        
    ComparativaObservacionesActualizadas:
        Filas actualizadas en tabla de observaciones comparativas.
        
    ComparativaEstadosActualizados:
        Filas actualizadas en tabla de estados comparativos.

================================================================================
"""



async def ejecutar_HU4_FG_OrdenDeCompra():
    """
    Ejecuta Puntos F y G - Validacion de Ordenes de Compra.
    
    Procesa documentos para verificar asociaciones con ordenes de compra,
    actualizando observaciones y estados en tablas comparativas.
    
    Returns:
        tuple: (bool, str|None)
            - bool: True si exito, False si error
            - str: Resumen de ejecucion o None si error
    
    Side Effects:
        Lee:
            - vLocDicConfig
            - vGblIntDiasMaximos (opcional, override)
            - vGblIntBatchSize (opcional, override)
        Escribe:
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
        
            ok, resumen = await ejecutar_HU4_FG_OrdenDeCompra()
            
            if ok:
                print(f"Exito: {resumen}")
                # Estado=OK | SP=CxP.HU4_FG_OrdenDeCompra | ...
    
    Note:
        - El SP debe existir en schema [CxP]
        - Se verifica existencia antes de ejecutar (error 2812)
        - Estado es siempre "OK" aunque procesados=0
        - Variables globales vGblInt* tienen prioridad sobre config
    """
    # ==========================================================================
    # IMPORTS
    # ==========================================================================
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback

    # ==========================================================================
    # FUNCIONES AUXILIARES
    # ==========================================================================
    
    def safe_str(v):
        """Convierte valor a string de forma segura."""
        try:
            return "" if v is None else str(v)
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
            SetVar("vGblStrMensajeError", safe_str(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else traceback.format_exc())
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

    def is_missing(v):
        """
        Verifica si un valor esta ausente o es invalido.
        
        Args:
            v: Valor a verificar.
        
        Returns:
            bool: True si el valor es vacio, None, o ERROR_NOT_VAR.
        
        Example:
            >>> is_missing(None)
            True
            >>> is_missing("ERROR_NOT_VAR")
            True
            >>> is_missing("123")
            False
        """
        return v in ("", None, "ERROR_NOT_VAR")

    def to_int(v, default):
        """
        Convierte valor a entero con manejo de ERROR_NOT_VAR.
        
        Args:
            v: Valor a convertir.
            default: Valor por defecto si conversion falla.
        
        Returns:
            int: Valor entero.
        
        Example:
            >>> to_int("100", 50)
            100
            >>> to_int("ERROR_NOT_VAR", 50)
            50
            >>> to_int(None, 50)
            50
        """
        if is_missing(v):
            return int(default)
        try:
            return int(float(safe_str(v).strip()))
        except Exception:
            return int(default)

    def read_one_row_as_dict(cur):
        """Lee una fila del cursor como diccionario."""
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
        """Consume todas las filas del ResultSet actual."""
        try:
            if cur.description:
                _ = cur.fetchall()
        except Exception:
            pass

    def build_summary(rs1):
        """
        Construye resumen de ejecucion del SP.
        
        Args:
            rs1: Diccionario con datos del ResultSet 1.
        
        Returns:
            str: Resumen formateado.
        
        Example:
            >>> build_summary({
            ...     "FechaEjecucion": "2025-01-15",
            ...     "DiasMaximos": 120,
            ...     "BatchSize": 500,
            ...     "RegistrosProcesados": 1500,
            ...     ...
            ... })
            "Estado=OK | SP=CxP.HU4_FG_OrdenDeCompra | FechaEjecucion=2025-01-15 | ..."
        """
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

        # Estado siempre OK (incluso si procesados=0)
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

        # DiasMaximos: primero busca en variable global, luego en config
        dias = to_int(
            GetVar("vGblIntDiasMaximos"),
            cfg.get("DiasMaximos", cfg.get("PlazoMaximo", 120))
        )
        
        # BatchSize: primero busca en variable global, luego en config
        batch = to_int(
            GetVar("vGblIntBatchSize"),
            cfg.get("BatchSize", cfg.get("Lote", 500))
        )
        
    except Exception as e:
        set_error_vars("Error configuracion SP HU4_FG_OrdenDeCompra", e)
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

            # Ejecutar SP con 2 parametros
            cur.execute("EXEC [CxP].[HU4_FG_OrdenDeCompra] ?, ?", dias, batch)

            # ResultSet 1: Resumen
            rs1 = read_one_row_as_dict(cur)

            # ResultSet 2: Detalle (solo consumir)
            if cur.nextset():
                consume_all_rows(cur)

            resumen = build_summary(rs1)
            return True, resumen

    # ==========================================================================
    # WRAPPER ASYNC
    # ==========================================================================
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


# ==============================================================================
# EJEMPLOS DE USO
# ==============================================================================
"""
EJEMPLO 1: Ejecucion basica con config
--------------------------------------
    SetVar("vLocDicConfig", {
        "ServidorBaseDatos": "SQLPROD\\CXP",
        "NombreBaseDatos": "CuentasPorPagar",
        "DiasMaximos": 120,
        "BatchSize": 500
    })
    
    ok, resumen = await ejecutar_HU4_FG_OrdenDeCompra()
    
    if ok:
        print(f"Resumen: {resumen}")
        # Estado=OK | SP=CxP.HU4_FG_OrdenDeCompra | FechaEjecucion=2025-01-15 |
        # DiasMaximos=120 | BatchSize=500 | Procesados=1500 | ...

EJEMPLO 2: Con override de variables globales
---------------------------------------------
    # Las variables globales tienen prioridad sobre config
    SetVar("vGblIntDiasMaximos", 90)   # Override: 90 dias
    SetVar("vGblIntBatchSize", 1000)   # Override: lotes de 1000
    
    SetVar("vLocDicConfig", {
        "ServidorBaseDatos": "SQLPROD\\CXP",
        "NombreBaseDatos": "CuentasPorPagar",
        "DiasMaximos": 120,  # Ignorado, se usa vGblIntDiasMaximos
        "BatchSize": 500     # Ignorado, se usa vGblIntBatchSize
    })
    
    ok, resumen = await ejecutar_HU4_FG_OrdenDeCompra()
    # resumen contendra DiasMaximos=90 | BatchSize=1000

EJEMPLO 3: Interpretacion de metricas
-------------------------------------
    ok, resumen = await ejecutar_HU4_FG_OrdenDeCompra()
    
    if ok:
        r = GetVar("vLocStrResumenSP")
        
        # Verificar exclusiones
        if "ExcluidosImportaciones=0" not in r:
            print("ℹAlgunos documentos fueron excluidos por ser importaciones")
        
        if "ExcluidosCostoIndirectoFletes=0" not in r:
            print("ℹAlgunos documentos fueron excluidos por costos de fletes")
        
        # Verificar actualizaciones
        if "ComparativaObsActualizadas=0" in r:
            print("No se actualizaron observaciones comparativas")

EJEMPLO 4: Manejo de SP inexistente
-----------------------------------
    # Si el SP no existe en la base de datos
    ok, resumen = await ejecutar_HU4_FG_OrdenDeCompra()
    
    # ok = False
    # resumen = None
    # GetVar("vGblStrMensajeError") = "Error ejecucion SP HU4_FG_OrdenDeCompra"
    # GetVar("vGblStrSystemError") contiene "RuntimeError: SP no existe: ..."
"""
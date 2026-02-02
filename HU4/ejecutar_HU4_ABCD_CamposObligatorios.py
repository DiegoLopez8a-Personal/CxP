#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: ejecutar_HU4_ABCD_CamposObligatorios.py
================================================================================

Descripcion General:
--------------------
    Este modulo ejecuta el Stored Procedure [CxP].[HU4_ABCD_CamposObligatorios]
    para validar campos obligatorios en documentos de Cuentas por Pagar (CxP).
    
    La funcion maneja la conversion de tipos DATETIMEOFFSET de SQL Server
    mediante un wrapper WITH RESULT SETS para evitar el error conocido de
    pyodbc "ODBC SQL type -155 is not yet supported".

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Fecha de Creacion: 2025
Ultima Modificacion: 2025

Stored Procedure:
-----------------
    [CxP].[HU4_ABCD_CamposObligatorios]
    
    Parametros del SP:
        @DiasMaximos INT - Dias maximos para considerar registros
        @BatchSize INT - Tamano del lote de procesamiento

Dependencias:
-------------
    - asyncio: Para ejecucion asincrona
    - pyodbc: Para conexion a SQL Server
    - json: Para parseo de configuracion y serializacion
    - ast: Para parseo alternativo de configuracion
    - traceback: Para captura de errores detallados
    - collections.Counter: Para estadisticas de resumen

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |           ejecutar_HU4_ABCD_CamposObligatorios()            |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  reset_vars()                                               |
    |  - vGblStrMensajeError = ""                                 |
    |  - vGblStrSystemError = ""                                  |
    |  - vLocStrResultadoSP = False                               |
    |  - vLocStrResumenSP = ""                                    |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Parsear configuracion desde vLocDicConfig                  |
    |  Extraer: DiasMaximos, BatchSize, Servidor, DB              |
    +-------------------------+-----------------------------------+
                              |
              +---------------+---------------+
              | ¿Error en configuracion?      |
              +---------------+---------------+
                     |                |
                     | SI             | NO
                     v                v
    +------------------------+   +--------------------------------+
    |  set_error_vars()      |   |  Construir connection string   |
    |  return False, None    |   |  ODBC Driver 17, Trusted_Conn  |
    +------------------------+   +-----------------+--------------+
                                                   |
                                                   v
    +-------------------------------------------------------------+
    |  Ejecutar SP con WITH RESULT SETS                           |
    |  (Convierte DATETIMEOFFSET -> NVARCHAR)                     |
    |                                                             |
    |  EXEC [CxP].[HU4_ABCD_CamposObligatorios] @dias, @batch     |
    |  WITH RESULT SETS (                                         |
    |    (FechaEjecucion NVARCHAR, DiasMaximos INT, ...)          |
    |  )                                                          |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Consumir ResultSet 1: RESUMEN (1 fila)                     |
    |  - FechaEjecucion, DiasMaximos, BatchSize                   |
    |  - RegistrosProcesados, RetomaSetDesdeNull                  |
    |  - MarcadosNoExitoso, MarcadosRechazado                     |
    |  - OKDentroDiasMaximos, FilasInsertadasComparativa          |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  Consumir ResultSet 2: DETALLE (N filas)                    |
    |  - ID, numero_de_factura, nit_emisor                        |
    |  - documenttype, Fecha_retoma, DiasTranscurridos            |
    |  - ResultadoFinalAntesEventos, EstadoFinalFase_4            |
    |  - ObservacionesFase_4                                      |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  build_summary()                                            |
    |  - Generar resumen legible con estadisticas                 |
    |  - Contar estados mas comunes (TopEstados)                  |
    |  - Contar observaciones mas frecuentes (TopObs)             |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  SetVar("vLocStrResultadoSP", True)                         |
    |  SetVar("vLocStrResumenSP", resumen_txt)                    |
    +-------------------------+-----------------------------------+
                              |
                              v
    +-------------------------------------------------------------+
    |  return True, payload_json                                  |
    |                     FIN                                     |
    +-------------------------------------------------------------+

================================================================================
VARIABLES DE ENTRADA/SALIDA
================================================================================

Variables de Entrada (GetVar):
------------------------------
    vLocDicConfig : dict o str
        Configuracion del sistema con las siguientes claves:
        
        {
            "ServidorBaseDatos": "SERVIDOR\\INSTANCIA",
            "NombreBaseDatos": "CxP_Database",
            "PlazoMaximo": 120,        # o "DiasMaximos"
            "Lote": 500,               # o "BatchSize"
            "CommandTimeout": 0        # Opcional, 0 = sin timeout
        }

Variables de Salida (SetVar):
-----------------------------
    vLocStrResultadoSP : bool
        True si la ejecucion fue exitosa, False si hubo error.
        
    vLocStrResumenSP : str
        Resumen legible de la ejecucion. Ejemplos:
        
        Exito:
        "OK | Fecha=2025-01-15 10:30:00 | DiasMax=120 | Batch=500 | 
         Procesados=1500 | RetomaSet=50 | NoExitoso=10 | Rechazados=5 | 
         OKDentroDias=1435 | FilasComparativa=1500 | TopEstados=..."
        
        Error:
        "ERROR | configuracion | HU4_ABCD | Ver vGblStrSystemError"
        
    vGblStrMensajeError : str
        Mensaje de error amigable para el usuario (vacio si exito).
        
    vGblStrSystemError : str
        Stack trace completo del error (vacio si exito).

================================================================================
ESTRUCTURA DEL PAYLOAD DE RETORNO
================================================================================

El segundo valor de retorno es un JSON string con la siguiente estructura:

{
    "resumen_general": {
        "FechaEjecucion": "2025-01-15 10:30:00",
        "DiasMaximos": 120,
        "BatchSize": 500,
        "RegistrosProcesados": 1500,
        "RetomaSetDesdeNull": 50,
        "MarcadosNoExitoso": 10,
        "MarcadosRechazado": 5,
        "OKDentroDiasMaximos": 1435,
        "FilasInsertadasComparativa": 1500
    },
    "detalle_registros": [
        {
            "ID": 12345,
            "numero_de_factura": "FAC-001",
            "nit_emisor_o_nit_del_proveedor": "900123456",
            "documenttype": "FC",
            "Fecha_de_retoma_antes_de_contabilizacion": "2025-01-10",
            "DiasTranscurridosDesdeRetoma": 5,
            "ResultadoFinalAntesEventos": "PENDIENTE",
            "EstadoFinalFase_4": "EN_PROCESO",
            "ObservacionesFase_4": "Campo X vacio"
        },
        ...
    ],
    "detalle_errores": [...]  // Alias de detalle_registros por compatibilidad
}

================================================================================
"""


async def ejecutar_HU4_ABCD_CamposObligatorios():
    """
    Ejecuta el SP [CxP].[HU4_ABCD_CamposObligatorios] para validacion de campos.
    
    Esta funcion ejecuta el Stored Procedure de validacion de campos obligatorios
    en documentos de Cuentas por Pagar. Maneja la conversion de tipos DATETIMEOFFSET
    mediante WITH RESULT SETS y procesa dos ResultSets: RESUMEN y DETALLE.
    
    Returns:
        tuple: (bool, str|None)
            - bool: True si ejecuto correctamente, False si hubo error
            - str: JSON con payload de resultados o None si hubo error
    
    Raises:
        No lanza excepciones directamente; los errores se capturan y se
        almacenan en vGblStrMensajeError y vGblStrSystemError.
    
    Side Effects:
        Lee:
            - vLocDicConfig
        Escribe:
            - vLocStrResultadoSP
            - vLocStrResumenSP
            - vGblStrMensajeError
            - vGblStrSystemError
    
    Example:
        Configuracion previa::
        
            SetVar("vLocDicConfig", {
                "ServidorBaseDatos": "SQLSERVER\\PROD",
                "NombreBaseDatos": "CxP_Produccion",
                "PlazoMaximo": 120,
                "Lote": 500
            })
        
        Ejecucion::
        
            ok, payload = await ejecutar_HU4_ABCD_CamposObligatorios()
            
            if ok:
                print(f"Exito: {GetVar('vLocStrResumenSP')}")
                data = json.loads(payload)
                print(f"Procesados: {data['resumen_general']['RegistrosProcesados']}")
            else:
                print(f"Error: {GetVar('vGblStrMensajeError')}")
    
    Note:
        - El SP NO debe modificarse; este wrapper maneja la conversion de tipos.
        - Se utiliza WITH RESULT SETS para evitar error pyodbc type -155.
        - La conexion usa autenticacion de Windows (Trusted_Connection=yes).
    """
    # ==========================================================================
    # IMPORTS
    # ==========================================================================
    import asyncio
    import pyodbc
    import json
    import ast
    import traceback
    from collections import Counter

    # ==========================================================================
    # FUNCIONES AUXILIARES (HELPERS)
    # ==========================================================================
    
    def safe_str(v):
        """
        Convierte cualquier valor a string de forma segura.
        
        Args:
            v: Valor a convertir.
        
        Returns:
            str: String del valor o "" si es None/error.
        """
        try:
            return "" if v is None else str(v)
        except Exception:
            return ""

    def as_int(x, default=0):
        """
        Convierte valor a entero con valor por defecto.
        
        Args:
            x: Valor a convertir.
            default: Valor por defecto si la conversion falla.
        
        Returns:
            int: Valor entero o default.
        
        Example:
            >>> as_int("123")
            123
            >>> as_int(None, 0)
            0
            >>> as_int("abc", -1)
            -1
        """
        try:
            if x is None:
                return default
            return int(x)
        except Exception:
            return default

    def reset_vars():
        """
        Inicializa todas las variables de salida a estado conocido.
        
        Establece valores por defecto antes de la ejecucion para
        garantizar un estado limpio.
        """
        try:
            SetVar("vGblStrMensajeError", "")
            SetVar("vGblStrSystemError", "")
            SetVar("vLocStrResultadoSP", False)  # Default: False hasta exito
            SetVar("vLocStrResumenSP", "")
        except Exception:
            pass

    def set_error_vars(user_msg, exc=None):
        """
        Establece variables de error con mensaje y stack trace.
        
        Args:
            user_msg: Mensaje amigable para el usuario.
            exc: Excepcion capturada (opcional) para stack trace.
        
        Example:
            >>> try:
            ...     # codigo que falla
            ... except Exception as e:
            ...     set_error_vars("Error en proceso X", e)
        """
        try:
            SetVar("vGblStrMensajeError", safe_str(user_msg))
            SetVar("vGblStrSystemError", "" if exc is None else traceback.format_exc())
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", "")
        except Exception:
            pass

    def parse_config(raw):
        """
        Parsea la configuracion desde JSON o literal de Python.
        
        Args:
            raw: dict, JSON string, o Python literal string.
        
        Returns:
            dict: Configuracion parseada.
        
        Raises:
            ValueError: Si la configuracion esta vacia.
        """
        if isinstance(raw, dict):
            return raw
        text = safe_str(raw).strip()
        if not text:
            raise ValueError("vLocDicConfig vacio")
        try:
            return json.loads(text)
        except Exception:
            return ast.literal_eval(text)

    def row_to_dict(cursor, row):
        """
        Convierte una fila de cursor a diccionario.
        
        Args:
            cursor: Cursor de pyodbc con descripcion de columnas.
            row: Fila de datos del cursor.
        
        Returns:
            dict: Diccionario {columna: valor} o None si row es None.
        
        Example:
            >>> row = cursor.fetchone()
            >>> data = row_to_dict(cursor, row)
            >>> print(data['FechaEjecucion'])
        """
        if row is None:
            return None
        cols = [c[0] for c in (cursor.description or [])]
        return {cols[i]: row[i] for i in range(len(cols))}

    def rows_to_dicts(cursor, rows):
        """
        Convierte multiples filas de cursor a lista de diccionarios.
        
        Args:
            cursor: Cursor de pyodbc con descripcion de columnas.
            rows: Lista de filas del cursor.
        
        Returns:
            list: Lista de diccionarios [{col: val}, ...].
        """
        cols = [c[0] for c in (cursor.description or [])]
        out = []
        for r in rows or []:
            d = {}
            for i, name in enumerate(cols):
                d[name] = r[i]
            out.append(d)
        return out

    def build_summary(resumen_dict, detalle_list):
        """
        Construye resumen legible de la ejecucion del SP.
        
        Genera un string con estadisticas de la ejecucion incluyendo
        conteos de estados mas comunes y observaciones frecuentes.
        
        Args:
            resumen_dict: Diccionario con datos del ResultSet 1.
            detalle_list: Lista de diccionarios del ResultSet 2.
        
        Returns:
            str: Resumen formateado para vLocStrResumenSP.
        
        Example:
            Output tipico:
            "OK | Fecha=2025-01-15 | DiasMax=120 | Batch=500 | 
             Procesados=1500 | RetomaSet=50 | NoExitoso=10 | 
             Rechazados=5 | OKDentroDias=1435 | FilasComparativa=1500 |
             TopEstados=PENDIENTE=800, APROBADO=500, RECHAZADO=200 |
             TopObs=Campo vacio=300, NIT invalido=150"
        """
        if not resumen_dict:
            return "INFO | Sin datos de RESUMEN"

        # Extraer valores del resumen
        fecha = safe_str(resumen_dict.get("FechaEjecucion"))
        dias = as_int(resumen_dict.get("DiasMaximos"))
        batch = as_int(resumen_dict.get("BatchSize"))
        proc = as_int(resumen_dict.get("RegistrosProcesados"))
        retoma = as_int(resumen_dict.get("RetomaSetDesdeNull"))
        noex = as_int(resumen_dict.get("MarcadosNoExitoso"))
        rech = as_int(resumen_dict.get("MarcadosRechazado"))
        ok = as_int(resumen_dict.get("OKDentroDiasMaximos"))
        filas_comp = as_int(resumen_dict.get("FilasInsertadasComparativa"))

        # Contadores para estadisticas del detalle
        c_estados = Counter()
        top_obs = Counter()
        
        for r in (detalle_list or []):
            # Contar estados
            est = safe_str(r.get("ResultadoFinalAntesEventos")).strip() or "SIN_ESTADO"
            c_estados[est] += 1
            
            # Contar observaciones
            obs = safe_str(r.get("ObservacionesFase_4")).strip()
            if obs:
                top_obs[obs] += 1

        # Construir extras del resumen
        extra = ""
        if c_estados:
            extra += " | TopEstados=" + ", ".join(
                [f"{k}={v}" for k, v in c_estados.most_common(3)]
            )
        if top_obs:
            top2 = []
            for k, v in top_obs.most_common(2):
                kk = (k[:60] + "...") if len(k) > 60 else k
                top2.append(f"{kk}={v}")
            extra += " | TopObs=" + ", ".join(top2)

        return (
            f"OK | Fecha={fecha} | DiasMax={dias} | Batch={batch} | "
            f"Procesados={proc} | RetomaSet={retoma} | NoExitoso={noex} | "
            f"Rechazados={rech} | OKDentroDias={ok} | "
            f"FilasComparativa={filas_comp}{extra}"
        )

    # ==========================================================================
    # INICIO DE EJECUCION
    # ==========================================================================
    
    # Paso 1: Resetear variables a estado conocido
    reset_vars()

    # ==========================================================================
    # CONFIGURACION
    # ==========================================================================
    
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))

        # Parametros del SP (acepta nombres alternativos)
        dias = int(cfg.get("PlazoMaximo", cfg.get("DiasMaximos", 120)))
        batch = int(cfg.get("Lote", cfg.get("BatchSize", 500)))

        # Conexion a base de datos
        servidor = safe_str(cfg["ServidorBaseDatos"]).replace("\\\\", "\\")
        db = safe_str(cfg["NombreBaseDatos"])

        # Timeout opcional (0 = sin timeout)
        cmd_timeout = int(cfg.get("CommandTimeout", 0) or 0)

    except Exception as e:
        set_error_vars("Error configuracion HU4_ABCD_CamposObligatorios", e)
        try:
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", "ERROR | configuracion | HU4_ABCD | Ver vGblStrSystemError")
        except Exception:
            pass
        return False, None

    # ==========================================================================
    # EJECUCION DEL SP (SINCRONA EN THREAD)
    # ==========================================================================
    
    def run_sp_sync():
        """
        Ejecuta el SP de forma sincrona (se llama desde executor).
        
        Esta funcion interna maneja la conexion a SQL Server y la
        ejecucion del SP con WITH RESULT SETS para conversion de tipos.
        
        Returns:
            str: JSON con payload de resultados.
        
        Raises:
            RuntimeError: Si hay error de pyodbc.
        """
        # Connection string para SQL Server con autenticacion Windows
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={servidor};"
            f"DATABASE={db};"
            "Trusted_Connection=yes;"
        )

        # SQL wrapper con WITH RESULT SETS
        # Esto convierte DATETIMEOFFSET -> NVARCHAR para evitar error type -155
        sql_exec = """
        EXEC [CxP].[HU4_ABCD_CamposObligatorios] ?, ?
        WITH RESULT SETS
        (
            (
                FechaEjecucion NVARCHAR(40),
                DiasMaximos INT,
                BatchSize INT,
                RegistrosProcesados INT,
                RetomaSetDesdeNull INT,
                MarcadosNoExitoso INT,
                MarcadosRechazado INT,
                OKDentroDiasMaximos INT,
                FilasInsertadasComparativa INT
            ),
            (
                ID BIGINT,
                numero_de_factura NVARCHAR(200),
                nit_emisor_o_nit_del_proveedor NVARCHAR(200),
                documenttype NVARCHAR(50),
                Fecha_de_retoma_antes_de_contabilizacion NVARCHAR(40),
                DiasTranscurridosDesdeRetoma INT,
                ResultadoFinalAntesEventos NVARCHAR(200),
                EstadoFinalFase_4 NVARCHAR(4000),
                ObservacionesFase_4 NVARCHAR(MAX)
            )
        );
        """

        try:
            with pyodbc.connect(conn_str) as c:
                c.autocommit = True
                cur = c.cursor()

                # Establecer timeout si esta configurado
                if cmd_timeout and cmd_timeout > 0:
                    try:
                        cur.timeout = cmd_timeout
                    except Exception:
                        pass

                # Ejecutar SP con parametros
                cur.execute(sql_exec, dias, batch)

                # ResultSet 1: Resumen (1 fila)
                r1 = cur.fetchone()
                resumen = row_to_dict(cur, r1) if r1 else None

                # ResultSet 2: Detalle (N filas)
                detalle = []
                if cur.nextset():
                    try:
                        rows = cur.fetchall()
                        detalle = rows_to_dicts(cur, rows)
                    except Exception:
                        detalle = []

                # Construir payload
                payload = {
                    "resumen_general": resumen,
                    "detalle_registros": detalle,
                    "detalle_errores": detalle,  # Alias por compatibilidad
                }
                return json.dumps(payload, ensure_ascii=False, default=str)

        except pyodbc.Error as e:
            raise RuntimeError(f"pyodbc.Error ejecutando HU4_ABCD: {safe_str(e)}") from e

    # ==========================================================================
    # WRAPPER ASYNC + SETVARS FINALES
    # ==========================================================================
    
    try:
        # Ejecutar funcion sincrona en thread pool
        loop = asyncio.get_running_loop()
        payload_json = await loop.run_in_executor(None, run_sp_sync)

        # Si llego aqui, ejecuto OK
        try:
            SetVar("vLocStrResultadoSP", True)
        except Exception:
            pass

        # Parsear payload para construir resumen
        try:
            payload = json.loads(payload_json) if payload_json else {}
        except Exception:
            payload = {}

        # Construir y guardar resumen
        resumen_txt = build_summary(
            payload.get("resumen_general"),
            payload.get("detalle_registros") or payload.get("detalle_errores")
        )

        try:
            SetVar("vLocStrResumenSP", resumen_txt)
        except Exception:
            pass

        return True, payload_json

    except Exception as e:
        set_error_vars("Error ejecucion HU4_ABCD_CamposObligatorios", e)
        try:
            SetVar("vLocStrResultadoSP", False)
            SetVar("vLocStrResumenSP", "ERROR | ejecucion | HU4_ABCD | Ver vGblStrSystemError")
        except Exception:
            pass
        return False, None


# ==============================================================================
# EJEMPLOS DE USO
# ==============================================================================
"""
EJEMPLO 1: Ejecucion basica exitosa
-----------------------------------
    # Configurar
    SetVar("vLocDicConfig", {
        "ServidorBaseDatos": "SQLPROD\\INSTANCIA",
        "NombreBaseDatos": "CxP_Produccion",
        "PlazoMaximo": 120,
        "Lote": 500
    })
    
    # Ejecutar
    ok, payload = await ejecutar_HU4_ABCD_CamposObligatorios()
    
    # Verificar resultado
    if ok:
        print("Exito!")
        print(f"Resumen: {GetVar('vLocStrResumenSP')}")
        
        # Procesar payload
        data = json.loads(payload)
        print(f"Procesados: {data['resumen_general']['RegistrosProcesados']}")
        print(f"Registros en detalle: {len(data['detalle_registros'])}")
    else:
        print(f"Error: {GetVar('vGblStrMensajeError')}")
        print(f"Detalle: {GetVar('vGblStrSystemError')}")

EJEMPLO 2: Manejo de errores
----------------------------
    # Configuracion invalida (servidor no existe)
    SetVar("vLocDicConfig", {
        "ServidorBaseDatos": "SERVIDOR_INEXISTENTE",
        "NombreBaseDatos": "DB",
        "PlazoMaximo": 120,
        "Lote": 500
    })
    
    ok, payload = await ejecutar_HU4_ABCD_CamposObligatorios()
    
    # ok = False
    # payload = None
    # GetVar("vLocStrResultadoSP") = False
    # GetVar("vGblStrMensajeError") contiene mensaje de error
    # GetVar("vGblStrSystemError") contiene stack trace

EJEMPLO 3: Con timeout personalizado
------------------------------------
    SetVar("vLocDicConfig", {
        "ServidorBaseDatos": "SQLPROD\\INSTANCIA",
        "NombreBaseDatos": "CxP_Produccion",
        "PlazoMaximo": 120,
        "Lote": 1000,
        "CommandTimeout": 300  # 5 minutos
    })
    
    ok, payload = await ejecutar_HU4_ABCD_CamposObligatorios()

EJEMPLO 4: Procesamiento del detalle
------------------------------------
    ok, payload = await ejecutar_HU4_ABCD_CamposObligatorios()
    
    if ok:
        data = json.loads(payload)
        
        # Filtrar registros rechazados
        rechazados = [
            r for r in data['detalle_registros']
            if r['ResultadoFinalAntesEventos'] == 'RECHAZADO'
        ]
        
        # Agrupar por NIT
        from collections import defaultdict
        por_nit = defaultdict(list)
        for r in data['detalle_registros']:
            nit = r['nit_emisor_o_nit_del_proveedor']
            por_nit[nit].append(r)
        
        print(f"NITs unicos: {len(por_nit)}")
        print(f"Rechazados: {len(rechazados)}")
"""
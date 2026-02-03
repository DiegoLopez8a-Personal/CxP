"""
================================================================================
SCRIPT: buscarCandidatos.py
================================================================================

Descripcion General:
--------------------
    Identifica y prepara registros candidatos para validacion en el proceso
    HU4.1 de Cuentas por Pagar. Cruza informacion entre DocumentsProcessing,
    HistoricoOrdenesCompra y DetailsProcessing para encontrar coincidencias
    validas y crear la tabla de candidatos para validacion.

Autor: Diego Ivan Lopez Ochoa
Version: 2.0.0 - Soporte para múltiples órdenes de compra separadas por coma
Plataforma: RocketBot RPA

================================================================================
CAMBIOS VERSION 2.0.0
================================================================================

    - Soporte para múltiples órdenes de compra separadas por coma en DP
    - Búsqueda en HOC para cada OC individual
    - Validación de que la combinación encontrada tenga al menos 1 registro
      de cada OC original
    - Nueva categoría de estadísticas: sin_representacion_oc

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |                  buscarCandidatos()                         |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Obtener configuracion desde vLocDicConfig                  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Conectar a base de datos SQL Server                        |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Consultar tablas fuente:                                   |
    |  - [CxP].[DocumentsProcessing] (df_dp)                      |
    |  - [CxP].[HistoricoOrdenesCompra] (df_hoc)                  |
    |  - [CxP].[DetailsProcessing] (df_ddp)                       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Crear indices MultiIndex para busquedas rapidas:           |
    |  - df_hoc: (NitCedula, NumOC)                               |
    |  - df_ddp: (nit, numero_factura)                            |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Para cada registro en df_dp:                               |
    |  +-------------------------------------------------------+  |
    |  |  SI no tiene OC:                                      |  |
    |  |    -> Marcar CON NOVEDAD "Sin orden de compra"        |  |
    |  +-------------------------------------------------------+  |
    |  |  SEPARAR OC por comas (puede tener 1 a 3 valores)     |  |
    |  |  BUSCAR registros en HOC para CADA OC                 |  |
    |  +-------------------------------------------------------+  |
    |  |  SI no encuentra en HOC o DDP:                        |  |
    |  |    -> Marcar EN ESPERA "No encontrado en historico"   |  |
    |  +-------------------------------------------------------+  |
    |  |  SI cant_hoc <= cant_ddp:                             |  |
    |  |    -> Validar representacion de cada OC               |  |
    |  |    -> Si OK: crear candidato directo                  |  |
    |  |    -> Si NO: marcar CON NOVEDAD                       |  |
    |  +-------------------------------------------------------+  |
    |  |  SI cant_hoc > cant_ddp:                              |  |
    |  |    -> Buscar combinacion optima de posiciones         |  |
    |  |    -> Validar que combinacion tenga al menos 1 de     |  |
    |  |       cada OC original                                |  |
    |  |    -> Si encuentra y valida: crear candidato          |  |
    |  |    -> Si no: marcar EN ESPERA o CON NOVEDAD           |  |
    |  +-------------------------------------------------------+  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Ejecutar batch updates en DocumentsProcessing y Comparativa|
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Crear/recrear tabla [CxP].[HU41_CandidatosValidacion]      |
    |  Insertar candidatos validos                                |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar estadisticas y configurar variables RocketBot     |
    +-------------------------------------------------------------+

================================================================================
VARIABLES DE ENTRADA (RocketBot)
================================================================================

    vLocDicConfig : str | dict
        Configuracion JSON con parametros:
        - ServidorBaseDatos: Servidor SQL Server
        - NombreBaseDatos: Nombre de la base de datos
        - Tolerancia: Tolerancia para comparacion de valores (default: 500)

    vGblStrUsuarioBaseDatos : str
        Usuario para conexion SQL Server

    vGblStrClaveBaseDatos : str
        Contrasena para conexion SQL Server

================================================================================
VARIABLES DE SALIDA (RocketBot)
================================================================================

    vLocStrResultadoSP : str
        "True" si exitoso, "False" si error

    vLocStrResumenSP : str
        Resumen del procesamiento con estadisticas

    vLocDfCandidatosJson : str
        DataFrame de candidatos en formato JSON

    vLocDicEstadisticas : str
        Diccionario de estadisticas en formato JSON

    vGblStrDetalleError : str
        Traceback en caso de error

    vGblStrSystemError : str
        Identificador del error del sistema

================================================================================
TABLAS UTILIZADAS
================================================================================

Tablas de Entrada:
------------------
    [CxP].[DocumentsProcessing]
        Documentos de facturacion electronica pendientes de validacion.
        Filtro: documenttype = 'FV' AND Fecha_de_retoma IS NOT NULL
        NOTA: El campo numero_de_liquidacion_u_orden_de_compra puede contener
              hasta 3 valores separados por coma (ej: "OC1,OC2,OC3")
        
    [CxP].[HistoricoOrdenesCompra]
        Historico de ordenes de compra desde SAP.
        Campos clave: NitCedula, DocCompra, PorCalcular
        
    [CxP].[DetailsProcessing]
        Detalles de lineas de factura XML.
        Campos clave: nit, numero_factura, Valor de la Compra LEA

Tablas de Salida:
-----------------
    [CxP].[HU41_CandidatosValidacion]
        Tabla de candidatos para validacion.
        Se recrea en cada ejecucion (DROP + CREATE).
        Todas las columnas son NVARCHAR(MAX).
        NOTA: El campo de OC se guarda SIN separar (valor original de DP)

    [dbo].[CxP.Comparativa]
        Actualizacion de estados y observaciones.

================================================================================
ALGORITMO DE COMBINACION (ACTUALIZADO v2.0)
================================================================================

Cuando hay mas posiciones en HOC que en DDP, se busca la combinacion
optima de posiciones que:
    1. Sume el valor mas cercano al total de la factura (tolerancia ±500)
    2. Contenga AL MENOS 1 registro de CADA orden de compra original

Pasos:
    1. Separar el campo OC por comas para obtener lista de OCs
    2. Buscar en HOC todos los registros que coincidan con NIT y cualquier OC
    3. Obtener suma total de "Valor de la Compra LEA" de DDP
    4. Obtener lista de valores "PorCalcular" de HOC con su OC asociada
    5. Generar combinaciones de N elementos (donde N = cant_ddp)
    6. Para cada combinacion:
       a. Verificar que la suma este dentro de la tolerancia
       b. Verificar que tenga al menos 1 registro de cada OC original
    7. Si encuentra combinacion valida: usar esas posiciones para el candidato
    8. Si no encuentra por suma: marcar "Sin combinacion valida"
    9. Si no encuentra por representacion: marcar "Sin representacion de OC"

================================================================================
ESTADISTICAS RETORNADAS
================================================================================

    total               : Total de registros procesados
    candidatos          : Registros que pasaron a tabla de candidatos
    sin_oc              : Registros sin orden de compra
    no_encontrados      : No encontrados en historico
    sin_combinacion     : Sin combinacion valida de posiciones (por suma)
    sin_representacion  : Sin representacion de todas las OC en la combinacion
    errores             : Errores durante procesamiento
    tiempo_total        : Tiempo total de ejecucion (segundos)
    tiempo_procesamiento: Tiempo de procesamiento de registros
    tiempo_updates      : Tiempo de actualizaciones en BD
    tiempo_tabla        : Tiempo de creacion de tabla candidatos

================================================================================
EJEMPLOS DE USO
================================================================================

    # Configurar variables en RocketBot
    SetVar("vLocDicConfig", json.dumps({
        "ServidorBaseDatos": "servidor.ejemplo.com",
        "NombreBaseDatos": "NotificationsPaddy",
        "Tolerancia": 500
    }))
    SetVar("vGblStrUsuarioBaseDatos", "usuario")
    SetVar("vGblStrClaveBaseDatos", "contrasena")
    
    # Ejecutar funcion
    buscarCandidatos()
    
    # Verificar resultado
    resultado = GetVar("vLocStrResultadoSP")
    resumen = GetVar("vLocStrResumenSP")

================================================================================
MANEJO DE ERRORES
================================================================================

    - Errores de conexion: Reintenta hasta 3 veces con backoff exponencial
    - Errores por registro: Continua con siguiente, incrementa contador errores
    - Errores criticos: Rollback, configura variables de error, retorna False
    - Observaciones se truncan a 3900 caracteres para evitar overflow

================================================================================
NOTAS TECNICAS
================================================================================

    - Usa context manager para conexion (garantiza cierre)
    - Procesa en memoria con pandas para velocidad
    - Batch updates para minimizar round-trips a BD
    - Tabla candidatos se recrea (no append) para consistencia
    - Todas las columnas NVARCHAR(MAX) para flexibilidad
    - Soporta hasta 3 OC separadas por coma en campo de DP

================================================================================
"""

def buscarCandidatos():
    import json
    import ast
    import traceback
    import pyodbc
    import pandas as pd
    import numpy as np
    from itertools import combinations
    from datetime import datetime
    from contextlib import contextmanager
    import time
    import warnings
    
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    def safe_str(v):
        if v is None:
            return ""
        if isinstance(v, str):
            return v.strip()
        if isinstance(v, bytes):
            try:
                return v.decode('latin-1', errors='replace').strip()
            except:
                return str(v).strip()
        if isinstance(v, (int, float)):
            if isinstance(v, float) and np.isnan(v):
                return ""
            return str(v)
        try:
            return str(v).strip()
        except:
            return ""
    
    def truncar_observacion(obs):
        """Truncar observación a 3900 caracteres para prevenir overflow"""
        if not obs:
            return ""
        obs_str = safe_str(obs)
        if len(obs_str) > 3900:
            return obs_str[:3900]
        return obs_str
    
    def parse_config(raw):
        if isinstance(raw, dict):
            if not raw:
                raise ValueError("Config empty")
            return raw
        text = safe_str(raw)
        if not text:
            raise ValueError("vLocDicConfig empty")
        try:
            config = json.loads(text)
            if not config:
                raise ValueError("Config empty JSON")
            return config
        except json.JSONDecodeError:
            pass
        try:
            config = ast.literal_eval(text)
            if not config:
                raise ValueError("Config empty literal")
            return config
        except (ValueError, SyntaxError) as e:
            raise ValueError("Invalid config: " + str(e))
    
    # =========================================================================
    # NUEVA FUNCIÓN: Separar órdenes de compra
    # =========================================================================
    def separar_ordenes_compra(oc_str):
        """
        Separa el campo de orden de compra que puede contener hasta 3 valores
        separados por coma.
        
        Args:
            oc_str: String con OC(s), puede ser "OC1" o "OC1,OC2" o "OC1,OC2,OC3"
        
        Returns:
            Lista de OC individuales limpias (sin espacios)
        """
        if not oc_str:
            return []
        
        # Separar por coma y limpiar cada valor
        ocs = [safe_str(oc.strip()) for oc in str(oc_str).split(',')]
        
        # Filtrar valores vacíos
        ocs = [oc for oc in ocs if oc]
        
        return ocs
    
    # =========================================================================
    # NUEVA FUNCIÓN: Buscar registros HOC para múltiples OC
    # =========================================================================
    def buscar_hoc_multiple_oc(df_hoc, nit, lista_ocs):
        """
        Busca en HOC todos los registros que coincidan con el NIT y cualquiera
        de las órdenes de compra en la lista.
        
        Args:
            df_hoc: DataFrame de HistoricoOrdenesCompra (indexado por NitCedula, DocCompra)
            nit: NIT del proveedor
            lista_ocs: Lista de órdenes de compra a buscar
        
        Returns:
            DataFrame con todos los registros encontrados, con columna adicional 'OC_Original'
        """
        if df_hoc.empty or not lista_ocs:
            return pd.DataFrame()
        
        resultados = []
        
        for oc in lista_ocs:
            try:
                if (nit, oc) in df_hoc.index:
                    df_temp = df_hoc.loc[[(nit, oc)]].copy()
                    df_temp['OC_Original'] = oc  # Marcar de qué OC viene cada registro
                    resultados.append(df_temp)
            except Exception as e:
                print(f"[DEBUG] Error buscando HOC para NIT={nit}, OC={oc}: {e}")
                continue
        
        if not resultados:
            return pd.DataFrame()
        
        # Combinar todos los resultados
        df_combinado = pd.concat(resultados, ignore_index=True)
        
        return df_combinado
    
    # =========================================================================
    # NUEVA FUNCIÓN: Validar representación de OC en combinación
    # =========================================================================
    def validar_representacion_oc(df_hoc_seleccionado, lista_ocs_originales):
        """
        Valida que en el DataFrame seleccionado haya al menos 1 registro
        de cada orden de compra original.
        
        Args:
            df_hoc_seleccionado: DataFrame con los registros HOC seleccionados
            lista_ocs_originales: Lista de OC que deben estar representadas
        
        Returns:
            tuple: (bool_valido, lista_ocs_faltantes)
        """
        if df_hoc_seleccionado.empty:
            return False, lista_ocs_originales
        
        # Obtener OCs presentes en la selección
        if 'OC_Original' in df_hoc_seleccionado.columns:
            ocs_presentes = set(df_hoc_seleccionado['OC_Original'].unique())
        elif 'DocCompra' in df_hoc_seleccionado.columns:
            ocs_presentes = set(df_hoc_seleccionado['DocCompra'].unique())
        else:
            return False, lista_ocs_originales
        
        # Verificar que todas las OC originales estén representadas
        ocs_requeridas = set(lista_ocs_originales)
        ocs_faltantes = ocs_requeridas - ocs_presentes
        
        return len(ocs_faltantes) == 0, list(ocs_faltantes)
    
    # =========================================================================
    # FUNCIÓN MODIFICADA: Buscar combinación con validación de OC
    # =========================================================================
    def buscar_combinacion_con_validacion_oc(df_hoc, objetivo, cantidad, lista_ocs_originales, tolerancia=500):
        """
        Busca la combinación óptima de posiciones que:
        1. Sume el valor más cercano al objetivo (dentro de tolerancia)
        2. Contenga al menos 1 registro de cada OC original
        
        Args:
            df_hoc: DataFrame con registros HOC (debe tener 'PorCalcular' y 'OC_Original')
            objetivo: Valor objetivo a alcanzar
            cantidad: Cantidad de posiciones a seleccionar
            lista_ocs_originales: Lista de OC que deben estar representadas
            tolerancia: Tolerancia para la comparación de valores
        
        Returns:
            tuple: (indices_encontrados, motivo_fallo)
                - indices_encontrados: lista de índices si se encontró, None si no
                - motivo_fallo: None si se encontró, string describiendo el fallo si no
        """
        try:
            if df_hoc.empty:
                return None, "DataFrame HOC vacío"
            
            n = len(df_hoc)
            if n < cantidad or cantidad <= 0:
                return None, f"Cantidad insuficiente: {n} registros para {cantidad} posiciones"
            
            valores = df_hoc['PorCalcular'].tolist()
            ocs = df_hoc['OC_Original'].tolist() if 'OC_Original' in df_hoc.columns else df_hoc['DocCompra'].tolist()
            
            # Caso especial: cantidad == 1
            if cantidad == 1:
                for i, v in enumerate(valores):
                    if abs(v - objetivo) <= tolerancia:
                        # Verificar representación de OC
                        oc_en_pos = ocs[i]
                        if oc_en_pos in lista_ocs_originales:
                            # Si solo hay 1 OC original, OK
                            if len(lista_ocs_originales) == 1:
                                return [i], None
                            else:
                                # Con 1 posición no podemos representar múltiples OC
                                continue
                # Si llegamos aquí, no encontramos
                return None, "Sin combinación válida para cantidad=1"
            
            # Para múltiples posiciones, generar combinaciones
            mejor_combo = None
            mejor_diff = float('inf')
            
            # Primero intentar con combinaciones que cumplan ambos criterios
            for combo_idx in combinations(range(n), cantidad):
                indices = list(combo_idx)
                suma = sum(valores[i] for i in indices)
                
                # Verificar tolerancia
                if abs(suma - objetivo) <= tolerancia:
                    # Verificar representación de OC
                    ocs_en_combo = set(ocs[i] for i in indices)
                    ocs_requeridas = set(lista_ocs_originales)
                    
                    if ocs_requeridas.issubset(ocs_en_combo):
                        # ¡Encontramos una combinación válida!
                        diff = abs(suma - objetivo)
                        if diff < mejor_diff:
                            mejor_diff = diff
                            mejor_combo = indices
            
            if mejor_combo is not None:
                return mejor_combo, None
            
            # Si no encontramos, determinar el motivo
            # Buscar si hay alguna combinación que cumpla solo la tolerancia
            hay_combo_tolerancia = False
            for combo_idx in combinations(range(n), cantidad):
                indices = list(combo_idx)
                suma = sum(valores[i] for i in indices)
                if abs(suma - objetivo) <= tolerancia:
                    hay_combo_tolerancia = True
                    break
            
            if hay_combo_tolerancia:
                return None, "SIN_REPRESENTACION_OC"
            else:
                return None, "SIN_COMBINACION_SUMA"
                
        except Exception as e:
            return None, f"Error en búsqueda: {str(e)}"
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            raise ValueError("Missing params: " + ', '.join(missing))

        usuario = GetVar("vGblStrUsuarioBaseDatos")
        contrasena = GetVar("vGblStrClaveBaseDatos")        
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=" + cfg['ServidorBaseDatos'] + ";"
            "DATABASE=" + cfg['NombreBaseDatos'] + ";"
            f"UID={usuario};"
            f"PWD={contrasena};"
            "autocommit=False;"
        )
        
        cx = None
        for attempt in range(max_retries):
            try:
                cx = pyodbc.connect(conn_str, timeout=30)
                cx.autocommit = False
                print("[DEBUG] Conexion SQL abierta (intento " + str(attempt + 1) + ")")
                break
            except pyodbc.Error:
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))
                    continue
                raise
        try:
            yield cx
            if cx:
                cx.commit()
                print("[DEBUG] Commit final de conexion exitoso")
        except Exception as e:
            if cx:
                cx.rollback()
                print("[ERROR] Rollback por error: " + str(e))
            raise
        finally:
            if cx:
                try:
                    cx.close()
                    print("[DEBUG] Conexion cerrada")
                except:
                    pass
    
    def tabla_existe(cx, schema, tabla):
        query = """
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
        cur = cx.cursor()
        cur.execute(query, (schema, tabla))
        count = cur.fetchone()[0]
        cx.commit()
        cur.close()
        return count > 0
    
    def crear_tabla_candidatos(cx, df_muestra):
        col_defs = []
        for col in df_muestra.columns:
            col_safe = "[" + col + "]"
            col_defs.append(col_safe + " NVARCHAR(MAX)")
        
        create_sql = (
            "CREATE TABLE [CxP].[HU41_CandidatosValidacion] (\n    " +
            ",\n    ".join(col_defs) +
            "\n)"
        )
        
        print("[DEBUG] Tabla [CxP].[HU41_CandidatosValidacion] creada - TODAS las columnas NVARCHAR(MAX)")
        
        cur = cx.cursor()
        cur.execute(create_sql)
        cx.commit()
        cur.close()
    
    def insertar_candidatos(cx, df):
        if df.empty:
            return
        
        try:
            columns = df.columns.tolist()
            
            placeholders = []
            for col in columns:
                placeholders.append("CAST(? AS NVARCHAR(MAX))")
            
            placeholders_str = ','.join(placeholders)
            columns_str = ','.join(['[' + col + ']' for col in columns])
            
            insert_sql = (
                "INSERT INTO [CxP].[HU41_CandidatosValidacion] (" +
                columns_str + ") VALUES (" + placeholders_str + ")"
            )
            
            print("[DEBUG] Preparando insercion de " + str(len(df)) + " registros")
            print("[DEBUG] TODAS las columnas se insertan como NVARCHAR(MAX)")
            
            cur = cx.cursor()
            
            rows_inserted = 0
            for idx, row in df.iterrows():
                try:
                    values = []
                    for col in columns:
                        val = row[col]
                        if pd.isna(val):
                            values.append(None)
                        else:
                            values.append(safe_str(val))
                    
                    cur.execute(insert_sql, tuple(values))
                    rows_inserted += 1
                    
                    if rows_inserted % 50 == 0:
                        print("[DEBUG] Insertados " + str(rows_inserted) + " registros...")
                        
                except Exception as e_row:
                    print("[ERROR] Error en fila " + str(idx) + ": " + str(e_row))
                    print("[DEBUG] Primeros 5 valores:")
                    for i, col in enumerate(columns[:5]):
                        v = str(row[col])[:50] if not pd.isna(row[col]) else "NULL"
                        print("  " + col + ": " + v)
                    raise
            
            cx.commit()
            cur.close()
            print("[DEBUG] Commit exitoso. Filas insertadas: " + str(rows_inserted))
            
        except Exception as e:
            print("[ERROR] Error insertando candidatos: " + str(e))
            cx.rollback()
            raise
    
    def unir_valores(df, columna):
        if df.empty or columna not in df.columns:
            return ""
        try:
            vals = df[columna].values
            str_vals = [safe_str(v) for v in vals]
            return '|'.join(str_vals)
        except:
            return ""
    
    def batch_update(cx, query, params_list, max_retries=3):
        if not params_list:
            return
        for attempt in range(max_retries):
            try:
                cur = cx.cursor()
                cur.fast_executemany = True
                for params in params_list:
                    safe_params = list(params)
                    if len(safe_params) >= 2:
                        safe_params[1] = truncar_observacion(safe_params[1])
                    
                    safe_params_tuple = tuple(
                        safe_str(p) if isinstance(p, str) else p 
                        for p in safe_params
                    )
                    cur.execute(query, safe_params_tuple)
                cx.commit()
                cur.close()
                return
            except pyodbc.Error:
                cx.rollback()
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise
    
    def crear_candidato(row_dp, df_ddp, df_hoc):
        """
        Crea un registro candidato combinando datos de DP, DDP y HOC.
        NOTA: El campo de OC se mantiene con el valor ORIGINAL de DP (sin separar)
        """
        candidato = {}
        for col, val in row_dp.items():
            candidato[col + "_dp"] = val
        for col in df_ddp.columns:
            candidato[col + "_ddp"] = unir_valores(df_ddp, col)
        for col in df_hoc.columns:
            # Excluir la columna auxiliar OC_Original del candidato final
            if col != 'OC_Original':
                candidato[col + "_hoc"] = unir_valores(df_hoc, col)
        candidato["indices_ddp"] = '|'.join(map(str, df_ddp.index.tolist()))
        candidato["indices_hoc"] = '|'.join(map(str, df_hoc.index.tolist()))
        return candidato
    
    def read_sql_safe(query, cx):
        try:
            return pd.read_sql(query, cx)
        except UnicodeDecodeError:
            cur = cx.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            data = []
            for row in rows:
                safe_row = []
                for val in row:
                    if isinstance(val, str):
                        safe_row.append(val)
                    elif isinstance(val, bytes):
                        safe_row.append(safe_str(val))
                    else:
                        safe_row.append(val)
                data.append(safe_row)
            cx.commit()
            cur.close()
            return pd.DataFrame(data, columns=columns)
    
    # =========================================================================
    # ESTADÍSTICAS ACTUALIZADAS: Nueva categoría sin_representacion
    # =========================================================================
    stats = {
        "total": 0, "candidatos": 0, "sin_oc": 0,
        "no_encontrados": 0, "sin_combinacion": 0, 
        "sin_representacion": 0,  # NUEVO: OC no representadas en combinación
        "errores": 0,
        "tiempo_carga": 0, "tiempo_procesamiento": 0,
        "tiempo_updates": 0, "tiempo_tabla": 0, "tiempo_total": 0
    }
    
    updates_dp = []
    updates_comp = []
    candidatos_lista = []
    t_inicio = time.time()
    
    print("="*80)
    print("[INICIO] Funcion buscarCandidatos() v2.0 iniciada")
    print("[INICIO] Soporte para múltiples OC separadas por coma")
    print("[INICIO] Timestamp: " + str(datetime.now()))
    print("="*80)
    
    try:
        print("[DEBUG] Obteniendo configuracion...")
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[DEBUG] Configuracion obtenida OK")
        print("[DEBUG] Servidor: " + cfg.get('ServidorBaseDatos', 'NO DEFINIDO'))
        print("[DEBUG] Base de datos: " + cfg.get('NombreBaseDatos', 'NO DEFINIDO'))
        tolerancia = cfg.get("Tolerancia", 500)
        max_retries = cfg.get("MaxRetries", 3)
        
        t_carga = time.time()
        
        with crear_conexion_db(cfg, max_retries) as cx:
            
            q1 = (
                "SELECT * FROM [CxP].[DocumentsProcessing] WITH (NOLOCK) "
                "WHERE documenttype = 'FV' "
                "AND (EstadoFinalFase_5 IS NULL OR LTRIM(RTRIM(EstadoFinalFase_5)) = '') "
                "AND (ResultadoFinalAntesEventos IS NULL OR ResultadoFinalAntesEventos NOT IN ("
                "'APROBADO','APROBADO CONTADO Y/O EVENTO MANUAL','APROBADO SIN CONTABILIZACION',"
                "'RECHAZADO - RETORNADO','RECLASIFICAR','EXCLUIDO IMPORTACIONES',"
                "'EXCLUIDO COSTO INDIRECTO FLETES','EXCLUIDO GRANOS','EXCLUIDO MAIZ',"
                "'NO EXITOSO','RECHAZADO','CON NOVEDAD'))"
            )
            
            q2 = "SELECT * FROM [CxP].[DocumentsDetailProcessing] WITH (NOLOCK)"
            
            q3 = (
                "SELECT * FROM [CxP].[HistoricoOrdenesCompra] WITH (NOLOCK) "
                "WHERE Marca IS NULL OR Marca != 'PROCESADO'"
            )
            
            df_dp = read_sql_safe(q1, cx)
            df_ddp = read_sql_safe(q2, cx)
            df_hoc = read_sql_safe(q3, cx)
            
            stats["total"] = len(df_dp)
            stats["tiempo_carga"] = time.time() - t_carga
            
            print(f"[DEBUG] Registros cargados: DP={len(df_dp)}, DDP={len(df_ddp)}, HOC={len(df_hoc)}")
            
            if not df_ddp.empty:
                df_ddp.set_index(['nit_emisor_o_nit_del_proveedor', 'numero_de_factura'],
                                inplace=True, drop=False)
                df_ddp.sort_index(inplace=True)
            if not df_hoc.empty:
                df_hoc.set_index(['NitCedula', 'DocCompra'], inplace=True, drop=False)
                df_hoc.sort_index(inplace=True)
            
            t_proc = time.time()
            
            # =========================================================================
            # CICLO PRINCIPAL MODIFICADO: Soporte para múltiples OC
            # =========================================================================
            for idx, row in df_dp.iterrows():
                try:
                    nit = safe_str(row["nit_emisor_o_nit_del_proveedor"])
                    factura = safe_str(row["numero_de_factura"])
                    oc_campo = row.get("numero_de_liquidacion_u_orden_de_compra")
                    forma_pago = row.get("valor_a_pagar")
                    oc_str_original = safe_str(oc_campo)  # Valor original sin modificar
                    
                    # =========================================================
                    # PASO 1: Verificar si tiene OC
                    # =========================================================
                    if not oc_str_original:
                        stats["sin_oc"] += 1
                        updates_dp.append((
                            "VALIDACION DATOS DE FACTURACION: Exitoso",
                            "Registro NO cuenta con Orden de compra",
                            "CON NOVEDAD", nit, factura
                        ))
                        updates_comp.append((
                            "SIN ORDEN DE COMPRA", "CON NOVEDAD",
                            "Registro NO cuenta con Orden de compra", nit, factura
                        ))
                        continue
                    
                    # =========================================================
                    # PASO 2: Separar OC por comas (NUEVO)
                    # =========================================================
                    lista_ocs = separar_ordenes_compra(oc_str_original)
                    
                    if not lista_ocs:
                        stats["sin_oc"] += 1
                        updates_dp.append((
                            "VALIDACION DATOS DE FACTURACION: Exitoso",
                            "Orden de compra vacía después de procesar",
                            "CON NOVEDAD", nit, factura
                        ))
                        updates_comp.append((
                            "SIN ORDEN DE COMPRA", "CON NOVEDAD",
                            "Orden de compra vacía después de procesar", nit, factura
                        ))
                        continue
                    
                    print(f"[DEBUG] Factura {factura}: OCs encontradas = {lista_ocs}")
                    
                    # =========================================================
                    # PASO 3: Buscar en HOC para TODAS las OC (NUEVO)
                    # =========================================================
                    hoc = buscar_hoc_multiple_oc(df_hoc, nit, lista_ocs)
                    
                    # =========================================================
                    # PASO 4: Buscar en DDP
                    # =========================================================
                    try:
                        ddp = (df_ddp.loc[[(nit, factura)]].copy().reset_index(drop=True)
                               if (nit, factura) in df_ddp.index else pd.DataFrame())
                    except:
                        ddp = pd.DataFrame()
                    
                    # =========================================================
                    # PASO 5: Verificar si se encontraron registros
                    # =========================================================
                    if hoc.empty or ddp.empty:
                        stats["no_encontrados"] += 1
                        estado = "EN ESPERA - CONTADO" if forma_pago in (1,"1","01") else "EN ESPERA"
                        obs = f"No se encuentra registro en historico para OC: {oc_str_original}"
                        updates_dp.append((
                            "VALIDACION DATOS DE FACTURACION: Exitoso", obs, estado, nit, factura
                        ))
                        updates_comp.append(("LLAVES NO ENCONTRADAS", estado, obs, nit, factura))
                        continue
                    
                    cant_hoc, cant_ddp = len(hoc), len(ddp)
                    print(f"[DEBUG] Factura {factura}: HOC={cant_hoc}, DDP={cant_ddp}")
                    
                    # =========================================================
                    # PASO 6: Procesar según cantidad de registros
                    # =========================================================
                    if cant_hoc <= cant_ddp:
                        # Caso simple: usar todos los registros HOC
                        # Pero debemos validar representación de OC
                        valido, ocs_faltantes = validar_representacion_oc(hoc, lista_ocs)
                        
                        if valido:
                            candidatos_lista.append(crear_candidato(row, ddp, hoc))
                            stats["candidatos"] += 1
                            print(f"[DEBUG] Factura {factura}: Candidato creado (caso simple)")
                        else:
                            # No todas las OC están representadas
                            stats["sin_representacion"] += 1
                            obs = f"OC no representadas en historico: {', '.join(ocs_faltantes)}"
                            updates_dp.append((
                                "VALIDACION DATOS DE FACTURACION: Exitoso", obs, "CON NOVEDAD", nit, factura
                            ))
                            updates_comp.append((
                                "SIN REPRESENTACION OC", "CON NOVEDAD", obs, nit, factura
                            ))
                            print(f"[DEBUG] Factura {factura}: Sin representación de OC: {ocs_faltantes}")
                    else:
                        # Caso complejo: buscar combinación óptima
                        suma_lea = ddp["Valor de la Compra LEA"].sum()
                        
                        # Usar la nueva función de búsqueda con validación de OC
                        combo, motivo_fallo = buscar_combinacion_con_validacion_oc(
                            hoc, suma_lea, cant_ddp, lista_ocs, tolerancia
                        )
                        
                        if combo is not None:
                            # Encontramos combinación válida
                            hoc_sel = hoc.iloc[combo].copy()
                            candidatos_lista.append(crear_candidato(row, ddp, hoc_sel))
                            stats["candidatos"] += 1
                            print(f"[DEBUG] Factura {factura}: Candidato creado (combinación encontrada)")
                        else:
                            # No se encontró combinación válida
                            estado = "EN ESPERA - CONTADO" if forma_pago in (1,"1","01") else "EN ESPERA"
                            
                            if motivo_fallo == "SIN_REPRESENTACION_OC":
                                stats["sin_representacion"] += 1
                                obs = f"Combinacion valida por suma pero sin representacion de todas las OC: {oc_str_original}"
                                estado = "CON NOVEDAD"
                                updates_comp.append((
                                    "SIN REPRESENTACION OC", estado, obs, nit, factura
                                ))
                            else:
                                stats["sin_combinacion"] += 1
                                obs = f"No se encuentra combinacion valida para OC: {oc_str_original}"
                                updates_comp.append((
                                    "LLAVES NO ENCONTRADAS", estado, obs, nit, factura
                                ))
                            
                            updates_dp.append((
                                "VALIDACION DATOS DE FACTURACION: Exitoso", obs, estado, nit, factura
                            ))
                            print(f"[DEBUG] Factura {factura}: {motivo_fallo}")
                
                except Exception as e:
                    stats["errores"] += 1
                    print(f"[ERROR] Error procesando registro {idx}: {e}")
                    continue
            
            stats["tiempo_procesamiento"] = time.time() - t_proc
            t_updates = time.time()
            
            if updates_dp:
                batch_update(cx,
                    "UPDATE [CxP].[DocumentsProcessing] SET EstadoFinalFase_4=?, "
                    "ObservacionesFase_4=?, ResultadoFinalAntesEventos=? "
                    "WHERE nit_emisor_o_nit_del_proveedor=? AND numero_de_factura=?",
                    updates_dp, max_retries)
            
            if updates_comp:
                params_comp_truncado = []
                for params in updates_comp:
                    params_lista = list(params)
                    if len(params_lista) >= 3:
                        params_lista[2] = truncar_observacion(params_lista[2])
                    params_comp_truncado.append(tuple(params_lista))
                
                batch_update(cx,
                    "UPDATE [dbo].[CxP.Comparativa] SET Orden_de_Compra=?, "
                    "Estado_validacion_antes_de_eventos=?, "
                    "Valor_XML=CASE WHEN Item='Observaciones' THEN ? ELSE Valor_XML END "
                    "WHERE NIT=? AND Factura=?",
                    params_comp_truncado, max_retries)
            
            stats["tiempo_updates"] = time.time() - t_updates
            
            if candidatos_lista:
                df_candidatos = pd.DataFrame(candidatos_lista)
                df_candidatos = df_candidatos.where(pd.notna(df_candidatos), None)
            else:
                df_candidatos = pd.DataFrame()
            
            t_tabla = time.time()
            
            if not df_candidatos.empty:
                print("[DEBUG] Iniciando proceso de tabla SQL...")
                print("[DEBUG] Candidatos a insertar: " + str(len(df_candidatos)))
                
                existe = tabla_existe(cx, "CxP", "HU41_CandidatosValidacion")
                print("[DEBUG] Tabla existe: " + str(existe))
                
                if existe:
                    print("[DEBUG] Borrando tabla existente para recrear...")
                    cur = cx.cursor()
                    cur.execute("DROP TABLE [CxP].[HU41_CandidatosValidacion]")
                    cx.commit()
                    cur.close()
                    print("[DEBUG] Tabla borrada OK")
                
                print("[DEBUG] Creando tabla con estructura actual...")
                crear_tabla_candidatos(cx, df_candidatos)
                print("[DEBUG] Tabla creada OK")
                
                print("[DEBUG] Insertando " + str(len(df_candidatos)) + " registros...")
                insertar_candidatos(cx, df_candidatos)
                print("[DEBUG] Registros insertados OK")
            else:
                print("[DEBUG] DataFrame de candidatos VACIO - no se insertara nada")
            
            stats["tiempo_tabla"] = time.time() - t_tabla
            print("[DEBUG] Tiempo tabla: " + str(stats["tiempo_tabla"]) + "s")
        
        stats["tiempo_total"] = time.time() - t_inicio
        
        # =========================================================================
        # RESUMEN ACTUALIZADO: Incluye sin_representacion
        # =========================================================================
        msg = ("Done. Total:" + str(stats['total']) + 
               " Candidatos:" + str(stats['candidatos']) +
               " SinOC:" + str(stats['sin_oc']) + 
               " NoEnc:" + str(stats['no_encontrados']) +
               " SinComb:" + str(stats['sin_combinacion']) + 
               " SinRep:" + str(stats['sin_representacion']) +  # NUEVO
               " Err:" + str(stats['errores']) +
               " Time:" + str(round(stats['tiempo_total'], 2)) + "s")
        
        print("[RESULTADO] " + msg)
        print("[ESTADISTICAS DETALLADAS]")
        print("  Total procesados: " + str(stats['total']))
        print("  Candidatos validos: " + str(stats['candidatos']))
        print("  Sin orden de compra: " + str(stats['sin_oc']))
        print("  No encontrados: " + str(stats['no_encontrados']))
        print("  Sin combinacion (suma): " + str(stats['sin_combinacion']))
        print("  Sin representacion OC: " + str(stats['sin_representacion']))  # NUEVO
        print("  Errores: " + str(stats['errores']))
        print("  Tiempo total: " + str(stats['tiempo_total']) + "s")
        
        print("="*80)
        print("[EXITO] Funcion completada exitosamente")
        print("="*80)
        
        SetVar("vLocDfCandidatosJson", df_candidatos.to_json(orient="records"))
        SetVar("vLocDicEstadisticas", json.dumps(stats))
        
        SetVar("vGblStrDetalleError", "")
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        
        SetVar("vLocStrResultadoSP", True)
        SetVar("vLocStrResumenSP", msg)
        
        print("[DEBUG] Variables Rocketbot configuradas:")
        print("  vLocStrResultadoSP = True")
        print("  vLocStrResumenSP = " + msg)
        
        return (True, msg, df_candidatos, stats)
    
    except Exception as e:
        stats["tiempo_total"] = time.time() - t_inicio
        err = "Error: " + str(e)
        
        print("="*80)
        print("[ERROR CRITICO] La funcion fallo")
        print("="*80)
        print("[ERROR] Tipo de error: " + type(e).__name__)
        print("[ERROR] Mensaje: " + str(e))
        print("[ERROR] Traceback completo:")
        print(traceback.format_exc())
        print("="*80)
        
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        
        SetVar("vLocStrResultadoSP", False)
        
        return (False, err, None, stats)


# # =========================================================================
# # PRUEBA LOCAL (comentar en producción)
# # =========================================================================
# if __name__ == "__main__":
#     # Mock de funciones RocketBot para pruebas locales
#     _variables = {}
    
#     def GetVar(name):
#         return _variables.get(name, "")
    
#     def SetVar(name, value):
#         _variables[name] = value
#         print(f"[MOCK] SetVar({name}) = {str(value)[:100]}...")
    
#     # Configuración de prueba
#     _variables["vLocDicConfig"] = json.dumps({
#         "ServidorBaseDatos": "localhost",
#         "NombreBaseDatos": "TestDB",
#         "Tolerancia": 500
#     })
#     _variables["vGblStrUsuarioBaseDatos"] = "test_user"
#     _variables["vGblStrClaveBaseDatos"] = "test_pass"
    
#     print("Script cargado. Para ejecutar: buscarCandidatos()")
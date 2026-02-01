# Author: Diego Ivan Lopez Ochoa
"""
Búsqueda inicial de candidatos.

LOGICA:
Identifica en `DocumentsProcessing` qué registros son aptos para iniciar el flujo de validación HU4.1.

VARIABLES ROCKETBOT:
- vLocDicConfig: Configuracion BD
- vLocStrResultadoSP: Resultado ejecucion
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
    
    # NUEVA FUNCIÓN: Truncar observaciones
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
    
    def buscar_combinacion(valores, objetivo, cantidad, tolerancia=500):
        try:
            n = len(valores)
            if n < cantidad or cantidad <= 0:
                return None
            if cantidad == 1:
                for i, v in enumerate(valores):
                    if abs(v - objetivo) <= tolerancia:
                        return [i]
                return None
            valores_sorted = sorted(valores)
            suma_min = sum(valores_sorted[:cantidad])
            suma_max = sum(valores_sorted[-cantidad:])
            if objetivo < suma_min - tolerancia or objetivo > suma_max + tolerancia:
                return None
            if abs(objetivo - suma_min) <= tolerancia:
                return [valores.index(v) for v in valores_sorted[:cantidad]]
            if abs(objetivo - suma_max) <= tolerancia:
                return [valores.index(v) for v in valores_sorted[-cantidad:]]
            promedio = objetivo / cantidad
            indexed = [(i, v, abs(v - promedio)) for i, v in enumerate(valores)]
            indexed_sorted = sorted(indexed, key=lambda x: x[2])
            indices_cercanos = [x[0] for x in indexed_sorted[:min(cantidad*2, n)]]
            if len(indices_cercanos) >= cantidad:
                for combo_idx in combinations(range(len(indices_cercanos)), cantidad):
                    indices = [indices_cercanos[i] for i in combo_idx]
                    suma = sum(valores[i] for i in indices)
                    if abs(suma - objetivo) <= tolerancia:
                        return indices
            indexed_vals = [(i, v) for i, v in enumerate(valores)]
            for combo in combinations(indexed_vals, cantidad):
                indices = [c[0] for c in combo]
                suma = sum(c[1] for c in combo)
                if abs(suma - objetivo) <= tolerancia:
                    return indices
            return None
        except:
            return None
    
    # CORRECCIÓN: batch_update ahora trunca observaciones
    def batch_update(cx, query, params_list, max_retries=3):
        if not params_list:
            return
        for attempt in range(max_retries):
            try:
                cur = cx.cursor()
                cur.fast_executemany = True
                for params in params_list:
                    # CORRECCIÓN: Truncar observaciones (segundo parámetro)
                    safe_params = list(params)
                    if len(safe_params) >= 2:
                        # Truncar el parámetro de observaciones (índice 1)
                        safe_params[1] = truncar_observacion(safe_params[1])
                    
                    # Convertir a tupla después de truncar
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
        candidato = {}
        for col, val in row_dp.items():
            candidato[col + "_dp"] = val
        for col in df_ddp.columns:
            candidato[col + "_ddp"] = unir_valores(df_ddp, col)
        for col in df_hoc.columns:
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
    
    stats = {
        "total": 0, "candidatos": 0, "sin_oc": 0,
        "no_encontrados": 0, "sin_combinacion": 0, "errores": 0,
        "tiempo_carga": 0, "tiempo_procesamiento": 0,
        "tiempo_updates": 0, "tiempo_tabla": 0, "tiempo_total": 0
    }
    
    updates_dp = []
    updates_comp = []
    candidatos_lista = []
    t_inicio = time.time()
    
    print("="*80)
    print("[INICIO] Funcion buscarCandidatos() iniciada")
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
            
            if not df_ddp.empty:
                df_ddp.set_index(['nit_emisor_o_nit_del_proveedor', 'numero_de_factura'],
                                inplace=True, drop=False)
                df_ddp.sort_index(inplace=True)
            if not df_hoc.empty:
                df_hoc.set_index(['NitCedula', 'DocCompra'], inplace=True, drop=False)
                df_hoc.sort_index(inplace=True)
            
            t_proc = time.time()
            
            for idx, row in df_dp.iterrows():
                try:
                    nit = safe_str(row["nit_emisor_o_nit_del_proveedor"])
                    factura = safe_str(row["numero_de_factura"])
                    oc = row.get("numero_de_liquidacion_u_orden_de_compra")
                    forma_pago = row.get("valor_a_pagar")
                    oc_str = safe_str(oc)
                    
                    if not oc_str:
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
                    
                    try:
                        hoc = (df_hoc.loc[[(nit, oc_str)]].copy().reset_index(drop=True)
                               if (nit, oc_str) in df_hoc.index else pd.DataFrame())
                    except:
                        hoc = pd.DataFrame()
                    
                    try:
                        ddp = (df_ddp.loc[[(nit, factura)]].copy().reset_index(drop=True)
                               if (nit, factura) in df_ddp.index else pd.DataFrame())
                    except:
                        ddp = pd.DataFrame()
                    
                    if hoc.empty or ddp.empty:
                        stats["no_encontrados"] += 1
                        estado = "EN ESPERA - CONTADO" if forma_pago in (1,"1","01") else "EN ESPERA"
                        obs = "No se encuentra registro en historico"
                        updates_dp.append((
                            "VALIDACION DATOS DE FACTURACION: Exitoso", obs, estado, nit, factura
                        ))
                        updates_comp.append(("LLAVES NO ENCONTRADAS", estado, obs, nit, factura))
                        continue
                    
                    cant_hoc, cant_ddp = len(hoc), len(ddp)
                    
                    if cant_hoc <= cant_ddp:
                        candidatos_lista.append(crear_candidato(row, ddp, hoc))
                        stats["candidatos"] += 1
                    else:
                        suma_lea = ddp["Valor de la Compra LEA"].sum()
                        valores_hoc = hoc["PorCalcular"].tolist()
                        combo = buscar_combinacion(valores_hoc, suma_lea, cant_ddp, tolerancia)
                        
                        if combo is not None:
                            hoc_sel = hoc.iloc[combo].copy()
                            candidatos_lista.append(crear_candidato(row, ddp, hoc_sel))
                            stats["candidatos"] += 1
                        else:
                            stats["sin_combinacion"] += 1
                            estado = "EN ESPERA - CONTADO" if forma_pago in (1,"1","01") else "EN ESPERA"
                            obs = "No se encuentra combinacion valida"
                            updates_dp.append((
                                "VALIDACION DATOS DE FACTURACION: Exitoso", obs, estado, nit, factura
                            ))
                            updates_comp.append(("LLAVES NO ENCONTRADAS", estado, obs, nit, factura))
                
                except Exception as e:
                    stats["errores"] += 1
                    continue
            
            stats["tiempo_procesamiento"] = time.time() - t_proc
            t_updates = time.time()
            
            # CORRECCIÓN: batch_update ahora trunca automáticamente
            if updates_dp:
                batch_update(cx,
                    "UPDATE [CxP].[DocumentsProcessing] SET EstadoFinalFase_4=?, "
                    "ObservacionesFase_4=?, ResultadoFinalAntesEventos=? "
                    "WHERE nit_emisor_o_nit_del_proveedor=? AND numero_de_factura=?",
                    updates_dp, max_retries)
            
            if updates_comp:
                # Para Comparativa, el tercer parámetro es Valor_XML
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
        
        msg = ("Done. Total:" + str(stats['total']) + " Candidatos:" + str(stats['candidatos']) +
               " SinOC:" + str(stats['sin_oc']) + " NoEnc:" + str(stats['no_encontrados']) +
               " SinComb:" + str(stats['sin_combinacion']) + " Err:" + str(stats['errores']) +
               " Time:" + str(round(stats['tiempo_total'], 2)) + "s")
        
        print("[RESULTADO] " + msg)
        print("[ESTADISTICAS DETALLADAS]")
        print("  Total procesados: " + str(stats['total']))
        print("  Candidatos validos: " + str(stats['candidatos']))
        print("  Sin orden de compra: " + str(stats['sin_oc']))
        print("  No encontrados: " + str(stats['no_encontrados']))
        print("  Sin combinacion: " + str(stats['sin_combinacion']))
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
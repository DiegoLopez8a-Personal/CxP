# Author: Diego Ivan Lopez Ochoa
"""
Actualización de histórico de novedades.

LOGICA:
Transfiere registros con novedades o errores a la tabla histórica `[CxP].[HistoricoNovedades]` para auditoría.

VARIABLES ROCKETBOT:
- vLocDicConfig: Configuracion BD
- vLocStrResultadoSP: Resultado ejecucion
"""
def ActualizarHistoricoNovedades():
    import json
    import ast
    import traceback
    import pyodbc
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from contextlib import contextmanager
    import time
    import warnings
    
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    print("=" * 80)
    print("[INICIO] Funcion ActualizarHistoricoNovedades() iniciada")
    print("[INICIO] Timestamp: " + str(datetime.now()))
    print("=" * 80)
    
    # ========================================================================
    # FUNCIONES AUXILIARES
    # ========================================================================
    
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
    
    def verificar_tabla_existe(cx, tabla):
        """Verificar si tabla existe"""
        query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'CxP' 
          AND TABLE_NAME = 'HistoricoNovedades'
        """
        cur = cx.cursor()
        cur.execute(query)
        count = cur.fetchone()[0]
        cx.commit()
        cur.close()
        return count > 0
    
    def crear_tabla_historico(cx):
        """Crear tabla HistoricoNovedades si no existe"""
        print("[TABLA] Creando tabla [CxP].[HistoricoNovedades]...")
        
        create_table = """
        CREATE TABLE [CxP].[HistoricoNovedades] (
            [Fecha_ejecucion] DATETIME NULL,
            [Fecha_de_retoma] DATETIME NULL,
            [ID_ejecucion] NVARCHAR(MAX) NULL,
            [ID_registro] NVARCHAR(MAX) NULL,
            [Nit] NVARCHAR(MAX) NULL,
            [Nombre_Proveedor] NVARCHAR(MAX) NULL,
            [Orden_de_compra] NVARCHAR(MAX) NULL,
            [Factura] NVARCHAR(MAX) NULL,
            [Fec_Doc] NVARCHAR(MAX) NULL,
            [Fec_Reg] NVARCHAR(MAX) NULL,
            [Observaciones] NVARCHAR(MAX) NULL
        )
        """
        
        cur = cx.cursor()
        cur.execute(create_table)
        cx.commit()
        cur.close()
        
        print("[TABLA] Tabla creada exitosamente")
    
    def buscar_fechas_comparativa(cx, nit, factura):
        """Buscar Fec.Doc y Fec.Reg en tabla Comparativa"""
        query = """
        SELECT TOP 1
            FecDoc,
            FecReg
        FROM [CxP].[HistoricoOrdenesCompra] WITH (NOLOCK)
        WHERE NitCedula = ?
          AND DocCompra = ?
          AND (FecDoc IS NOT NULL OR FecReg IS NOT NULL)
        """
        
        cur = cx.cursor()
        cur.execute(query, (nit, factura))
        row = cur.fetchone()
        cx.commit()
        cur.close()
        
        if row:
            fec_doc = safe_str(row[0]) if row[0] else "NO ENCONTRADO"
            fec_reg = safe_str(row[1]) if row[1] else "NO ENCONTRADO"
            return fec_doc, fec_reg
        else:
            return "NO ENCONTRADO", "NO ENCONTRADO"
    
    # ========================================================================
    # INICIO DE PROCESO
    # ========================================================================
    
    try:
        print("[DEBUG] Obteniendo configuracion...")
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[DEBUG] Configuracion obtenida OK")
        
        stats = {
            'total_registros': 0,
            'nuevos_insertados': 0,
            'actualizados': 0,
            'actualizados_no_encontrado': 0,
            'tiempo_total': 0
        }
        
        t_inicio = time.time()
        fecha_ejecucion = datetime.now()
        
        with crear_conexion_db(cfg) as cx:
            
            # ================================================================
            # PASO 0: Verificar/Crear tabla HistoricoNovedades
            # ================================================================
            
            print("")
            print("[PASO 0] Verificando tabla HistoricoNovedades...")
            
            if not verificar_tabla_existe(cx, '[CxP].[HistoricoNovedades]'):
                print("[INFO] Tabla no existe, creando...")
                crear_tabla_historico(cx)
            else:
                print("[INFO] Tabla existe OK")
            
            # ================================================================
            # PASO 1: Consultar registros con CON NOVEDAD
            # ================================================================
            
            print("")
            print("[PASO 1] Consultando registros con CON NOVEDAD...")
            
            query_novedades = """
            SELECT 
                ID,
                executionNum,
                Fecha_primer_proceso,
                nit_emisor_o_nit_del_proveedor,
                nombre_emisor,
                numero_de_liquidacion_u_orden_de_compra,
                numero_de_factura,
                ObservacionesFase_4,
                documenttype
            FROM [CxP].[DocumentsProcessing] WITH (NOLOCK)
            WHERE documenttype = 'FV'
              AND ResultadoFinalAntesEventos LIKE '%CON NOVEDAD%'
            """
            
            df_novedades = pd.read_sql(query_novedades, cx)
            print("[DEBUG] Registros con CON NOVEDAD: " + str(len(df_novedades)))
            
            if df_novedades.empty:
                print("[INFO] No hay registros con CON NOVEDAD")
            else:
                stats['total_registros'] = len(df_novedades)
                
                # ============================================================
                # PASO 2: Consultar histórico existente
                # ============================================================
                
                print("")
                print("[PASO 2] Consultando HistoricoNovedades existente...")
                
                query_historico = """
                SELECT 
                    Nit,
                    Factura,
                    Fec_Doc,
                    Fec_Reg
                FROM [CxP].[HistoricoNovedades] WITH (NOLOCK)
                """
                
                df_historico = pd.read_sql(query_historico, cx)
                print("[DEBUG] Registros en histórico: " + str(len(df_historico)))
                
                # ============================================================
                # PASO 3: Procesar cada registro con CON NOVEDAD
                # ============================================================
                
                print("")
                print("[PASO 3] Procesando registros con CON NOVEDAD...")
                
                for idx, row in df_novedades.iterrows():
                    try:
                        print("")
                        print("[REGISTRO " + str(idx + 1) + "/" + str(len(df_novedades)) + "]")
                        
                        id_registro = safe_str(row['ID'])
                        id_ejecucion = safe_str(row['executionNum'])
                        fecha_retoma = row['Fecha_primer_proceso'] if pd.notna(row['Fecha_primer_proceso']) else fecha_ejecucion
                        nit = safe_str(row['nit_emisor_o_nit_del_proveedor'])
                        nombre = safe_str(row['nombre_emisor'])
                        orden = safe_str(row['numero_de_liquidacion_u_orden_de_compra'])
                        factura = safe_str(row['numero_de_factura'])
                        observaciones = safe_str(row['ObservacionesFase_4'])
                        
                        print("[DEBUG] NIT: " + nit + " | Factura: " + factura)
                        
                        # Buscar fechas en Comparativa
                        fec_doc, fec_reg = buscar_fechas_comparativa(cx, nit, orden)
                        print("[DEBUG] Fec.Doc: " + fec_doc + " | Fec.Reg: " + fec_reg)
                        
                        # Verificar si existe en histórico
                        existe = False
                        tiene_fechas = False
                        
                        if not df_historico.empty:
                            mask = (df_historico['Nit'] == nit) & (df_historico['Factura'] == factura)
                            registros_existentes = df_historico[mask]
                            
                            if not registros_existentes.empty:
                                existe = True
                                # Verificar si tiene Fec.Doc o Fec.Reg
                                fec_doc_hist = safe_str(registros_existentes.iloc[0]['Fec_Doc'])
                                fec_reg_hist = safe_str(registros_existentes.iloc[0]['Fec_Reg'])
                                
                                if (fec_doc_hist and fec_doc_hist != 'NO ENCONTRADO') or \
                                   (fec_reg_hist and fec_reg_hist != 'NO ENCONTRADO'):
                                    tiene_fechas = True
                        
                        # ====================================================
                        # CASO 1: NO existe en histórico - INSERT
                        # ====================================================
                        
                        if not existe:
                            print("[ACCION] Registro nuevo - INSERT")
                            
                            cur = cx.cursor()
                            
                            insert_query = """
                            INSERT INTO [CxP].[HistoricoNovedades] (
                                Fecha_ejecucion,
                                Fecha_de_retoma,
                                ID_ejecucion,
                                ID_registro,
                                Nit,
                                Nombre_Proveedor,
                                Orden_de_compra,
                                Factura,
                                Fec_Doc,
                                Fec_Reg,
                                Observaciones
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """
                            
                            cur.execute(insert_query, (
                                fecha_ejecucion,
                                fecha_retoma,
                                id_ejecucion,
                                id_registro,
                                nit,
                                nombre,
                                orden,
                                factura,
                                fec_doc,
                                fec_reg,
                                observaciones
                            ))
                            cx.commit()
                            cur.close()
                            stats['nuevos_insertados'] += 1
                            print("[INSERT] Nuevo registro insertado")
                        
                        # ====================================================
                        # CASO 2: Existe y tiene fechas - UPDATE
                        # ====================================================
                        
                        elif existe and tiene_fechas:
                            print("[ACCION] Registro existe con fechas - UPDATE")
                            
                            cur = cx.cursor()
                            
                            update_query = """
                            UPDATE [CxP].[HistoricoNovedades]
                            SET Fecha_ejecucion = ?,
                                Fec_Doc = ?,
                                Fec_Reg = ?,
                                Observaciones = ?
                            WHERE Nit = ?
                              AND Factura = ?
                            """
                            
                            cur.execute(update_query, (
                                fecha_ejecucion,
                                fec_doc,
                                fec_reg,
                                observaciones,
                                nit,
                                factura
                            ))
                            cx.commit()
                            cur.close()
                            stats['actualizados'] += 1
                            print("[UPDATE] Registro actualizado")
                        
                        # ====================================================
                        # CASO 3: Existe sin fechas - SKIP
                        # ====================================================
                        
                        else:
                            print("[INFO] Registro existe sin fechas - SKIP")
                    
                    except Exception as e_row:
                        print("[ERROR] Error procesando registro " + str(idx) + ": " + str(e_row))
                        continue
            
            # ================================================================
            # PASO 4: Actualizar registros con NO ENCONTRADO
            # ================================================================
            
            print("")
            print("[PASO 4] Actualizando registros con NO ENCONTRADO...")
            
            # Buscar registros en histórico con NO ENCONTRADO
            query_no_encontrado = """
            SELECT 
                Nit,
                Factura,
                Fec_Doc,
                Fec_Reg
            FROM [CxP].[HistoricoNovedades] WITH (NOLOCK)
            WHERE Fec_Doc = 'NO ENCONTRADO'
               OR Fec_Reg = 'NO ENCONTRADO'
               OR Fec_Doc IS NULL
               OR Fec_Reg IS NULL
            """
            
            df_no_encontrado = pd.read_sql(query_no_encontrado, cx)
            print("[DEBUG] Registros con NO ENCONTRADO: " + str(len(df_no_encontrado)))
            
            if not df_no_encontrado.empty:
                # Consultar registros APROBADOS
                query_aprobados = """
                SELECT 
                    nit_emisor_o_nit_del_proveedor,
                    numero_de_factura,
                    ObservacionesFase_4
                FROM [CxP].[DocumentsProcessing] WITH (NOLOCK)
                WHERE documenttype = 'FV'
                  AND (ResultadoFinalAntesEventos = 'APROBADO'
                    OR ResultadoFinalAntesEventos = 'APROBADO SIN CONTABILIZACION'
                    OR ResultadoFinalAntesEventos = 'APROBADO CONTADO')
                """
                
                df_aprobados = pd.read_sql(query_aprobados, cx)
                print("[DEBUG] Registros APROBADOS: " + str(len(df_aprobados)))
                
                # Cruzar registros
                for idx, row_hist in df_no_encontrado.iterrows():
                    nit_hist = safe_str(row_hist['Nit'])
                    factura_hist = safe_str(row_hist['Factura'])
                    
                    # Buscar en aprobados
                    mask = (df_aprobados['nit_emisor_o_nit_del_proveedor'] == nit_hist) & \
                           (df_aprobados['numero_de_factura'] == factura_hist)
                    
                    registros_aprobados = df_aprobados[mask]
                    
                    if not registros_aprobados.empty:
                        print("[MATCH] Encontrado en APROBADOS: " + nit_hist + " - " + factura_hist)
                        
                        # Buscar fechas en Comparativa
                        fec_doc, fec_reg = buscar_fechas_comparativa(cx, nit_hist, factura_hist)
                        
                        # Solo actualizar si encontró fechas
                        if fec_doc != "NO ENCONTRADO" or fec_reg != "NO ENCONTRADO":
                            observaciones = safe_str(registros_aprobados.iloc[0]['ObservacionesFase_4'])
                            
                            cur = cx.cursor()
                            
                            update_query = """
                            UPDATE [CxP].[HistoricoNovedades]
                            SET Fecha_ejecucion = ?,
                                Fec_Doc = ?,
                                Fec_Reg = ?,
                                Observaciones = ?
                            WHERE Nit = ?
                              AND Factura = ?
                            """
                            
                            cur.execute(update_query, (
                                fecha_ejecucion,
                                fec_doc,
                                fec_reg,
                                observaciones,
                                nit_hist,
                                factura_hist
                            ))
                            cx.commit()
                            cur.close()
                            stats['actualizados_no_encontrado'] += 1
                            print("[UPDATE] Fechas actualizadas")
            
            # ================================================================
            # FIN DE PROCESO
            # ================================================================
            
            stats['tiempo_total'] = time.time() - t_inicio
            
            print("")
            print("=" * 80)
            print("[FIN] Historico de novedades actualizado")
            print("=" * 80)
            print("[ESTADISTICAS]")
            print("  Total registros procesados: " + str(stats['total_registros']))
            print("  Nuevos insertados: " + str(stats['nuevos_insertados']))
            print("  Actualizados: " + str(stats['actualizados']))
            print("  Actualizados (NO ENCONTRADO): " + str(stats['actualizados_no_encontrado']))
            print("  Tiempo total: " + str(round(stats['tiempo_total'], 2)) + "s")
            print("=" * 80)
            
            msg = ("Historico actualizado. Nuevos:" + str(stats['nuevos_insertados']) + 
                   " Actualizados:" + str(stats['actualizados'] + stats['actualizados_no_encontrado']))
            
            SetVar("vLocStrResultadoSP", "True")
            SetVar("vLocStrResumenSP", msg)
            SetVar("vLocDicEstadisticas", str(stats))
            
            return True, msg, None, stats
    
    except Exception as e:
        exc_type = type(e).__name__
        print("")
        print("=" * 80)
        print("[ERROR CRITICO] La funcion fallo")
        print("=" * 80)
        print("[ERROR] Tipo de error: " + exc_type)
        print("[ERROR] Mensaje: " + str(e))
        print("[ERROR] Traceback completo:")
        print(traceback.format_exc())
        print("=" * 80)
        
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        SetVar("vLocStrResultadoSP", "False")
        
        return False, str(e), None, {}
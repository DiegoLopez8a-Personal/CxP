# Author: Diego Ivan Lopez Ochoa
"""
Validación de montos en COP (ZPCN/ZPPA).

LOGICA:
Compara el valor de la factura en pesos colombianos contra el valor registrado en el sistema con una tolerancia definida.

VARIABLES ROCKETBOT:
- vLocDicConfig: Configuracion BD
- vLocStrResultadoSP: Resultado ejecucion
"""
def ZPCN_ZPPA_ValidarCOP():
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
    print("[INICIO] Funcion ZPCN_ZPPA_ValidarCOP() iniciada")
    print("[INICIO] Timestamp: " + str(datetime.now()))
    print("=" * 80)
    
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
    
    def split_valores(valor_str):
        if not valor_str or valor_str == "" or pd.isna(valor_str):
            return []
        valores = str(valor_str).split('|')
        return [v.strip() for v in valores if v.strip()]
    
    def contiene_valor(campo, valor_buscado):
        valores = split_valores(campo)
        return valor_buscado in valores
    
    def obtener_primer_valor(campo):
        valores = split_valores(campo)
        if valores:
            return valores[0]
        return ""
    
    def sumar_valores(campo):
        valores = split_valores(campo)
        total = 0.0
        for val in valores:
            try:
                total += float(val)
            except:
                pass
        return total
    
    # CORRECCIÓN SQL: Obtener min_id primero, sin subconsulta
    def verificar_y_crear_item(cx, nit, factura, item_name):
        cur = cx.cursor()
        
        check_query = """
        SELECT COUNT(*)
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ?
          AND Factura = ?
          AND Item = ?
        """
        cur.execute(check_query, (nit, factura, item_name))
        count = cur.fetchone()[0]
        
        if count > 0:
            print("[DEBUG] Item '" + item_name + "' ya existe")
            cx.commit()
            cur.close()
            return True
        
        print("[INFO] Creando Item '" + item_name + "'...")
        
        # CORRECCIÓN: Obtener min_id primero
        get_min_id = """
        SELECT MIN(ID_registro)
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ?
          AND Factura = ?
        """
        cur.execute(get_min_id, (nit, factura))
        result = cur.fetchone()
        
        if not result or not result[0]:
            print("[ERROR] No se encontro registro base")
            cx.commit()
            cur.close()
            return False
        
        min_id = result[0]
        
        # CORRECCIÓN: Usar min_id directamente (sin subconsulta)
        insert_query = """
        INSERT INTO [dbo].[CxP.Comparativa] (
            Fecha_de_ejecucion, Fecha_de_retoma_antes_de_contabilizacion,
            ID_ejecucion, Tipo_de_documento, Orden_de_Compra,
            Clase_de_pedido, NIT, Nombre_Proveedor, Factura,
            Item, Valor_XML, Valor_Orden_de_Compra,
            Valor_Orden_de_Compra_Comercializados, Aprobado,
            Estado_validacion_antes_de_eventos,
            Fecha_de_retoma_contabilizacion, Estado_contabilizacion,
            Fecha_de_retoma_compensacion, Estado_compensacion
        )
        SELECT 
            Fecha_de_ejecucion, Fecha_de_retoma_antes_de_contabilizacion,
            ID_ejecucion, Tipo_de_documento, Orden_de_Compra,
            Clase_de_pedido, NIT, Nombre_Proveedor, Factura,
            ?, NULL, NULL,
            NULL, NULL,
            NULL,
            NULL, NULL,
            NULL, NULL
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ?
          AND Factura = ?
          AND ID_registro = ?
        """
        cur.execute(insert_query, (item_name, nit, factura, min_id))
        print("[DEBUG] Item '" + item_name + "' creado exitosamente")
        cx.commit()
        cur.close()
        return True
    
    try:
        print("[DEBUG] Obteniendo configuracion...")
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[DEBUG] Configuracion obtenida OK")
        
        stats = {
            'total_registros': 0,
            'aprobados': 0,
            'con_novedad': 0,
            'errores': 0,
            'tiempo_total': 0
        }
        
        t_inicio = time.time()
        
        with crear_conexion_db(cfg) as cx:
            
            print("")
            print("[PASO 1] Consultando tabla HU41_CandidatosValidacion...")
            
            query_candidatos = """
            SELECT
                ID_dp,
                nit_emisor_o_nit_del_proveedor_dp,
                numero_de_factura_dp,
                numero_de_liquidacion_u_orden_de_compra_dp,
                forma_de_pago_dp,
                ClaseDePedido_hoc,
                Moneda_hoc,
                PorCalcular_hoc,
                [Valor de la Compra LEA_ddp],
                DocCompra_hoc,
                NitCedula_hoc,
                TextoBreve_hoc
            FROM [CxP].[HU41_CandidatosValidacion] WITH (NOLOCK)
            WHERE 1=1
            """
            
            df_candidatos = pd.read_sql(query_candidatos, cx)
            print("[DEBUG] Registros consultados: " + str(len(df_candidatos)))
            
            if df_candidatos.empty:
                print("[INFO] No hay registros en HU41_CandidatosValidacion")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros para procesar")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay registros para procesar", None, stats
            
            print("[PASO 2] Aplicando filtros...")
            
            mask_clase = df_candidatos['ClaseDePedido_hoc'].apply(
                lambda x: contiene_valor(x, 'ZPPA') or contiene_valor(x, 'ZPCN') or contiene_valor(x, '42') if pd.notna(x) else False
            )
            
            mask_moneda = df_candidatos['Moneda_hoc'].apply(
                lambda x: contiene_valor(x, 'COP') or contiene_valor(x, '') if pd.notna(x) else True
            )
            
            df_filtrado = df_candidatos[mask_clase & mask_moneda].copy()
            
            print("[DEBUG] Registros despues de filtros: " + str(len(df_filtrado)))
            
            if df_filtrado.empty:
                print("[INFO] No hay registros que cumplan los filtros")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros con ClaseDePedido ZPPA/ZPCN/42 y Moneda COP/vacio")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay registros que cumplan filtros", None, stats
            
            stats['total_registros'] = len(df_filtrado)
            
            print("")
            print("[PASO 3] Procesando VALIDACION: Suma de valores...")
            
            for idx, row in df_filtrado.iterrows():
                try:
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    forma_pago = safe_str(row['forma_de_pago_dp'])
                    
                    suma_porcalcular = sumar_valores(row['PorCalcular_hoc'])
                    suma_valor_compra = sumar_valores(row['Valor de la Compra LEA_ddp'])
                    
                    diferencia = abs(suma_porcalcular - suma_valor_compra)
                    tolerancia = 500.0
                    
                    print("")
                    print("[DEBUG] PorCalcular sum: " + str(suma_porcalcular))
                    print("[DEBUG] Valor_Compra_LEA sum: " + str(suma_valor_compra))
                    print("[DEBUG] Diferencia: " + str(diferencia))
                    print("[DEBUG] Tolerancia: " + str(tolerancia))
                    
                    if diferencia > tolerancia:
                        stats['con_novedad'] += 1
                        
                        if forma_pago == '1' or forma_pago == '01':
                            estado_final = 'CON NOVEDAD - CONTADO'
                        else:
                            estado_final = 'CON NOVEDAD'
                        
                        cur = cx.cursor()
                        
                        verificar_y_crear_item(cx, nit, factura, 'Valor')
                        verificar_y_crear_item(cx, nit, factura, 'Observaciones')
                        
                        update_fase4 = """
                        UPDATE [CxP].[DocumentsProcessing]
                        SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
                        WHERE nit_emisor_o_nit_del_proveedor = ?
                          AND numero_de_factura = ?
                          AND numero_de_liquidacion_u_orden_de_compra = ?
                        """
                        cur.execute(update_fase4, (nit, factura, oc))
                        
                        select_obs = """
                        SELECT ObservacionesFase_4
                        FROM [CxP].[DocumentsProcessing]
                        WHERE nit_emisor_o_nit_del_proveedor = ?
                          AND numero_de_factura = ?
                          AND numero_de_liquidacion_u_orden_de_compra = ?
                        """
                        cur.execute(select_obs, (nit, factura, oc))
                        result_obs = cur.fetchone()
                        
                        obs_actual = safe_str(result_obs[0]) if result_obs and result_obs[0] else ""
                        nueva_obs = "No se encuentra coincidencia en el valor total de la factura vs la informacion reportada en SAP"
                        
                        if obs_actual:
                            obs_final = nueva_obs + ", " + obs_actual
                        else:
                            obs_final = nueva_obs
                        
                        obs_final = truncar_observacion(obs_final)
                        
                        update_obs = """
                        UPDATE [CxP].[DocumentsProcessing]
                        SET ObservacionesFase_4 = ?
                        WHERE nit_emisor_o_nit_del_proveedor = ?
                          AND numero_de_factura = ?
                          AND numero_de_liquidacion_u_orden_de_compra = ?
                        """
                        cur.execute(update_obs, (obs_final, nit, factura, oc))
                        
                        update_resultado = """
                        UPDATE [CxP].[DocumentsProcessing]
                        SET ResultadoFinalAntesEventos = ?
                        WHERE nit_emisor_o_nit_del_proveedor = ?
                          AND numero_de_factura = ?
                          AND numero_de_liquidacion_u_orden_de_compra = ?
                        """
                        cur.execute(update_resultado, (estado_final, nit, factura, oc))
                        
                        update_valor_no = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Aprobado = 'NO'
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'Valor'
                        """
                        cur.execute(update_valor_no, (nit, factura))
                        
                        select_obs_comp = """
                        SELECT Valor_XML
                        FROM [dbo].[CxP.Comparativa]
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'Observaciones'
                        """
                        cur.execute(select_obs_comp, (nit, factura))
                        result_obs_comp = cur.fetchone()
                        
                        obs_comp_actual = safe_str(result_obs_comp[0]) if result_obs_comp and result_obs_comp[0] else ""
                        
                        if obs_comp_actual:
                            obs_comp_final = nueva_obs + ", " + obs_comp_actual
                        else:
                            obs_comp_final = nueva_obs
                        
                        obs_comp_final = truncar_observacion(obs_comp_final)
                        
                        update_obs_comp = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Valor_XML = ?
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'Observaciones'
                        """
                        cur.execute(update_obs_comp, (obs_comp_final, nit, factura))
                        
                        update_estado_todos = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Estado_validacion_antes_de_eventos = ?
                        WHERE NIT = ?
                          AND Factura = ?
                        """
                        cur.execute(update_estado_todos, (estado_final, nit, factura))
                        
                        valores_doccompra = split_valores(row['DocCompra_hoc'])
                        valores_nitcedula = split_valores(row['NitCedula_hoc'])
                        valores_porcalcular = split_valores(row['PorCalcular_hoc'])
                        valores_textobreve = split_valores(row['TextoBreve_hoc'])
                        
                        for i in range(max(len(valores_doccompra), len(valores_nitcedula), len(valores_porcalcular), len(valores_textobreve))):
                            doccompra_val = valores_doccompra[i] if i < len(valores_doccompra) else ""
                            nitcedula_val = valores_nitcedula[i] if i < len(valores_nitcedula) else ""
                            porcalcular_val = valores_porcalcular[i] if i < len(valores_porcalcular) else ""
                            textobreve_val = valores_textobreve[i] if i < len(valores_textobreve) else ""
                            
                            if doccompra_val and nitcedula_val:
                                update_marca = """
                                UPDATE [CxP].[HistoricoOrdenesCompra]
                                SET Marca = 'PROCESADO'
                                WHERE DocCompra = ?
                                  AND NitCedula = ?
                                  AND PorCalcular = ?
                                  AND TextoBreve = ?
                                """
                                cur.execute(update_marca, (doccompra_val, nitcedula_val, porcalcular_val, textobreve_val))
                        cx.commit()
                        cur.close()
                    else:
                        stats['aprobados'] += 1
                
                except Exception as e_row:
                    print("[ERROR] Error procesando registro " + str(idx) + ": " + str(e_row))
                    stats['errores'] += 1
                    continue
            
            stats['tiempo_total'] = time.time() - t_inicio
            
            msg = ("Proceso OK. Total:" + str(stats['total_registros']) + 
                   " Aprobados:" + str(stats['aprobados']) + 
                   " ConNovedad:" + str(stats['con_novedad']))
            
            SetVar("vLocStrResultadoSP", "True")
            SetVar("vLocStrResumenSP", msg)
            SetVar("vLocDicEstadisticas", str(stats))
            
            return True, msg, None, stats
    
    except Exception as e:
        print(traceback.format_exc())
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        SetVar("vLocStrResultadoSP", "False")
        return False, str(e), None, {}
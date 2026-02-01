# Author: Diego Ivan Lopez Ochoa
"""
Validación de Orden de Registro.

LOGICA:
Verifica la secuencia y existencia de la orden de registro contable.

VARIABLES ROCKETBOT:
- vLocDicConfig: Configuracion BD
- vLocStrResultadoSP: Resultado ejecucion
"""
def ZPCN_ZPPA_ValidarOrdenRegistro():
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
    print("[INICIO] Funcion ZPCN_ZPPA_ValidarOrdenRegistro() iniciada")
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
    
    # CORRECCIÓN: Función para truncar observaciones
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
        """Dividir string por | y retornar lista de valores"""
        if not valor_str or valor_str == "" or pd.isna(valor_str):
            return []
        valores = str(valor_str).split('|')
        return [v.strip() for v in valores if v.strip()]
    
    def contiene_valor(campo, valor_buscado):
        """Verificar si campo (que puede tener valores separados por |) contiene valor buscado"""
        valores = split_valores(campo)
        return valor_buscado in valores
    
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
    

    def actualizar_item_comparativa(cx, nit, factura, item_name, aprobado=None, observacion=None, estado=None):
        """Actualizar campos de un Item en Comparativa"""
        cur = cx.cursor()
        
        if aprobado is not None:
            update_aprobado = """
            UPDATE [dbo].[CxP.Comparativa]
            SET Aprobado = ?
            WHERE NIT = ?
              AND Factura = ?
              AND Item = ?
            """
            cur.execute(update_aprobado, (aprobado, nit, factura, item_name))
            print("[UPDATE] Item '" + item_name + "' Aprobado = " + aprobado)
        
        if observacion is not None:
            # Obtener observaciones actuales
            select_obs = """
            SELECT Valor_XML
            FROM [dbo].[CxP.Comparativa]
            WHERE NIT = ?
              AND Factura = ?
              AND Item = 'Observaciones'
            """
            cur.execute(select_obs, (nit, factura))
            result = cur.fetchone()
            
            obs_actual = safe_str(result[0]) if result and result[0] else ""
            
            if obs_actual:
                obs_final = observacion + ", " + obs_actual
            else:
                obs_final = observacion
            
            # CORRECCIÓN: Truncar antes de UPDATE
            obs_final = truncar_observacion(obs_final)
            
            update_obs = """
            UPDATE [dbo].[CxP.Comparativa]
            SET Valor_XML = ?
            WHERE NIT = ?
              AND Factura = ?
              AND Item = 'Observaciones'
            """
            cur.execute(update_obs, (obs_final, nit, factura))
            print("[UPDATE] Observaciones actualizadas")
        
        if estado is not None:
            update_estado = """
            UPDATE [dbo].[CxP.Comparativa]
            SET Estado_validacion_antes_de_eventos = ?
            WHERE NIT = ?
              AND Factura = ?
            """
            cur.execute(update_estado, (estado, nit, factura))
            print("[UPDATE] Estado_validacion_antes_de_eventos = " + estado)
        cx.commit()
        cur.close()
    
    def actualizar_documents_processing(cx, nit, factura, oc, observacion, forma_pago):
        """Actualizar DocumentsProcessing con novedad"""
        cur = cx.cursor()
        
        # Determinar estado
        if forma_pago == '1' or forma_pago == '01':
            estado_final = 'CON NOVEDAD - CONTADO'
        else:
            estado_final = 'CON NOVEDAD'
        
        # Actualizar EstadoFinalFase_4
        update_fase4 = """
        UPDATE [CxP].[DocumentsProcessing]
        SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
        WHERE nit_emisor_o_nit_del_proveedor = ?
          AND numero_de_factura = ?
          AND numero_de_liquidacion_u_orden_de_compra = ?
        """
        cur.execute(update_fase4, (nit, factura, oc))
        
        # Obtener observaciones actuales
        select_obs = """
        SELECT ObservacionesFase_4
        FROM [CxP].[DocumentsProcessing]
        WHERE nit_emisor_o_nit_del_proveedor = ?
          AND numero_de_factura = ?
          AND numero_de_liquidacion_u_orden_de_compra = ?
        """
        cur.execute(select_obs, (nit, factura, oc))
        result = cur.fetchone()
        
        obs_actual = safe_str(result[0]) if result and result[0] else ""
        
        if obs_actual:
            obs_final = observacion + ", " + obs_actual
        else:
            obs_final = observacion
        
        # CORRECCIÓN: Truncar antes de UPDATE
        obs_final = truncar_observacion(obs_final)
        
        # Actualizar observaciones
        update_obs = """
        UPDATE [CxP].[DocumentsProcessing]
        SET ObservacionesFase_4 = ?
        WHERE nit_emisor_o_nit_del_proveedor = ?
          AND numero_de_factura = ?
          AND numero_de_liquidacion_u_orden_de_compra = ?
        """
        cur.execute(update_obs, (obs_final, nit, factura, oc))
        
        # Actualizar ResultadoFinalAntesEventos
        update_resultado = """
        UPDATE [CxP].[DocumentsProcessing]
        SET ResultadoFinalAntesEventos = ?
        WHERE nit_emisor_o_nit_del_proveedor = ?
          AND numero_de_factura = ?
          AND numero_de_liquidacion_u_orden_de_compra = ?
        """
        cur.execute(update_resultado, (estado_final, nit, factura, oc))
        cx.commit()
        cur.close()
        print("[UPDATE] DocumentsProcessing actualizado (CON NOVEDAD)")
    
    def actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list):
        """Actualizar HistoricoOrdenesCompra marcando como PROCESADO"""
        cur = cx.cursor()
        
        num_actualizados = 0
        max_len = max(len(doccompra_list), len(nitcedula_list), len(porcalcular_list), len(textobreve_list))
        
        for i in range(max_len):
            doccompra = doccompra_list[i] if i < len(doccompra_list) else ""
            nitcedula = nitcedula_list[i] if i < len(nitcedula_list) else ""
            porcalcular = porcalcular_list[i] if i < len(porcalcular_list) else ""
            textobreve = textobreve_list[i] if i < len(textobreve_list) else ""
            
            if doccompra and nitcedula:
                update_marca = """
                UPDATE [CxP].[HistoricoOrdenesCompra]
                SET Marca = 'PROCESADO'
                WHERE DocCompra = ?
                  AND NitCedula = ?
                  AND PorCalcular = ?
                  AND TextoBreve = ?
                """
                cur.execute(update_marca, (doccompra, nitcedula, porcalcular, textobreve))
                num_actualizados += 1
        cx.commit()
        cur.close()
        print("[UPDATE] HistoricoOrdenesCompra: " + str(num_actualizados) + " registros marcados como PROCESADO")
    
    # ========================================================================
    # INICIO DE PROCESO
    # ========================================================================
    
    try:
        print("[DEBUG] Obteniendo configuracion...")
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[DEBUG] Configuracion obtenida OK")
        
        stats = {
            'total_registros': 0,
            'validaciones_ok': 0,
            'validaciones_novedad': 0,
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
                Orden_hoc,
                IndicadorImpuestos_hoc,
                CentroDeCoste_hoc,
                Cuenta_hoc,
                ClaseDeOrden_hoc,
                DocCompra_hoc,
                NitCedula_hoc,
                PorCalcular_hoc,
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
            
            print("[DEBUG] Registros con ClaseDePedido = ZPPA, ZPCN o 42: " + str(mask_clase.sum()))
            
            df_filtrado = df_candidatos[mask_clase].copy()
            
            print("[DEBUG] Registros despues de filtros: " + str(len(df_filtrado)))
            
            if df_filtrado.empty:
                print("[INFO] No hay registros que cumplan los filtros")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros con ClaseDePedido ZPPA/ZPCN/42")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay registros que cumplan filtros", None, stats
            
            stats['total_registros'] = len(df_filtrado)
            
            print("")
            print("[PASO 3] Procesando validaciones por posicion...")
            
            for idx, row in df_filtrado.iterrows():
                try:
                    print("")
                    print("[REGISTRO " + str(idx + 1) + "/" + str(len(df_filtrado)) + "]")
                    
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    forma_pago = safe_str(row['forma_de_pago_dp'])
                    
                    print("[DEBUG] NIT: " + nit)
                    print("[DEBUG] Factura: " + factura)
                    print("[DEBUG] OC: " + oc)
                    
                    ordenes = split_valores(row['Orden_hoc'])
                    ind_impuestos = split_valores(row['IndicadorImpuestos_hoc'])
                    centros_coste = split_valores(row['CentroDeCoste_hoc'])
                    cuentas = split_valores(row['Cuenta_hoc'])
                    clases_orden = split_valores(row['ClaseDeOrden_hoc'])
                    
                    doccompra_list = split_valores(row['DocCompra_hoc'])
                    nitcedula_list = split_valores(row['NitCedula_hoc'])
                    porcalcular_list = split_valores(row['PorCalcular_hoc'])
                    textobreve_list = split_valores(row['TextoBreve_hoc'])
                    
                    print("[DEBUG] Total posiciones a procesar: " + str(len(ordenes)))
                    
                    hay_novedad = False
                    
                    for pos in range(len(ordenes)):
                        print("")
                        print("[POSICION " + str(pos + 1) + "/" + str(len(ordenes)) + "]")
                        
                        orden = ordenes[pos] if pos < len(ordenes) else ""
                        ind_imp = ind_impuestos[pos] if pos < len(ind_impuestos) else ""
                        centro = centros_coste[pos] if pos < len(centros_coste) else ""
                        cuenta = cuentas[pos] if pos < len(cuentas) else ""
                        clase_ord = clases_orden[pos] if pos < len(clases_orden) else ""
                        
                        print("[DEBUG] Orden: '" + orden + "'")
                        print("[DEBUG] IndicadorImpuestos: '" + ind_imp + "'")
                        print("[DEBUG] CentroDeCoste: '" + centro + "'")
                        print("[DEBUG] Cuenta: '" + cuenta + "'")
                        print("[DEBUG] ClaseDeOrden: '" + clase_ord + "'")
                        
                        if not orden:
                            print("[INFO] Orden vacio, saltando posicion")
                            continue
                        
                        if pos > 0:
                            verificar_y_crear_item(cx, nit, factura, 'IndicadorImpuestos')
                            verificar_y_crear_item(cx, nit, factura, 'CentroCoste')
                            verificar_y_crear_item(cx, nit, factura, 'Cuenta')
                            verificar_y_crear_item(cx, nit, factura, 'ClaseOrden')
                            verificar_y_crear_item(cx, nit, factura, 'Observaciones')
                        
                        if len(orden) == 9 and orden.startswith('15'):
                            print("[VALIDACION] Orden 15 detectada (9 caracteres)")
                            
                            ind_validos = ['H4', 'H5', 'H6', 'H7', 'VP', 'CO', 'IC', 'CR']
                            
                            if ind_imp in ind_validos:
                                print("[OK] IndicadorImpuestos '" + ind_imp + "' es valido")
                                actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='SI')
                            
                            elif not ind_imp or ind_imp not in ind_validos:
                                print("[NOVEDAD] IndicadorImpuestos NO valido o vacio")
                                hay_novedad = True
                                obs = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden 15, pero campo 'Indicador impuestos' NO se encuentra diligenciado o NO corresponde alguna de las opciones H4, H5, H6, H7, VP, CO, IC, CR"
                                actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='NO')
                                actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                            
                            if not centro:
                                print("[OK] CentroDeCoste vacio (correcto para Orden 15)")
                                actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='SI')
                            
                            else:
                                print("[NOVEDAD] CentroDeCoste tiene valor (incorrecto para Orden 15)")
                                hay_novedad = True
                                obs = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden 15, pero Campo 'Centro de coste' se encuentra diligenciado cuando NO debe estarlo"
                                actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='NO')
                                actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                            
                            if cuenta == '5199150001':
                                print("[OK] Cuenta es 5199150001 (correcto)")
                                actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='SI')
                            
                            else:
                                print("[NOVEDAD] Cuenta diferente a 5199150001")
                                hay_novedad = True
                                obs = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden 15, pero Campo 'Cuenta' es diferente a 5199150001"
                                actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='NO')
                                actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                            
                            clase_correcta = False
                            obs_clase = ""
                            
                            if not clase_ord:
                                obs_clase = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden 15, pero Campo 'Clase orden' NO se encuentra diligenciado"
                            elif ind_imp in ['H4', 'H5']:
                                clase_correcta = (clase_ord == 'ZINV')
                                if not clase_correcta:
                                    obs_clase = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden 15, pero Campo 'Clase orden' NO se encuentra aplicado correctamente segun reglas 'H4 y H5 = ZINV', 'H6 y H7 = ZADM' o 'VP, CO, CR o IC = ZINV o ZADM'"
                            elif ind_imp in ['H6', 'H7']:
                                clase_correcta = (clase_ord == 'ZADM')
                                if not clase_correcta:
                                    obs_clase = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden 15, pero Campo 'Clase orden' NO se encuentra aplicado correctamente segun reglas 'H4 y H5 = ZINV', 'H6 y H7 = ZADM' o 'VP, CO, CR o IC = ZINV o ZADM'"
                            elif ind_imp in ['VP', 'CO', 'CR', 'IC']:
                                clase_correcta = (clase_ord in ['ZINV', 'ZADM'])
                                if not clase_correcta:
                                    obs_clase = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden 15, pero Campo 'Clase orden' NO se encuentra aplicado correctamente segun reglas 'H4 y H5 = ZINV', 'H6 y H7 = ZADM' o 'VP, CO, CR o IC = ZINV o ZADM'"
                            else:
                                clase_correcta = True
                            
                            if clase_correcta:
                                print("[OK] ClaseDeOrden correcta segun IndicadorImpuestos")
                                actualizar_item_comparativa(cx, nit, factura, 'ClaseOrden', aprobado='SI')
                            else:
                                print("[NOVEDAD] ClaseDeOrden incorrecta")
                                hay_novedad = True
                                actualizar_documents_processing(cx, nit, factura, oc, obs_clase, forma_pago)
                                actualizar_item_comparativa(cx, nit, factura, 'ClaseOrden', aprobado='NO')
                                actualizar_item_comparativa(cx, nit, factura, None, observacion=obs_clase)
                                estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                        
                        elif len(orden) == 8 and orden.isdigit():
                            print("[VALIDACION] Orden de 8 digitos detectada")
                            
                            if orden.startswith('53'):
                                print("[DEBUG] Orden 53 (ESTADISTICAS)")
                                
                                if centro:
                                    print("[OK] CentroDeCoste tiene valor (correcto para Orden 53)")
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='SI')
                                
                                else:
                                    print("[NOVEDAD] CentroDeCoste vacio (incorrecto para Orden 53)")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden 53, pero Campo 'Centro de coste' se encuentra vacio para pedidos ESTADISTICAS"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                            
                            else:
                                print("[DEBUG] Orden NO 53 (NO ESTADISTICAS)")
                                
                                if not centro:
                                    print("[OK] CentroDeCoste vacio (correcto para Orden NO 53)")
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='SI')
                                
                                else:
                                    print("[NOVEDAD] CentroDeCoste tiene valor (incorrecto para Orden NO 53)")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden diferente a 53, pero Campo 'Centro de coste' se encuentra diligenciado para pedidos NO ESTADISTICAS"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                
                                cuenta_ok = (cuenta == '5299150099') or (len(cuenta) == 10 and cuenta.startswith('73'))
                                
                                if cuenta_ok:
                                    print("[OK] Cuenta correcta (5299150099 o 10 digitos empezando con 73)")
                                    actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='SI')
                                
                                else:
                                    print("[NOVEDAD] Cuenta incorrecta")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden diferente a 53, pero Campo 'Cuenta' es diferente a 5299150099 y/o NO cumple regla 'inicia con 7 y tiene 10 digitos'"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                        
                        elif len(orden) == 10 and orden.startswith('73'):
                            print("[VALIDACION] Orden de 10 digitos empezando con 73")
                            cuenta_ok = (cuenta == '5299150099') or (len(cuenta) == 10 and cuenta.startswith('73'))
                            
                            if cuenta_ok:
                                print("[OK] Cuenta correcta")
                                actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='SI')
                            else:
                                print("[NOVEDAD] Cuenta incorrecta")
                                hay_novedad = True
                                obs = "Pedido corresponde a ZPCN o ZPPA y cuenta con Orden diferente a 53, pero Campo 'Cuenta' es diferente a 5299150099 y/o NO cumple regla 'inicia con 7 y tiene 10 digitos'"
                                actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='NO')
                                actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                        
                        else:
                            print("[INFO] Orden no cumple ningun criterio de validacion")
                    
                    if hay_novedad:
                        stats['validaciones_novedad'] += 1
                    else:
                        stats['validaciones_ok'] += 1
                    
                except Exception as e_row:
                    print("[ERROR] Error procesando registro " + str(idx) + ": " + str(e_row))
                    stats['errores'] += 1
                    continue
            
            stats['tiempo_total'] = time.time() - t_inicio
            
            print("")
            print("=" * 80)
            print("[FIN] Proceso completado")
            print("=" * 80)
            print("[ESTADISTICAS]")
            print("  Total registros: " + str(stats['total_registros']))
            print("  Validaciones OK: " + str(stats['validaciones_ok']))
            print("  Validaciones con novedad: " + str(stats['validaciones_novedad']))
            print("  Errores: " + str(stats['errores']))
            print("  Tiempo total: " + str(round(stats['tiempo_total'], 2)) + "s")
            print("=" * 80)
            
            msg = ("Proceso OK. Total:" + str(stats['total_registros']) + 
                   " OK:" + str(stats['validaciones_ok']) + 
                   " Novedad:" + str(stats['validaciones_novedad']))
            
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
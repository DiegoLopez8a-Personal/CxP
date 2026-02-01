def ZPCN_ZPPA_ValidarActivoFijo():
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
    import os
    import unicodedata
    
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    print("=" * 80)
    print("[INICIO] Funcion ZPCN_ZPPA_ValidarActivoFijo() iniciada")
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

    def truncar_observacion(obs):
        """Truncar observación a 3900 caracteres para prevenir overflow"""
        if not obs:
            return ""
        obs_str = safe_str(obs)
        if len(obs_str) > 3900:
            return obs_str[:3900]
        return obs_str
    
    def normalizar_columna(nombre):
        """Quitar tildes y normalizar nombre de columna"""
        if not nombre:
            return ""
        # Quitar tildes
        nfd = unicodedata.normalize('NFD', nombre)
        sin_tildes = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
        return unicodedata.normalize('NFC', sin_tildes)
    
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
    
    def cargar_excel_impuestos(ruta_archivo):
        """
        Cargar archivo Excel de impuestos especiales.
        FIXED: Normaliza nombres de columnas para evitar problemas de encoding
        Retorna DataFrame o genera error critico.
        """
        print("")
        print("[EXCEL] Cargando archivo de Impuestos Especiales...")
        print("[EXCEL] Ruta: " + ruta_archivo)
        
        # Verificar existencia del archivo
        if not os.path.exists(ruta_archivo):
            raise ValueError("ERROR CRITICO: Archivo Excel no existe en ruta: " + ruta_archivo)
        
        # Verificar que no esta vacio
        if os.path.getsize(ruta_archivo) == 0:
            raise ValueError("ERROR CRITICO: Archivo Excel esta vacio: " + ruta_archivo)
        
        try:
            # Intentar cargar el archivo
            df = pd.read_excel(ruta_archivo, sheet_name='IVA CECO')
            
            if df.empty:
                raise ValueError("ERROR CRITICO: Hoja 'IVA CECO' esta vacia")
            
            print("[EXCEL] Archivo cargado exitosamente")
            print("[EXCEL] Registros: " + str(len(df)))
            
            # FIXED: Normalizar nombres de columnas (quitar tildes)
            print("[EXCEL] Normalizando nombres de columnas...")
            df.columns = [normalizar_columna(col) for col in df.columns]
            
            print("[EXCEL] Columnas despues de normalizar:")
            for col in df.columns:
                print("  - " + col)
            
            # Verificar estructura (SIN TILDES)
            columnas_requeridas = ['CECO', 'Codigo Ind. Iva aplicable']
            columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
            
            if columnas_faltantes:
                print("[ERROR] Columnas disponibles: " + str(list(df.columns)))
                raise ValueError("ERROR CRITICO: Estructura incorrecta. Columnas faltantes: " + ', '.join(columnas_faltantes))
            
            print("[EXCEL] Estructura verificada OK")
            return df
            
        except ValueError as ve:
            # Re-lanzar errores de validacion
            raise
        except Exception as e:
            raise ValueError("ERROR CRITICO: Error al cargar Excel: " + str(e))
    
    def buscar_indicadores_permitidos(df_excel, centro_coste):
        """
        Buscar en Excel los indicadores permitidos para un Centro de Coste.
        FIXED: Usa columna sin tildes
        Retorna lista de indicadores permitidos o None si no se encuentra.
        """
        try:
            # Convertir centro_coste a int para buscar
            ceco_int = int(centro_coste) if centro_coste.isdigit() else None
            
            if ceco_int is None:
                print("[EXCEL] CentroDeCoste no es numerico: '" + centro_coste + "'")
                return None
            
            # Buscar en DataFrame
            resultado = df_excel[df_excel['CECO'] == ceco_int]
            
            if resultado.empty:
                print("[EXCEL] CECO " + str(ceco_int) + " NO encontrado en Excel")
                return None
            
            # Extraer codigo (SIN TILDES)
            codigo = str(resultado.iloc[0]['Codigo Ind. Iva aplicable'])
            print("[EXCEL] CECO " + str(ceco_int) + " encontrado -> Codigo: '" + codigo + "'")
            
            # Separar por guion
            if '-' in codigo:
                indicadores = codigo.split('-')
            else:
                indicadores = [codigo]
            
            print("[EXCEL] Indicadores permitidos: " + str(indicadores))
            return indicadores
            
        except Exception as e:
            print("[ERROR] Error buscando en Excel: " + str(e))
            return None
    
    # ========================================================================
    # INICIO DE PROCESO
    # ========================================================================
    
    try:
        print("[DEBUG] Obteniendo configuracion...")
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[DEBUG] Configuracion obtenida OK")
        
        # Verificar ruta del Excel
        if 'DocImpuestosEspeciales' not in cfg:
            raise ValueError("ERROR CRITICO: Configuracion no contiene 'DocImpuestosEspeciales'")
        
        ruta_excel = cfg['DocImpuestosEspeciales']
        
        # Cargar Excel (puede generar error critico)
        df_impuestos = cargar_excel_impuestos(ruta_excel)
        
        stats = {
            'total_registros': 0,
            'posiciones_procesadas': 0,
            'validaciones_ok': 0,
            'validaciones_novedad': 0,
            'errores': 0,
            'tiempo_total': 0
        }
        
        t_inicio = time.time()
        
        with crear_conexion_db(cfg) as cx:
            
            # ================================================================
            # PASO 1: Consultar candidatos de HU41_CandidatosValidacion
            # ================================================================
            
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
                ActivoFijo_hoc,
                IndicadorImpuestos_hoc,
                CentroDeCoste_hoc,
                Cuenta_hoc,
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
            
            # ================================================================
            # PASO 2: Aplicar filtros
            # ================================================================
            
            print("[PASO 2] Aplicando filtros...")
            
            # Filtro: ClaseDePedido_hoc contiene 'ZPPA', 'ZPCN' o '42'
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
            
            # ================================================================
            # PASO 3: Procesar cada registro - VALIDACIONES POR POSICION
            # ================================================================
            
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
                    
                    # Obtener listas de valores
                    activos_fijos = split_valores(row['ActivoFijo_hoc'])
                    ind_impuestos = split_valores(row['IndicadorImpuestos_hoc'])
                    centros_coste = split_valores(row['CentroDeCoste_hoc'])
                    cuentas = split_valores(row['Cuenta_hoc'])
                    
                    # Para HistoricoOrdenesCompra
                    doccompra_list = split_valores(row['DocCompra_hoc'])
                    nitcedula_list = split_valores(row['NitCedula_hoc'])
                    porcalcular_list = split_valores(row['PorCalcular_hoc'])
                    textobreve_list = split_valores(row['TextoBreve_hoc'])
                    
                    max_posiciones = max(len(activos_fijos), len(ind_impuestos), len(centros_coste), len(cuentas))
                    print("[DEBUG] Total posiciones: " + str(max_posiciones))
                    
                    hay_novedad = False
                    posiciones_procesadas = 0
                    
                    # PROCESAR POR POSICION
                    for pos in range(max_posiciones):
                        print("")
                        print("[POSICION " + str(pos + 1) + "/" + str(max_posiciones) + "]")
                        
                        activo_fijo = activos_fijos[pos] if pos < len(activos_fijos) else ""
                        ind_imp = ind_impuestos[pos] if pos < len(ind_impuestos) else ""
                        centro = centros_coste[pos] if pos < len(centros_coste) else ""
                        cuenta = cuentas[pos] if pos < len(cuentas) else ""
                        
                        print("[DEBUG] ActivoFijo: '" + activo_fijo + "'")
                        print("[DEBUG] IndicadorImpuestos: '" + ind_imp + "'")
                        print("[DEBUG] CentroDeCoste: '" + centro + "'")
                        print("[DEBUG] Cuenta: '" + cuenta + "'")
                        
                        posiciones_procesadas += 1
                        
                        # Verificar/crear Items necesarios (a partir de segunda iteracion)
                        if pos > 0:
                            verificar_y_crear_item(cx, nit, factura, 'IndicadorImpuestos')
                            verificar_y_crear_item(cx, nit, factura, 'CentroCoste')
                            verificar_y_crear_item(cx, nit, factura, 'Cuenta')
                            verificar_y_crear_item(cx, nit, factura, 'Observaciones')
                        
                        # ====================================================
                        # CAMINO 1: ACTIVO FIJO CON VALOR
                        # ====================================================
                        
                        if activo_fijo:
                            # Verificar tipo de Activo Fijo
                            es_diferido = (len(activo_fijo) == 10 and activo_fijo.startswith('2000'))
                            es_bonos = (len(activo_fijo) == 9 and activo_fijo.startswith('8000'))
                            
                            if not es_diferido and not es_bonos:
                                print("[INFO] ActivoFijo '" + activo_fijo + "' no es DIFERIDO ni BONOS, ignorando posicion")
                                continue
                            
                            # ============================================
                            # CAMINO 1.1: DIFERIDO (2000* - 10 digitos)
                            # ============================================
                            
                            if es_diferido:
                                print("[VALIDACION] Activo Fijo DIFERIDO detectado")
                                
                                # Validacion 1: IndicadorImpuestos
                                ind_validos_diferido = ['C1', 'FA', 'VP', 'CO', 'CR']
                                
                                if ind_imp in ind_validos_diferido:
                                    print("[OK] IndicadorImpuestos '" + ind_imp + "' valido para DIFERIDO")
                                    actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='SI')
                                elif not ind_imp:
                                    print("[NOVEDAD] IndicadorImpuestos vacio para DIFERIDO")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA con Activo Fijo, pero campo 'Indicador impuestos' NO se encuentra diligenciado para pedido DIFERIDO"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                else:
                                    print("[NOVEDAD] IndicadorImpuestos NO valido para DIFERIDO: '" + ind_imp + "'")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA Activo Fijo, pero campo 'Indicador impuestos' NO corresponde alguna de las opciones 'C1', 'FA', 'VP', 'CO' o 'CR' para pedido DIFERIDO"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                
                                # Validacion 2: CentroDeCoste (debe estar vacio)
                                if not centro:
                                    print("[OK] CentroDeCoste vacio (correcto para DIFERIDO)")
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='SI')
                                else:
                                    print("[NOVEDAD] CentroDeCoste tiene valor para DIFERIDO")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA con Activo Fijo, pero Campo 'Centro de coste' se encuentra diligenciado cuando NO debe estarlo para pedido DIFERIDO"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                
                                # Validacion 3: Cuenta (debe ser 2695950020)
                                if cuenta == '2695950020':
                                    print("[OK] Cuenta es 2695950020 (correcto para DIFERIDO)")
                                    actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='SI')
                                else:
                                    print("[NOVEDAD] Cuenta diferente a 2695950020 para DIFERIDO")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA con Activo Fijo, pero Campo 'Cuenta' no es igual 2695950020 pedido DIFERIDO"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                            
                            # ============================================
                            # CAMINO 1.2: BONOS (8000* - 9 digitos)
                            # ============================================
                            
                            elif es_bonos:
                                print("[VALIDACION] Activo Fijo BONOS detectado")
                                
                                # Validacion 1: IndicadorImpuestos (sin FA)
                                ind_validos_bonos = ['C1', 'VP', 'CO', 'CR']
                                
                                if ind_imp in ind_validos_bonos:
                                    print("[OK] IndicadorImpuestos '" + ind_imp + "' valido para BONOS")
                                    actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='SI')
                                elif not ind_imp:
                                    print("[NOVEDAD] IndicadorImpuestos vacio para BONOS")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA con Activo Fijo, pero campo 'Indicador impuestos' NO se encuentra diligenciado para pedido EQUIVALENTE AL EFECTIVO - BONOS"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                else:
                                    print("[NOVEDAD] IndicadorImpuestos NO valido para BONOS: '" + ind_imp + "'")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA con Activo Fijo, pero campo 'Indicador impuestos' NO corresponde alguna de las opciones 'C1', 'VP', 'CO' o 'CR' para pedido EQUIVALENTE AL EFECTIVO - BONOS"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                
                                # Validacion 2: CentroDeCoste (debe estar vacio)
                                if not centro:
                                    print("[OK] CentroDeCoste vacio (correcto para BONOS)")
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='SI')
                                else:
                                    print("[NOVEDAD] CentroDeCoste tiene valor para BONOS")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA con Activo Fijo, pero Campo 'Centro de coste' se encuentra diligenciado cuando NO debe estarlo para pedido EQUIVALENTE AL EFECTIVO - BONOS"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                
                                # Validacion 3: Cuenta (debe ser 2695950020)
                                if cuenta == '2695950020':
                                    print("[OK] Cuenta es 2695950020 (correcto para BONOS)")
                                    actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='SI')
                                else:
                                    print("[NOVEDAD] Cuenta diferente a 2695950020 para BONOS")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA con Activo Fijo, pero Campo 'Cuenta' NO ES IGUAL A 2695950020, para pedido EQUIVALENTE AL EFECTIVO - BONOS"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                        
                        # ====================================================
                        # CAMINO 2: ACTIVO FIJO VACIO (GENERALES)
                        # ====================================================
                        
                        else:
                            print("[VALIDACION] Sin Activo Fijo (GENERALES)")
                            
                            # Validacion 1: Cuenta debe tener valor
                            if cuenta:
                                print("[OK] Cuenta tiene valor (correcto para GENERALES)")
                                actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='SI')
                            else:
                                print("[NOVEDAD] Cuenta vacia para GENERALES")
                                hay_novedad = True
                                obs = "Pedido corresponde a ZPCN o ZPPA sin Activo Fijo, pero Campo 'Cuenta' NO se encuentra diligenciado cuando debe estarlo para pedido GENERALES"
                                actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                actualizar_item_comparativa(cx, nit, factura, 'Cuenta', aprobado='NO')
                                actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                continue  # No continuar con validaciones de GENERALES
                            
                            # Validacion 2: IndicadorImpuestos
                            if ind_imp:
                                print("[DEBUG] IndicadorImpuestos tiene valor: '" + ind_imp + "'")
                                
                                # Validacion 3: CentroDeCoste
                                if centro:
                                    print("[DEBUG] CentroDeCoste tiene valor: '" + centro + "'")
                                    
                                    # Marcar CentroCoste como SI inicialmente
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='SI')
                                    
                                    # Buscar en Excel
                                    indicadores_permitidos = buscar_indicadores_permitidos(df_impuestos, centro)
                                    
                                    if indicadores_permitidos is None:
                                        # CentroDeCoste NO encontrado en Excel
                                        print("[NOVEDAD] CentroDeCoste NO encontrado en Excel")
                                        hay_novedad = True
                                        obs = "Pedido corresponde a ZPCN o ZPPA sin Activo Fijo, pero Campo 'Centro de coste' NO se encuentra diligenciado en el archivo Impuestos especiales CXP, cuando debe estarlo para pedido GENERALES"
                                        actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                        actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='NO')
                                        actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='NO')
                                        actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                        estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                        actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                        actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                    else:
                                        # Validar que IndicadorImpuestos este en lista permitida
                                        if ind_imp in indicadores_permitidos:
                                            print("[OK] IndicadorImpuestos '" + ind_imp + "' es valido segun Excel")
                                            actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='SI')
                                        else:
                                            print("[NOVEDAD] IndicadorImpuestos NO valido segun Excel")
                                            hay_novedad = True
                                            obs = "Pedido corresponde a ZPCN o ZPPA sin Activo Fijo, pero campo 'Indicador impuestos' NO se encuentra diligenciado correctamente segun los indicadores (" + ', '.join(indicadores_permitidos) + ")"
                                            actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                            actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='NO')
                                            actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                            estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                            actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                            actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                                
                                else:
                                    # CentroDeCoste vacio
                                    print("[NOVEDAD] CentroDeCoste vacio para GENERALES")
                                    hay_novedad = True
                                    obs = "Pedido corresponde a ZPCN o ZPPA sin Activo Fijo, pero Campo 'Centro de coste' NO se encuentra diligenciado cuando debe estarlo para pedido GENERALES"
                                    actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                    actualizar_item_comparativa(cx, nit, factura, 'CentroCoste', aprobado='NO')
                                    actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                    estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                    actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                    actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                            
                            else:
                                # IndicadorImpuestos vacio
                                print("[NOVEDAD] IndicadorImpuestos vacio para GENERALES")
                                hay_novedad = True
                                obs = "Pedido corresponde a ZPCN o ZPPA sin Activo Fijo, pero campo 'Indicador impuestos' NO se encuentra diligenciado para pedido GENERALES"
                                actualizar_documents_processing(cx, nit, factura, oc, obs, forma_pago)
                                actualizar_item_comparativa(cx, nit, factura, 'IndicadorImpuestos', aprobado='NO')
                                actualizar_item_comparativa(cx, nit, factura, None, observacion=obs)
                                estado = 'CON NOVEDAD - CONTADO' if forma_pago in ['1', '01'] else 'CON NOVEDAD'
                                actualizar_item_comparativa(cx, nit, factura, None, estado=estado)
                                actualizar_historico_ordenes(cx, doccompra_list, nitcedula_list, porcalcular_list, textobreve_list)
                    
                    # Actualizar estadisticas
                    stats['posiciones_procesadas'] += posiciones_procesadas
                    if hay_novedad:
                        stats['validaciones_novedad'] += 1
                    else:
                        stats['validaciones_ok'] += 1
                    
                except Exception as e_row:
                    print("[ERROR] Error procesando registro " + str(idx) + ": " + str(e_row))
                    stats['errores'] += 1
                    continue
            
            # ================================================================
            # FIN DE PROCESO
            # ================================================================
            
            stats['tiempo_total'] = time.time() - t_inicio
            
            print("")
            print("=" * 80)
            print("[FIN] Proceso completado")
            print("=" * 80)
            print("[ESTADISTICAS]")
            print("  Total registros: " + str(stats['total_registros']))
            print("  Posiciones procesadas: " + str(stats['posiciones_procesadas']))
            print("  Validaciones OK: " + str(stats['validaciones_ok']))
            print("  Validaciones con novedad: " + str(stats['validaciones_novedad']))
            print("  Errores: " + str(stats['errores']))
            print("  Tiempo total: " + str(round(stats['tiempo_total'], 2)) + "s")
            print("=" * 80)
            
            msg = ("Proceso OK. Total:" + str(stats['total_registros']) + 
                   " Posiciones:" + str(stats['posiciones_procesadas']) +
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

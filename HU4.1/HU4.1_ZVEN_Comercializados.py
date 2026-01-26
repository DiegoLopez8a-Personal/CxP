def ZVEN_ValidarComercializados():
    """
    Función para procesar las validaciones de ZVEN/50 (Pedidos Comercializados).
    Versión: 3.1 - Integración 100% Rocketbot (GetVar/SetVar) y Validación de Asociación.
    """
    
    # =========================================================================
    # IMPORTS
    # =========================================================================
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
    import shutil
    import re
    
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    # =========================================================================
    # 1. FUNCIONES AUXILIARES 
    # =========================================================================
    
    def safe_str(v):
        if v is None: return ""
        if isinstance(v, str): return v.strip()
        if isinstance(v, bytes):
            try: return v.decode('latin-1', errors='replace').strip()
            except: return str(v).strip()
        if isinstance(v, (int, float)):
            if isinstance(v, float) and (np.isnan(v) or pd.isna(v)): return ""
            return str(v)
        try: return str(v).strip()
        except: return ""
    
    def truncar_observacion(obs, max_len=3900):
        if not obs: return ""
        obs_str = safe_str(obs)
        if len(obs_str) > max_len: return obs_str[:max_len - 3] + "..."
        return obs_str
    
    def normalizar_decimal(valor):
        if pd.isna(valor) or valor == '' or valor is None: return 0.0
        if isinstance(valor, (int, float)):
            if np.isnan(valor) if isinstance(valor, float) else False: return 0.0
            return float(valor)
        valor_str = str(valor).strip().replace(',', '.')
        valor_str = re.sub(r'[^\d.\-]', '', valor_str)
        try: return float(valor_str)
        except: return 0.0

    def parse_config(raw):
        if isinstance(raw, dict): return raw
        text = safe_str(raw)
        if not text: raise ValueError("vLocDicConfig vacio")
        try: return json.loads(text)
        except json.JSONDecodeError: pass
        try: return ast.literal_eval(text)
        except: raise ValueError("Config invalida")

    def expandir_posiciones_string(valor_string, separador='|'):
        if pd.isna(valor_string) or valor_string == '' or valor_string is None: return []
        valor_str = safe_str(valor_string)
        if '|' in valor_str: return [v.strip() for v in valor_str.split('|') if v.strip()]
        if ',' in valor_str: return [v.strip() for v in valor_str.split(',') if v.strip()]
        return [valor_str.strip()]
        
    def normalizar_nombre_empresa(nombre):
        if pd.isna(nombre) or nombre == "": return ""
        nombre = safe_str(nombre).upper().strip()
        nombre_limpio = re.sub(r'[,.\s]', '', nombre)
        reemplazos = {
            'SAS': ['SAS', 'S.A.S.', 'S.A.S', 'SAAS', 'S A S', 'S,A.S.', 'S,AS'],
            'LTDA': ['LIMITADA', 'LTDA', 'LTDA.', 'LTDA,'],
            'SENC': ['S.ENC.', 'SENC', 'SENCA', 'COMANDITA', 'SENCS', 'S.EN.C.'],
            'SA': ['SA', 'S.A.', 'S.A']
        }
        for clave, variantes in reemplazos.items():
            for variante in variantes:
                variante_limpia = re.sub(r'[,.\s]', '', variante)
                if variante_limpia in nombre_limpio:
                    nombre_limpio = nombre_limpio.replace(variante_limpia, clave)
        return nombre_limpio

    def comparar_nombres_proveedor(nombre_xml, nombre_sap):
        if pd.isna(nombre_xml) or pd.isna(nombre_sap): return False
        nombre_xml_limpio = normalizar_nombre_empresa(str(nombre_xml))
        nombre_sap_limpio = normalizar_nombre_empresa(str(nombre_sap))
        return sorted(nombre_xml_limpio.split()) == sorted(nombre_sap_limpio.split())

    # =========================================================================
    # 2. FUNCIONES DE BASE DE DATOS
    # =========================================================================
    @contextmanager
    def crear_conexion_db(cfg, usuario, contrasena, max_retries=3):
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing: raise ValueError(f"Parametros faltantes: {', '.join(missing)}")
        
        conn_str_auth = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg['ServidorBaseDatos']};DATABASE={cfg['NombreBaseDatos']};UID={usuario};PWD={contrasena};autocommit=False;"
        conn_str_trusted = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg['ServidorBaseDatos']};DATABASE={cfg['NombreBaseDatos']};Trusted_Connection=yes;autocommit=False;"

        cx = None
        conectado = False
        ultimo_error = None

        # PASO 1: Intentar obtener la conexión (Lógica de reintentos)
        for conn_str in [conn_str_auth, conn_str_trusted]:
            for attempt in range(max_retries):
                try:
                    cx = pyodbc.connect(conn_str, timeout=30)
                    cx.autocommit = False
                    conectado = True
                    break 
                except pyodbc.Error as e:
                    ultimo_error = e
                    time.sleep(1)
            if conectado:
                break # Salimos del bucle de métodos de conexión

        if not conectado:
            raise ultimo_error or Exception("Fallo conexion a BD tras multiples intentos")

        # PASO 2: Administrar el contexto (yield) de forma segura
        try:
            yield cx
            cx.commit() # Si el código del usuario termina bien, hacemos commit
        except Exception as e:
            cx.rollback() # Si el código del usuario falla, deshacemos todo
            raise e # Relanzamos el error para que el bot principal se entere
        finally:
            cx.close() # GARANTIZADO: Siempre se cierra la conexión

    def actualizar_bd_cxp(cx, registro_id, campos_actualizar):
        sets, parametros = [], []
        for campo, valor in campos_actualizar.items():
            if valor is not None:
                if campo == 'ObservacionesFase_4':
                    sets.append(f"[{campo}] = CASE WHEN [{campo}] IS NULL OR [{campo}] = '' THEN ? ELSE [{campo}] + ', ' + ? END")
                    parametros.extend([valor, valor])
                else:
                    sets.append(f"[{campo}] = ?")
                    parametros.append(valor)
    
        if sets:
            parametros.append(registro_id)
            # MEJORA: Usar el cursor con 'with' asegura que se cierre incluso si execute falla
            with cx.cursor() as cur:
                cur.execute(f"UPDATE [CxP].[DocumentsProcessing] SET {', '.join(sets)} WHERE [ID] = ?", parametros)

    def actualizar_items_comparativa(registro, cx, nit, factura, nombre_item, 
                                     actualizar_valor_xml=True, valor_xml=None,
                                     actualizar_aprobado=True, valor_aprobado=None, 
                                     actualizar_orden_compra=True, val_orden_de_compra=None,
                                     actualizar_orden_compra_comercializados=True, val_orden_de_compra_comercializados=None):
        cur = cx.cursor()
        def safe_db_val(v):
            if v is None: return None
            s = str(v).strip()
            return None if not s or s.lower() in ('none', 'null') else s

        cur.execute("SELECT COUNT(*) FROM [dbo].[CxP.Comparativa] WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?", (nit, factura, nombre_item, registro['ID_dp']))
        count_existentes = cur.fetchone()[0]

        lista_compra = [x.strip() for x in val_orden_de_compra.replace(',', '|').split('|') if x.strip()] if isinstance(val_orden_de_compra, str) else val_orden_de_compra         
        lista_xml = [x.strip() for x in valor_xml.replace(',', '|').split('|') if x.strip()] if isinstance(valor_xml, str) else valor_xml
        lista_aprob = [x.strip() for x in valor_aprobado.replace(',', '|').split('|') if x.strip()] if isinstance(valor_aprobado, str) else valor_aprobado     
        lista_comer = [x.strip() for x in val_orden_de_compra_comercializados.replace(',', '|').split('|') if x.strip()] if isinstance(val_orden_de_compra_comercializados, str) else val_orden_de_compra_comercializados  
        
        
        lista_compra = [] if lista_compra == None else lista_compra
        lista_xml = [] if lista_xml == None else lista_xml
        lista_aprob = [] if lista_aprob == None else lista_aprob
        lista_comer = [] if lista_comer == None else lista_comer
        
        count_nuevos = max(len(lista_compra), len(lista_xml), len(lista_aprob))
        count_nuevos = 1 if count_nuevos == 0 else count_nuevos

        for i in range(count_nuevos):
            val_compra = safe_db_val(lista_compra[i] if i < len(lista_compra) else None)
            val_xml = safe_db_val(lista_xml[i] if i < len(lista_xml) else None)
            val_aprob = safe_db_val(lista_aprob[i] if i < len(lista_aprob) else None)
            val_comer = safe_db_val(lista_comer[i] if i < len(lista_comer) else None)
            
            if i < count_existentes:
                set_clauses, params = [], []
                if actualizar_orden_compra: set_clauses.append("Valor_Orden_de_Compra = ?"); params.append(val_compra)
                if actualizar_orden_compra_comercializados: set_clauses.append("Valor_Orden_de_Compra_Comercializados = ?"); params.append(val_comer)
                if actualizar_valor_xml: set_clauses.append("Valor_XML = ?"); params.append(val_xml)
                if actualizar_aprobado: set_clauses.append("Aprobado = ?"); params.append(val_aprob)
                if not set_clauses: continue
                final_params = params + [nit, factura, nombre_item, registro['ID_dp'], i + 1]
                cur.execute(f"WITH CTE AS (SELECT Valor_Orden_de_Compra, Valor_Orden_de_Compra_Comercializados, Valor_XML, Aprobado, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn FROM [dbo].[CxP.Comparativa] WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?) UPDATE CTE SET {', '.join(set_clauses)} WHERE rn = ?", final_params)
            else:
                cur.execute("""INSERT INTO [dbo].[CxP.Comparativa] (Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra, Valor_Orden_de_Compra_Comercializados, Valor_XML, Aprobado) 
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                               (registro.get('Fecha_de_retoma_antes_de_contabilizacion_dp'), registro.get('documenttype_dp'), registro.get('numero_de_liquidacion_u_orden_de_compra_dp'), registro.get('nombre_emisor_dp'), registro['ID_dp'], nit, factura, nombre_item, val_compra, val_xml, val_aprob))
        cur.close()

    def marcar_orden_procesada(cx, oc_numero, posiciones_string):
        cur = cx.cursor()
        for pos in posiciones_string.split('|'):
            if pos.strip():
                cur.execute("UPDATE [CxP].[HistoricoOrdenesCompra] SET Marca = 'PROCESADO' WHERE DocCompra = ? AND Posicion = ?", (oc_numero, pos.strip()))
        cx.commit(); cur.close()

    def actualizar_estado_comparativa(cx, nit, factura, estado):
        cur = cx.cursor()
        cur.execute("UPDATE [dbo].[CxP.Comparativa] SET Estado_validacion_antes_de_eventos = ? WHERE NIT = ? AND Factura = ?", (estado, nit, factura))
        cur.close()

    # =========================================================================
    # 3. FUNCIONES ESPECÍFICAS DE ARCHIVOS Y VALIDACIONES
    # =========================================================================

    def validar_maestro_comercializados(ruta):
        """Valida que exista y tenga las columnas correctas el Maestro Comercializados"""
        if not os.path.exists(ruta): raise FileNotFoundError(f"No existe archivo: {ruta}")
        df = pd.read_excel(ruta)
        df.columns = df.columns.str.strip().str.upper()
        cols_req = ['OC', 'FACTURA', 'VALOR TOTAL OC', 'POSICION', 'POR CALCULAR (VALOR UNITARIO)', 'POR CALCULAR (ME)']
        if any(c not in df.columns for c in cols_req): raise ValueError(f"Faltan columnas en Maestro de Comercializados. Requeridas: {cols_req}")
        
        # Normalizar para búsquedas
        df['OC'] = df['OC'].astype(str).str.strip()
        df['FACTURA'] = df['FACTURA'].astype(str).str.strip()
        df['POSICION'] = df['POSICION'].astype(str).str.strip()
        return df

    def validar_asociacion_cuentas(ruta):
        """Valida hoja y columnas exactas de Asociación cuenta indicador según HU"""
        if not os.path.exists(ruta): raise FileNotFoundError(f"No existe archivo: {ruta}")
        xls = pd.ExcelFile(ruta)
        hoja_req = next((h for h in xls.sheet_names if 'grupo cuentas agrupacion provee' in h.lower()), None)
        if not hoja_req: raise ValueError("Hoja 'Grupo cuentas prove' no encontrada en Asociación cuenta indicador")
        
        df = pd.read_excel(ruta, sheet_name=hoja_req)
        df.columns = df.columns.str.strip().str.upper()
        
        cols_req = ['CTA MAYOR', 'NOMBRE CUENTA', 'TIPO RET.', 'IND.RETENCION', 'DESCRIPCION IND.RET.', 'AGRUPACION CODIGO', 'NOMBRE CODIGO']
        
        # Búsqueda flexible para evitar errores de espacios o puntos
        cols_faltantes = []
        for col in cols_req:
            if not any(col.replace('.', '').replace(' ', '') in c.replace('.', '').replace(' ', '') for c in df.columns):
                cols_faltantes.append(col)
                
        if cols_faltantes: raise ValueError(f"Faltan columnas en Asociación cuenta indicador: {cols_faltantes}")
        return df

    def mover_insumos_en_espera(registro, ruta_destino_base):
        try:
            ruta_origen = safe_str(registro.get('RutaArchivo_dp', ''))
            nombre_archivos = safe_str(registro.get('actualizacionNombreArchivos_dp', '')).split(',')
            if not ruta_origen or not nombre_archivos: return False, None
            ruta_destino = os.path.join(ruta_destino_base, "INSUMO")
            os.makedirs(ruta_destino, exist_ok=True)
            archivos_movidos = 0
            for archivo in nombre_archivos:
                origen = os.path.join(ruta_origen, archivo.strip())
                if os.path.exists(origen):
                    shutil.copy2(origen, os.path.join(ruta_destino, archivo.strip()))
                    archivos_movidos += 1
            return archivos_movidos > 0, ruta_destino
        except Exception: return False, None

    # =========================================================================
    # INICIO DEL PROCESO PRINCIPAL
    # =========================================================================
    try:
        t_inicio = time.time()
        print("="*80 + "\n[INICIO] Procesamiento ZVEN/50 - Comercializados\n" + "="*80)
        
        # 1. OBTENER VARIABLES DE ROCKETBOT
        #cfg = parse_config(GetVar("vLocDicConfig"))
        ####################################################################################################
        cfg = {}
        cfg['ServidorBaseDatos'] = 'localhost\SQLEXPRESS'
        cfg['NombreBaseDatos'] = 'NotificationsPaddy'
        cfg['UsuarioBaseDatos'] = 'aa'
        cfg['ClaveBaseDatos'] = 'aa'
        cfg['RutaInsumosComercializados'] = r'C:\Users\diego\Documents\GitHub\CxP\INSUMOS\Comercializados.xlsx'
        cfg['RutaInsumoAsociacion'] = r'C:\Users\diego\Documents\GitHub\CxP\INSUMOS\Asociacion cuenta indicador.xlsx'
        cfg['CarpetaDestinoComercializados'] = 'asdf'
        
        usuario_db = cfg["UsuarioBaseDatos"]
        clave_db = cfg["ClaveBaseDatos"]
        
        # Validar parámetros de configuración
        req_cfg = ['ServidorBaseDatos', 'NombreBaseDatos', 'RutaInsumosComercializados', 'RutaInsumoAsociacion', 'CarpetaDestinoComercializados']
        if any(not cfg.get(k) for k in req_cfg): raise ValueError("Faltan parametros en vLocDicConfig")
        
        # 2. VALIDAR ARCHIVOS MAESTROS (Si falla, va a excepción y detiene el bot)
        print("[INFO] Validando Maestro de Comercializados...")
        df_maestro = validar_maestro_comercializados(cfg['RutaInsumosComercializados'])
        
        print("[INFO] Validando Asociacion cuenta indicador...")
        df_asociacion = validar_asociacion_cuentas(cfg['RutaInsumoAsociacion'])
        
        cnt_proc, cnt_ok, cnt_nov, cnt_esp = 0, 0, 0, 0

        # 3. CONEXIÓN Y PROCESAMIENTO
        with crear_conexion_db(cfg, usuario_db, clave_db) as cx:
            df_registros = pd.read_sql("SELECT * FROM [CxP].[HU41_CandidatosValidacion] WHERE [ClaseDePedido_hoc] IN ('ZVEN', '50')", cx)
            print(f"[INFO] {len(df_registros)} registros ZVEN/50 para procesar.")

            for idx, registro in df_registros.iterrows():
                registro_id = safe_str(registro.get('ID_dp', ''))
                numero_oc = safe_str(registro.get('numero_de_liquidacion_u_orden_de_compra_dp', ''))
                numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
                nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
                payment_means = safe_str(registro.get('forma_de_pago_dp', ''))
                obs_existente = safe_str(registro.get('ObservacionesFase_4_dp', ''))
                
                sufijo_contado = " CONTADO" if payment_means in ["01", "1"] else ""

                # BÚSQUEDA EN MAESTRO COMERCIALIZADOS
                matches = df_maestro[(df_maestro['OC'] == numero_oc) & (df_maestro['FACTURA'] == numero_factura)]
                
                # CASO 1: NO EXISTE EN MAESTRO -> EN ESPERA
                if matches.empty:
                    movido_ok, nueva_ruta = mover_insumos_en_espera(registro, cfg['CarpetaDestinoComercializados'])
                    if movido_ok:
                        obs = f"No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados, {obs_existente}"
                        campos_db = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS {sufijo_contado}", 'RutaArchivo': nueva_ruta}
                    else:
                        obs = "No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados - No se logran mover insumos a carpeta COMERCIALIZADOS, {obs_existente}"
                        campos_db = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS {sufijo_contado}"}

                    actualizar_bd_cxp(cx, registro_id, campos_db)
                    actualizar_items_comparativa(registro, cx, nit, numero_factura, 'Observaciones', valor_xml=truncar_observacion(obs), val_orden_de_compra=None)
                    actualizar_estado_comparativa(cx, nit, numero_factura, f"EN ESPERA - COMERCIALIZADOS {sufijo_contado}")
                    cnt_esp += 1; cnt_proc += 1
                    continue

                # CASO 2: EXISTE EN MAESTRO -> PROCESAR
                pos_maestro = matches['POSICION'].tolist()
                vals_unitario = [normalizar_decimal(v) for v in matches['POR CALCULAR (VALOR UNITARIO)']]
                vals_me = [normalizar_decimal(v) for v in matches['POR CALCULAR (ME)']]
                
                actualizar_bd_cxp(cx, registro_id, {
                    'Posicion_Comercializado': ','.join(pos_maestro),
                    'Valor_a_pagar_Comercializado': ','.join(map(str, vals_unitario)),
                    'Valor_a_pagar_Comercializado_ME': ','.join(map(str, vals_me))
                })
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Posicion',
                                            valor_xml=pos_maestro, valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=pos_maestro)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularPosicion',
                                            valor_xml=vals_unitario, valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=vals_unitario)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularMEPosicion',
                                            valor_xml=vals_me, valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=vals_me)
                
                marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
#################################################################################################
                sap_posiciones = [int(p) for p in expandir_posiciones_string(registro.get('Posicion_hoc', '')) if p.strip().isdigit()]
                sap_posiciones = [str(p) for p in sap_posiciones]
                sap_por_calcular = expandir_posiciones_string(registro.get('PorCalcular_hoc', ''))
                
                # VALIDACIÓN 1: VALORES MAESTRO VS SAP VS XML
                usa_me = any(v > 0 for v in vals_me)
                valores_maestro_a_usar = vals_me if usa_me else vals_unitario
                suma_maestro = sum(valores_maestro_a_usar)
                vlr_factura_target = normalizar_decimal(registro.get('Valor de la Compra LEA_ddp', 0))

                coincide_valor = abs(suma_maestro - vlr_factura_target) <= 500
                coinciden_posiciones = True
                
                for i, pos in enumerate(pos_maestro):
                    # Validar si la posición existe en el histórico de SAP
                    if pos not in sap_posiciones: 
                        coinciden_posiciones = False
                        break
                    
                    # Obtener el índice de la posición en SAP
                    idx_sap = sap_posiciones.index(pos)
                    
                    # --- APLICACIÓN DE LA REGLA DE NEGOCIO ---
                    # Si hay valor en ME (> 0), usa ME. Si no, usa Valor Unitario.
                    valor_maestro_actual = vals_me[i] if int(vals_me[i]) > 0 else vals_unitario[i]

                    # Comparar SAP vs el valor seleccionado del Maestro
                    if abs(normalizar_decimal(sap_por_calcular[idx_sap]) - valor_maestro_actual) > 0.01: 
                        coinciden_posiciones = False
                        break

                if not (coincide_valor and coinciden_posiciones):
                    obs = f"No se encuentra coincidencia del Valor a pagar de la factura, {obs_existente}"
                    res_final = f"CON NOVEDAD - COMERCIALIZADOS {sufijo_contado}"
                    
                    actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': res_final})
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='LineExtensionAmount',
                                            valor_xml=vlr_factura_target, valor_aprobado='NO', val_orden_de_compra='NO ENCONTRADO', val_orden_de_compra_comercializados=None)

                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='VlrPagarCop',
                                            valor_xml=registro['VlrPagarCop_dp'], valor_aprobado='NO', val_orden_de_compra='NO ENCONTRADO', val_orden_de_compra_comercializados=None)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Observaciones',
                                            valor_xml=truncar_observacion(obs), valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=None)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularMEPosicion',
                                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=vals_me, val_orden_de_compra_comercializados=None)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularPosicion',
                                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=vals_unitario, val_orden_de_compra_comercializados=None)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='FecDoc',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra='NO ENCONTRADO', val_orden_de_compra_comercializados=None)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='FecReg',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra='NO ENCONTRADO', val_orden_de_compra_comercializados=None)
                    
                    actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                    
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    cnt_nov += 1; cnt_proc += 1
                    
                    continue
                
                
                # MARCAR PROCESADO EN SAP
                marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularSAP',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['PorCalcular_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='TipoNIF',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['TipoNif_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Acreedor',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Acreedor_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Acreedor',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Acreedor_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='FecDoc',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['FecDoc_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='FecReg',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['FecReg_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='FechaContGasto',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['FecContGasto_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='IndicadorImpuestos',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['IndicadorImpuestos_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='TextoBreve',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Texto_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ClaseImpuesto',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['ClaseDeImpuesto_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='DocFIEntrada',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['DocFiEntrada_hoc'], val_orden_de_compra_comercializados=None)
                
                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='CTA26',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Cuenta26_hoc'], val_orden_de_compra_comercializados=None)
                
                if usa_me:
                    sum_unitario = sum(vals_unitario)
                    if abs(int(sum_unitario) - int(registro['valor_a_pagar_dp'])) <= 500 and abs(int(vals_me) - int(registro['VlrPagarCop_dp'])) <= 500:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='LineExtensionAmount',
                                            valor_xml=registro['valor_a_pagar_dp'], valor_aprobado='NO', val_orden_de_compra=registro['PorCalcular_hoc'], val_orden_de_compra_comercializados=str(sum(vals_unitario)))

                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='VlrPagarCop',
                                            valor_xml=registro['VlrPagarCop_dp'], valor_aprobado='SI', val_orden_de_compra=sum_por_calcular, val_orden_de_compra_comercializados=sum_me)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Posicion',
                                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Posicion_hoc'], val_orden_de_compra_comercializados=", ".join(sap_posiciones))

                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularPosicion',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['PorCalcular_hoc'], val_orden_de_compra_comercializados=vals_unitario)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularMEPosicion',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=vals_me)
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    else:
                        obs = f"No se encuentra coincidencia del Valor a pagar de la factura, {obs_existente}"
                        res_final = f"CON NOVEDAD - COMERCIALIZADOS {sufijo_contado}"

                        actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': res_final})
                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='LineExtensionAmount',
                                            valor_xml=registro['valor_a_pagar_dp'], valor_aprobado='NO', val_orden_de_compra=registro['PorCalcular_hoc'], val_orden_de_compra_comercializados=str(sum(vals_me)))
                
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='VlrPagarCop',
                                            valor_xml=registro['VlrPagarCop_dp'], valor_aprobado='NO', val_orden_de_compra=sum_por_calcular, val_orden_de_compra_comercializados=sum_me)

                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Observaciones',
                                            valor_xml=truncar_observacion(obs), valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=None)
                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='VlrPagarCop',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Posicion_hoc'], val_orden_de_compra_comercializados=", ".join(sap_posiciones))

                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularPosicion',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['PorCalcular_hoc'], val_orden_de_compra_comercializados=vals_unitario)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularMEPosicion',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=vals_me)
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, res_final)

                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        
                else:
                    sum_me = sum(vals_me)
                    sum_por_calcular = sum(int(float(x.strip())) for x in registro['PorCalcular_hoc'].split('|') if x.strip())
                    if abs(normalizar_decimal(sum_me) - normalizar_decimal(registro['valor_a_pagar_dp'])) <= 500 and \
                        abs(normalizar_decimal(sum_por_calcular) - normalizar_decimal(registro['valor_a_pagar_dp'])) <= 500:
                            
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='LineExtensionAmount',
                                            valor_xml=registro['valor_a_pagar_dp'], valor_aprobado='SI', val_orden_de_compra=registro['PorCalcular_hoc'], val_orden_de_compra_comercializados=str(sum(vals_unitario)))
                
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='VlrPagarCop',
                                            valor_xml=registro['VlrPagarCop_dp'], valor_aprobado='SI', val_orden_de_compra=sum_por_calcular, val_orden_de_compra_comercializados=sum_me)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Posicion',
                                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Posicion_hoc'], val_orden_de_compra_comercializados=", ".join(sap_posiciones))

                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularPosicion',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['PorCalcular_hoc'], val_orden_de_compra_comercializados=vals_unitario)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularMEPosicion',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=vals_me)
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    else:
                        obs = f"No se encuentra coincidencia del Valor a pagar de la factura, {obs_existente}"
                        res_final = f"CON NOVEDAD - COMERCIALIZADOS {sufijo_contado}"

                        actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': res_final})
                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='LineExtensionAmount',
                                            valor_xml=registro['valor_a_pagar_dp'], valor_aprobado='NO', val_orden_de_compra=registro['PorCalcular_hoc'], val_orden_de_compra_comercializados=str(sum(vals_unitario)))
                
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='VlrPagarCop',
                                            valor_xml=registro['VlrPagarCop_dp'], valor_aprobado='NO', val_orden_de_compra=sum_por_calcular, val_orden_de_compra_comercializados=sum_me)

                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Observaciones',
                                            valor_xml=truncar_observacion(obs), valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=None)
                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='VlrPagarCop',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Posicion_hoc'], val_orden_de_compra_comercializados=", ".join(sap_posiciones))

                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularPosicion',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['PorCalcular_hoc'], val_orden_de_compra_comercializados=vals_unitario)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='ValorPorCalcularMEPosicion',
                                            valor_xml=None, valor_aprobado=None, val_orden_de_compra=None, val_orden_de_compra_comercializados=vals_me)
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                    
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                # VALIDACIÓN 2: TRM
                print(f"[DEBUG] Validando TRM...")
                
                trm_xml = normalizar_decimal(registro.get('CalculationRate_dp', 0))
                trm_sap = normalizar_decimal(registro['Trm_hoc'].split('|')[0])
                es_usd = registro['Moneda_hoc'][0] == 'USD'
                
                if es_usd:
                    # Solo validar TRM si hay valores
                    if trm_xml > 0 or trm_sap > 0:
                        trm_coincide = abs(trm_xml - trm_sap) < 0.01
                        
                        if not trm_coincide:
                            print(f"[INFO] TRM no coincide: XML {trm_xml} vs SAP {trm_sap}")
                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                            observacion = f"No se encuentra coincidencia en el campo TRM de la factura vs la informacion reportada en SAP, {registro['ObservacionesFase_4_dp']}"
                            hay_novedad = True
                            
                            campos_novedad_trm = {
                                'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_trm)
                            
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TRM',
                                                valor_xml=registro['CalculationRate_dp'], valor_aprobado=None, val_orden_de_compra=None)
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Observaciones',
                                                valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TRM',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Trm_hoc'])
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TRM',
                                                valor_xml=None, valor_aprobado='NO', val_orden_de_compra=None)
                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                    
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        else:
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TRM',
                                                valor_xml=registro['CalculationRate_dp'], valor_aprobado=None, val_orden_de_compra=None)
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Observaciones',
                                                valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TRM',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Trm_hoc'])
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TRM',
                                                valor_xml=None, valor_aprobado='SI', val_orden_de_compra=None)
                            
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                            
                # -------------------------------------------------------------
                # 3. Validar Cantidad y Precio (Tolerancia 1) - Listas vs Listas
                # -------------------------------------------------------------
                # 1. Convertir datos del XML (_ddp) a listas
                cant_xml_list = expandir_posiciones_string(registro.get('Cantidad de producto_ddp', ''))
                prec_xml_list = expandir_posiciones_string(registro.get('Precio Unitario del producto_ddp', ''))
                
                # 2. Obtener datos de SAP (_hoc) que ya son listas
                cant_sap_list = expandir_posiciones_string(registro.get('CantPedido_hoc', ''))
                prec_sap_list = expandir_posiciones_string(registro.get('PrecioUnitario_hoc', ''))
                
                fallo_cp = False
                aprobados_cant, aprobados_prec = [], []
                
                # 3. Comparar posición por posición (índice i vs índice i)
                for i in range(len(sap_posiciones)):
                    # Extraer valores seguros (si no existe el índice, pone 0)
                    cant_sap = normalizar_decimal(cant_sap_list[i] if i < len(cant_sap_list) else 0)
                    prec_sap = normalizar_decimal(prec_sap_list[i] if i < len(prec_sap_list) else 0)
                    
                    cant_xml = normalizar_decimal(cant_xml_list[i] if i < len(cant_xml_list) else 0)
                    prec_xml = normalizar_decimal(prec_xml_list[i] if i < len(prec_xml_list) else 0)
                    
                    # Validación con tolerancia de 1
                    if abs(cant_xml - cant_sap) > 1 or abs(prec_xml - prec_sap) > 1:
                        fallo_cp = True
                        aprobados_cant.append('NO')
                        aprobados_prec.append('NO')
                    else: 
                        aprobados_cant.append('SI')
                        aprobados_prec.append('SI')

                # 4. Registrar resultados en BD
                if fallo_cp:
                    obs = f"No se encuentra coincidencia en cantidad y/o precio unitario de la factura vs la información reportada en SAP, {registro['ObservacionesFase_4_dp']}"
                    res_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                    
                    actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': res_final})
                    
                    # Unimos las listas originales del XML para dejarlas en la tabla comparativa
                    str_cant_xml = '|'.join([str(x) for x in cant_xml_list])
                    str_prec_xml = '|'.join([str(x) for x in prec_xml_list])
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='PrecioUnitarioProducto',
                                                valor_xml=registro.get('Precio Unitario del producto_ddp', ''), valor_aprobado='NO', val_orden_de_compra=registro.get('PrecioUnitario_hoc', ''))
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CantidadProducto',
                                                valor_xml=registro.get('Cantidad de producto_ddp', ''), valor_aprobado='NO', val_orden_de_compra=registro.get('CantPedido_hoc', ''))
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Observaciones',
                                                valor_xml=truncar_observacion(obs), valor_aprobado=None, val_orden_de_compra=None)
                    actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                    
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    cnt_nov += 1; cnt_proc += 1
                    continue
                else:
                    str_cant_xml = '|'.join([str(x) for x in cant_xml_list])
                    str_prec_xml = '|'.join([str(x) for x in prec_xml_list])
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='PrecioUnitarioProducto',
                                                valor_xml=registro.get('Precio Unitario del producto_ddp', ''), valor_aprobado='SI', val_orden_de_compra=registro.get('PrecioUnitario_hoc', ''))
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CantidadProducto',
                                                valor_xml=registro.get('Cantidad de producto_ddp', ''), valor_aprobado='SI', val_orden_de_compra=registro.get('CantPedido_hoc', ''))
                    
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                
                
                # VALIDACIÓN 4: NOMBRE EMISOR
                print(f"[DEBUG] Validando nombre emisor...")
                
                nombre_emisor_xml = safe_str(registro.get('nombre_emisor_dp', ''))
                nombre_proveedor_sap = safe_str(registro.get('nombre_emisor_dp', '').split("|")[0])
                
                nombres_coinciden = comparar_nombres_proveedor(nombre_emisor_xml, nombre_proveedor_sap)
                
                if not nombres_coinciden:
                    print(f"[INFO] Nombre emisor no coincide: XML '{nombre_emisor_xml}' vs SAP '{nombre_proveedor_sap}'")
                    observacion = f"No se encuentra coincidencia en Nombre Emisor de la factura vs la informacion reportada en SAP, {registro['ObservacionesFase_4_dp']}"
                    hay_novedad = True
                    resultado_final = f"CON NOVEDAD {sufijo_contado}"
                    
                    campos_novedad_nombre = {
                        'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                        'ObservacionesFase_4': truncar_observacion(observacion),
                        'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                    }
                    actualizar_bd_cxp(cx, registro_id, campos_novedad_nombre)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                        nombre_item='NombreEmisor',
                        valor_xml=registro['nombre_emisor_dp'], valor_aprobado='NO', val_orden_de_compra=registro['NProveedor_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                        nombre_item='Observaciones',
                        valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                    
                    actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                    
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    
                else:
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                        nombre_item='NombreEmisor',
                        valor_xml=registro['nombre_emisor_dp'], valor_aprobado='SI', val_orden_de_compra=registro['NProveedor_hoc'])
                    
                # -------------------------------------------------------------
                # FIN DEL PROCESO EXITOSO
                # -------------------------------------------------------------
                res_final = f"PROCESADO {sufijo_contado}"
                actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ResultadoFinalAntesEventos': res_final})
                actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                cnt_ok += 1; cnt_proc += 1

        # 4. SALIDA EXITOSA A ROCKETBOT
        print("="*80 + "\n[FIN] Procesamiento ZVEN/50 completado\n" + "="*80)
        resumen = f"Procesados {cnt_proc} registros ZVEN. Exitosos: {cnt_ok}, Con novedad: {cnt_nov}, En espera: {cnt_esp}"
        SetVar("vLocStrResultadoSP", "True")
        SetVar("vLocStrResumenSP", resumen)
        
    except Exception as e:
        # 5. SALIDA DE ERROR A ROCKETBOT
        print(f"[CRÍTICO] Fallo general ZVEN: {str(e)}")
        print(traceback.format_exc())
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "Error_HU4.1_ZVEN")
        SetVar("vLocStrResultadoSP", "False")

# Ejecución de la función
ZVEN_ValidarComercializados()
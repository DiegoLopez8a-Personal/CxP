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
                                     actualizar_orden_compra=True, val_orden_de_compra=None):
        cur = cx.cursor()
        def safe_db_val(v):
            if v is None: return None
            s = str(v).strip()
            return None if not s or s.lower() in ('none', 'null') else s

        cur.execute("SELECT COUNT(*) FROM [dbo].[CxP.Comparativa] WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?", (nit, factura, nombre_item, registro['ID_dp']))
        count_existentes = cur.fetchone()[0]

        lista_compra = val_orden_de_compra.split('|') if val_orden_de_compra else []
        lista_xml = valor_xml.split('|') if valor_xml else []
        lista_aprob = valor_aprobado.split('|') if valor_aprobado else []
        count_nuevos = max(len(lista_compra), len(lista_xml), len(lista_aprob))
        count_nuevos = 1 if count_nuevos == 0 else count_nuevos

        for i in range(count_nuevos):
            val_compra = safe_db_val(lista_compra[i] if i < len(lista_compra) else None)
            val_xml = safe_db_val(lista_xml[i] if i < len(lista_xml) else None)
            val_aprob = safe_db_val(lista_aprob[i] if i < len(lista_aprob) else None)

            if i < count_existentes:
                set_clauses, params = [], []
                if actualizar_orden_compra: set_clauses.append("Valor_Orden_de_Compra = ?"); params.append(val_compra)
                if actualizar_valor_xml: set_clauses.append("Valor_XML = ?"); params.append(val_xml)
                if actualizar_aprobado: set_clauses.append("Aprobado = ?"); params.append(val_aprob)
                if not set_clauses: continue
                final_params = params + [nit, factura, nombre_item, registro['ID_dp'], i + 1]
                cur.execute(f"WITH CTE AS (SELECT Valor_Orden_de_Compra, Valor_XML, Aprobado, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn FROM [dbo].[CxP.Comparativa] WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?) UPDATE CTE SET {', '.join(set_clauses)} WHERE rn = ?", final_params)
            else:
                cur.execute("""INSERT INTO [dbo].[CxP.Comparativa] (Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra, Valor_XML, Aprobado) 
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
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
        hoja_req = next((h for h in xls.sheet_names if 'grupo cuentas prove' in h.lower()), None)
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
        cfg = parse_config(GetVar("vLocDicConfig"))
        usuario_db = cfg["UsuarioBaseDatos"]
        clave_db = cfg["ClaveBaseDatos"]
        
        # Validar parámetros de configuración
        req_cfg = ['ServidorBaseDatos', 'NombreBaseDatos', 'RutaInsumosComercializados', 'RutaInsumoAsociacion', 'CarpetaDestinoComercializados']
        if any(not cfg.get(k) for k in req_cfg): raise ValueError("Faltan parámetros en vLocDicConfig")
        
        # 2. VALIDAR ARCHIVOS MAESTROS (Si falla, va a excepción y detiene el bot)
        print("[INFO] Validando Maestro de Comercializados...")
        df_maestro = validar_maestro_comercializados(cfg['RutaInsumosComercializados'])
        
        print("[INFO] Validando Asociación cuenta indicador...")
        df_asociacion = validar_asociacion_cuentas(cfg['RutaInsumoAsociacion'])
        
        cnt_proc, cnt_ok, cnt_nov, cnt_esp = 0, 0, 0, 0

        # 3. CONEXIÓN Y PROCESAMIENTO
        with crear_conexion_db(cfg, usuario_db, clave_db) as cx:
            df_registros = pd.read_sql("SELECT * FROM [CxP].[HU41_CandidatosValidacion] WHERE [ClaseDePedido_hoc] IN ('ZVEN', '50')", cx)
            print(f"[INFO] {len(df_registros)} registros ZVEN/50 para procesar.")

            for idx, registro in df_registros.iterrows():
                try:
                    registro_id = safe_str(registro.get('ID_dp', ''))
                    numero_oc = safe_str(registro.get('numero_de_liquidacion_u_orden_de_compra_dp', ''))
                    numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
                    nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
                    payment_means = safe_str(registro.get('forma_de_pago_dp', ''))
                    
                    sufijo_contado = " CONTADO" if payment_means in ["01", "1"] else ""

                    # BÚSQUEDA EN MAESTRO COMERCIALIZADOS
                    matches = df_maestro[(df_maestro['OC'] == numero_oc) & (df_maestro['FACTURA'] == numero_factura)]
                    
                    # CASO 1: NO EXISTE EN MAESTRO -> EN ESPERA
                    if matches.empty:
                        movido_ok, nueva_ruta = mover_insumos_en_espera(registro, cfg['CarpetaDestinoComercializados'])
                        if movido_ok:
                            obs = "No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados"
                            campos_db = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}", 'rutaRespaldo_dp': nueva_ruta}
                        else:
                            obs = "No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados - No se logran mover insumos a carpeta COMERCIALIZADOS"
                            campos_db = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}"}

                        actualizar_bd_cxp(cx, registro_id, campos_db)
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'Observaciones', valor_xml=truncar_observacion(obs), val_orden_de_compra=None)
                        actualizar_estado_comparativa(cx, nit, numero_factura, f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}")
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

                    sap_posiciones = expandir_posiciones_string(registro.get('Posicion_hoc', ''))
                    sap_por_calcular = expandir_posiciones_string(registro.get('PorCalcular_hoc', ''))
                    
                    # VALIDACIÓN 1: VALORES MAESTRO VS SAP VS XML
                    usa_me = any(v > 0 for v in vals_me)
                    valores_maestro_a_usar = vals_me if usa_me else vals_unitario
                    suma_maestro = sum(valores_maestro_a_usar)
                    vlr_factura_target = normalizar_decimal(registro.get('VlrPagarCop_dp' if usa_me else 'Valor de la Compra LEA_ddp', 0))

                    coincide_valor = abs(suma_maestro - vlr_factura_target) <= 500
                    coinciden_posiciones = True
                    for i, pos in enumerate(pos_maestro):
                        if pos not in sap_posiciones: coinciden_posiciones = False; break
                        idx_sap = sap_posiciones.index(pos)
                        if abs(normalizar_decimal(sap_por_calcular[idx_sap]) - valores_maestro_a_usar[i]) > 0.01: coinciden_posiciones = False; break

                    if not (coincide_valor and coinciden_posiciones):
                        obs = "No se encuentra coincidencia del Valor a pagar de la factura"
                        res_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                        actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': res_final})
                        item_xml = 'VlrPagarCop' if usa_me else 'LineExtensionAmount'
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, item_xml, valor_xml=str(vlr_factura_target), val_orden_de_compra='NO ENCONTRADO', valor_aprobado='NO')
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'Observaciones', valor_xml=truncar_observacion(obs))
                        actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                        cnt_nov += 1; cnt_proc += 1
                        continue

                    # MARCAR PROCESADO EN SAP
                    marcar_orden_procesada(cx, numero_oc, '|'.join(pos_maestro))
                    item_xml = 'VlrPagarCop' if usa_me else 'LineExtensionAmount'
                    actualizar_items_comparativa(registro, cx, nit, numero_factura, item_xml, valor_xml=str(vlr_factura_target), val_orden_de_compra=str(suma_maestro), valor_aprobado='SI')
                    
                    for campo in ['TipoNif_hoc', 'Acreedor_hoc', 'FecDoc_hoc', 'FecReg_hoc', 'FecContGasto_hoc', 'IndicadorImpuestos_hoc', 'TextoBreve_hoc', 'ClaseDeImpuesto_hoc', 'Cuenta_hoc', 'CiudadProveedor_hoc', 'DocFiEntrada_hoc', 'Cuenta26_hoc']:
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, campo.replace('_hoc', ''), val_orden_de_compra=registro.get(campo, ''), valor_aprobado='SI')

                    # VALIDACIÓN 2: TRM
                    trm_xml = normalizar_decimal(registro.get('CalculationRate_dp', 0))
                    trm_sap_list = expandir_posiciones_string(registro.get('Trm_hoc', ''))
                    trm_sap = normalizar_decimal(trm_sap_list[0] if trm_sap_list else 0)

                    if abs(trm_xml - trm_sap) >= 0.01:
                        obs = "No se encuentra coincidencia en el campo TRM de la factura vs la información reportada en SAP"
                        res_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                        actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': res_final})
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'TRM', valor_xml=str(trm_xml), val_orden_de_compra=str(trm_sap), valor_aprobado='NO')
                        actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                        cnt_nov += 1; cnt_proc += 1
                        continue
                    else:
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'TRM', valor_xml=str(trm_xml), val_orden_de_compra=str(trm_sap), valor_aprobado='SI')

                    # VALIDACIÓN 3: CANTIDAD Y PRECIO
                    cant_xml = normalizar_decimal(registro.get('Cantidad de producto_ddp', 0))
                    prec_xml = normalizar_decimal(registro.get('Precio Unitario del producto_ddp', 0))
                    cant_sap_list = expandir_posiciones_string(registro.get('CantPedido_hoc', ''))
                    prec_sap_list = expandir_posiciones_string(registro.get('PrecioUnitario_hoc', ''))
                    
                    fallo_cp = False
                    aprobados_cant, aprobados_prec = [], []
                    for i in range(len(sap_posiciones)):
                        cant_sap = normalizar_decimal(cant_sap_list[i] if i < len(cant_sap_list) else 0)
                        prec_sap = normalizar_decimal(prec_sap_list[i] if i < len(prec_sap_list) else 0)
                        if abs(cant_xml - cant_sap) > 1 or abs(prec_xml - prec_sap) > 1:
                            fallo_cp = True; aprobados_cant.append('NO'); aprobados_prec.append('NO')
                        else: aprobados_cant.append('SI'); aprobados_prec.append('SI')

                    if fallo_cp:
                        obs = "No se encuentra coincidencia en cantidad y/o precio unitario de la factura vs la información reportada en SAP"
                        res_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                        actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': res_final})
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'Cantidad de producto', valor_xml=str(cant_xml), val_orden_de_compra=registro.get('CantPedido_hoc'), valor_aprobado='|'.join(aprobados_cant))
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'Precio Unitario del producto', valor_xml=str(prec_xml), val_orden_de_compra=registro.get('PrecioUnitario_hoc'), valor_aprobado='|'.join(aprobados_prec))
                        actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                        cnt_nov += 1; cnt_proc += 1
                        continue
                    else:
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'Cantidad de producto', valor_xml=str(cant_xml), val_orden_de_compra=registro.get('CantPedido_hoc'), valor_aprobado='SI')
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'Precio Unitario del producto', valor_xml=str(prec_xml), val_orden_de_compra=registro.get('PrecioUnitario_hoc'), valor_aprobado='SI')

                    # VALIDACIÓN 4: NOMBRE EMISOR
                    nombre_xml = safe_str(registro.get('nombre_emisor_dp', ''))
                    nombre_sap = safe_str(registro.get('NProveedor_hoc', ''))
                    if not comparar_nombres_proveedor(nombre_xml, nombre_sap):
                        obs = "No se encuentra coincidencia en Nombre Emisor de la factura vs la información reportada en SAP"
                        res_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                        actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': res_final})
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'Nombre emisor', valor_xml=nombre_xml, val_orden_de_compra=nombre_sap, valor_aprobado='NO')
                        actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                        cnt_nov += 1; cnt_proc += 1
                        continue
                    else:
                        actualizar_items_comparativa(registro, cx, nit, numero_factura, 'Nombre emisor', valor_xml=nombre_xml, val_orden_de_compra=nombre_sap, valor_aprobado='SI')

                    # -------------------------------------------------------------
                    # FIN DEL PROCESO EXITOSO
                    # -------------------------------------------------------------
                    res_final = f"PROCESADO{sufijo_contado}"
                    actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso', 'ResultadoFinalAntesEventos': res_final})
                    actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                    cnt_ok += 1; cnt_proc += 1

                except Exception as e:
                    print(f"[ERROR] En registro {idx}: {str(e)}")
                    cnt_nov += 1; cnt_proc += 1

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
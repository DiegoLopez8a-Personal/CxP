def HU42_ValidarNotasCreditoDebito():
    """
    Función para procesar las validaciones de Notas Crédito (NC) y Notas Débito (ND).
    
    VERSIÓN: 3.0 - Reestructuracion completa basada en estandares ZPAF
    """
    
    import json
    import ast
    import traceback
    import pyodbc
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    from contextlib import contextmanager
    import time
    import warnings
    import re
    import os
    import unicodedata
    
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    # =========================================================================
    # FUNCIONES AUXILIARES BÁSICAS
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
        if len(obs_str) > max_len:
            return obs_str[:max_len - 3] + "..."
        return obs_str
    
    def parse_config(raw):
        if isinstance(raw, dict):
            if not raw: raise ValueError("Config vacia (dict)")
            return raw
        text = safe_str(raw)
        if not text: raise ValueError("vLocDicConfig vacio")
        try:
            config = json.loads(text)
            if not config: raise ValueError("Config vacia (JSON)")
            return config
        except json.JSONDecodeError: pass
        try:
            config = ast.literal_eval(text)
            if not config: raise ValueError("Config vacia (literal)")
            return config
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Config invalida: {str(e)}")
    
    def normalizar_decimal(valor):
        if pd.isna(valor) or valor == '' or valor is None: return 0.0
        try: return float(str(valor).strip().replace(',', '.').replace(r'[^\d.\-]', ''))
        except: return 0.0

    def campo_vacio(valor):
        return safe_str(valor) == "" or safe_str(valor).lower() in ('null', 'none', 'nan')
    
    def campo_con_valor(valor):
        return not campo_vacio(valor)

    def quitar_tildes(texto):
        if not texto: return ""
        return ''.join([c for c in unicodedata.normalize('NFKD', texto) if not unicodedata.combining(c)])

    def calcular_dias_diferencia(fecha_inicio, fecha_fin):
        try:
            if isinstance(fecha_inicio, str):
                for fmt in ['%Y-%m-%d', '%d/%m/%Y']:
                    try: fecha_inicio = datetime.strptime(fecha_inicio, fmt); break
                    except: continue
            if isinstance(fecha_fin, str):
                for fmt in ['%Y-%m-%d', '%d/%m/%Y']:
                    try: fecha_fin = datetime.strptime(fecha_fin, fmt); break
                    except: continue
            if isinstance(fecha_inicio, datetime) and isinstance(fecha_fin, datetime):
                return (fecha_fin - fecha_inicio).days
            return 0
        except: return 0

    # =========================================================================
    # FUNCIONES DE CONEXIÓN A BASE DE DATOS
    # =========================================================================

    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing: raise ValueError(f"Parametros faltantes: {', '.join(missing)}")

        usuario = cfg.get('UsuarioBaseDatos', '')
        contrasena = cfg.get('ClaveBaseDatos', '')
        
        conn_str_auth = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={cfg['ServidorBaseDatos']};"
            f"DATABASE={cfg['NombreBaseDatos']};"
            f"UID={usuario};PWD={contrasena};autocommit=False;"
        )
        
        conn_str_trusted = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={cfg['ServidorBaseDatos']};"
            f"DATABASE={cfg['NombreBaseDatos']};"
            "Trusted_Connection=yes;autocommit=False;"
        )

        cx = None
        conectado = False
        excepcion_final = None

        print("[DEBUG] Intentando conexion con Usuario/Contraseña...")
        for attempt in range(max_retries):
            try:
                cx = pyodbc.connect(conn_str_auth, timeout=30)
                cx.autocommit = False
                conectado = True
                print(f"[DEBUG] Conexion SQL (Auth) abierta exitosamente (intento {attempt + 1})")
                break
            except pyodbc.Error as e:
                excepcion_final = e
                if attempt < max_retries - 1: time.sleep(1)

        if not conectado:
            print("[DEBUG] Intentando conexion Trusted Connection (Windows Auth)...")
            for attempt in range(max_retries):
                try:
                    cx = pyodbc.connect(conn_str_trusted, timeout=30)
                    cx.autocommit = False
                    conectado = True
                    print(f"[DEBUG] Conexion SQL (Trusted) abierta exitosamente (intento {attempt + 1})")
                    break
                except pyodbc.Error as e:
                    excepcion_final = e
                    if attempt < max_retries - 1: time.sleep(1)

        if not conectado:
            raise excepcion_final or Exception("No se pudo conectar a la base de datos")
        
        try:
            yield cx
            if cx: cx.commit()
        except Exception as e:
            if cx: cx.rollback()
            raise
        finally:
            if cx:
                try: cx.close()
                except: pass

    # =========================================================================
    # FUNCIONES DE ACTUALIZACIÓN DE BASE DE DATOS
    # =========================================================================

    def actualizar_bd_cxp(cx, registro_id, campos_actualizar):
        try:
            sets = []
            parametros = []
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
                sql = f"UPDATE [CxP].[DocumentsProcessing] SET {', '.join(sets)} WHERE [ID] = ?"
                cur = cx.cursor()
                cur.execute(sql, parametros)
                cur.close()
        except Exception as e:
            print(f"[ERROR] Error actualizando DocumentsProcessing: {str(e)}")
            raise

    def actualizar_items_comparativa(registro_id, cx, nit, factura, nombre_item,
                                 actualizar_valor_xml=True, valor_xml=None,
                                 actualizar_aprobado=True, valor_aprobado=None, 
                                 actualizar_orden_compra=True, val_orden_de_compra=None,
                                 # Datos adicionales para INSERT
                                 fecha_retoma=None, tipo_doc=None, orden_compra=None, nombre_proveedor=None):
        """
        Actualiza o inserta items en [dbo].[CxP.Comparativa].
        """
        cur = cx.cursor()
        
        def safe_db_val(v):
            if v is None: return None
            s = str(v).strip()
            if not s or s.lower() == 'none' or s.lower() == 'null': return None
            return s

        # 1. Contar items existentes
        query_count = """
        SELECT COUNT(*)
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?
        """
        cur.execute(query_count, (nit, factura, nombre_item, registro_id))
        count_existentes = cur.fetchone()[0]

        # 2. Manejo seguro de los Splits
        lista_compra = val_orden_de_compra.split('|') if val_orden_de_compra else []
        lista_xml = valor_xml.split('|') if valor_xml else []
        lista_aprob = valor_aprobado.split('|') if valor_aprobado else []

        # 3. Obtener el máximo conteo
        maximo_conteo = max(len(lista_compra), len(lista_xml), len(lista_aprob))
        maximo_conteo = 1 if maximo_conteo == 0 else maximo_conteo

        # Iterar sobre los valores nuevos para actualizar o insertar
        for i in range(maximo_conteo):
            item_compra = lista_compra[i] if i < len(lista_compra) else None
            item_xml = lista_xml[i] if i < len(lista_xml) else None
            item_aprob = lista_aprob[i] if i < len(lista_aprob) else None

            val_compra = safe_db_val(item_compra)
            val_xml = safe_db_val(item_xml)
            val_aprob = safe_db_val(item_aprob)

            if i < count_existentes:
                # UPDATE
                set_clauses = []
                params = []
                if actualizar_orden_compra:
                    set_clauses.append("Valor_Orden_de_Compra = ?")
                    params.append(val_compra)
                if actualizar_valor_xml:
                    set_clauses.append("Valor_XML = ?")
                    params.append(val_xml)
                if actualizar_aprobado:
                    set_clauses.append("Aprobado = ?")
                    params.append(val_aprob)

                if not set_clauses: continue

                update_query = f"""
                WITH CTE AS (
                    SELECT Valor_Orden_de_Compra, Valor_XML, Aprobado,
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn
                    FROM [dbo].[CxP.Comparativa]
                    WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?
                )
                UPDATE CTE
                SET {", ".join(set_clauses)}
                WHERE rn = ?
                """
                # Corregido orden de parámetros: CTE params -> SET params -> WHERE rn param
                final_params = [nit, factura, nombre_item, registro_id] + params + [i + 1]
                cur.execute(update_query, final_params)

            else:
                # INSERT
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra,
                    Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(insert_query, (
                    fecha_retoma, tipo_doc, orden_compra, nombre_proveedor,
                    registro_id, nit, factura, nombre_item, val_compra, val_xml, val_aprob
                ))
        
        cur.close()

    def actualizar_estado_comparativa(cx, nit, factura, estado):
        cur = cx.cursor()
        update_sql = """
        UPDATE [dbo].[CxP.Comparativa]
        SET Estado_validacion_antes_de_eventos = ?
        WHERE NIT = ? AND Factura = ?
        """
        cur.execute(update_sql, (estado, nit, factura))
        cur.close()
        
    def actualizar_fecha_retoma(cx, nit, factura, fecha):
        cur = cx.cursor()
        update_sql = """
        UPDATE [dbo].[CxP.Comparativa]
        SET Fecha_de_retoma_antes_de_contabilizacion = ?
        WHERE NIT = ? AND Factura = ?
        """
        cur.execute(update_sql, (fecha, nit, factura))
        cur.close()

    def actualizar_nota_credito_referenciada_fv(cx, fv_id, numero_nc):
        try:
            cx.cursor().execute("UPDATE [CxP].[DocumentsProcessing] SET [NotaCreditoReferenciada] = ? WHERE [ID] = ?", (numero_nc, fv_id))
        except Exception as e: print(f"Error Update NC Ref: {e}")

    # =========================================================================
    # VALIDACIONES HU4.2
    # =========================================================================

    def validar_nombre_receptor(nombre):
        if campo_vacio(nombre): return False
        nombre = re.sub(r'[,.\s]', '', quitar_tildes(safe_str(nombre).upper()))
        return nombre in ['DIANACORPORACIONSAS', 'DICORPSAS']

    def validar_nit_receptor(nit):
        return re.sub(r'\D', '', safe_str(nit)) == '860031606'

    def validar_tipo_persona(tipo):
        return safe_str(tipo) == '31'

    def validar_digito_verificacion(digito):
        return safe_str(digito) == '6'

    def validar_tax_level_code(tax_code):
        if campo_vacio(tax_code): return False
        tax = safe_str(tax_code).upper()
        return any(v in tax for v in ['O-13', 'O-15', 'O-23', 'O-47', 'R-99-PN'])

    def buscar_factura_correspondiente(cx, nit, referencia, fecha_ejecucion):
        try:
            if isinstance(fecha_ejecucion, str):
                try: fecha_ejecucion = datetime.strptime(fecha_ejecucion, '%Y-%m-%d')
                except: pass
            
            p_mes = fecha_ejecucion.replace(day=1)
            p_ant = (p_mes - timedelta(days=1)).replace(day=1)
            
            cur = cx.cursor()
            # La factura referenciada no debe tener ya una nota credito aplicada (o null)
            # Ordenamos para priorizar la factura 'RECHAZADO' o 'APROBADO' segun logica original (RECHAZADO 1, APROBADO 2, RESTO 3)
            # Asumo que queremos encontrar la factura original para cruzarla.
            cur.execute("SELECT TOP 1 * FROM [CxP].[DocumentsProcessing] WHERE [tipo_de_documento]='FV' AND [nit_emisor_o_nit_del_proveedor]=? AND [numero_de_factura]=? AND [fecha_de_emision_documento]>=? AND ([NotaCreditoReferenciada] IS NULL OR [NotaCreditoReferenciada]='') ORDER BY CASE [ResultadoFinalAntesEventos] WHEN 'RECHAZADO' THEN 1 WHEN 'APROBADO' THEN 2 ELSE 3 END ASC", (nit, referencia, p_ant))
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))
            return None
        except: return None

    def generar_insumo_retorno_nc(registros, ruta):
        try:
            if not registros: return
            import openpyxl
            os.makedirs(os.path.dirname(ruta), exist_ok=True)
            wb = openpyxl.load_workbook(ruta) if os.path.exists(ruta) else openpyxl.Workbook()
            ws = wb['NC'] if 'NC' in wb.sheetnames else wb.create_sheet('NC')
            if ws.max_row == 1: ws.append(['ID', 'Fecha_Carga', 'Nit', 'Numero_Nota_Credito', 'Estado_CXP_Bot'])

            now_str = datetime.now().strftime('%Y-%m-%d')
            for r in registros:
                ws.append([r.get('ID'), now_str, r.get('nit_emisor_o_nit_del_proveedor'), r.get('numero_de_nota_credito'), r.get('estado')])
            if 'Sheet' in wb.sheetnames: del wb['Sheet']
            wb.save(ruta)
        except Exception as e: print(f"Error generando insumo retorno: {e}")

    # =========================================================================
    # LOGICA PRINCIPAL
    # =========================================================================

    try:
        print("")
        print("=" * 80)
        print("[INICIO] HU4.2 - Validacion NC/ND")
        print("=" * 80)

        # 1. Configuración
        try:
            cfg = parse_config(GetVar("vLocDicConfig"))
        except:
            # Fallback para pruebas locales si GetVar falla
            cfg = {}
            # Raise si es critico en produccion, aqui permitimos que cfg falle luego si faltan datos

        plazo_max = int(cfg.get('PlazoMaximoRetoma', 120))
        ruta_ret = cfg.get('RutaInsumoRetorno', '')
        config_manual = cfg.get('EsRetornoManual', False)
        
        now = datetime.now()
        
        with crear_conexion_db(cfg) as cx:

            # -----------------------------------------------------------------
            # PROCESAMIENTO DE NOTAS CREDITO (NC)
            # -----------------------------------------------------------------
            df_nc = pd.read_sql("SELECT * FROM [CxP].[DocumentsProcessing] WHERE [tipo_de_documento]='NC' AND ([ResultadoFinalAntesEventos] IS NULL OR [ResultadoFinalAntesEventos] NOT IN ('ENCONTRADO', 'NO EXITOSO'))", cx)
            print(f"[INFO] Procesando {len(df_nc)} Notas Credito (NC)...")
            
            cnt_nc = 0
            list_nov = []
            
            for idx, r in df_nc.iterrows():
                try:
                    reg_id = safe_str(r.get('ID', ''))
                    nit = safe_str(r.get('nit_emisor_o_nit_del_proveedor', ''))
                    num_nc = safe_str(r.get('numero_de_nota_credito', ''))
                    num_factura = r.get('numero_de_factura') # Puede ser None
                    obs_anterior = r.get('ObservacionesFase_4')

                    # Metadata para Comparativa INSERT
                    meta_fecha_retoma = r.get('Fecha_retoma_contabilizacion')
                    meta_tipo_doc = r.get('tipo_de_documento')
                    meta_orden = r.get('numero_de_liquidacion_u_orden_de_compra')
                    meta_nombre_prov = r.get('nombre_emisor')

                    print(f"  > NC: {num_nc} (ID: {reg_id})")

                    # Determinar si es retorno manual (Config Global o EstadoFase_3 Exitoso)
                    es_manual = config_manual
                    if not es_manual:
                        estado_f3 = safe_str(r.get('EstadoFase_3', '')).upper()
                        if estado_f3 == 'EXITOSO':
                            es_manual = True
                            print(f"    [INFO] Factura {num_factura} marcada como Retorno Manual (EstadoFase_3=Exitoso).")

                    # Validacion Retoma (Solo si no es manual)
                    if not es_manual:
                        f_ret = r.get('fecha_de_retoma')
                        if campo_con_valor(f_ret):
                            dias = calcular_dias_diferencia(f_ret, now)
                            if dias > plazo_max:
                                obs = f"Registro excede el plazo maximo de retoma, {obs_anterior}"
                                actualizar_bd_cxp(cx, reg_id, {
                                    'EstadoFinalFase_4': 'NO EXITOSO',
                                    'ObservacionesFase_4': truncar_observacion(obs),
                                    'ResultadoFinalAntesEventos': 'NO EXITOSO'
                                })
                                
                                actualizar_items_comparativa(reg_id, cx, nit, num_factura, 'Observaciones',
                                                           valor_xml=truncar_observacion(obs),
                                                           fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                           orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)
                                
                                actualizar_estado_comparativa(cx, nit, num_factura, "NO EXITOSO")
                                actualizar_fecha_retoma(cx, nit, num_factura, r.get('Fecha_retoma_contabilizacion'))
                                cnt_nc += 1
                                continue
                        else:
                            # Establecer fecha retoma inicial
                            cx.cursor().execute("UPDATE [CxP].[DocumentsProcessing] SET [Fecha_de_retoma_antes_de_contabilizacion]=? WHERE [ID]=?", (now.strftime('%Y-%m-%d'), reg_id))

                    # Diccionario de validaciones
                    validaciones = {
                        'NombreEmisor': {'val': r.get('nombre_emisor'), 'check': campo_con_valor},
                        'NITEmisor': {'val': r.get('nit_emisor_o_nit_del_proveedor'), 'check': campo_con_valor},
                        'FechaEmisionDocumento': {'val': r.get('fecha_de_emision_documento'), 'check': campo_con_valor},
                        'NombreReceptor': {'val': r.get('nombre_del_adquiriente'), 'check': validar_nombre_receptor},
                        'NitReceptor': {'val': r.get('nit_del_adquiriente'), 'check': validar_nit_receptor},
                        'TipoPersonaReceptor': {'val': r.get('tipo_persona'), 'check': validar_tipo_persona},
                        'DigitoVerificacionReceptor': {'val': r.get('digito_de_verificacion'), 'check': validar_digito_verificacion},
                        'TaxLevelCodeReceptor': {'val': r.get('responsabilidad_tributaria_adquiriente'), 'check': validar_tax_level_code}
                    }

                    # Validar Tipo Documento NC = 20
                    tipo_nc = safe_str(r.get('tipo_de_nota_credito', ''))
                    
                    if tipo_nc != '20':
                        obs = "Nota credito sin referencia"
                        actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': obs, 'ResultadoFinalAntesEventos': 'Con Novedad'})
                        
                        # Actualizar Comparativa
                        for k, v in validaciones.items():
                            actualizar_items_comparativa(reg_id, cx, nit, num_nc, k,
                                                       valor_xml=safe_str(v['val']),
                                                       valor_aprobado='SI' if v['check'](v['val']) else 'NO',
                                                       fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                       orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                        actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'InvoiceTypecode', valor_xml=tipo_nc,
                                                   fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                   orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                        actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'Observaciones', valor_xml=obs,
                                                   fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                   orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                        list_nov.append({'ID': reg_id, 'nit_emisor_o_nit_del_proveedor': nit, 'numero_de_nota_credito': num_nc, 'estado': 'CON NOVEDAD'})
                        cnt_nc += 1
                        continue

                    # Referencia
                    ref = safe_str(r.get('prefijo_y_numero', ''))
                    
                    # Buscar Factura
                    fv = buscar_factura_correspondiente(cx, nit, ref, now)
                    
                    if not fv:
                        obs = "Nota credito con referencia no encontrada"
                        actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': obs, 'ResultadoFinalAntesEventos': 'Con novedad'})

                        for k, v in validaciones.items():
                            actualizar_items_comparativa(reg_id, cx, nit, num_nc, k,
                                                       valor_xml=safe_str(v['val']),
                                                       valor_aprobado='SI' if v['check'](v['val']) else 'NO',
                                                       fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                       orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                        actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'InvoiceTypecode', valor_xml=tipo_nc,
                                                   fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                   orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                        actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'NotaCreditoReferenciada', valor_xml=ref, valor_aprobado='NO',
                                                   fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                   orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                        actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'Observaciones', valor_xml=obs,
                                                   fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                   orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                        list_nov.append({'ID': reg_id, 'nit_emisor_o_nit_del_proveedor': nit, 'numero_de_nota_credito': num_nc, 'estado': 'CON NOVEDAD'})
                        cnt_nc += 1
                        continue
                    
                    # Factura Encontrada
                    v_nc = normalizar_decimal(r.get('valor_a_pagar_nc', 0))
                    v_fv = normalizar_decimal(fv.get('valor_a_pagar', 0))
                    
                    # Actualizar DocumentsProcessing NC
                    actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ResultadoFinalAntesEventos': 'Encontrado'})
                    
                    # Actualizar Factura original con referencia
                    actualizar_nota_credito_referenciada_fv(cx, safe_str(fv.get('ID', '')), num_nc)
                    
                    # Guardar Trazabilidad Completa
                    for k, v in validaciones.items():
                        actualizar_items_comparativa(reg_id, cx, nit, num_nc, k,
                                                   valor_xml=safe_str(v['val']),
                                                   valor_aprobado='SI' if v['check'](v['val']) else 'NO',
                                                   fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                   orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'InvoiceTypecode', valor_xml=tipo_nc,
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'NotaCreditoReferenciada', valor_xml=ref, valor_aprobado='SI',
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'LineExtensionAmount',
                                               valor_xml=str(v_nc), val_orden_de_compra=str(v_fv), valor_aprobado='SI',
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'ActualizacionNombreArchivos', valor_xml=safe_str(r.get('actualizacion_nombre_archivos', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'RutaRespaldo', valor_xml=safe_str(r.get('ruta_respaldo', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'Observaciones', valor_xml=safe_str(r.get('ObservacionesFase_4', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nc, 'CufeUUID', valor_xml=safe_str(r.get('codigo_cufe_de_la_factura', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)
                    
                    cnt_nc += 1

                except Exception as e:
                    print(f"Error procesando NC {r.get('ID')}: {e}")
                    print(traceback.format_exc())
                    cnt_nc += 1
            
            # Generar reporte de novedades
            if list_nov and ruta_ret:
                generar_insumo_retorno_nc(list_nov, ruta_ret)
            
            # -----------------------------------------------------------------
            # PROCESAMIENTO DE NOTAS DEBITO (ND)
            # -----------------------------------------------------------------
            df_nd = pd.read_sql("SELECT * FROM [CxP].[DocumentsProcessing] WHERE [tipo_de_documento]='ND' AND ([ResultadoFinalAntesEventos] IS NULL OR [ResultadoFinalAntesEventos] NOT IN ('Exitoso')) ORDER BY [executionDate] DESC", cx)
            print(f"[INFO] Procesando {len(df_nd)} Notas Debito (ND)...")
            
            cnt_nd = 0
            for idx, r in df_nd.iterrows():
                try:
                    reg_id = safe_str(r.get('ID', ''))
                    nit = safe_str(r.get('nit_emisor_o_nit_del_proveedor', ''))
                    num_nd = safe_str(r.get('numero_de_nota_debito', ''))
                    
                    meta_fecha_retoma = r.get('Fecha_retoma_contabilizacion')
                    meta_tipo_doc = r.get('tipo_de_documento')
                    meta_orden = r.get('numero_de_liquidacion_u_orden_de_compra')
                    meta_nombre_prov = r.get('nombre_emisor')
                    
                    print(f"  > ND: {num_nd} (ID: {reg_id})")

                    validaciones = {
                        'NombreEmisor': {'val': r.get('nombre_emisor'), 'check': campo_con_valor},
                        'NITEmisor': {'val': r.get('nit_emisor_o_nit_del_proveedor'), 'check': campo_con_valor},
                        'FechaEmisionDocumento': {'val': r.get('fecha_de_emision_documento'), 'check': campo_con_valor},
                        'NombreReceptor': {'val': r.get('nombre_del_adquiriente'), 'check': validar_nombre_receptor},
                        'NitReceptor': {'val': r.get('nit_del_adquiriente'), 'check': validar_nit_receptor},
                        'TipoPersonaReceptor': {'val': r.get('tipo_persona'), 'check': validar_tipo_persona},
                        'DigitoVerificacionReceptor': {'val': r.get('digito_de_verificacion'), 'check': validar_digito_verificacion},
                        'TaxLevelCodeReceptor': {'val': r.get('responsabilidad_tributaria_adquiriente'), 'check': validar_tax_level_code}
                    }

                    # Actualizar DocumentsProcessing
                    actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ResultadoFinalAntesEventos': 'Exitoso'})
                    
                    # Actualizar Comparativa
                    for k, v in validaciones.items():
                        actualizar_items_comparativa(reg_id, cx, nit, num_nd, k,
                                                   valor_xml=safe_str(v['val']),
                                                   valor_aprobado='SI' if v['check'](v['val']) else 'NO',
                                                   fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                                   orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nd, 'LineExtensionAmount', valor_xml=safe_str(r.get('valor_a_pagar', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nd, 'InvoiceTypecode', valor_xml=safe_str(r.get('tipo_de_nota_debito', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nd, 'NotaCreditoReferenciada', valor_xml=safe_str(r.get('prefijo_y_numero', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nd, 'CufeUUID', valor_xml=safe_str(r.get('codigo_cufe_de_la_factura', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nd, 'ActualizacionNombreArchivos', valor_xml=safe_str(r.get('actualizacion_nombre_archivos', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nd, 'RutaRespaldo', valor_xml=safe_str(r.get('ruta_respaldo', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)

                    actualizar_items_comparativa(reg_id, cx, nit, num_nd, 'Observaciones', valor_xml=safe_str(r.get('ObservacionesFase_4', '')),
                                               fecha_retoma=meta_fecha_retoma, tipo_doc=meta_tipo_doc,
                                               orden_compra=meta_orden, nombre_proveedor=meta_nombre_prov)
                    
                    cnt_nd += 1
                except Exception as e:
                    print(f"Error procesando ND {r.get('ID')}: {e}")
                    print(traceback.format_exc())
                    cnt_nd += 1

            print(f"[FIN] Procesamiento completado. NC: {cnt_nc}, ND: {cnt_nd}")

            # Resultado Exitóso RocketBot
            SetVar("vLocStrResultadoSP", "True")
            SetVar("vLocStrResumenSP", f"Procesamiento Finalizado. NC: {cnt_nc}, ND: {cnt_nd}")

    except Exception as e:
        print("")
        print("=" * 80)
        print("[ERROR CRITICO] HU4.2 Fallo")
        print(f"Mensaje: {str(e)}")
        print(traceback.format_exc())
        print("=" * 80)

        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4.2_NC_ND")
        SetVar("vLocStrResultadoSP", "False")

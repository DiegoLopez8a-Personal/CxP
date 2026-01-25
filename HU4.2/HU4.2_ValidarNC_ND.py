def HU42_ValidarNotasCreditoDebito():
    """
    Función para procesar las validaciones de Notas Crédito (NC) y Notas Débito (ND).
    
    VERSIÓN: 2.2 - Corrección CufeUUID en NC
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
    
    def parse_config(raw):
        if isinstance(raw, dict): return raw
        try: return json.loads(safe_str(raw))
        except:
            try: return ast.literal_eval(safe_str(raw))
            except: raise ValueError("Config invalida")
    
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
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        if any(k not in cfg for k in required): raise ValueError("Faltan parametros BD")
        
        conn_str_auth = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg['ServidorBaseDatos']};DATABASE={cfg['NombreBaseDatos']};UID={GetVar('vGblStrUsuarioBaseDatos')};PWD={GetVar('vGblStrClaveBaseDatos')};autocommit=False;"
        conn_str_trusted = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg['ServidorBaseDatos']};DATABASE={cfg['NombreBaseDatos']};Trusted_Connection=yes;autocommit=False;"
        
        cx = None
        for attempt in range(max_retries):
            try:
                cx = pyodbc.connect(conn_str_auth, timeout=30)
                break
            except: time.sleep(1)

        if not cx:
            for attempt in range(max_retries):
                try:
                    cx = pyodbc.connect(conn_str_trusted, timeout=30)
                    break
                except: time.sleep(1)

        if not cx: raise Exception("Fallo conexion BD")
        
        try:
            yield cx
            cx.commit()
        except:
            cx.rollback(); raise
        finally:
            try: cx.close()
            except: pass
    
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
    
    def actualizar_bd_cxp(cx, reg_id, campos):
        try:
            sets, params = [], []
            for k, v in campos.items():
                if v is not None:
                    if k == 'ObservacionesFase_4':
                        sets.append(f"[{k}] = CASE WHEN [{k}] IS NULL OR [{k}] = '' THEN ? ELSE ? + ', ' + [{k}] END")
                        params.extend([v, v])
                    else:
                        sets.append(f"[{k}] = ?"); params.append(v)
            if sets:
                params.append(reg_id)
                cx.cursor().execute(f"UPDATE [CxP].[DocumentsProcessing] SET {', '.join(sets)} WHERE [ID] = ?", params)
        except Exception as e: print(f"Error Update BD: {e}")
    
    def actualizar_nota_credito_referenciada_fv(cx, fv_id, numero_nc):
        try:
            cx.cursor().execute("UPDATE [CxP].[DocumentsProcessing] SET [NotaCreditoReferenciada] = ? WHERE [ID] = ?", (numero_nc, fv_id))
        except Exception as e: print(f"Error Update NC Ref: {e}")
    
    def actualizar_items_comparativa(registro, cx, nit, factura, nombre_item,
                                 actualizar_valor_xml=True, valor_xml=None,
                                 actualizar_aprobado=True, valor_aprobado=None,
                                 actualizar_orden_compra=True, val_orden_de_compra=None):
        cur = cx.cursor()
        def safe_db_val(v): return str(v).strip() if v and str(v).strip().lower() not in ('none','null') else None

        cur.execute("SELECT COUNT(*) FROM [dbo].[CxP.Comparativa] WHERE NIT=? AND Factura=? AND Item=? AND ID_registro=?", (nit, factura, nombre_item, registro['ID_dp']))
        count_exist = cur.fetchone()[0]

        l_compra = val_orden_de_compra.split('|') if val_orden_de_compra else []
        l_xml = valor_xml.split('|') if valor_xml else []
        l_aprob = valor_aprobado.split('|') if valor_aprobado else []
        if isinstance(valor_aprobado, list): l_aprob = valor_aprobado

        max_len = max(len(l_compra), len(l_xml), len(l_aprob))
        max_len = 1 if max_len == 0 else max_len

        for i in range(max_len):
            v_comp = l_compra[i] if i < len(l_compra) else None
            v_xml = l_xml[i] if i < len(l_xml) else None
            v_aprob = l_aprob[i] if i < len(l_aprob) else None

            v_comp_db = safe_db_val(v_comp)
            v_xml_db = safe_db_val(v_xml)
            v_aprob_db = safe_db_val(v_aprob)

            if i < count_exist:
                sets, params = [], []
                if actualizar_orden_compra: sets.append("Valor_Orden_de_Compra=?"); params.append(v_comp_db)
                if actualizar_valor_xml: sets.append("Valor_XML=?"); params.append(v_xml_db)
                if actualizar_aprobado: sets.append("Aprobado=?"); params.append(v_aprob_db)
                if sets:
                    params.extend([nit, factura, nombre_item, registro['ID_dp'], i+1])
                    cur.execute(f"WITH CTE AS (SELECT Valor_Orden_de_Compra, Valor_XML, Aprobado, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn FROM [dbo].[CxP.Comparativa] WHERE NIT=? AND Factura=? AND Item=? AND ID_registro=?) UPDATE CTE SET {','.join(sets)} WHERE rn=?", params)
            else:
                cur.execute("INSERT INTO [dbo].[CxP.Comparativa] (Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra, Valor_XML, Aprobado) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (registro.get('fecha_de_retoma'), registro.get('tipo_de_documento'), None, registro.get('nombre_emisor'), registro['ID_dp'], nit, factura, nombre_item, v_comp_db, v_xml_db, v_aprob_db))
        cur.close()

    def buscar_factura_correspondiente(cx, nit, referencia, fecha_ejecucion):
        try:
            if isinstance(fecha_ejecucion, str):
                try: fecha_ejecucion = datetime.strptime(fecha_ejecucion, '%Y-%m-%d')
                except: pass
            
            p_mes = fecha_ejecucion.replace(day=1)
            p_ant = (p_mes - timedelta(days=1)).replace(day=1)
            
            cur = cx.cursor()
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

            now = datetime.now().strftime('%Y-%m-%d')
            for r in registros:
                ws.append([r.get('ID'), now, r.get('nit_emisor_o_nit_del_proveedor'), r.get('numero_de_nota_credito'), r.get('estado')])
            if 'Sheet' in wb.sheetnames: del wb['Sheet']
            wb.save(ruta)
        except: pass

    try:
        print("[INICIO] HU4.2 NC/ND")
        cfg = parse_config(GetVar("vLocDicConfig"))
        plazo_max = int(cfg.get('plazo_maximo_retoma_dias', 120))
        ruta_ret = cfg.get('RutaInsumoRetorno', '')
        manual = cfg.get('EsRetornoManual', False)
        now = datetime.now()
        
        with crear_conexion_db(cfg) as cx:
            # NC
            df_nc = pd.read_sql("SELECT * FROM [CxP].[DocumentsProcessing] WHERE [tipo_de_documento]='NC' AND ([ResultadoFinalAntesEventos] IS NULL OR [ResultadoFinalAntesEventos] NOT IN ('Encontrado', 'No exitoso')) ORDER BY [executionDate] DESC", cx)
            print(f"NC: {len(df_nc)}")
            
            cnt_nc, cnt_nov = 0, 0
            list_nov = []
            
            for idx, r in df_nc.iterrows():
                try:
                    r['ID_dp'] = r.get('ID') # Comp
                    reg_id = safe_str(r.get('ID', ''))
                    num_nc = safe_str(r.get('numero_de_nota_credito', ''))
                    nit = safe_str(r.get('nit_emisor_o_nit_del_proveedor', ''))
                    print(f"NC {num_nc}")
                    
                    if not manual:
                        f_ret = r.get('fecha_de_retoma')
                        if campo_con_valor(f_ret):
                            if calcular_dias_diferencia(f_ret, now) > plazo_max:
                                obs = "Registro excede el plazo maximo de retoma"
                                actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'No exitoso', 'ObservacionesFase_4': obs, 'ResultadoFinalAntesEventos': 'No exitoso'})
                                actualizar_items_comparativa(r, cx, nit, num_nc, 'Observaciones', valor_xml=obs)
                                cnt_nc += 1; continue
                        else:
                            cx.cursor().execute("UPDATE [CxP].[DocumentsProcessing] SET [fecha_de_retoma]=? WHERE [ID]=?", (now.strftime('%Y-%m-%d'), reg_id))

                    items = {}
                    items['NombreEmisor'] = {'xml': safe_str(r.get('nombre_emisor', '')), 'ok': 'SI' if campo_con_valor(r.get('nombre_emisor')) else 'NO'}
                    items['NITEmisor'] = {'xml': safe_str(r.get('nit_emisor_o_nit_del_proveedor', '')), 'ok': 'SI' if campo_con_valor(r.get('nit_emisor_o_nit_del_proveedor')) else 'NO'}
                    items['FechaEmisionDocumento'] = {'xml': safe_str(r.get('fecha_de_emision_documento', '')), 'ok': 'SI' if campo_con_valor(r.get('fecha_de_emision_documento')) else 'NO'}
                    items['NombreReceptor'] = {'xml': safe_str(r.get('nombre_del_adquiriente', '')), 'ok': 'SI' if validar_nombre_receptor(r.get('nombre_del_adquiriente')) else 'NO'}
                    items['NitReceptor'] = {'xml': safe_str(r.get('nit_del_adquiriente', '')), 'ok': 'SI' if validar_nit_receptor(r.get('nit_del_adquiriente')) else 'NO'}
                    items['TipoPersonaReceptor'] = {'xml': safe_str(r.get('tipo_persona', '')), 'ok': 'SI' if validar_tipo_persona(r.get('tipo_persona')) else 'NO'}
                    items['DigitoVerificacionReceptor'] = {'xml': safe_str(r.get('digito_de_verificacion', '')), 'ok': 'SI' if validar_digito_verificacion(r.get('digito_de_verificacion')) else 'NO'}
                    items['TaxLevelCodeReceptor'] = {'xml': safe_str(r.get('responsabilidad_tributaria_adquiriente', '')), 'ok': 'SI' if validar_tax_level_code(r.get('responsabilidad_tributaria_adquiriente')) else 'NO'}
                    
                    tipo = safe_str(r.get('tipo_de_nota_credito', ''))
                    items['InvoiceTypecode'] = {'xml': tipo, 'ok': ''}
                    
                    if tipo != '20':
                        obs = "Nota credito sin referencia"
                        actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': obs, 'ResultadoFinalAntesEventos': 'Con Novedad'})
                        for k,v in items.items(): actualizar_items_comparativa(r, cx, nit, num_nc, k, valor_xml=v['xml'], valor_aprobado=v['ok'])
                        actualizar_items_comparativa(r, cx, nit, num_nc, 'Observaciones', valor_xml=obs)
                        list_nov.append({'ID': reg_id, 'nit_emisor_o_nit_del_proveedor': nit, 'numero_de_nota_credito': num_nc, 'estado': 'CON NOVEDAD'})
                        cnt_nc += 1; cnt_nov += 1; continue
                        
                    ref = safe_str(r.get('prefijo_y_numero', ''))
                    items['NotaCreditoReferenciada'] = {'xml': ref, 'ok': ''}
                    
                    fv = buscar_factura_correspondiente(cx, nit, ref, now)
                    
                    if not fv:
                        obs = "Nota credito con referencia no encontrada"
                        actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': obs, 'ResultadoFinalAntesEventos': 'Con novedad'})
                        items['NotaCreditoReferenciada']['ok'] = 'NO'
                        for k,v in items.items(): actualizar_items_comparativa(r, cx, nit, num_nc, k, valor_xml=v['xml'], valor_aprobado=v['ok'])
                        actualizar_items_comparativa(r, cx, nit, num_nc, 'Observaciones', valor_xml=obs)
                        list_nov.append({'ID': reg_id, 'nit_emisor_o_nit_del_proveedor': nit, 'numero_de_nota_credito': num_nc, 'estado': 'CON NOVEDAD'})
                        cnt_nc += 1; cnt_nov += 1; continue

                    items['NotaCreditoReferenciada']['ok'] = 'SI'
                    
                    v_nc = normalizar_decimal(r.get('valor_a_pagar_nc', 0))
                    v_fv = normalizar_decimal(fv.get('valor_a_pagar', 0))
                    
                    actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ResultadoFinalAntesEventos': 'Encontrado'})
                    actualizar_nota_credito_referenciada_fv(cx, safe_str(fv.get('ID', '')), num_nc)
                    
                    for k,v in items.items(): actualizar_items_comparativa(r, cx, nit, num_nc, k, valor_xml=v['xml'], valor_aprobado=v['ok'])
                    
                    actualizar_items_comparativa(r, cx, nit, num_nc, 'LineExtensionAmount', valor_xml=str(v_nc), val_orden_de_compra=str(v_fv), valor_aprobado='SI')
                    actualizar_items_comparativa(r, cx, nit, num_nc, 'ActualizacionNombreArchivos', valor_xml=safe_str(r.get('actualizacion_nombre_archivos', '')))
                    actualizar_items_comparativa(r, cx, nit, num_nc, 'RutaRespaldo', valor_xml=safe_str(r.get('ruta_respaldo', '')))
                    actualizar_items_comparativa(r, cx, nit, num_nc, 'Observaciones', valor_xml=safe_str(r.get('ObservacionesFase_4', '')))
                    actualizar_items_comparativa(r, cx, nit, num_nc, 'CufeUUID', valor_xml=safe_str(r.get('codigo_cufe_de_la_factura', ''))) # Added CufeUUID
                    
                    cnt_nc += 1
                except Exception as e: print(f"Err NC: {e}"); cnt_nc += 1
            
            if list_nov and ruta_ret: generar_insumo_retorno_nc(list_nov, ruta_ret)
            
            # ND
            df_nd = pd.read_sql("SELECT * FROM [CxP].[DocumentsProcessing] WHERE [tipo_de_documento]='ND' AND ([ResultadoFinalAntesEventos] IS NULL OR [ResultadoFinalAntesEventos] NOT IN ('Exitoso')) ORDER BY [executionDate] DESC", cx)
            print(f"ND: {len(df_nd)}")
            cnt_nd = 0
            
            for idx, r in df_nd.iterrows():
                try:
                    r['ID_dp'] = r.get('ID')
                    reg_id = safe_str(r.get('ID', ''))
                    num_nd = safe_str(r.get('numero_de_nota_debito', ''))
                    nit = safe_str(r.get('nit_emisor_o_nit_del_proveedor', ''))
                    
                    items = {}
                    items['NombreEmisor'] = {'xml': safe_str(r.get('nombre_emisor', '')), 'ok': 'SI' if campo_con_valor(r.get('nombre_emisor')) else 'NO'}
                    items['NITEmisor'] = {'xml': safe_str(r.get('nit_emisor_o_nit_del_proveedor', '')), 'ok': 'SI' if campo_con_valor(r.get('nit_emisor_o_nit_del_proveedor')) else 'NO'}
                    items['FechaEmisionDocumento'] = {'xml': safe_str(r.get('fecha_de_emision_documento', '')), 'ok': 'SI' if campo_con_valor(r.get('fecha_de_emision_documento')) else 'NO'}
                    items['NombreReceptor'] = {'xml': safe_str(r.get('nombre_del_adquiriente', '')), 'ok': 'SI' if validar_nombre_receptor(r.get('nombre_del_adquiriente')) else 'NO'}
                    items['NitReceptor'] = {'xml': safe_str(r.get('nit_del_adquiriente', '')), 'ok': 'SI' if validar_nit_receptor(r.get('nit_del_adquiriente')) else 'NO'}
                    items['TipoPersonaReceptor'] = {'xml': safe_str(r.get('tipo_persona', '')), 'ok': 'SI' if validar_tipo_persona(r.get('tipo_persona')) else 'NO'}
                    items['DigitoVerificacionReceptor'] = {'xml': safe_str(r.get('digito_de_verificacion', '')), 'ok': 'SI' if validar_digito_verificacion(r.get('digito_de_verificacion')) else 'NO'}
                    items['TaxLevelCodeReceptor'] = {'xml': safe_str(r.get('responsabilidad_tributaria_adquiriente', '')), 'ok': 'SI' if validar_tax_level_code(r.get('responsabilidad_tributaria_adquiriente')) else 'NO'}
                    
                    actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ResultadoFinalAntesEventos': 'Exitoso'})
                    
                    for k, v in items.items(): actualizar_items_comparativa(r, cx, nit, num_nd, k, valor_xml=v['xml'], valor_aprobado=v['ok'])
                    
                    actualizar_items_comparativa(r, cx, nit, num_nd, 'LineExtensionAmount', valor_xml=safe_str(r.get('valor_a_pagar', '')))
                    actualizar_items_comparativa(r, cx, nit, num_nd, 'InvoiceTypecode', valor_xml=safe_str(r.get('tipo_de_nota_debito', '')))
                    actualizar_items_comparativa(r, cx, nit, num_nd, 'NotaCreditoReferenciada', valor_xml=safe_str(r.get('prefijo_y_numero', '')))
                    actualizar_items_comparativa(r, cx, nit, num_nd, 'CufeUUID', valor_xml=safe_str(r.get('codigo_cufe_de_la_factura', '')))
                    actualizar_items_comparativa(r, cx, nit, num_nd, 'ActualizacionNombreArchivos', valor_xml=safe_str(r.get('actualizacion_nombre_archivos', '')))
                    actualizar_items_comparativa(r, cx, nit, num_nd, 'RutaRespaldo', valor_xml=safe_str(r.get('ruta_respaldo', '')))
                    actualizar_items_comparativa(r, cx, nit, num_nd, 'Observaciones', valor_xml=safe_str(r.get('ObservacionesFase_4', '')))
                    
                    cnt_nd += 1
                except Exception as e: print(f"Err ND: {e}"); cnt_nd += 1

        print(f"FIN: NC {cnt_nc}, ND {cnt_nd}")

    except Exception as e: print(f"Crit: {e}")

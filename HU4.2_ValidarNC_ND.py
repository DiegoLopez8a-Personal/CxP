def HU42_ValidarNotasCreditoDebito():
    """
    Función para procesar las validaciones de Notas Crédito (NC) y Notas Débito (ND).
    
    VERSIÓN: 1.0 - 12 Enero 2026
    
    FLUJO PRINCIPAL NC:
        1. Verificar fecha de retoma (plazo máximo parametrizable)
        2. Validar campos básicos: Nombre Emisor, NIT Emisor, Fecha emisión
        3. Validar campos receptor: Nombre, NIT (860031606), Tipo Persona (31), 
           DigitoVerificacion (6), TaxLevelCode
        4. Validar referencia (Tipo nota crédito = 20)
        5. Comparar NC con FV en BD (NIT + Referencia)
        6. Comparar valores (Valor a Pagar nc vs Valor a Pagar FV)
        7. Generar insumo de retorno para NC con novedad
    
    FLUJO PRINCIPAL ND:
        1. Validar campos básicos
        2. Validar campos receptor
        3. Marcar como Exitoso
    
    Returns:
        None: Actualiza variables globales en RocketBot
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
        """Convierte un valor a string de manera segura."""
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
            if isinstance(v, float) and (np.isnan(v) or pd.isna(v)):
                return ""
            return str(v)
        try:
            return str(v).strip()
        except:
            return ""
    
    def truncar_observacion(obs, max_len=3900):
        """Trunca observación para prevenir overflow en BD."""
        if not obs:
            return ""
        obs_str = safe_str(obs)
        if len(obs_str) > max_len:
            return obs_str[:max_len - 3] + "..."
        return obs_str
    
    def parse_config(raw):
        """Parsea la configuración desde RocketBot."""
        if isinstance(raw, dict):
            if not raw:
                raise ValueError("Config vacia (dict)")
            return raw
        text = safe_str(raw)
        if not text:
            raise ValueError("vLocDicConfig vacio")
        try:
            config = json.loads(text)
            if not config:
                raise ValueError("Config vacia (JSON)")
            return config
        except json.JSONDecodeError:
            pass
        try:
            config = ast.literal_eval(text)
            if not config:
                raise ValueError("Config vacia (literal)")
            return config
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Config invalida: {str(e)}")
    
    def normalizar_decimal(valor):
        """Normaliza valores decimales con punto o coma."""
        if pd.isna(valor) or valor == '' or valor is None:
            return 0.0
        if isinstance(valor, (int, float)):
            if np.isnan(valor) if isinstance(valor, float) else False:
                return 0.0
            return float(valor)
        valor_str = str(valor).strip()
        valor_str = valor_str.replace(',', '.')
        valor_str = re.sub(r'[^\d.\-]', '', valor_str)
        try:
            return float(valor_str)
        except:
            return 0.0
    
    def campo_vacio(valor):
        """Verifica si un campo está vacío."""
        valor_str = safe_str(valor)
        return valor_str == "" or valor_str.lower() in ('null', 'none', 'nan')
    
    def campo_con_valor(valor):
        """Verifica si un campo tiene valor."""
        return not campo_vacio(valor)
    
    def quitar_tildes(texto):
        """Elimina tildes de un texto."""
        if not texto:
            return ""
        nfkd = unicodedata.normalize('NFKD', texto)
        return ''.join([c for c in nfkd if not unicodedata.combining(c)])
    
    # =========================================================================
    # CONEXIÓN A BASE DE DATOS
    # =========================================================================
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        """
        Crea conexión a la base de datos con reintentos y manejo de transacciones.
        """
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            raise ValueError(f"Parametros faltantes: {', '.join(missing)}")

        usuario = GetVar("vGblStrUsuarioBaseDatos")
        contrasena = GetVar("vGblStrClaveBaseDatos")
        
        conn_str_auth = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={cfg['ServidorBaseDatos']};"
            f"DATABASE={cfg['NombreBaseDatos']};"
            f"UID={usuario};"
            f"PWD={contrasena};"
            "autocommit=False;"
        )
        
        conn_str_trusted = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={cfg['ServidorBaseDatos']};"
            f"DATABASE={cfg['NombreBaseDatos']};"
            "Trusted_Connection=yes;"
            "autocommit=False;"
        )

        cx = None
        conectado = False
        excepcion_final = None

        # Intento 1: Autenticacion SQL
        # for attempt in range(max_retries):
        #     try:
        #         cx = pyodbc.connect(conn_str_auth, timeout=30)
        #         cx.autocommit = False
        #         conectado = True
        #         print(f"[DEBUG] Conexion SQL (Auth) abierta exitosamente (intento {attempt + 1})")
        #         break
        #     except pyodbc.Error as e:
        #         print(f"[WARNING] Fallo conexion con Usuario/Contraseña (intento {attempt + 1}): {str(e)}")
        #         excepcion_final = e
        #         if attempt < max_retries - 1:
        #             time.sleep(1)

        # Intento 2: Trusted Connection
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
                    print(f"[WARNING] Fallo conexion Trusted Connection (intento {attempt + 1}): {str(e)}")
                    excepcion_final = e
                    if attempt < max_retries - 1:
                        time.sleep(1)

        if not conectado:
            raise excepcion_final or Exception("No se pudo conectar a la base de datos con ningun metodo")
        
        try:
            yield cx
            if cx:
                cx.commit()
                print("[DEBUG] Commit final exitoso")
        except Exception as e:
            if cx:
                cx.rollback()
                print(f"[ERROR] Rollback por error: {str(e)}")
            raise
        finally:
            if cx:
                try:
                    cx.close()
                    print("[DEBUG] Conexion cerrada")
                except:
                    pass
    
    # =========================================================================
    # FUNCIONES DE VALIDACION ESPECÍFICAS
    # =========================================================================
    
    def validar_nombre_receptor(nombre):
        """Valida que el nombre receptor sea DIANA CORPORACIÓN SAS o variantes permitidas."""
        if campo_vacio(nombre):
            return False
        
        nombre_str = safe_str(nombre).upper().strip()
        nombre_sin_tilde = quitar_tildes(nombre_str)
        nombre_limpio = re.sub(r'[,.\s]', '', nombre_sin_tilde)
        
        patrones_validos = [
            'DIANACORPORACIONSAS',
            'DICORPSAS'
        ]
        
        for patron in patrones_validos:
            if nombre_limpio == patron:
                return True
        
        return False
    
    def validar_nit_receptor(nit):
        """Valida que NIT receptor sea 860031606."""
        nit_str = safe_str(nit).strip()
        nit_limpio = re.sub(r'\D', '', nit_str)
        return nit_limpio == '860031606'
    
    def validar_tipo_persona(tipo):
        """Valida que Tipo Persona sea 31."""
        tipo_str = safe_str(tipo).strip()
        return tipo_str == '31'
    
    def validar_digito_verificacion(digito):
        """Valida que Dígito de verificación sea 6."""
        digito_str = safe_str(digito).strip()
        return digito_str == '6'
    
    def validar_tax_level_code(tax_code):
        """Valida que TaxLevelCode esté dentro del estándar."""
        if campo_vacio(tax_code):
            return False
        
        tax_str = safe_str(tax_code).upper().strip()
        valores_permitidos = ['O-13', 'O-15', 'O-23', 'O-47', 'R-99-PN']
        
        for valor in valores_permitidos:
            if valor in tax_str:
                return True
        
        return False
    
    def calcular_dias_diferencia(fecha_inicio, fecha_fin):
        """Calcula la diferencia en días entre dos fechas."""
        try:
            if isinstance(fecha_inicio, str):
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        fecha_inicio = datetime.strptime(fecha_inicio, fmt)
                        break
                    except:
                        continue
            
            if isinstance(fecha_fin, str):
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        fecha_fin = datetime.strptime(fecha_fin, fmt)
                        break
                    except:
                        continue
            
            if isinstance(fecha_inicio, datetime) and isinstance(fecha_fin, datetime):
                return (fecha_fin - fecha_inicio).days
            
            return 0
        except:
            return 0
    
    # =========================================================================
    # FUNCIONES DE ACTUALIZACIÓN DE BD
    # =========================================================================
    
    def actualizar_bd_cxp(cx, registro_id, campos_actualizar):
        """Actualiza campos en [CxP].[DocumentsProcessing]."""
        try:
            sets = []
            parametros = []
            
            for campo, valor in campos_actualizar.items():
                if valor is not None:
                    if campo == 'ObservacionesFase_4':
                        sets.append(f"[{campo}] = CASE WHEN [{campo}] IS NULL OR [{campo}] = '' THEN ? ELSE ? + ', ' + [{campo}] END")
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
                
                print(f"[UPDATE] DocumentsProcessing actualizada - ID {registro_id}")
            
        except Exception as e:
            print(f"[ERROR] Error actualizando DocumentsProcessing: {str(e)}")
            raise
    
    def actualizar_nota_credito_referenciada_fv(cx, fv_id, numero_nc):
        """Actualiza el campo Nota crédito referenciada en la FV."""
        try:
            cur = cx.cursor()
            sql = """
            UPDATE [CxP].[DocumentsProcessing]
            SET [NotaCreditoReferenciada] = ?
            WHERE [ID] = ?
            """
            cur.execute(sql, (numero_nc, fv_id))
            cur.close()
            print(f"[UPDATE] FV {fv_id} - NotaCreditoReferenciada = {numero_nc}")
        except Exception as e:
            print(f"[ERROR] Error actualizando NotaCreditoReferenciada: {str(e)}")
    
    def actualizar_items_comparativa(registro, cx, nit, factura, nombre_item,
                                 actualizar_valor_xml=True, valor_xml=None,
                                 actualizar_aprobado=True, valor_aprobado=None,
                                 actualizar_orden_compra=True, val_orden_de_compra=None):
        """
        Actualiza o inserta items en [dbo].[CxP.Comparativa].
        Adpated logic from ZPAF.
        Here 'factura' parameter will hold the NC/ND number as Document ID.
        'val_orden_de_compra' will hold the Reference Value or SAP value if applicable.
        """
        cur = cx.cursor()
        
        def safe_db_val(v):
            if v is None: return None
            s = str(v).strip()
            if not s or s.lower() == 'none' or s.lower() == 'null': return None
            return s

        # Contar items existentes - Note: We use 'ID_registro' to scope to the NC/ND
        # 'ID_dp' is expected in registro dict.
        registro_id = registro.get('ID', '') # In DocumentsProcessing table it is ID
        
        # Check if ID key exists, if not try ID_dp (compatibility)
        if not registro_id and 'ID_dp' in registro:
            registro_id = registro['ID_dp']

        query_count = """
        SELECT COUNT(*)
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?
        """
        cur.execute(query_count, (nit, factura, nombre_item, registro_id))
        count_existentes = cur.fetchone()[0]

        # Handle list vs string inputs (ZPAF expects strings with | usually, but we might pass scalars)
        # We wrap in list to loop
        lista_compra = val_orden_de_compra.split('|') if val_orden_de_compra else []
        lista_xml = valor_xml.split('|') if valor_xml else []
        lista_aprob = valor_aprobado.split('|') if valor_aprobado else []

        maximo_conteo = max(len(lista_compra), len(lista_xml), len(lista_aprob))
        maximo_conteo = 1 if maximo_conteo == 0 else maximo_conteo

        for i in range(maximo_conteo):
            item_compra = lista_compra[i] if i < len(lista_compra) else None
            item_xml = lista_xml[i] if i < len(lista_xml) else None
            item_aprob = lista_aprob[i] if i < len(lista_aprob) else None

            val_compra = safe_db_val(item_compra)
            val_xml = safe_db_val(item_xml)
            val_aprob = safe_db_val(item_aprob)

            if i < count_existentes:
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
                final_params = params + [nit, factura, nombre_item, registro_id, i + 1]
                cur.execute(update_query, final_params)
            else:
                # Need extra fields for INSERT.
                # ZPAF uses keys like 'Fecha_de_retoma_antes_de_contabilizacion_dp', 'documenttype_dp'.
                # In NC/ND from DocumentsProcessing, keys are different.
                # We map available data or pass defaults.

                fecha_retoma = registro.get('fecha_de_retoma', '')
                tipo_doc = registro.get('tipo_de_documento', '')
                orden_compra = '' # NC/ND usually don't have OC in this context
                nombre_prov = registro.get('nombre_emisor', '')

                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, Orden_de_Compra,
                    Nombre_Proveedor, ID_registro, NIT, Factura, Item,
                    Valor_Orden_de_Compra, Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(insert_query, (
                    fecha_retoma, tipo_doc, orden_compra, nombre_prov,
                    registro_id, nit, factura, nombre_item,
                    val_compra, val_xml, val_aprob
                ))
        
        cur.close()
        print(f"[PROCESADO] Item '{nombre_item}' - {count_nuevos if 'count_nuevos' in locals() else maximo_conteo} valor(es)")

    # =========================================================================
    # FUNCIONES DE BÚSQUEDA DE FACTURAS
    # =========================================================================
    
    def buscar_factura_correspondiente(cx, nit, referencia, fecha_ejecucion):
        """
        Busca la factura FV correspondiente a la NC.
        """
        try:
            if isinstance(fecha_ejecucion, str):
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        fecha_ejecucion = datetime.strptime(fecha_ejecucion, fmt)
                        break
                    except:
                        continue
            
            primer_dia_mes_actual = fecha_ejecucion.replace(day=1)
            primer_dia_mes_anterior = (primer_dia_mes_actual - timedelta(days=1)).replace(day=1)
            
            cur = cx.cursor()
            
            query = """
            SELECT TOP 1 *
            FROM [CxP].[DocumentsProcessing]
            WHERE [tipo_de_documento] = 'FV'
              AND [nit_emisor_o_nit_del_proveedor] = ?
              AND [numero_de_factura] = ?
              AND [fecha_de_emision_documento] >= ?
              AND ([NotaCreditoReferenciada] IS NULL OR [NotaCreditoReferenciada] = '')
            ORDER BY 
                CASE [ResultadoFinalAntesEventos] 
                    WHEN 'RECHAZADO' THEN 1 
                    WHEN 'APROBADO' THEN 2 
                    ELSE 3 
                END ASC
            """
            
            cur.execute(query, (nit, referencia, primer_dia_mes_anterior))
            
            row = cur.fetchone()
            
            if row:
                columns = [desc[0] for desc in cur.description]
                result = dict(zip(columns, row))
                cur.close()
                return result
            
            cur.close()
            return None
            
        except Exception as e:
            print(f"[ERROR] Error buscando factura: {str(e)}")
            return None
    
    # =========================================================================
    # FUNCIONES DE GENERACIÓN DE INSUMO DE RETORNO
    # =========================================================================
    
    def generar_insumo_retorno_nc(registros_novedad, ruta_insumo):
        """Genera el archivo Reporte_de_Retorno_Bot.xlsx para NC con novedad."""
        try:
            if not registros_novedad:
                return
            
            import openpyxl
            from openpyxl import Workbook
            
            os.makedirs(os.path.dirname(ruta_insumo), exist_ok=True)
            
            if os.path.exists(ruta_insumo):
                wb = openpyxl.load_workbook(ruta_insumo)
            else:
                wb = Workbook()
            
            if 'NC' in wb.sheetnames:
                ws = wb['NC']
            else:
                ws = wb.create_sheet('NC')
                ws.append(['ID', 'Fecha_Carga', 'Nit', 'Numero_Nota_Credito', 'Estado_CXP_Bot'])
            
            fecha_carga = datetime.now().strftime('%Y-%m-%d')
            
            for reg in registros_novedad:
                ws.append([
                    reg.get('ID', ''),
                    fecha_carga,
                    reg.get('nit_emisor_o_nit_del_proveedor', ''),
                    reg.get('numero_de_nota_credito', ''),
                    reg.get('estado', 'CON NOVEDAD')
                ])
            
            if 'Sheet' in wb.sheetnames:
                del wb['Sheet']
            
            wb.save(ruta_insumo)
            print(f"[INFO] Insumo de retorno generado: {ruta_insumo}")
            
        except Exception as e:
            print(f"[ERROR] Error generando insumo de retorno: {str(e)}")
    
    # =========================================================================
    # PROCESAMIENTO PRINCIPAL
    # =========================================================================
    
    try:
        print("")
        print("=" * 80)
        print("[INICIO] Procesamiento HU4.2 - VALIDACION NC y ND")
        print("=" * 80)
        
        t_inicio = time.time()
        
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[INFO] Configuracion cargada exitosamente")
        
        plazo_maximo_retoma = int(cfg.get('plazo_maximo_retoma_dias', 120))
        ruta_insumo_retorno = cfg.get('RutaInsumoRetorno', '')
        es_retorno_manual = cfg.get('EsRetornoManual', False)
        
        fecha_ejecucion = datetime.now()
        fecha_ejecucion_str = fecha_ejecucion.strftime('%Y-%m-%d')
        
        with crear_conexion_db(cfg) as cx:
            
            # =========================================================
            # PROCESAR NOTAS CRÉDITO (NC)
            # =========================================================
            
            print("\n" + "=" * 40)
            print("[PROCESO] Procesando Notas Crédito (NC)")
            print("=" * 40)
            
            query_nc = """
                SELECT * FROM [CxP].[DocumentsProcessing]
                WHERE [tipo_de_documento] = 'NC'
                  AND ([ResultadoFinalAntesEventos] IS NULL 
                       OR [ResultadoFinalAntesEventos] NOT IN ('Encontrado', 'No exitoso'))
                ORDER BY [executionDate] DESC
            """
            
            df_nc = pd.read_sql(query_nc, cx)
            print(f"[INFO] Obtenidas {len(df_nc)} notas credito para procesar")
            
            nc_procesadas = 0
            nc_con_novedad = 0
            registros_nc_novedad = []
            
            for idx, registro in df_nc.iterrows():
                try:
                    registro_id = safe_str(registro.get('ID', ''))
                    numero_nc = safe_str(registro.get('numero_de_nota_credito', ''))
                    nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor', ''))
                    
                    print(f"\n[NC] Procesando {nc_procesadas + 1}/{len(df_nc)}: NC {numero_nc}, NIT {nit}")
                    
                    # 1. Validar retoma
                    if not es_retorno_manual:
                        fecha_retoma = registro.get('fecha_de_retoma')
                        if campo_con_valor(fecha_retoma):
                            dias = calcular_dias_diferencia(fecha_retoma, fecha_ejecucion)
                            if dias > plazo_maximo_retoma:
                                print(f"[INFO] NC {numero_nc} excede plazo")
                                observacion = "Registro excede el plazo maximo de retoma"
                                actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4':'No exitoso', 'ObservacionesFase_4':observacion, 'ResultadoFinalAntesEventos':'No exitoso'})
                                
                                actualizar_items_comparativa(registro, cx, nit, numero_nc, 'Observaciones', valor_xml=observacion, val_orden_de_compra=None)
                                nc_procesadas += 1
                                continue
                        else:
                            cur = cx.cursor()
                            cur.execute("UPDATE [CxP].[DocumentsProcessing] SET [fecha_de_retoma] = ? WHERE [ID] = ?", (fecha_ejecucion_str, registro_id))
                            cur.close()
                    
                    # Validaciones
                    items_val = {}
                    
                    # Emisor
                    items_val['NombreEmisor'] = {'xml': safe_str(registro.get('nombre_emisor', '')), 'ok': 'SI' if campo_con_valor(registro.get('nombre_emisor', '')) else 'NO'}
                    items_val['NITEmisor'] = {'xml': safe_str(registro.get('nit_emisor_o_nit_del_proveedor', '')), 'ok': 'SI' if campo_con_valor(registro.get('nit_emisor_o_nit_del_proveedor', '')) else 'NO'}
                    items_val['FechaEmisionDocumento'] = {'xml': safe_str(registro.get('fecha_de_emision_documento', '')), 'ok': 'SI' if campo_con_valor(registro.get('fecha_de_emision_documento', '')) else 'NO'}
                    
                    # Receptor
                    items_val['NombreReceptor'] = {'xml': safe_str(registro.get('nombre_del_adquiriente', '')), 'ok': 'SI' if validar_nombre_receptor(registro.get('nombre_del_adquiriente', '')) else 'NO'}
                    items_val['NitReceptor'] = {'xml': safe_str(registro.get('nit_del_adquiriente', '')), 'ok': 'SI' if validar_nit_receptor(registro.get('nit_del_adquiriente', '')) else 'NO'}
                    items_val['TipoPersonaReceptor'] = {'xml': safe_str(registro.get('tipo_persona', '')), 'ok': 'SI' if validar_tipo_persona(registro.get('tipo_persona', '')) else 'NO'}
                    items_val['DigitoVerificacionReceptor'] = {'xml': safe_str(registro.get('digito_de_verificacion', '')), 'ok': 'SI' if validar_digito_verificacion(registro.get('digito_de_verificacion', '')) else 'NO'}
                    items_val['TaxLevelCodeReceptor'] = {'xml': safe_str(registro.get('responsabilidad_tributaria_adquiriente', '')), 'ok': 'SI' if validar_tax_level_code(registro.get('responsabilidad_tributaria_adquiriente', '')) else 'NO'}
                    
                    # Referencia
                    tipo_nc = safe_str(registro.get('tipo_de_nota_credito', ''))
                    items_val['InvoiceTypecode'] = {'xml': tipo_nc, 'ok': ''} # Mapped to InvoiceTypecode
                    
                    if tipo_nc != '20':
                        # Error
                        observacion = "Nota credito sin referencia"
                        actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4':'Exitoso', 'ObservacionesFase_4':observacion, 'ResultadoFinalAntesEventos':'Con Novedad'})
                        # Log items
                        for k, v in items_val.items():
                            actualizar_items_comparativa(registro, cx, nit, numero_nc, k, valor_xml=v['xml'], valor_aprobado=v['ok'], val_orden_de_compra=None)
                        actualizar_items_comparativa(registro, cx, nit, numero_nc, 'Observaciones', valor_xml=observacion, val_orden_de_compra=None)

                        registros_nc_novedad.append({'ID': registro_id, 'nit_emisor_o_nit_del_proveedor': nit, 'numero_de_nota_credito': numero_nc, 'estado': 'CON NOVEDAD'})
                        nc_procesadas += 1
                        nc_con_novedad += 1
                        continue

                    # Buscar FV
                    prefijo_numero = safe_str(registro.get('prefijo_y_numero', ''))
                    items_val['NotaCreditoReferenciada'] = {'xml': prefijo_numero, 'ok': ''}
                    
                    fv = buscar_factura_correspondiente(cx, nit, prefijo_numero, fecha_ejecucion)
                    
                    if not fv:
                        observacion = "Nota credito con referencia no encontrada"
                        actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4':'Exitoso', 'ObservacionesFase_4':observacion, 'ResultadoFinalAntesEventos':'Con novedad'})
                        items_val['NotaCreditoReferenciada']['ok'] = 'NO'
                        
                        for k, v in items_val.items():
                            actualizar_items_comparativa(registro, cx, nit, numero_nc, k, valor_xml=v['xml'], valor_aprobado=v['ok'], val_orden_de_compra=None)
                        actualizar_items_comparativa(registro, cx, nit, numero_nc, 'Observaciones', valor_xml=observacion, val_orden_de_compra=None)
                        
                        registros_nc_novedad.append({'ID': registro_id, 'nit_emisor_o_nit_del_proveedor': nit, 'numero_de_nota_credito': numero_nc, 'estado': 'CON NOVEDAD'})
                        nc_procesadas += 1
                        nc_con_novedad += 1
                        continue
                    
                    # Exitoso
                    items_val['NotaCreditoReferenciada']['ok'] = 'SI'
                    
                    valor_nc = normalizar_decimal(registro.get('valor_a_pagar_nc', 0))
                    valor_fv = normalizar_decimal(fv.get('valor_a_pagar', 0))
                    
                    actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4':'Exitoso', 'ResultadoFinalAntesEventos':'Encontrado'})
                    actualizar_nota_credito_referenciada_fv(cx, safe_str(fv.get('ID', '')), numero_nc)
                    
                    for k, v in items_val.items():
                        actualizar_items_comparativa(registro, cx, nit, numero_nc, k, valor_xml=v['xml'], valor_aprobado=v['ok'], val_orden_de_compra=None)
                    
                    actualizar_items_comparativa(registro, cx, nit, numero_nc, 'LineExtensionAmount', valor_xml=str(valor_nc), val_orden_de_compra=str(valor_fv), valor_aprobado='SI')
                    actualizar_items_comparativa(registro, cx, nit, numero_nc, 'ActualizacionNombreArchivos', valor_xml=safe_str(registro.get('actualizacion_nombre_archivos', '')), val_orden_de_compra=None)
                    actualizar_items_comparativa(registro, cx, nit, numero_nc, 'RutaRespaldo', valor_xml=safe_str(registro.get('ruta_respaldo', '')), val_orden_de_compra=None)
                    actualizar_items_comparativa(registro, cx, nit, numero_nc, 'Observaciones', valor_xml=safe_str(registro.get('ObservacionesFase_4', '')), val_orden_de_compra=None)
                    
                    nc_procesadas += 1
                    
                except Exception as e:
                    print(f"[ERROR] NC {idx}: {str(e)}")
                    nc_procesadas += 1
            
            if registros_nc_novedad and ruta_insumo_retorno:
                generar_insumo_retorno_nc(registros_nc_novedad, ruta_insumo_retorno)

            # =========================================================
            # PROCESAR ND
            # =========================================================
            print("\n" + "=" * 40)
            print("[PROCESO] Procesando Notas Debito (ND)")
            print("=" * 40)
            
            query_nd = """
                SELECT * FROM [CxP].[DocumentsProcessing]
                WHERE [tipo_de_documento] = 'ND'
                  AND ([ResultadoFinalAntesEventos] IS NULL OR [ResultadoFinalAntesEventos] NOT IN ('Exitoso'))
                ORDER BY [executionDate] DESC
            """
            df_nd = pd.read_sql(query_nd, cx)
            nd_procesadas = 0
            
            for idx, registro in df_nd.iterrows():
                try:
                    registro_id = safe_str(registro.get('ID', ''))
                    numero_nd = safe_str(registro.get('numero_de_nota_debito', ''))
                    nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor', ''))
                    
                    items_val = {}
                    items_val['NombreEmisor'] = {'xml': safe_str(registro.get('nombre_emisor', '')), 'ok': 'SI' if campo_con_valor(registro.get('nombre_emisor', '')) else 'NO'}
                    items_val['NITEmisor'] = {'xml': safe_str(registro.get('nit_emisor_o_nit_del_proveedor', '')), 'ok': 'SI' if campo_con_valor(registro.get('nit_emisor_o_nit_del_proveedor', '')) else 'NO'}
                    items_val['FechaEmisionDocumento'] = {'xml': safe_str(registro.get('fecha_de_emision_documento', '')), 'ok': 'SI' if campo_con_valor(registro.get('fecha_de_emision_documento', '')) else 'NO'}
                    items_val['NombreReceptor'] = {'xml': safe_str(registro.get('nombre_del_adquiriente', '')), 'ok': 'SI' if validar_nombre_receptor(registro.get('nombre_del_adquiriente', '')) else 'NO'}
                    items_val['NitReceptor'] = {'xml': safe_str(registro.get('nit_del_adquiriente', '')), 'ok': 'SI' if validar_nit_receptor(registro.get('nit_del_adquiriente', '')) else 'NO'}
                    items_val['TipoPersonaReceptor'] = {'xml': safe_str(registro.get('tipo_persona', '')), 'ok': 'SI' if validar_tipo_persona(registro.get('tipo_persona', '')) else 'NO'}
                    items_val['DigitoVerificacionReceptor'] = {'xml': safe_str(registro.get('digito_de_verificacion', '')), 'ok': 'SI' if validar_digito_verificacion(registro.get('digito_de_verificacion', '')) else 'NO'}
                    items_val['TaxLevelCodeReceptor'] = {'xml': safe_str(registro.get('responsabilidad_tributaria_adquiriente', '')), 'ok': 'SI' if validar_tax_level_code(registro.get('responsabilidad_tributaria_adquiriente', '')) else 'NO'}
                    
                    actualizar_bd_cxp(cx, registro_id, {'EstadoFinalFase_4':'Exitoso', 'ResultadoFinalAntesEventos':'Exitoso'})
                    
                    for k, v in items_val.items():
                        actualizar_items_comparativa(registro, cx, nit, numero_nd, k, valor_xml=v['xml'], valor_aprobado=v['ok'], val_orden_de_compra=None)
                    
                    actualizar_items_comparativa(registro, cx, nit, numero_nd, 'LineExtensionAmount', valor_xml=safe_str(registro.get('valor_a_pagar', '')), val_orden_de_compra=None)
                    actualizar_items_comparativa(registro, cx, nit, numero_nd, 'InvoiceTypecode', valor_xml=safe_str(registro.get('tipo_de_nota_debito', '')), val_orden_de_compra=None) # Mapped
                    actualizar_items_comparativa(registro, cx, nit, numero_nd, 'NotaCreditoReferenciada', valor_xml=safe_str(registro.get('prefijo_y_numero', '')), val_orden_de_compra=None) # Mapped Reference
                    actualizar_items_comparativa(registro, cx, nit, numero_nd, 'CufeUUID', valor_xml=safe_str(registro.get('codigo_cufe_de_la_factura', '')), val_orden_de_compra=None) # Mapped
                    actualizar_items_comparativa(registro, cx, nit, numero_nd, 'ActualizacionNombreArchivos', valor_xml=safe_str(registro.get('actualizacion_nombre_archivos', '')), val_orden_de_compra=None)
                    actualizar_items_comparativa(registro, cx, nit, numero_nd, 'RutaRespaldo', valor_xml=safe_str(registro.get('ruta_respaldo', '')), val_orden_de_compra=None)
                    actualizar_items_comparativa(registro, cx, nit, numero_nd, 'Observaciones', valor_xml=safe_str(registro.get('ObservacionesFase_4', '')), val_orden_de_compra=None)
                    
                    nd_procesadas += 1
                    
                except Exception as e:
                    print(f"[ERROR] ND {idx}: {str(e)}")
                    nd_procesadas += 1
        
        #SetVar("vLocStrResultadoSP", "True")
        #SetVar("vLocStrResumenSP", f"NC: {nc_procesadas}, ND: {nd_procesadas}")
        
    except Exception as e:
        print(f"[ERROR CRITICO] {str(e)}")
        print(traceback.format_exc())
        #SetVar("vGblStrDetalleError", traceback.format_exc())
        #SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        #SetVar("vLocStrResultadoSP", "False")


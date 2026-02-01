def HU42_ValidarNotasCreditoDebito():
    """
    Funcion principal para orquestar la validacion de Notas Credito (NC) y Notas Debito (ND) en el flujo HU4.2.
    
    Esta funcion implementa un proceso secuencial de validacion que incluye:
    1.  **Limpieza de Tablas**: Prepara las tablas de comparativa (`[CxP].[Comparativa_NC]` y `[CxP].[Comparativa_ND]`) eliminando datos previos.
    2.  **Poblado Inicial (Snapshot)**: Carga masivamente los registros pendientes de validacion en las tablas de comparativa para iniciar con un estado base.
    3.  **Procesamiento de Notas Credito (NC)**:
        - Verifica reglas de tiempo (plazo maximo de retoma).
        - Valida datos tributarios del emisor y receptor (NIT, Tipo Persona, TaxLevelCode).
        - Verifica la existencia y consistencia de la Factura de Venta (FV) referenciada.
        - Valida montos (coincidencia con tolerancia).
    4.  **Procesamiento de Notas Debito (ND)**:
        - Realiza validaciones tributarias similares a las NC.
    5.  **Generacion de Reportes**: Crea archivos Excel de retorno con las novedades encontradas para su gestion manual.

    Variables de entrada (RocketBot):
        - vLocDicConfig (str | dict): Configuracion JSON.
            - ServidorBaseDatos
            - NombreBaseDatos
            - UsuarioBaseDatos
            - ClaveBaseDatos
            - PlazoMaximoRetoma
            - RutaBaseReporteNC
            - NombreReporteNC

    Variables de salida (RocketBot):
        - vLocStrResultadoSP (str): "True" / "False".
        - vLocStrResumenSP (str): Resumen estadistico.
        - vGblStrDetalleError (str): Detalle tecnico en caso de fallo.
    
    Author:
        Diego Ivan Lopez Ochoa
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
    from dateutil.relativedelta import relativedelta
    import random
    from openpyxl import load_workbook, Workbook 

    # Ignoramos advertencias de pandas sobre SQLAlchemy para mantener la consola limpia
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

    # =========================================================================
    # FUNCIONES AUXILIARES BASICAS
    # =========================================================================

    def safe_str(v):
        """Convierte de manera segura cualquier entrada a un string limpio."""
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
        """Trunca un texto de observacion para evitar errores de desbordamiento en BD."""
        if not obs:
            return ""
        obs_str = safe_str(obs)
        if len(obs_str) > max_len:
            return obs_str[:max_len - 3] + "..."
        return obs_str
    
    def parse_config(raw):
        """Analiza la configuracion de entrada (JSON o Dict string)."""
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
        """Normaliza un valor numerico o string monetario a float estandar."""
        if pd.isna(valor) or valor == '' or valor is None: return 0.0
        try: return float(str(valor).strip().replace(',', '.').replace(r'[^\d.\-]', ''))
        except: return 0.0

    def campo_vacio(valor):
        """Verifica si un campo se considera 'vacio'."""
        return safe_str(valor) == "" or safe_str(valor).lower() in ('null', 'none', 'nan')
    
    def campo_con_valor(valor):
        """Inverso de campo_vacio."""
        return not campo_vacio(valor)

    def quitar_tildes(texto):
        """Elimina acentos y diacriticos de un texto."""
        if not texto: return ""
        return ''.join([c for c in unicodedata.normalize('NFKD', texto) if not unicodedata.combining(c)])

    def calcular_dias_diferencia(fecha_inicio, fecha_fin):
        """Calcula los dias transcurridos entre dos fechas soportando multiples formatos."""
        try:
            formatos = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']
            def convertir_a_datetime(fecha):
                if isinstance(fecha, datetime): return fecha
                if isinstance(fecha, str):
                    if '.' in fecha: fecha = fecha.split('.')[0]
                    for fmt in formatos:
                        try: return datetime.strptime(fecha, fmt)
                        except ValueError: continue
                return None

            dt_inicio = convertir_a_datetime(fecha_inicio)
            dt_fin = convertir_a_datetime(fecha_fin)
            if dt_inicio and dt_fin:
                return (dt_fin - dt_inicio).days
            return 0
        except Exception:
            return 0

    # =========================================================================
    # FUNCIONES DE CONEXION A BASE DE DATOS
    # =========================================================================

    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        """Context Manager para establecer conexion segura a SQL Server."""
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

        print("[INFO] Iniciando protocolo de conexion a Base de Datos...")
        for attempt in range(max_retries):
            try:
                cx = pyodbc.connect(conn_str_auth, timeout=30)
                cx.autocommit = False
                conectado = True
                print(f"[INFO] Conexion SQL (Auth) establecida exitosamente (intento {attempt + 1})")
                break
            except pyodbc.Error as e:
                excepcion_final = e
                if attempt < max_retries - 1: time.sleep(1)

        if not conectado:
            print("[WARNING] Fallo Auth SQL. Intentando Trusted Connection...")
            for attempt in range(max_retries):
                try:
                    cx = pyodbc.connect(conn_str_trusted, timeout=30)
                    cx.autocommit = False
                    conectado = True
                    print(f"[INFO] Conexion SQL (Trusted) establecida exitosamente (intento {attempt + 1})")
                    break
                except pyodbc.Error as e:
                    excepcion_final = e
                    if attempt < max_retries - 1: time.sleep(1)

        if not conectado:
            print(f"[ERROR] No se pudo conectar a la BD tras varios intentos.")
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
    # FUNCIONES DE ACTUALIZACION DE BASE DE DATOS
    # =========================================================================

    def actualizar_bd_cxp(cx, registro_id, campos_actualizar):
        """Actualiza campos especificos en la tabla principal [CxP].[DocumentsProcessing]."""
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
                cx.commit()
                cur.close()
        except Exception as e:
            print(f"[ERROR] Fallo al actualizar DocumentsProcessing ID {registro_id}: {str(e)}")
            raise

    def actualizar_items_comparativa_nc(registro_id, cx, nit, nombre_item,
                                    actualizar_valor_xml=True, valor_xml=None,
                                    actualizar_aprobado=True, valor_aprobado=None, 
                                    actualizar_valor_factura=True, valor_factura=None, 
                                    actualizar_estado=True, valor_estado=None, 
                                    id_ejecucion=None, fecha_retoma=None, nombre_proveedor=None):
        """Actualiza o inserta items en la tabla [CxP].[Comparativa_NC]."""
        try:
            cur = cx.cursor()
            def safe_db_val(v):
                if v is None: return None
                s = str(v).strip()
                if not s or s.lower() == 'none' or s.lower() == 'null': return None
                return s

            query_count = "SELECT COUNT(*) FROM [CxP].[Comparativa_NC] WHERE NIT = ? AND Item = ? AND ID_Registro = ?"
            cur.execute(query_count, (nit, nombre_item, registro_id))
            count_existentes = cur.fetchone()[0]

            lista_factura = str(valor_factura).split('|') if valor_factura else []
            lista_xml = str(valor_xml).split('|') if valor_xml else []
            lista_aprob = str(valor_aprobado).split('|') if valor_aprobado else []
            lista_estado = str(valor_estado).split('|') if valor_estado else []

            maximo_conteo = max(len(lista_factura), len(lista_xml), len(lista_aprob), len(lista_estado))
            maximo_conteo = 1 if maximo_conteo == 0 else maximo_conteo

            for i in range(maximo_conteo):
                val_factura = safe_db_val(lista_factura[i] if i < len(lista_factura) else None)
                val_xml = safe_db_val(lista_xml[i] if i < len(lista_xml) else None)
                val_aprob = safe_db_val(lista_aprob[i] if i < len(lista_aprob) else None)
                val_estado = safe_db_val(lista_estado[i] if i < len(lista_estado) else None)

                if i < count_existentes:
                    set_clauses, params = [], []
                    if actualizar_valor_factura: set_clauses.append("Valor_Factura = ?"); params.append(val_factura)
                    if actualizar_valor_xml: set_clauses.append("Valor_XML = ?"); params.append(val_xml)
                    if actualizar_aprobado: set_clauses.append("Aprobado = ?"); params.append(val_aprob)
                    if actualizar_estado: set_clauses.append("Estado = ?"); params.append(val_estado)
                    if not set_clauses: continue

                    update_query = f"""
                    ;WITH CTE AS (
                        SELECT Valor_Factura, Valor_XML, Aprobado, Estado, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn
                        FROM [CxP].[Comparativa_NC] WHERE NIT = ?  AND Item = ? AND ID_Registro = ?
                    ) UPDATE CTE SET {", ".join(set_clauses)} WHERE rn = ?
                    """
                    final_params = params + [nit, nombre_item, registro_id, i + 1]
                    cur.execute(update_query, final_params)
                else:
                    insert_query = """
                    INSERT INTO [CxP].[Comparativa_NC] (
                        Fecha_de_ejecucion, Fecha_de_retoma, ID_ejecucion, ID_Registro, 
                        NIT, Nombre_Proveedor, Item, Valor_Factura, Valor_XML, Aprobado, Estado
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    cur.execute(insert_query, (datetime.now(), fecha_retoma, id_ejecucion, registro_id, nit, nombre_proveedor, nombre_item, val_factura, val_xml, val_aprob, val_estado or 'PENDIENTE'))
            cx.commit()
        except Exception as e:
            print(f"[ERROR] Fallo en actualizar_items_comparativa_nc para ID {registro_id}: {e}")
            cx.rollback()
            raise e
   
    def actualizar_items_comparativa_nd(registro_id, cx, nit, nombre_item,
                                        actualizar_valor_xml=True, valor_xml=None,
                                        actualizar_aprobado=True, valor_aprobado=None, 
                                        actualizar_estado=True, valor_estado=None,
                                        id_ejecucion=None, nombre_proveedor=None):
        """Actualiza o inserta items en la tabla [CxP].[Comparativa_ND]."""
        try:
            cur = cx.cursor()
            def safe_db_val(v):
                if v is None: return None
                s = str(v).strip()
                if not s or s.lower() == 'none' or s.lower() == 'null': return None
                return s

            query_count = "SELECT COUNT(*) FROM [CxP].[Comparativa_ND] WHERE NIT = ? AND Item = ? AND ID_Registro = ?"
            cur.execute(query_count, (nit, nombre_item, registro_id))
            count_existentes = cur.fetchone()[0]

            lista_xml = str(valor_xml).split('|') if valor_xml else []
            lista_aprob = str(valor_aprobado).split('|') if valor_aprobado else []
            lista_estado = str(valor_estado).split('|') if valor_estado else []

            maximo_conteo = max(len(lista_xml), len(lista_aprob), len(lista_estado))
            maximo_conteo = 1 if maximo_conteo == 0 else maximo_conteo

            for i in range(maximo_conteo):
                val_xml = safe_db_val(lista_xml[i] if i < len(lista_xml) else None)
                val_aprob = safe_db_val(lista_aprob[i] if i < len(lista_aprob) else None)
                val_estado = safe_db_val(lista_estado[i] if i < len(lista_estado) else None)

                if i < count_existentes:
                    set_clauses, params = [], []
                    if actualizar_valor_xml: set_clauses.append("Valor_XML = ?"); params.append(val_xml)
                    if actualizar_aprobado: set_clauses.append("Aprobado = ?"); params.append(val_aprob)
                    if actualizar_estado: set_clauses.append("Estado = ?"); params.append(val_estado)
                    if not set_clauses: continue

                    update_query = f"""
                    ;WITH CTE AS (
                        SELECT Valor_XML, Aprobado, Estado, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn
                        FROM [CxP].[Comparativa_ND] WHERE NIT = ? AND Item = ? AND ID_Registro = ?
                    ) UPDATE CTE SET {", ".join(set_clauses)} WHERE rn = ?
                    """
                    final_params = params + [nit, nombre_item, registro_id, i + 1]
                    cur.execute(update_query, final_params)
                else:
                    insert_query = """
                    INSERT INTO [CxP].[Comparativa_ND] (
                        Fecha_de_ejecucion, ID_ejecucion, ID_Registro, 
                        NIT, Nombre_Proveedor, Item, Valor_XML, Aprobado, Estado
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    cur.execute(insert_query, (datetime.now(), id_ejecucion, registro_id, nit, nombre_proveedor, nombre_item, val_xml, val_aprob, val_estado or 'PENDIENTE'))
            cx.commit()
        except Exception as e:
            print(f"[ERROR] Fallo en actualizar_items_comparativa_nd para ID {registro_id}: {e}")
            cx.rollback()
            raise e
    
    def actualizar_estado_comparativa_nc(cx, nit, registro_id, estado):
        """Actualiza la columna 'Estado' masivamente para todos los items de una NC especifica."""
        try:
            cur = cx.cursor()
            update_sql = "UPDATE [CxP].[Comparativa_NC] SET Estado = ? WHERE NIT = ? AND [ID_Registro] = ?"
            cur.execute(update_sql, (estado, nit, registro_id))
            cx.commit()
            cur.close()
        except Exception as e:
            print(f"[ERROR] Error actualizando estado global en Comparativa_NC: {e}")
            cx.rollback()
            raise e
    
    def limpiar_tablas_comparativas(cx):
        """Ejecuta TRUNCATE TABLE sobre las tablas comparativas."""
        try:
            cursor = cx.cursor()
            print("[INFO] Limpiando tablas comparativas...")
            cursor.execute("TRUNCATE TABLE [CxP].[Comparativa_NC]")
            cursor.execute("TRUNCATE TABLE [CxP].[Comparativa_ND]")
            cx.commit()
            print("[SUCCESS] Tablas comparativas limpiadas exitosamente.")
        except Exception as e:
            print(f"[ERROR] Fallo critico al limpiar tablas: {e}")
            cx.rollback()
            raise e
    
    def poblar_inicial_comparativa_nc(cx, df):
        """Realiza una insercion masiva (Bulk Insert) inicial en Comparativa_NC."""
        try:
            items_a_validar = [
                "Nombre Emisor", "NIT Emisor", "Nombre Receptor", "Nit Receptor",
                "Tipo Persona Receptor", "DigitoVerificacion Receptor", "TaxLevelCode Receptor",
                "Fecha emision del documento", "LineExtensionAmount", "Tipo de nota credito",
                "Referencia", "Codigo CUFE de la factura", "Cude de la Nota Credito",
                "ActualizacionNombreArchivos", "RutaRespaldo", "Observaciones"
            ]
            fecha_actual = datetime.now()
            datos_para_insertar = []

            for row in df.itertuples(index=False):
                id_registro = getattr(row, 'ID', None) 
                id_ejecucion = getattr(row, 'executionNum', None) 
                nit = getattr(row, 'nit_emisor_o_nit_del_proveedor', None)
                nombre_proveedor = getattr(row, 'nombre_emisor', None) 
                nota_credito = getattr(row, 'Numero_de_nota_credito', None)
                fecha_retoma = getattr(row, 'Fecha_retoma_contabilizacion', None)
                estado = getattr(row, 'ResultadoFinalAntesEventos', None)

                for item in items_a_validar:
                    tupla_sql = (
                        fecha_actual, fecha_retoma, str(id_ejecucion), str(id_registro),
                        str(nit), str(nombre_proveedor), str(nota_credito), item,
                        '', '', '', estado
                    )
                    datos_para_insertar.append(tupla_sql)

            if datos_para_insertar:
                cursor = cx.cursor()
                sql_insert = """
                INSERT INTO [CxP].[Comparativa_NC] (
                    [Fecha_de_ejecucion], [Fecha_de_retoma], [ID_ejecucion], [ID_Registro], 
                    [NIT], [Nombre_Proveedor], [Nota_Credito], [Item], 
                    [Valor_XML], [Valor_Factura], [Aprobado], [Estado]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                print(f"[INFO] Insertando {len(datos_para_insertar)} registros masivos en Comparativa_NC...")
                cursor.executemany(sql_insert, datos_para_insertar)
                cx.commit()
                print("[SUCCESS] Poblado inicial NC completado con exito.")
            else:
                print("[WARNING] El DataFrame de NC estaba vacio, no se insertaron registros.")
        except Exception as e:
            print(f"[ERROR] Fallo al poblar tabla inicial NC: {e}")
            cx.rollback()
            raise e

    def poblar_inicial_comparativa_nd(cx, df):
        """Realiza una insercion masiva (Bulk Insert) inicial en Comparativa_ND."""
        try:
            items_a_validar = [
                "Nombre Emisor", "NIT Emisor", "Nombre Receptor", "Nit Receptor",
                "Tipo Persona Receptor", "DigitoVerificacion Receptor", "TaxLevelCode Receptor",
                "Fecha emision del documento", "LineExtensionAmount", "Tipo de nota debito",
                "Referencia", "Codigo CUFE de la factura", "Cude de la Nota Debito",
                "ActualizacionNombreArchivos", "RutaRespaldo", "Observaciones"
            ]
            fecha_actual = datetime.now()
            datos_para_insertar = []

            for row in df.itertuples(index=False):
                executionNum = getattr(row, 'executionNum', None)
                id_registro = getattr(row, 'ID', None)
                nit = getattr(row, 'nit_emisor_o_nit_del_proveedor', None)
                nombre_proveedor = getattr(row, 'nombre_emisor', None) 
                nota_debito = getattr(row, 'numero_de_nota_debito', None) or getattr(row, 'Numero_de_nota_debito', None)

                for item in items_a_validar:
                    tupla_sql = (
                        fecha_actual, str(executionNum), str(id_registro), str(nit),
                        str(nombre_proveedor), str(nota_debito), item, '', '', 'PENDIENTE'
                    )
                    datos_para_insertar.append(tupla_sql)

            if datos_para_insertar:
                cursor = cx.cursor()
                sql_insert = """
                INSERT INTO [CxP].[Comparativa_ND] (
                    [Fecha_de_ejecucion], [ID_ejecucion], [ID_Registro], 
                    [NIT], [Nombre_Proveedor], [Nota_Debito], [Item], 
                    [Valor_XML], [Aprobado], [Estado]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                print(f"[INFO] Insertando {len(datos_para_insertar)} registros masivos en Comparativa_ND...")
                cursor.executemany(sql_insert, datos_para_insertar)
                cx.commit()
                print("[SUCCESS] Poblado inicial de ND completado con exito.")
            else:
                print("[WARNING] El DataFrame de ND estaba vacio, no se insertaron registros.")
        except Exception as e:
            print(f"[ERROR] Fallo al poblar tabla inicial ND: {e}")
            cx.rollback()
            raise e
    
    # =========================================================================
    # VALIDACIONES DE NEGOCIO HU4.2
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
   
    def generar_reporte_retorno_nc(cx, ruta_base, nombre_reporte):
        """Genera o actualiza un archivo Excel con el reporte de registros 'CON NOVEDAD'."""
        try:
            fecha_actual = datetime.now()
            anio = fecha_actual.year
            fecha_solo = fecha_actual.date()
            meses = {1: "01. Enero", 2: "02. Febrero", 3: "03. Marzo", 4: "04. Abril", 5: "05. Mayo", 6: "06. Junio", 7: "07. Julio", 8: "08. Agosto", 9: "09. Septiembre", 10: "10. Octubre", 11: "11. Noviembre", 12: "12. Diciembre"}
            nombre_mes = meses[fecha_actual.month]
            ruta_carpeta = os.path.join(ruta_base, str(anio), nombre_mes, "INSUMO DE RETORNO")
            
            if not os.path.exists(ruta_carpeta):
                os.makedirs(ruta_carpeta)

            ruta_completa = os.path.join(ruta_carpeta, f"{nombre_reporte}_{fecha_solo}.xlsx")

            query_novedades = """
            SELECT executionNum as ID, nit_emisor_o_nit_del_proveedor as Nit, numero_de_factura as Numero_Nota_Credito, ResultadoFinalAntesEventos as Estado_CXP_Bot
            FROM [CxP].[DocumentsProcessing] WHERE documenttype = 'NC' AND ResultadoFinalAntesEventos = 'CON NOVEDAD'
            """
            
            df_novedades = pd.read_sql(query_novedades, cx)

            if not df_novedades.empty:
                wb = None
                ws = None
                encabezados = ["ID", "Fecha_Carga", "Nit", "Numero_Nota_Credito", "Estado_CXP_Bot"]

                if not os.path.exists(ruta_completa):
                    wb = Workbook()
                    ws = wb.active
                    ws.title = 'NC'
                    ws.append(encabezados) 
                else:
                    wb = load_workbook(ruta_completa)
                    if 'NC' not in wb.sheetnames:
                        ws = wb.create_sheet('NC')
                        ws.append(encabezados)
                    else:
                        ws = wb['NC']

                fecha_carga = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')
                
                for index, row in df_novedades.iterrows():
                    fila_excel = [row['ID'], fecha_carga, row['Nit'], row['Numero_Nota_Credito'], row['Estado_CXP_Bot']]
                    ws.append(fila_excel)

                wb.save(ruta_completa)
                wb.close()
                print("[SUCCESS] Reporte guardado exitosamente.")
            else:
                print("[INFO] No hay novedades para reportar en este ciclo.")
        except Exception as e:
            print(f"[ERROR] Error generando reporte de retorno: {e}")
            raise e
        
    # =========================================================================
    # LOGICA PRINCIPAL DE ORQUESTACION
    # =========================================================================

    try:
        print("=" * 80 + "\n[INFO] INICIO HU4.2 - Validacion NC/ND\n" + "=" * 80)

        cfg = parse_config(GetVar("vLocDicConfig"))
        plazo_max = int(cfg.get('PlazoMaximoRetoma', 120))
        now = datetime.now()
        
        with crear_conexion_db(cfg) as cx:
            # -----------------------------------------------------------------
            # FASE 1: PROCESAMIENTO DE NOTAS CREDITO (NC)
            # -----------------------------------------------------------------
            df_nc = pd.read_sql("SELECT * FROM [CxP].[DocumentsProcessing] WHERE [tipo_de_documento]='NC' AND ([ResultadoFinalAntesEventos] IS NULL OR [ResultadoFinalAntesEventos] NOT IN ('ENCONTRADO', 'NO EXITOSO'))", cx)
            print(f"[INFO] Procesando {len(df_nc)} Notas Credito (NC)...")
            
            try:
                limpiar_tablas_comparativas(cx)
                poblar_inicial_comparativa_nc(cx, df_nc)
            except Exception as e:
                error_msg = traceback.format_exc() 
                SetVar("vGblStrDetalleError", error_msg)
                SetVar("vGblStrSystemError", "ErrorHU4_4.1")
                SetVar("vLocStrResultadoSP", "False")
                raise e
            
            try:
                fecha_ejecucion = datetime.now()
                fecha_inicio_filtro = (fecha_ejecucion.replace(day=1) - relativedelta(months=1)).strftime('%Y-%m-%d')
                fecha_fin_filtro = fecha_ejecucion.strftime('%Y-%m-%d')
                query_fv = f"SELECT * FROM [CxP].[DocumentsProcessing] WHERE documenttype = 'FV' AND fecha_de_emision_documento >= '{fecha_inicio_filtro}' AND fecha_de_emision_documento <= '{fecha_fin_filtro}' AND (NotaCreditoReferenciada IS NULL OR NotaCreditoReferenciada = '')"
                df_fv = pd.read_sql(query_fv, cx)
                condicion = df_fv['ResultadoFinalAntesEventos'].str.contains('EXITOSO|RECHAZADO', case=False, na=False, regex=True)
                df_fv_final = df_fv[condicion].copy()
            except Exception as e:
                error_msg = traceback.format_exc() 
                SetVar("vGblStrDetalleError", error_msg)
                SetVar("vGblStrSystemError", "ErrorHU4_4.1")
                SetVar("vLocStrResultadoSP", "False")
                raise e
                
            cnt_nc = 0
            
            for idx, r in df_nc.iterrows():
                retorno_manual = True if 'EXITOSO' in str(r.get('EstadoFase_3').upper()) else False
                reg_id = safe_str(r.get('ID', ''))
                nit = safe_str(r.get('nit_emisor_o_nit_del_proveedor', ''))
                num_nc = safe_str(r.get('numero_de_nota_credito', ''))
                obs_anterior = safe_str(r.get('ObservacionesFase_4')) 
                
                print(f"[INFO] Analizando NC: {num_nc} (ID: {reg_id})")

                if not retorno_manual:
                    f_ret = r.get('Fecha_retoma_contabilizacion')
                    if campo_con_valor(f_ret):
                        dias = calcular_dias_diferencia(f_ret, now)
                        if dias > plazo_max:
                            obs = f"Registro excede el plazo maximo de retoma, {obs_anterior}"
                            actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: No exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': 'NO EXITOSO'})
                            actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='Observaciones', valor_xml=truncar_observacion(obs))
                            actualizar_estado_comparativa_nc(cx, nit, reg_id, 'NO EXITOSO')
                            cnt_nc += 1
                            continue
                    else:
                        cx.cursor().execute("UPDATE [CxP].[DocumentsProcessing] SET [Fecha_de_retoma_antes_de_contabilizacion]=? WHERE [ID]=?", (now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], reg_id))

                validaciones = {
                    'Nombre Emisor': {'val': r.get('nombre_emisor'), 'check': campo_con_valor},
                    'NIT Emisor': {'val': r.get('nit_emisor_o_nit_del_proveedor'), 'check': campo_con_valor},
                    'Fecha emision del documento': {'val': r.get('fecha_de_emision_documento'), 'check': campo_con_valor},
                    'Nombre Receptor': {'val': r.get('nombre_del_adquiriente'), 'check': validar_nombre_receptor},
                    'Nit Receptor': {'val': r.get('nit_del_adquiriente'), 'check': validar_nit_receptor},
                    'Tipo Persona Receptor': {'val': r.get('tipo_persona'), 'check': validar_tipo_persona},
                    'DigitoVerificacion Receptor': {'val': r.get('digito_de_verificacion'), 'check': validar_digito_verificacion},
                    'TaxLevelCode Receptor': {'val': r.get('responsabilidad_tributaria_adquiriente'), 'check': validar_tax_level_code}
                }
                
                for k, v in validaciones.items():
                    actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item=k, valor_xml=safe_str(v['val']), valor_aprobado='SI' if v['check'](v['val']) else 'NO')
                
                tiene_estado_final = True if safe_str(r.get('ResultadoFinalAntesEventos')) else False
                
                if not tiene_estado_final:
                    tipo_nc = safe_str(r.get('tipo_de_nota_credito', ''))
                    
                    if tipo_nc == '20':
                        actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='Tipo de nota credito', valor_xml=r.get('Tipo_de_nota_cred_deb'), valor_aprobado='SI' if campo_con_valor(r.get('Tipo_de_nota_cred_deb')) else 'NO')
                        actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='Codigo CUFE de la factura', valor_xml=r.get('cufeuuid', ''), valor_aprobado='SI' if campo_con_valor(r.get('cufeuuid')) else 'NO')
                        actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='Cude de la Nota Credito', valor_xml=r.get('cufe_fe', ''), valor_aprobado='SI' if campo_con_valor(r.get('cufe_fe')) else 'NO')
                    else:
                        obs = f"Nota credito sin referencia, {obs_anterior}"
                        actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': 'CON NOVEDAD'})
                        actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='Tipo de nota credito', valor_xml=r.get('Tipo_de_nota_cred_deb'), valor_aprobado='NO')
                        actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='Observaciones', valor_xml=truncar_observacion(obs), valor_aprobado='NO')
                        actualizar_estado_comparativa_nc(cx, nit, reg_id, 'CON NOVEDAD')
                        cnt_nc += 1
                        continue
                        
                condicion = (df_fv_final['numero_de_factura'] == r.get('PrefijoYNumero')) & (df_fv_final['nit_emisor_o_nit_del_proveedor'] == nit)
                resultado = df_fv_final[condicion]
                
                if not resultado.empty:
                    print(f"[SUCCESS] Encontrada FV para NC {r.get('numero_de_factura')}")
                    actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='Referencia', valor_xml=r.get('PrefijoYNumero'), valor_aprobado='SI', valor_factura=r.get('numero_de_factura'))
                    fv_encontrada = resultado.iloc[0]
                    v_nc = normalizar_decimal(r.get('valor_a_pagar_nc'))
                    v_fv = normalizar_decimal(fv_encontrada['valor_a_pagar'])
                    
                    if abs(v_nc - v_fv) < 0.01:
                        actualizar_bd_cxp(cx, fv_encontrada['ID'], {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ResultadoFinalAntesEventos': 'ENCONTRADO','NotaCreditoReferenciada':f"{r.get('Numero_de_nota_credito')}"})
                        actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='LineExtensionAmount', valor_xml=r.get('valor_a_pagar_nc'), valor_aprobado='SI', valor_factura=r.get('valor_a_pagar'))
                        actualizar_estado_comparativa_nc(cx, nit, reg_id, 'ENCONTRADO')
                else:
                    obs = f"Nota credito con referencia no encontrada, {obs_anterior}"
                    actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': 'CON NOVEDAD'})
                    actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='Referencia', valor_xml=r.get('PrefijoYNumero'), valor_aprobado='NO', valor_factura=r.get('numero_de_factura'))
                    actualizar_items_comparativa_nc(reg_id, cx, nit, nombre_item='Observaciones', valor_xml=obs, valor_aprobado=None)
                    actualizar_estado_comparativa_nc(cx, nit, reg_id, 'CON NOVEDAD')
                    cnt_nc += 1
                    continue
                
            try:
                generar_reporte_retorno_nc(cx, cfg['RutaBaseReporteNC'], cfg['NombreReporteNC'])
            except Exception as e:
                error_msg = traceback.format_exc() 
                SetVar("vGblStrDetalleError", error_msg)
                SetVar("vGblStrSystemError", "ErrorHU4_4.1")
                SetVar("vLocStrResultadoSP", "False")
                print(f"[ERROR] Critico generando reporte: {error_msg}")
            
            # -----------------------------------------------------------------
            # FASE 2: PROCESAMIENTO DE NOTAS DEBITO (ND)
            # -----------------------------------------------------------------
            df_nd = pd.read_sql("SELECT * FROM [CxP].[DocumentsProcessing] WHERE [tipo_de_documento]='ND' AND ([ResultadoFinalAntesEventos] IS NULL OR [ResultadoFinalAntesEventos] NOT IN ('Exitoso')) ORDER BY [executionDate] DESC", cx)
            print(f"[INFO] Procesando {len(df_nd)} Notas Debito (ND)...")
            
            try:
                poblar_inicial_comparativa_nd(cx, df_nd)
            except Exception as e:
                error_msg = traceback.format_exc() 
                SetVar("vGblStrDetalleError", error_msg)
                SetVar("vGblStrSystemError", "ErrorHU4_4.1")
                SetVar("vLocStrResultadoSP", "False")
                raise e
            
            cnt_nd = 0
            for idx, r in df_nd.iterrows():
                try:
                    reg_id = safe_str(r.get('ID', ''))
                    nit = safe_str(r.get('nit_emisor_o_nit_del_proveedor', ''))
                    num_nd = safe_str(r.get('numero_de_nota_debito', ''))
                    print(f"[INFO] Analizando ND: {num_nd} (ID: {reg_id})")

                    validaciones = {
                        'Nombre Emisor': {'val': r.get('nombre_emisor'), 'check': campo_con_valor},
                        'NIT Emisor': {'val': r.get('nit_emisor_o_nit_del_proveedor'), 'check': campo_con_valor},
                        'Fecha emision del documento': {'val': r.get('fecha_de_emision_documento'), 'check': campo_con_valor},
                        'Nombre Receptor': {'val': r.get('nombre_del_adquiriente'), 'check': validar_nombre_receptor},
                        'Nit Receptor': {'val': r.get('nit_del_adquiriente'), 'check': validar_nit_receptor},
                        'Tipo Persona Receptor': {'val': r.get('tipo_persona'), 'check': validar_tipo_persona},
                        'DigitoVerificacion Receptor': {'val': r.get('digito_de_verificacion'), 'check': validar_digito_verificacion},
                        'TaxLevelCode Receptor': {'val': r.get('responsabilidad_tributaria_adquiriente'), 'check': validar_tax_level_code}
                    }

                    for k, v in validaciones.items():
                        actualizar_items_comparativa_nd(reg_id, cx, nit, nombre_item=k, valor_xml=safe_str(v['val']), valor_aprobado='SI' if v['check'](v['val']) else 'NO')

                    actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 'ResultadoFinalAntesEventos': 'EXITOSO'})
                    actualizar_items_comparativa_nd(reg_id, cx, nit, nombre_item='LineExtensionAmount', valor_xml=safe_str(r.get('valor_a_pagar')))
                    actualizar_items_comparativa_nd(reg_id, cx, nit, nombre_item='Tipo de nota debito', valor_xml=safe_str(r.get('Tipo_de_nota_cred_deb')))
                    actualizar_items_comparativa_nd(reg_id, cx, nit, nombre_item='Referencia', valor_xml=safe_str(r.get('PrefijoYNumero')))
                    actualizar_items_comparativa_nd(reg_id, cx, nit, nombre_item='Codigo CUFE de la factura', valor_xml=safe_str(r.get('cufeuuid')))
                    actualizar_items_comparativa_nd(reg_id, cx, nit, nombre_item='Cude de la Nota Debito', valor_xml=safe_str(r.get('cufe_fe')))
                    cnt_nd += 1
                except Exception as e:
                    print(f"[ERROR] Procesando ND {r.get('ID')}: {e}")
                    cnt_nd += 1

            print(f"[SUCCESS] Procesamiento completado. NC procesadas: {cnt_nc}, ND procesadas: {cnt_nd}")
            SetVar("vLocStrResultadoSP", "True")
            SetVar("vLocStrResumenSP", f"Procesamiento Finalizado. NC: {cnt_nc}, ND: {cnt_nd}")

    except Exception as e:
        print(f"[ERROR] FALLO CRITICO EN HU4.2: {str(e)}")
        print(traceback.format_exc())
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        SetVar("vLocStrResultadoSP", "False")
        raise e
def ZVEN_ValidarComercializados():
    """
    Función para procesar las validaciones de ZVEN/50 (Pedidos Comercializados).
    
    VERSIÓN: 2.2 - Corrección UnboundLocalError y Broadcasting de Aprobaciones
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
    from decimal import Decimal, ROUND_HALF_UP
    
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    # =========================================================================
    # FUNCIONES AUXILIARES BÁSICAS
    # =========================================================================
    
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
            if isinstance(v, float) and (np.isnan(v) or pd.isna(v)):
                return ""
            return str(v)
        try:
            return str(v).strip()
        except:
            return ""
    
    def truncar_observacion(obs, max_len=3900):
        if not obs:
            return ""
        obs_str = safe_str(obs)
        if len(obs_str) > max_len:
            return obs_str[:max_len - 3] + "..."
        return obs_str
    
    def parse_config(raw):
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
    
    # =========================================================================
    # CONEXIÓN A BASE DE DATOS
    # =========================================================================
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
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
        for attempt in range(max_retries):
            try:
                cx = pyodbc.connect(conn_str_auth, timeout=30)
                cx.autocommit = False
                conectado = True
                print(f"[DEBUG] Conexion SQL (Auth) abierta exitosamente (intento {attempt + 1})")
                break
            except pyodbc.Error as e:
                print(f"[WARNING] Fallo conexion con Usuario/Contraseña (intento {attempt + 1}): {str(e)}")
                excepcion_final = e
                if attempt < max_retries - 1:
                    time.sleep(1)

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
    # FUNCIONES DE VALIDACION DE ARCHIVOS MAESTROS
    # =========================================================================
    
    def validar_archivo_maestro_comercializados(ruta_archivo):
        try:
            if not os.path.exists(ruta_archivo):
                return False, f"No existe el archivo: {ruta_archivo}", None
            
            df = pd.read_excel(ruta_archivo)
            df.columns = df.columns.str.strip()
            
            columnas_requeridas = [
                'OC', 'FACTURA', 'VALOR TOTAL OC', 'POSICION',
                'PorCalcular_hoc (VALOR UNITARIO)', 'PorCalcular_hoc (ME)'
            ]
            
            columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
            
            if columnas_faltantes:
                return False, f"Columnas faltantes en Maestro de comercializados: {columnas_faltantes}", None
            
            if len(df) == 0:
                return False, "Maestro de comercializados esta vacio", None
            
            df['OC'] = df['OC'].astype(str).str.strip()
            df['FACTURA'] = df['FACTURA'].astype(str).str.strip()
            df['POSICION'] = df['POSICION'].astype(str).str.strip()
            
            return True, "Estructura valida", df
            
        except Exception as e:
            return False, f"Error validando Maestro de comercializados: {str(e)}", None
    
    def validar_archivo_asociacion_cuenta(ruta_archivo):
        try:
            if not os.path.exists(ruta_archivo):
                return False, f"No existe el archivo: {ruta_archivo}", None
            
            xl = pd.ExcelFile(ruta_archivo)
            hojas_posibles = ['Grupo cuentas prove', 'grupo cuentas agrupacion provee', 
                           'Grupo cuentas agrupacion provee']
            
            hoja_encontrada = None
            for hoja in hojas_posibles:
                if hoja in xl.sheet_names:
                    hoja_encontrada = hoja
                    break
            
            if not hoja_encontrada:
                for hoja in xl.sheet_names:
                    if 'cuentas' in hoja.lower():
                        hoja_encontrada = hoja
                        break
            
            if not hoja_encontrada:
                return False, f"No se encontro hoja de cuentas en: {xl.sheet_names}", None
            
            df = pd.read_excel(ruta_archivo, sheet_name=hoja_encontrada)
            
            columnas_requeridas_base = ['Cta Mayor', 'Nombre cuenta', 'TIPO RET.', 'IND.RETENCION']
            columnas_faltantes = []

            for col in columnas_requeridas_base:
                if col not in df.columns:
                    encontrado = False
                    for df_col in df.columns:
                        if col.lower().replace('.', '').replace(' ', '') in df_col.lower().replace('.', '').replace(' ', ''):
                            encontrado = True
                            break
                    if not encontrado:
                        columnas_faltantes.append(col)
            
            if columnas_faltantes:
                return False, f"Columnas faltantes en Asociacion cuenta indicador: {columnas_faltantes}", None
            
            return True, "Estructura valida", df
            
        except Exception as e:
            return False, f"Error validando Asociacion cuenta indicador: {str(e)}", None
    
    # =========================================================================
    # FUNCIONES DE NORMALIZACIÓN Y COMPARACIÓN DE NOMBRES
    # =========================================================================
    
    def normalizar_nombre_empresa(nombre):
        if pd.isna(nombre) or nombre == "":
            return ""
        
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
        if pd.isna(nombre_xml) or pd.isna(nombre_sap):
            return False
        
        nombre_xml_limpio = normalizar_nombre_empresa(str(nombre_xml))
        nombre_sap_limpio = normalizar_nombre_empresa(str(nombre_sap))
        
        lista_xml = nombre_xml_limpio.split()
        lista_sap = nombre_sap_limpio.split()
        
        if len(lista_xml) != len(lista_sap):
            return False

        return sorted(lista_xml) == sorted(lista_sap)
    
    # =========================================================================
    # FUNCIONES DE VALIDACION DE DATOS
    # =========================================================================
    
    def validar_tolerancia_numerica(valor1, valor2, tolerancia=500):
        try:
            val1 = normalizar_decimal(valor1)
            val2 = normalizar_decimal(valor2)
            return abs(val1 - val2) <= tolerancia
        except:
            return False
    
    def validar_cantidad_precio_tolerancia(cantidad_xml, precio_xml, cantidad_sap, precio_sap, valor_total_factura):
        try:
            cantidad_xml = normalizar_decimal(cantidad_xml)
            precio_xml = normalizar_decimal(precio_xml)
            cantidad_sap = normalizar_decimal(cantidad_sap)
            precio_sap = normalizar_decimal(precio_sap)
            valor_total = normalizar_decimal(valor_total_factura)
            
            cantidad_ok = abs(cantidad_xml - cantidad_sap) <= 1
            precio_ok = abs(precio_xml - precio_sap) <= 1
            
            if cantidad_ok and precio_ok:
                valor_calculado = cantidad_xml * precio_xml
                if valor_calculado > valor_total + 500:
                    return False, False
            
            return cantidad_ok, precio_ok
            
        except Exception:
            return False, False
    
    # =========================================================================
    # FUNCIONES DE MANEJO DE ARCHIVOS
    # =========================================================================
    
    def copiar_insumos_a_carpeta_destino(ruta_origen, nombre_archivo, ruta_destino):
        try:
            os.makedirs(ruta_destino, exist_ok=True)
            archivo_origen = os.path.join(ruta_origen, nombre_archivo)
            archivo_destino = os.path.join(ruta_destino, nombre_archivo)
            
            if os.path.exists(archivo_origen):
                shutil.copy2(archivo_origen, archivo_destino)
                print(f"[DEBUG] Archivo copiado: {archivo_destino}")
                return True, archivo_destino
            else:
                return False, f"No se encuentra el archivo origen: {archivo_origen}"
        except Exception as e:
            return False, f"Error copiando insumos: {str(e)}"
    
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
                print(f"[UPDATE] DocumentsProcessing actualizada - ID {registro_id}")
            
        except Exception as e:
            print(f"[ERROR] Error actualizando DocumentsProcessing: {str(e)}")
            raise
    
    def actualizar_items_comparativa(registro, cx, nit, factura, nombre_item,
                                 actualizar_valor_xml=True, valor_xml=None,
                                 actualizar_aprobado=True, valor_aprobado=None,
                                 actualizar_orden_compra=True, val_orden_de_compra=None):
        cur = cx.cursor()
        
        def safe_db_val(v):
            if v is None: return None
            s = str(v).strip()
            if not s or s.lower() == 'none' or s.lower() == 'null': return None
            return s

        query_count = """
        SELECT COUNT(*)
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?
        """
        cur.execute(query_count, (nit, factura, nombre_item, registro['ID_dp']))
        count_existentes = cur.fetchone()[0]

        lista_compra = val_orden_de_compra.split('|') if val_orden_de_compra else []
        lista_xml = valor_xml.split('|') if valor_xml else []
        lista_aprob = valor_aprobado.split('|') if valor_aprobado else []
        
        if isinstance(valor_aprobado, list):
             lista_aprob = valor_aprobado

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
                final_params = params + [nit, factura, nombre_item, registro['ID_dp'], i + 1]
                cur.execute(update_query, final_params)
            else:
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra,
                    Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(insert_query, (registro['Fecha_de_retoma_antes_de_contabilizacion_dp'],registro['documenttype_dp'],registro['numero_de_liquidacion_u_orden_de_compra_dp'],registro['nombre_emisor_dp'], registro['ID_dp'], nit, factura, nombre_item, val_compra, val_xml, val_aprob))
        
        cur.close()
        print(f"[PROCESADO] Item '{nombre_item}' - {maximo_conteo} valor(es)")
    
    def actualizar_estado_comparativa(cx, nit, factura, estado):
        cur = cx.cursor()
        update_sql = """
        UPDATE [dbo].[CxP.Comparativa]
        SET Estado_validacion_antes_de_eventos = ?
        WHERE NIT = ? AND Factura = ?
        """
        cur.execute(update_sql, (estado, nit, factura))
        cur.close()
    
    def marcar_posiciones_procesadas(cx, doc_compra, posiciones):
        try:
            cur = cx.cursor()
            update_sql = "UPDATE [CxP].[Trans_Candidatos_HU41] SET Marca_hoc = 'PROCESADO' WHERE DocCompra_hoc = ?"
            cur.execute(update_sql, (doc_compra,))
            cur.close()
        except Exception as e:
            print(f"[ERROR] Error marcando posiciones: {str(e)}")
            raise
    
    def expandir_posiciones_string(valor_string, separador='|'):
        if pd.isna(valor_string) or valor_string == '' or valor_string is None:
            return []
        valor_str = safe_str(valor_string)
        if '|' in valor_str:
            return [v.strip() for v in valor_str.split('|') if v.strip()]
        if ',' in valor_str:
            return [v.strip() for v in valor_str.split(',') if v.strip()]
        return [valor_str.strip()]
    
    def expandir_posiciones_historico(registro):
        try:
            posiciones = expandir_posiciones_string(registro.get('Posicion_hoc', ''))
            if not posiciones: return {}
            
            por_calcular = expandir_posiciones_string(registro.get('PorCalcular_hoc', ''))
            cant_pedido = expandir_posiciones_string(registro.get('CantPedido_hoc', ''))
            precio_unit = expandir_posiciones_string(registro.get('PrecioUnitario_hoc', ''))
            trm_list = expandir_posiciones_string(registro.get('Trm_hoc', ''))
            fec_doc_list = expandir_posiciones_string(registro.get('FecDoc_hoc', ''))
            fec_reg_list = expandir_posiciones_string(registro.get('FecReg_hoc', ''))
            fec_cont_gasto_list = expandir_posiciones_string(registro.get('FecContGasto_hoc', ''))
            ind_impuestos_list = expandir_posiciones_string(registro.get('IndicadorImpuestos_hoc', ''))
            texto_breve_list = expandir_posiciones_string(registro.get('TextoBreve_hoc', ''))
            clase_impuesto_list = expandir_posiciones_string(registro.get('ClaseDeImpuesto_hoc', ''))
            cuenta_list = expandir_posiciones_string(registro.get('Cuenta_hoc', ''))
            ciudad_prov_list = expandir_posiciones_string(registro.get('CiudadProveedor_hoc', ''))
            doc_fi_entrada_list = expandir_posiciones_string(registro.get('DocFiEntrada_hoc', ''))
            cuenta26_list = expandir_posiciones_string(registro.get('Cuenta26_hoc', ''))
            tipo_nif_list = expandir_posiciones_string(registro.get('TipoNif_hoc', ''))
            acreedor_list = expandir_posiciones_string(registro.get('Acreedor_hoc', ''))
            n_proveedor = safe_str(registro.get('NProveedor_hoc', ''))
            
            datos_por_posicion = {}
            for i, posicion in enumerate(posiciones):
                datos_por_posicion[posicion] = {
                    'Posicion': posicion,
                    'PorCalcular': por_calcular[i] if i < len(por_calcular) else '',
                    'CantPedido': cant_pedido[i] if i < len(cant_pedido) else '',
                    'PrecioUnitario': precio_unit[i] if i < len(precio_unit) else '',
                    'Trm': trm_list[i] if i < len(trm_list) else (trm_list[0] if trm_list else ''),
                    'TipoNif': tipo_nif_list[i] if i < len(tipo_nif_list) else (tipo_nif_list[0] if tipo_nif_list else ''),
                    'NProveedor': n_proveedor,
                    'Acreedor': acreedor_list[i] if i < len(acreedor_list) else (acreedor_list[0] if acreedor_list else ''),
                    'FecDoc': fec_doc_list[i] if i < len(fec_doc_list) else (fec_doc_list[0] if fec_doc_list else ''),
                    'FecReg': fec_reg_list[i] if i < len(fec_reg_list) else (fec_reg_list[0] if fec_reg_list else ''),
                    'FecContGasto': fec_cont_gasto_list[i] if i < len(fec_cont_gasto_list) else (fec_cont_gasto_list[0] if fec_cont_gasto_list else ''),
                    'IndicadorImpuestos': ind_impuestos_list[i] if i < len(ind_impuestos_list) else (ind_impuestos_list[0] if ind_impuestos_list else ''),
                    'TextoBreve': texto_breve_list[i] if i < len(texto_breve_list) else (texto_breve_list[0] if texto_breve_list else ''),
                    'ClaseDeImpuesto': clase_impuesto_list[i] if i < len(clase_impuesto_list) else (clase_impuesto_list[0] if clase_impuesto_list else ''),
                    'Cuenta': cuenta_list[i] if i < len(cuenta_list) else (cuenta_list[0] if cuenta_list else ''),
                    'CiudadProveedor': ciudad_prov_list[i] if i < len(ciudad_prov_list) else (ciudad_prov_list[0] if ciudad_prov_list else ''),
                    'DocFiEntrada': doc_fi_entrada_list[i] if i < len(doc_fi_entrada_list) else (doc_fi_entrada_list[0] if doc_fi_entrada_list else ''),
                    'Cuenta26': cuenta26_list[i] if i < len(cuenta26_list) else (cuenta26_list[0] if cuenta26_list else '')
                }
            return datos_por_posicion
        except Exception as e:
            print(f"[ERROR] Error expandiendo posiciones del historico: {str(e)}")
            return {}
    
    def procesar_registro_sin_datos_maestro(cx, registro, cfg, sufijo_contado):
        registro_id = safe_str(registro.get('ID_dp', ''))
        nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
        numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
        
        ruta, nombre = os.path.split(safe_str(registro.get('RutaArchivo_dp', '')))
        carpeta_destino = cfg.get('CarpetaDestinoComercializados', '')
        
        if ruta and nombre and carpeta_destino:
            copiado, resultado_copia = copiar_insumos_a_carpeta_destino(ruta, nombre, carpeta_destino)
            if copiado:
                observacion = "No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados"
                campos_actualizar = {
                    'EstadoFinalFase_4': 'Exitoso',
                    'ObservacionesFase_4': truncar_observacion(observacion),
                    'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}",
                    'RutaArchivo': carpeta_destino
                }
            else:
                observacion = "No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados - No se logran mover insumos a carpeta COMERCIALIZADOS"
                campos_actualizar = {
                    'EstadoFinalFase_4': 'Exitoso',
                    'ObservacionesFase_4': truncar_observacion(observacion),
                    'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}"
                }
        else:
            observacion = "No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados - No se logran identificar insumos"
            campos_actualizar = {
                'EstadoFinalFase_4': 'Exitoso',
                'ObservacionesFase_4': truncar_observacion(observacion),
                'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}"
            }
        
        actualizar_bd_cxp(cx, registro_id, campos_actualizar)
        
        actualizar_items_comparativa(
            registro=registro, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Observaciones', val_orden_de_compra=None,
            actualizar_valor_xml=True, valor_xml=observacion
        )
        
        actualizar_estado_comparativa(cx, nit, numero_factura, f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}")
        return 'EN_ESPERA'
    
    def procesar_sin_coincidencia_valores(cx, registro, posiciones_maestro, valores_unitario, valores_me, sufijo_contado):
        registro_id = safe_str(registro.get('ID_dp', ''))
        nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
        numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
        valor_a_pagar = normalizar_decimal(registro.get('valor_a_pagar_dp', 0))
        vlr_pagar_cop = normalizar_decimal(registro.get('VlrPagarCop_dp', 0))
        
        observacion = "No se encuentra coincidencia del Valor a pagar de la factura"
        resultado_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
        
        campos_novedad = {
            'EstadoFinalFase_4': 'Exitoso',
            'ObservacionesFase_4': truncar_observacion(observacion),
            'ResultadoFinalAntesEventos': resultado_final
        }
        actualizar_bd_cxp(cx, registro_id, campos_novedad)
        
        n_pos = len(posiciones_maestro)

        actualizar_items_comparativa(registro=registro, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='LineExtensionAmount', val_orden_de_compra='NO ENCONTRADO',
            actualizar_valor_xml=True, valor_xml=str(valor_a_pagar),
            actualizar_aprobado=True, valor_aprobado='|'.join(['NO'] * n_pos))
        
        actualizar_items_comparativa(registro=registro, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='VlrPagarCop', val_orden_de_compra='NO ENCONTRADO',
            actualizar_valor_xml=True, valor_xml=str(vlr_pagar_cop),
            actualizar_aprobado=True, valor_aprobado='|'.join(['NO'] * n_pos))
        
        actualizar_items_comparativa(registro=registro, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Observaciones', val_orden_de_compra=None,
            actualizar_valor_xml=True, valor_xml=observacion)
        
        actualizar_items_comparativa(registro=registro, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='NombreEmisor', val_orden_de_compra='NO ENCONTRADO',
            actualizar_aprobado=True, valor_aprobado='|'.join(['NO'] * n_pos))
        
        actualizar_items_comparativa(registro=registro, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Posicion', val_orden_de_compra='|'.join([str(p) for p in posiciones_maestro]),
            actualizar_aprobado=True, valor_aprobado='|'.join(['NO'] * n_pos))
        
        valores_calc = []
        for i in range(len(posiciones_maestro)):
            if valores_me[i] > 0: valores_calc.append(str(valores_me[i]))
            else: valores_calc.append(str(valores_unitario[i]))
        
        actualizar_items_comparativa(registro=registro, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='ValorPorCalcularPosicion', val_orden_de_compra='|'.join(valores_calc),
            actualizar_aprobado=True, valor_aprobado='|'.join(['NO'] * n_pos))
        
        actualizar_items_comparativa(registro=registro, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='FecDoc', val_orden_de_compra='NO ENCONTRADO',
            actualizar_aprobado=True, valor_aprobado='|'.join(['NO'] * n_pos))
        
        actualizar_items_comparativa(registro=registro, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='FecReg', val_orden_de_compra='NO ENCONTRADO',
            actualizar_aprobado=True, valor_aprobado='|'.join(['NO'] * n_pos))
        
        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
    
    # =========================================================================
    # INICIO DEL PROCESAMIENTO PRINCIPAL
    # =========================================================================
    
    try:
        print("="*80 + "\n[INICIO] Procesamiento ZVEN/50 - Comercializados\n" + "="*80)
        t_inicio = time.time()
        
        cfg = parse_config(GetVar("vLocDicConfig"))
        
        required_config = ['RutaInsumosComercializados', 'RutaInsumoAsociacion', 'CarpetaDestinoComercializados', 'ServidorBaseDatos', 'NombreBaseDatos']
        if any(not cfg.get(k) for k in required_config): raise ValueError(f"Faltan parametros")
        
        ruta_maestro = cfg.get('RutaInsumosComercializados', '')
        ruta_asociacion = cfg.get('RutaInsumoAsociacion', '')
        
        if not validar_archivo_maestro_comercializados(ruta_maestro)[0]: raise FileNotFoundError("Maestro invalido")
        if not validar_archivo_asociacion_cuenta(ruta_asociacion)[0]: raise FileNotFoundError("Asociacion invalido")
        _, _, df_maestro = validar_archivo_maestro_comercializados(ruta_maestro)
        
        with crear_conexion_db(cfg) as cx:
            query_zven = "SELECT * FROM [CxP].[HU41_CandidatosValidacion] WHERE [ClaseDePedido_hoc] IN ('ZVEN', '50') ORDER BY [executionDate_dp] DESC"
            df_registros = pd.read_sql(query_zven, cx)
            print(f"[INFO] {len(df_registros)} registros para procesar")
            
            if df_registros.empty: return
            
            cnt_proc, cnt_nov, cnt_esp, cnt_ok = 0, 0, 0, 0
            
            for idx, registro in df_registros.iterrows():
                try:
                    reg_id = safe_str(registro.get('ID_dp', ''))
                    num_oc = safe_str(registro.get('numero_de_liquidacion_u_orden_de_compra_dp', ''))
                    num_fac = safe_str(registro.get('numero_de_factura_dp', ''))
                    pay_means = safe_str(registro.get('forma_de_pago_dp', ''))
                    nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
                    print(f"\n[PROCESO] {cnt_proc+1}: OC {num_oc}, Factura {num_fac}")
                    
                    sufijo = " CONTADO" if pay_means == "01" else ""
                    
                    matches = df_maestro[(df_maestro['OC'] == num_oc) & (df_maestro['FACTURA'] == num_fac)]
                    if matches.empty:
                        procesar_registro_sin_datos_maestro(cx, registro, cfg, sufijo)
                        cnt_esp += 1; cnt_proc += 1
                        continue
                    
                    pos_maestro = matches['POSICION'].astype(str).tolist()
                    vals_unit = [normalizar_decimal(v) for v in matches['PorCalcular_hoc (VALOR UNITARIO)']]
                    vals_me = [normalizar_decimal(v) for v in matches['PorCalcular_hoc (ME)']]
                    n_pos = len(pos_maestro)

                    # Update DocsProcessing
                    actualizar_bd_cxp(cx, reg_id, {
                        'Posicion_Comercializado': ','.join(pos_maestro),
                        'Valor_a_pagar_Comercializado': ','.join(map(str, vals_unit)),
                        'Valor_a_pagar_Comercializado_ME': ','.join(map(str, vals_me))
                    })

                    datos_hist = expandir_posiciones_historico(registro)
                    if not datos_hist:
                        obs = "No se encuentran datos del historico en el registro"
                        actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': f"CON NOVEDAD - COMERCIALIZADOS{sufijo}"})
                        actualizar_estado_comparativa(cx, nit, num_fac, f"CON NOVEDAD - COMERCIALIZADOS{sufijo}")
                        cnt_nov += 1; cnt_proc += 1
                        continue
                    
                    coincide = True
                    for i, p in enumerate(pos_maestro):
                        d = datos_hist.get(p)
                        if not d:
                            coincide = False; break
                        v_hist = normalizar_decimal(d.get('PorCalcular', 0))
                        target = vals_me[i] if vals_me[i] > 0 else vals_unit[i]
                        if abs(v_hist - target) > 0.01:
                            coincide = False; break
                            
                    if not coincide:
                        procesar_sin_coincidencia_valores(cx, registro, pos_maestro, vals_unit, vals_me, sufijo)
                        cnt_nov += 1; cnt_proc += 1
                        continue
                    
                    # Validar Sumas
                    vlr_pagar = normalizar_decimal(registro.get('Valor de la Compra LEA_ddp', 0))
                    vlr_cop = normalizar_decimal(registro.get('VlrPagarCop_dp', 0))
                    sum_unit = sum(vals_unit)
                    sum_me = sum(vals_me)
                    
                    sum_ok = True
                    if any(v>0 for v in vals_me):
                        if not validar_tolerancia_numerica(sum_unit, vlr_pagar, 500): sum_ok = False
                        if vlr_cop > 0 and not validar_tolerancia_numerica(sum_me, vlr_cop, 500): sum_ok = False
                    else:
                        if not validar_tolerancia_numerica(sum_unit, vlr_pagar, 500): sum_ok = False

                    if not sum_ok:
                        obs = "No se encuentra coincidencia del Valor a pagar de la factura"
                        actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': f"CON NOVEDAD - COMERCIALIZADOS{sufijo}"})
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'LineExtensionAmount', val_orden_de_compra=str(sum_unit), actualizar_valor_xml=True, valor_xml=str(vlr_pagar), actualizar_aprobado=True, valor_aprobado='|'.join(['NO']*n_pos))
                        if any(v>0 for v in vals_me):
                            actualizar_items_comparativa(registro, cx, nit, num_fac, 'VlrPagarCop', val_orden_de_compra=str(sum_me), actualizar_valor_xml=True, valor_xml=str(vlr_cop), actualizar_aprobado=True, valor_aprobado='|'.join(['NO']*n_pos))
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'Observaciones', valor_xml=obs)
                        actualizar_estado_comparativa(cx, nit, num_fac, f"CON NOVEDAD - COMERCIALIZADOS{sufijo}")
                        cnt_nov += 1; cnt_proc += 1
                        continue

                    # Validar TRM
                    trm_sap = 0.0 # Initialize variable
                    trm_xml = 0.0
                    
                    if registro.get('DocumentCurrencyCode_dp') != 'COP':
                        trm_xml = normalizar_decimal(registro.get('CalculationRate_dp', 0))
                        d_prim = datos_hist.get(pos_maestro[0], {})
                        trm_sap = normalizar_decimal(d_prim.get('Trm', 0))

                        if (trm_xml > 0 or trm_sap > 0) and abs(trm_xml - trm_sap) >= 0.01:
                            obs = "No se encuentra coincidencia en el campo TRM"
                            actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': f"CON NOVEDAD - COMERCIALIZADOS{sufijo}"})
                            actualizar_items_comparativa(registro, cx, nit, num_fac, 'TRM', val_orden_de_compra='|'.join([str(trm_sap)]*n_pos), actualizar_valor_xml=True, valor_xml=str(trm_xml), actualizar_aprobado=True, valor_aprobado='|'.join(['NO']*n_pos))
                            actualizar_items_comparativa(registro, cx, nit, num_fac, 'Observaciones', valor_xml=obs)
                            actualizar_estado_comparativa(cx, nit, num_fac, f"CON NOVEDAD - COMERCIALIZADOS{sufijo}")
                            cnt_nov += 1; cnt_proc += 1
                            continue

                    # Validar Cantidad/Precio
                    cant_xml = normalizar_decimal(registro.get('Cantidad de producto_ddp', 0))
                    prec_xml = normalizar_decimal(registro.get('Precio Unitario del producto_ddp', 0))
                    
                    cp_ok = True
                    err_c, err_p = [], []
                    
                    for p in pos_maestro:
                        d = datos_hist.get(p, {})
                        c_sap = normalizar_decimal(d.get('CantPedido', 0))
                        pr_sap = normalizar_decimal(d.get('PrecioUnitario', 0))
                        
                        cok, pok = validar_cantidad_precio_tolerancia(cant_xml, prec_xml, c_sap, pr_sap, vlr_pagar)
                        if not cok: err_c.append(p); cp_ok = False
                        if not pok: err_p.append(p); cp_ok = False
                        
                    if not cp_ok:
                        obs = "No se encuentra coincidencia en cantidad y/o precio unitario"
                        actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': f"CON NOVEDAD - COMERCIALIZADOS{sufijo}"})
                        
                        vals_c_sap = [str(normalizar_decimal(datos_hist.get(p, {}).get('CantPedido', 0))) for p in pos_maestro]
                        vals_p_sap = [str(normalizar_decimal(datos_hist.get(p, {}).get('PrecioUnitario', 0))) for p in pos_maestro]
                        
                        aprob_c = '|'.join(['NO' if p in err_c else 'SI' for p in pos_maestro])
                        aprob_p = '|'.join(['NO' if p in err_p else 'SI' for p in pos_maestro])
                        
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'CantidadProducto', val_orden_de_compra='|'.join(vals_c_sap), valor_xml=str(cant_xml), valor_aprobado=aprob_c)
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'PrecioUnitarioProducto', val_orden_de_compra='|'.join(vals_p_sap), valor_xml=str(prec_xml), valor_aprobado=aprob_p)
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'Observaciones', valor_xml=obs)
                        actualizar_estado_comparativa(cx, nit, num_fac, f"CON NOVEDAD - COMERCIALIZADOS{sufijo}")
                        cnt_nov += 1; cnt_proc += 1
                        continue
                    else:
                        # Log success for CP
                        vals_c_sap = [str(normalizar_decimal(datos_hist.get(p, {}).get('CantPedido', 0))) for p in pos_maestro]
                        vals_p_sap = [str(normalizar_decimal(datos_hist.get(p, {}).get('PrecioUnitario', 0))) for p in pos_maestro]
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'CantidadProducto', val_orden_de_compra='|'.join(vals_c_sap), valor_xml=str(cant_xml), valor_aprobado='|'.join(['SI']*n_pos))
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'PrecioUnitarioProducto', val_orden_de_compra='|'.join(vals_p_sap), valor_xml=str(prec_xml), valor_aprobado='|'.join(['SI']*n_pos))

                    # Validar Nombre
                    nom_xml = safe_str(registro.get('nombre_emisor_dp', ''))
                    nom_sap = safe_str(datos_hist.get(pos_maestro[0], {}).get('Acreedor', ''))

                    if not comparar_nombres_proveedor(nom_xml, nom_sap):
                        obs = "No se encuentra coincidencia en Nombre Emisor"
                        actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ObservacionesFase_4': truncar_observacion(obs), 'ResultadoFinalAntesEventos': f"CON NOVEDAD - COMERCIALIZADOS{sufijo}"})
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'NombreEmisor', val_orden_de_compra=nom_sap, valor_xml=nom_xml, valor_aprobado='|'.join(['NO']*n_pos))
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'Observaciones', valor_xml=obs)
                        actualizar_estado_comparativa(cx, nit, num_fac, f"CON NOVEDAD - COMERCIALIZADOS{sufijo}")
                        cnt_nov += 1; cnt_proc += 1
                        continue

                    # SUCCESS
                    marcar_posiciones_procesadas(cx, safe_str(registro.get('DocCompra_hoc', '')), safe_str(registro['Posicion_hoc']))
                    actualizar_bd_cxp(cx, reg_id, {'EstadoFinalFase_4': 'Exitoso', 'ResultadoFinalAntesEventos': f"PROCESADO{sufijo}"})
                    
                    # Log all success
                    actualizar_items_comparativa(registro, cx, nit, num_fac, 'LineExtensionAmount', val_orden_de_compra='|'.join([str(v) for v in vals_unit]), valor_xml=str(vlr_pagar), valor_aprobado='|'.join(['SI']*n_pos))
                    
                    vlr_final = sum_me if sum_me > 0 else sum_unit
                    vlr_xml_final = vlr_cop if vlr_cop > 0 else vlr_pagar
                    actualizar_items_comparativa(registro, cx, nit, num_fac, 'VlrPagarCop', val_orden_de_compra=str(vlr_final), valor_xml=str(vlr_xml_final), valor_aprobado='|'.join(['SI']*n_pos))
                    
                    actualizar_items_comparativa(registro, cx, nit, num_fac, 'NombreEmisor', val_orden_de_compra=nom_sap, valor_xml=nom_xml, valor_aprobado='|'.join(['SI']*n_pos))
                    actualizar_items_comparativa(registro, cx, nit, num_fac, 'Posicion', val_orden_de_compra='|'.join([str(p) for p in pos_maestro]), valor_aprobado='|'.join(['SI']*n_pos))
                    
                    actualizar_items_comparativa(registro, cx, nit, num_fac, 'ValorPorCalcularPosicion', val_orden_de_compra='|'.join([str(vals_unit[i]) for i in range(n_pos)]), valor_aprobado='|'.join(['SI']*n_pos))
                    
                    if any(v>0 for v in vals_me):
                        actualizar_items_comparativa(registro, cx, nit, num_fac, 'ValorPorCalcularMEPosicion', val_orden_de_compra='|'.join([str(vals_me[i]) for i in range(n_pos) if vals_me[i]>0]), valor_aprobado='|'.join(['SI']*n_pos))

                    actualizar_items_comparativa(registro, cx, nit, num_fac, 'TRM', val_orden_de_compra='|'.join([str(trm_sap)]*n_pos), valor_xml=str(trm_xml), valor_aprobado='|'.join(['SI']*n_pos))
                    
                    actualizar_items_comparativa(registro, cx, nit, num_fac, 'ValorPorCalcularSAP', val_orden_de_compra='|'.join([str(normalizar_decimal(datos_hist.get(p, {}).get('PorCalcular', 0))) for p in pos_maestro]), valor_aprobado='|'.join(['SI']*n_pos))
                    
                    campos_hist = [('TipoNIF', 'TipoNif'), ('Acreedor', 'Acreedor'), ('FecDoc', 'FecDoc'), ('FecReg', 'FecReg'), ('FechaContGasto', 'FecContGasto'), ('IndicadorImpuestos', 'IndicadorImpuestos'), ('TextoBreve', 'TextoBreve'), ('ClaseImpuesto', 'ClaseDeImpuesto'), ('Cuenta', 'Cuenta'), ('CiudadProveedor', 'CiudadProveedor'), ('DocFIEntrada', 'DocFiEntrada'), ('CTA26', 'Cuenta26')]
                    
                    for item, field in campos_hist:
                        val = '|'.join([safe_str(datos_hist.get(p, {}).get(field, '')) for p in pos_maestro])
                        actualizar_items_comparativa(registro, cx, nit, num_fac, item, val_orden_de_compra=val, valor_aprobado='|'.join(['SI']*n_pos))

                    actualizar_estado_comparativa(cx, nit, num_fac, f"PROCESADO{sufijo}")
                    cnt_ok += 1; cnt_proc += 1
                    
                except Exception as e:
                    print(f"Error {str(e)}")
                    cnt_nov += 1; cnt_proc += 1
                    
        print(f"FIN: {cnt_proc} procesados. OK: {cnt_ok}, NOV: {cnt_nov}, ESP: {cnt_esp}")
        
    except Exception as e:
        print(f"CRITICO: {str(e)}")

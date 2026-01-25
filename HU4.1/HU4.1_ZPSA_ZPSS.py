def ZPSA_ZPSS_ValidarServicios():
    """
    Función para procesar las validaciones de ZPSA/ZPSS/43 (Pedidos de Servicios).
    
    VERSIÓN: 2.2 - Broadcasting de Aprobaciones y Cast String
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
    import re
    import os
    from itertools import zip_longest
    
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
    
    # =========================================================================
    # CONEXIÓN A BASE DE DATOS
    # =========================================================================
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        """Crea conexión a la base de datos con reintentos."""
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            raise ValueError(f"Parametros faltantes: {', '.join(missing)}")

        usuario = cfg['UsuarioBaseDatos']
        contrasena = cfg['ClaveBaseDatos']
        
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
    # FUNCIONES DE NORMALIZACIÓN DE NOMBRES
    # =========================================================================
    
    def normalizar_nombre_empresa(nombre):
        """Normaliza nombres de empresas según las reglas de la HU."""
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
        """
        Compara nombres de proveedores dividiendo por espacios y verificando
        que contengan exactamente las mismas palabras sin importar el orden (LOGICA ZPAF).
        """
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
    # FUNCIONES DE VALIDACION
    # =========================================================================
    
    def validar_tolerancia_numerica(valor1, valor2, tolerancia=500):
        """Valida si dos valores numéricos están dentro del rango de tolerancia."""
        try:
            val1 = normalizar_decimal(valor1)
            val2 = normalizar_decimal(valor2)
            return abs(val1 - val2) <= tolerancia
        except:
            return False

    def comparar_suma_total(valores_por_calcular, valor_objetivo, tolerancia=500):
        """
        Suma TODOS los valores de la lista y compara si el total coincide
        con el valor objetivo dentro de un rango de tolerancia (LOGICA ZPAF).
        """
        valor_objetivo = normalizar_decimal(valor_objetivo)
        
        if valor_objetivo <= 0 or not valores_por_calcular:
            return False, [], 0

        suma_total = sum(normalizar_decimal(valor) for posicion, valor in valores_por_calcular)
        
        if abs(suma_total - valor_objetivo) <= tolerancia:
            todas_las_posiciones = [posicion for posicion, valor in valores_por_calcular]
            return True, todas_las_posiciones, suma_total

        return False, [], 0
    
    def validar_indicador_servicios_orden15(indicador):
        """
        Valida indicador de impuestos para Orden 15.
        Permitidos: H4, H5, H6, H7, VP, CO, IC, CR
        """
        indicadores_validos = {'H4', 'H5', 'H6', 'H7', 'VP', 'CO', 'IC', 'CR'}
        ind_str = safe_str(indicador).upper().strip()
        return ind_str in indicadores_validos
    
    def validar_indicador_diferido(indicador):
        """
        Valida indicador de impuestos para Activo Fijo DIFERIDO (inicia con 2000).
        Permitidos: C1, FA, VP, CO, CR
        """
        indicadores_validos = {'C1', 'FA', 'VP', 'CO', 'CR'}
        ind_str = safe_str(indicador).upper().strip()
        return ind_str in indicadores_validos
    
    def validar_clase_orden(indicador, clase_orden):
        """
        Valida Clase orden según Indicador impuestos para Orden 15.
        Reglas:
            - H4/H5 → ZINV
            - H6/H7 → ZADM
            - VP/CO/CR/IC → ZINV o ZADM
        """
        indicador_str = safe_str(indicador).upper().strip()
        clase_str = safe_str(clase_orden).upper().strip()
        
        if not clase_str:
            return False, "NO se encuentra diligenciado"
        
        if indicador_str in ('H4', 'H5'):
            if clase_str == 'ZINV':
                return True, ""
            else:
                return False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = ZINV", "H6 y H7 = ZADM" o "VP, CO, CR o IC = ZINV o ZADM"'
        
        elif indicador_str in ('H6', 'H7'):
            if clase_str == 'ZADM':
                return True, ""
            else:
                return False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = ZINV", "H6 y H7 = ZADM" o "VP, CO, CR o IC = ZINV o ZADM"'
        
        elif indicador_str in ('VP', 'CO', 'CR', 'IC'):
            if clase_str in ('ZINV', 'ZADM'):
                return True, ""
            else:
                return False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = ZINV", "H6 y H7 = ZADM" o "VP, CO, CR o IC = ZINV o ZADM"'
        
        return False, "Indicador impuestos no reconocido"
    
    def validar_emplazamiento(indicador, emplazamiento):
        """
        Valida Emplazamiento según Indicador impuestos para Elemento PEP.
        Reglas:
            - H4/H5 → DCTO_01
            - H6/H7 → GTO_02
            - VP/CO/CR/IC → DCTO_01 o GTO_02
        """
        indicador_str = safe_str(indicador).upper().strip()
        empl_str = safe_str(emplazamiento).upper().strip()
        
        if not empl_str:
            return False, "NO se encuentra diligenciado"
        
        if indicador_str in ('H4', 'H5'):
            if empl_str == 'DCTO_01':
                return True, ""
            else:
                return False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = DCTO_01", "H6 y H7 = GTO_02" o "VP, CO, CR o IC = DCTO_01 o GTO_02"'
        
        elif indicador_str in ('H6', 'H7'):
            if empl_str == 'GTO_02':
                return True, ""
            else:
                return False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = DCTO_01", "H6 y H7 = GTO_02" o "VP, CO, CR o IC = DCTO_01 o GTO_02"'
        
        elif indicador_str in ('VP', 'CO', 'CR', 'IC'):
            if empl_str in ('DCTO_01', 'GTO_02'):
                return True, ""
            else:
                return False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = DCTO_01", "H6 y H7 = GTO_02" o "VP, CO, CR o IC = DCTO_01 o GTO_02"'
        
        return False, "Indicador impuestos no reconocido"
    
    def validar_cuenta_orden_no_15(cuenta):
        """
        Valida Cuenta para Orden diferente a 15.
        Debe ser 5299150099 O iniciar con "7" y tener 10 dígitos.
        """
        cuenta_str = safe_str(cuenta).strip()
        
        if cuenta_str == '5299150099':
            return True
        
        if cuenta_str.startswith('7') and len(cuenta_str) == 10 and cuenta_str.isdigit():
            return True
        
        return False
    
    def campo_vacio(valor):
        """Verifica si un campo está vacío."""
        valor_str = safe_str(valor)
        return valor_str == "" or valor_str.lower() in ('null', 'none', 'nan')
    
    def campo_con_valor(valor):
        """Verifica si un campo tiene valor (no vacío)."""
        return not campo_vacio(valor)
    
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
    
    def actualizar_items_comparativa(registro, cx, nit, factura, nombre_item,
                                 actualizar_valor_xml=True, valor_xml=None,
                                 actualizar_aprobado=True, valor_aprobado=None,
                                 actualizar_orden_compra=True, val_orden_de_compra=None):
        """
        Actualiza o inserta items en [dbo].[CxP.Comparativa].
        """
        cur = cx.cursor()
        
        def safe_db_val(v):
            """Convierte strings vacios o 'None' a None real para BD"""
            if v is None:
                return None
            s = str(v).strip()
            if not s or s.lower() == 'none' or s.lower() == 'null':
                return None
            return s

        # 1. Contar items existentes
        query_count = """
        SELECT COUNT(*)
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?
        """
        # Se agregó ID_registro para ser precisos con la agrupación
        cur.execute(query_count, (nit, factura, nombre_item, registro['ID_dp']))
        count_existentes = cur.fetchone()[0]

        # 2. Manejo seguro de los Splits (convirtiendo None a lista vacía)
        lista_compra = val_orden_de_compra.split('|') if val_orden_de_compra else []
        lista_xml = valor_xml.split('|') if valor_xml else []
        lista_aprob = valor_aprobado.split('|') if valor_aprobado else []
        
        # En caso de que valor_aprobado sea una lista y no un string
        if isinstance(valor_aprobado, list):
             lista_aprob = valor_aprobado

        # 3. Obtener el máximo conteo
        maximo_conteo = max(len(lista_compra), len(lista_xml), len(lista_aprob))
        count_nuevos = maximo_conteo # Corregido: Ya es un entero, no necesita len()
        
        maximo_conteo = 1 if maximo_conteo == 0 else maximo_conteo

        # Iterar sobre los valores nuevos para actualizar o insertar
        for i in range(maximo_conteo):
            # Extraer item de forma segura (si no existe índice, es None)
            item_compra = lista_compra[i] if i < len(lista_compra) else None
            item_xml = lista_xml[i] if i < len(lista_xml) else None
            item_aprob = lista_aprob[i] if i < len(lista_aprob) else None

            # Limpiar valores para la BD
            val_compra = safe_db_val(item_compra)
            val_xml = safe_db_val(item_xml)
            val_aprob = safe_db_val(item_aprob)

            if i < count_existentes:
                # UPDATE: Construcción dinámica de la consulta
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

                # Si no hay nada que actualizar, continuamos al siguiente ciclo
                if not set_clauses:
                    continue

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
                # Los parámetros del WHERE van antes del rn
                final_params = params + [nit, factura, nombre_item, registro['ID_dp'], i + 1]
                cur.execute(update_query, final_params)

            else:
                # INSERT: Corregido el número de parámetros (ahora son 7)
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra,
                    Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(insert_query, (registro['Fecha_de_retoma_antes_de_contabilizacion_dp'],registro['documenttype_dp'],registro['numero_de_liquidacion_u_orden_de_compra_dp'],registro['nombre_emisor_dp'], registro['ID_dp'], nit, factura, nombre_item, val_compra, val_xml, val_aprob))
        
        cur.close()
        print(f"[PROCESADO] Item '{nombre_item}' - {count_nuevos} valor(es) enviados (Existían: {count_existentes})")
    
    def actualizar_estado_comparativa(cx, nit, factura, estado):
        """
        Actualiza el Estado_validacion_antes_de_eventos en CxP_Comparativa.
        
        Args:
            cx: Conexión a BD
            nit: NIT del proveedor
            factura: Número de factura
            estado: Estado a establecer
        """
        cur = cx.cursor()
        update_sql = """
        UPDATE [dbo].[CxP.Comparativa]
        SET Estado_validacion_antes_de_eventos = ?
        WHERE NIT = ? AND Factura = ?
        """
        cur.execute(update_sql, (estado, nit, factura))
        cur.close()
        print(f"[UPDATE] Estado comparativa: {estado}")
    
    def marcar_orden_procesada(cx, oc_numero, posiciones_string):
        """
        Actualiza la tabla a 'PROCESADO' para una OC específica y sus posiciones.
        Ejemplo: marcar_orden_procesada(cx, '4300449290', '00010|00020|00030|00040')
        """
        cur = cx.cursor()
        
        # 1. Hacemos el split de las posiciones
        lista_posiciones = posiciones_string.split('|')
        
        # 2. Preparamos el SQL de actualización
        update_query = """
        UPDATE [CxP].[HistoricoOrdenesCompra]
        SET Marca = 'PROCESADO'
        WHERE DocCompra = ? AND Posicion = ?
        """
        
        # 3. Iteramos solo por las posiciones que llegaron y ejecutamos el UPDATE
        for posicion in lista_posiciones:
            # Quitamos espacios en blanco por seguridad
            pos = posicion.strip() 
            
            if pos: # Validamos que no esté vacío
                cur.execute(update_query, (oc_numero, pos))
                
        # Guardamos los cambios en la base de datos
        cx.commit() 
        cur.close()
        
        print(f"✅ OC {oc_numero}: Se marcaron {len(lista_posiciones)} posiciones como 'PROCESADO'.")
    
    # =========================================================================
    # FUNCIONES DE PROCESAMIENTO DE POSICIONES
    # =========================================================================
    
    def expandir_posiciones_string(valor_string, separador='|'):
        """Expande valores separados por | o comas."""
        if pd.isna(valor_string) or valor_string == '' or valor_string is None:
            return []
        
        valor_str = safe_str(valor_string)
        
        if '|' in valor_str:
            return [v.strip() for v in valor_str.split('|') if v.strip()]
        
        if ',' in valor_str:
            return [v.strip() for v in valor_str.split(',') if v.strip()]
        
        return [valor_str.strip()]
    
    def expandir_posiciones_historico(registro):
        """Expande las posiciones del histórico que están concatenadas."""
        try:
            posiciones = expandir_posiciones_string(registro.get('Posicion_hoc', ''))
            
            if not posiciones:
                return []
            
            por_calcular = expandir_posiciones_string(registro.get('PorCalcular_hoc', ''))
            trm_list = expandir_posiciones_string(registro.get('Trm_hoc', ''))
            tipo_nif_list = expandir_posiciones_string(registro.get('TipoNif_hoc', ''))
            acreedor_list = expandir_posiciones_string(registro.get('Acreedor_hoc', ''))
            fec_doc_list = expandir_posiciones_string(registro.get('FecDoc_hoc', ''))
            fec_reg_list = expandir_posiciones_string(registro.get('FecReg_hoc', ''))
            fec_cont_gasto_list = expandir_posiciones_string(registro.get('FecContGasto_hoc', ''))
            ind_impuestos_list = expandir_posiciones_string(registro.get('IndicadorImpuestos_hoc', ''))
            texto_breve_list = expandir_posiciones_string(registro.get('TextoBreve_hoc', ''))
            clase_impuesto_list = expandir_posiciones_string(registro.get('ClaseDeImpuesto_hoc', ''))
            cuenta_list = expandir_posiciones_string(registro.get('Cuenta_hoc', ''))
            poblacion_servicio_list = expandir_posiciones_string(registro.get('PoblacionServicio_hoc', ''))
            doc_fi_entrada_list = expandir_posiciones_string(registro.get('DocFiEntrada_hoc', ''))
            cuenta26_list = expandir_posiciones_string(registro.get('Cuenta26_hoc', ''))
            activo_fijo_list = expandir_posiciones_string(registro.get('ActivoFijo_hoc', ''))
            orden_list = expandir_posiciones_string(registro.get('Orden_hoc', ''))
            centro_coste_list = expandir_posiciones_string(registro.get('CentroCoste_hoc', ''))
            clase_orden_list = expandir_posiciones_string(registro.get('ClaseOrden_hoc', ''))
            elemento_pep_list = expandir_posiciones_string(registro.get('ElementoPEP_hoc', ''))
            emplazamiento_list = expandir_posiciones_string(registro.get('Emplazamiento_hoc', ''))
            moneda_list = expandir_posiciones_string(registro.get('Moneda_hoc', ''))
            
            n_proveedor = safe_str(registro.get('NProveedor_hoc', ''))
            
            datos_posiciones = []
            
            for i, posicion in enumerate(posiciones):
                datos_pos = {
                    'Posicion': posicion,
                    'PorCalcular': por_calcular[i] if i < len(por_calcular) else '',
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
                    'PoblacionServicio': poblacion_servicio_list[i] if i < len(poblacion_servicio_list) else (poblacion_servicio_list[0] if poblacion_servicio_list else ''),
                    'DocFiEntrada': doc_fi_entrada_list[i] if i < len(doc_fi_entrada_list) else (doc_fi_entrada_list[0] if doc_fi_entrada_list else ''),
                    'Cuenta26': cuenta26_list[i] if i < len(cuenta26_list) else (cuenta26_list[0] if cuenta26_list else ''),
                    'ActivoFijo': activo_fijo_list[i] if i < len(activo_fijo_list) else (activo_fijo_list[0] if activo_fijo_list else ''),
                    'Orden': orden_list[i] if i < len(orden_list) else (orden_list[0] if orden_list else ''),
                    'CentroCoste': centro_coste_list[i] if i < len(centro_coste_list) else (centro_coste_list[0] if centro_coste_list else ''),
                    'ClaseOrden': clase_orden_list[i] if i < len(clase_orden_list) else (clase_orden_list[0] if clase_orden_list else ''),
                    'ElementoPEP': elemento_pep_list[i] if i < len(elemento_pep_list) else (elemento_pep_list[0] if elemento_pep_list else ''),
                    'Emplazamiento': emplazamiento_list[i] if i < len(emplazamiento_list) else (emplazamiento_list[0] if emplazamiento_list else ''),
                    'Moneda': moneda_list[i] if i < len(moneda_list) else (moneda_list[0] if moneda_list else '')
                }
                datos_posiciones.append(datos_pos)
            
            return datos_posiciones
            
        except Exception as e:
            print(f"[ERROR] Error expandiendo posiciones del historico: {str(e)}")
            return []
    
    # =========================================================================
    # FUNCIONES DE CARGA DE ARCHIVO IMPUESTOS ESPECIALES
    # =========================================================================
    

    def cargar_archivo_impuestos_especiales(ruta_archivo):
        """
        Carga el archivo Impuestos especiales CXP.xlsx, valida su estructura completa
        y extrae el mapeo de la hoja IVA_CECO.
        """
        try:
            if not os.path.exists(ruta_archivo):
                print(f"[WARNING] Archivo no encontrado: {ruta_archivo}")
                raise Exception(f'Archivo no encontrado: {ruta_archivo}') 
            
            # 1. VALIDACIÓN DE ESTRUCTURA (Hojas requeridas)
            xls = pd.ExcelFile(ruta_archivo)
            hojas_requeridas = ['TRIBUTO', 'TARIFAS ESPECIALES', 'IVA CECO']
            hojas_faltantes = [h for h in hojas_requeridas if h not in xls.sheet_names]
            
            if hojas_faltantes:
                print(f"[WARNING] Estructura inválida. Hojas faltantes: {hojas_faltantes}")
                raise Exception(f'Estructura inválida. Hojas faltantes: {hojas_faltantes}') 
            
            df_iva_ceco = pd.read_excel(xls, sheet_name='IVA CECO')
            df_iva_ceco.columns = df_iva_ceco.columns.str.strip()
            
            # 2. VALIDACIÓN DE COLUMNAS (Buscar las requeridas exactas)
            col_ceco = None
            col_codigo_iva = None
            
            for col in df_iva_ceco.columns:
                col_upper = col.upper()
                if 'CECO' in col_upper and 'NOMBRE' not in col_upper:
                    col_ceco = col
                if 'CÓDIGO IND. IVA APLICABLE' in col_upper or ('CODIGO' in col_upper and 'IVA' in col_upper and 'APLICABLE' in col_upper):
                    col_codigo_iva = col
            
            if not col_ceco or not col_codigo_iva:
                print("[WARNING] Columnas requeridas no encontradas en IVA_CECO")
                raise Exception(f'Columnas requeridas no encontradas en IVA_CECO')
                
            # Crear diccionario de mapeo
            mapeo_ceco = {}
            for _, row in df_iva_ceco.iterrows():
                ceco = safe_str(row[col_ceco])
                codigo_iva = safe_str(row[col_codigo_iva])
                
                if ceco and codigo_iva:
                    # Separar por guión o coma y limpiar espacios
                    indicadores = [ind.strip().upper() for ind in codigo_iva.replace('-', ',').split(',') if ind.strip()]
                    mapeo_ceco[ceco.upper()] = indicadores
                    
            print(f"[INFO] Archivo Impuestos cargado: {len(mapeo_ceco)} CECOs")
            return mapeo_ceco
            
        except Exception as e:
            print(f"[ERROR] Error cargando archivo: {str(e)}")
            raise Exception(traceback.format_exc())
    
    # =========================================================================
    # PROCESAMIENTO PRINCIPAL
    # =========================================================================
    
    try:
        print("")
        print("=" * 80)
        print("[INICIO] Procesamiento ZPSA/ZPSS/43 - Pedidos de Servicios")
        print("=" * 80)
        
        t_inicio = time.time()
        
        # 1. Obtener y validar configuración
        #####################
        #cfg = parse_config(GetVar("vLocDicConfig"))
        cfg = {}
        cfg['ServidorBaseDatos'] = 'localhost\SQLEXPRESS'
        cfg['NombreBaseDatos'] = 'NotificationsPaddy'
        cfg['UsuarioBaseDatos'] = 'aa'
        cfg['ClaveBaseDatos'] = 'aa'
        cfg['RutaImpuestosEspeciales'] = r'C:\Users\diego\Documents\GitHub\CxP\INSUMOS\Impuestos especiales CXP.xlsx'
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        # cfg[''] = 
        
        print("[INFO] Configuracion cargada exitosamente")
        
        required_config = ['ServidorBaseDatos', 'NombreBaseDatos']
        missing_config = [k for k in required_config if not cfg.get(k)]
        if missing_config:
            raise ValueError(f"Faltan parametros de configuracion: {', '.join(missing_config)}")
        
        # Cargar archivo de impuestos especiales si está configurado
        ruta_impuestos = cfg.get('RutaImpuestosEspeciales', '')
        mapeo_ceco_impuestos = None
        if ruta_impuestos:
            try:
                mapeo_ceco_impuestos = cargar_archivo_impuestos_especiales(ruta_impuestos)
            except Exception as e:
                ""
                #SetVar("vGblStrDetalleError", traceback.format_exc())
                #SetVar("vGblStrSystemError", "ErrorHU4_4.1")
                #SetVar("vLocStrResultadoSP", "False")
                return
                
        
        # 2. Conectar a base de datos
        with crear_conexion_db(cfg) as cx:
            print("[INFO] Obteniendo registros ZPSA/ZPSS/43 para procesar...")
            
            query_zpsa = """
                SELECT * FROM [CxP].[HU41_CandidatosValidacion]
                WHERE [ClaseDePedido_hoc] IN ('ZPSA', 'ZPSS', '43')
            """
            
            df_registros = pd.read_sql(query_zpsa, cx)
            
            print(f"[INFO] Obtenidos {len(df_registros)} registros ZPSA/ZPSS/43 para procesar")
            
            if len(df_registros) == 0:
                print("[INFO] No hay registros ZPSA/ZPSS/43 pendientes de procesar")
                #SetVar("vLocStrResultadoSP", "True")
                #SetVar("vLocStrResumenSP", "No hay registros ZPSA/ZPSS/43 pendientes de procesar")
                return
            
            # Variables de conteo
            registros_procesados = 0
            registros_con_novedad = 0
            registros_exitosos = 0
            
            # 3. Procesar cada registro
            for idx, registro in df_registros.iterrows():
                registro_id = safe_str(registro.get('ID_dp', ''))
                numero_oc = safe_str(registro.get('numero_de_liquidacion_u_orden_de_compra_dp', ''))
                numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
                payment_means = safe_str(registro.get('forma_de_pago_dp', ''))
                nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
                clase_pedido = safe_str(registro.get('ClaseDePedido_hoc', '')).upper()
                
                # Determinar etiqueta para mensajes (ZPSA o ZPSS)
                tipo_pedido = 'ZPSA' if clase_pedido in ('ZPSA', '43') else 'ZPSS'
                
                print(f"\n[PROCESO] Registro {registros_procesados + 1}/{len(df_registros)}: OC {numero_oc}, Factura {numero_factura}, Tipo {tipo_pedido}")
                
                sufijo_contado = " CONTADO" if payment_means in ["01", "1"] else ""
                
                # 4. Expandir posiciones del histórico
                datos_posiciones = expandir_posiciones_historico(registro)
                
                # 5. Determinar si es USD
                es_usd = True if 'USD' in safe_str(registro['Moneda_hoc']).upper() else False
                
                # 6. Obtener valor a comparar según moneda
                if es_usd:
                    valor_xml = normalizar_decimal(registro.get('VlrPagarCop_dp', 0))
                else:
                    valor_xml = normalizar_decimal(registro.get('Valor de la Compra LEA_ddp', 0))
                
                # 7. Buscar combinación de posiciones (USANDO LOGICA ZPAF: COMPARAR SUMA TOTAL)
                valores_por_calcular = [(d['Posicion'], d['PorCalcular']) for d in datos_posiciones]
                
                coincidencia_encontrada, posiciones_usadas, suma_encontrada = comparar_suma_total(
                    valores_por_calcular, valor_xml, tolerancia=500
                )
                
                if not coincidencia_encontrada:
                    print(f"[INFO] No se encuentra coincidencia del valor a pagar para OC {numero_oc}")
                    observacion = f"No se encuentra coincidencia del Valor a pagar de la factura, {registro['ObservacionesFase_4_dp']}"
                    resultado_final = f"CON NOVEDAD {sufijo_contado}"
                    
                    campos_novedad = {
                        'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                        'ObservacionesFase_4': truncar_observacion(observacion),
                        'ResultadoFinalAntesEventos': resultado_final
                    }
                    actualizar_bd_cxp(cx, registro_id, campos_novedad)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='LineExtensionAmount',
                                                valor_xml=registro['valor_a_pagar_dp'], valor_aprobado=None, val_orden_de_compra=None)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Observaciones',
                                                valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='VlrPagarCop',
                                                valor_xml=registro['VlrPagarCop_dp'], valor_aprobado='NO', val_orden_de_compra='NO ENCONTRADO')
                    
                    actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                    
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    
                    registros_con_novedad += 1
                    registros_procesados += 1
                    continue
                
                else:
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='LineExtensionAmount',
                                                valor_xml=registro['valor_a_pagar_dp'], valor_aprobado=None, val_orden_de_compra=None)
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Posicion',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Posicion_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='ValorPorCalcularSAP',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['PorCalcular_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TipoNIF',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['TipoNif_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Acreedor',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Acreedor_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='FecDoc',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['FecDoc_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='FecReg',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['FecReg_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='FechaContGasto',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['FecContGasto_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='IndicadorImpuestos',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TextoBreve',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Texto_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='ClaseImpuesto',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['ClaseDeImpuesto_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Cuenta',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Cuenta_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CiudadProveedor',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['CiudadProveedor_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='DocFIEntrada',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['DocFiEntrada_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CTA26',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['Cuenta26_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='ActivoFijo',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['ActivoFijo_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CapitalizadoEl',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['CapitalizadoEl_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CriterioClasif2',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro['CriterioClasif2_hoc'])
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='LineExtensionAmount',
                                                valor_xml=None, valor_aprobado='SI', val_orden_de_compra=None)
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    
                    # Variable para rastrear si hubo alguna novedad
                    hay_novedad = False
                
                    print(f"[DEBUG] Coincidencia encontrada con posiciones: {posiciones_usadas}")
                
                # 9. Validar TRM (LOGICA ZPAF: 0.01 tolerancia)
                print(f"[DEBUG] Validando TRM...")
                
                trm_xml = normalizar_decimal(registro.get('CalculationRate_dp', 0))
                # 1. Obtenemos el texto de forma segura (si es None, trae un texto vacío '')
                texto_trm_sap = registro.get('Trm_hoc', '')

                # 2. Hacemos el split y tomamos el primero (si el texto está vacío, usamos 0)
                primer_trm_sap = texto_trm_sap.split('|')[0] if texto_trm_sap else 0

                # 3. Normalizamos
                trm_sap = normalizar_decimal(primer_trm_sap)
                
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
                
                # 10. Validar Nombre Emisor (LOGICA ZPAF)
                print(f"[DEBUG] Validando nombre emisor...")
                
                nombre_emisor_xml = safe_str(registro.get('nombre_emisor_dp', ''))

                # 1. Obtenemos el texto completo
                texto_sap = registro.get('NProveedor_hoc', '')

                # 2. Lo dividimos y tomamos el primero (si hay texto, sino usamos vacío '')
                primer_nombre_sap = texto_sap.split('|')[0] if texto_sap else ''

                # 3. Lo limpiamos con tu función
                nombre_proveedor_sap = safe_str(primer_nombre_sap)
                
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
                    
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                
                # =========================================================
                # 11. VALIDACIONES ESPECÍFICAS SEGÚN ORDEN/ELEMENTO PEP/ACTIVO FIJO
                # =========================================================
                
                # Determinar qué ruta de VALIDACION seguir
                ListaOrden = registro['Orden_hoc'].split('|') if registro['Orden_hoc'] else []
                ListaPeP = registro['ElementoPEP_hoc'].split('|') if registro['ElementoPEP_hoc'] else []
                ListaActivoFijo = registro['ActivoFijo_hoc'].split('|') if registro['ActivoFijo_hoc'] else []
                ListaIndicador = registro['IndicadorImpuestos_hoc'].split('|') if registro['IndicadorImpuestos_hoc'] else []
                ListaCentroCoste = registro['CentroDeCoste_hoc'].split('|') if registro['CentroDeCoste_hoc'] else []
                ListaCuenta = registro['Cuenta_hoc'].split('|') if registro['Cuenta_hoc'] else []
                ListaClaseOrden = registro['ClaseDeOrden_hoc'].split('|') if registro['ClaseDeOrden_hoc'] else []
                ListaEmplazamiento = registro['Emplazamiento_hoc'].split('|') if registro['Emplazamiento_hoc'] else []
                
                tiene_orden = any(campo_con_valor(d) for d in ListaOrden)
                tiene_elemento_pep = any(campo_con_valor(d) for d in ListaPeP)
                tiene_activo_fijo = any(campo_con_valor(d) for d in ListaActivoFijo)
                
                print(f"[DEBUG] Tiene Orden: {tiene_orden}, Elemento PEP: {tiene_elemento_pep}, Activo Fijo: {tiene_activo_fijo}")
                
                # =========================================================
                # RUTA A: TIENE ORDEN
                # =========================================================
                if tiene_orden:
                    # Obtener primer valor de Orden para determinar tipo
                    indicador_valido = True
                    
                    for item in ListaOrden:
                        if indicador_valido:
                            orden_valor = safe_str(item)
                            orden_limpio = re.sub(r'\D', '', orden_valor)  # Solo dígitos
                            
                            print(f"[DEBUG] Orden valor: {orden_valor}, limpio: {orden_limpio}")
                            
                            # Determinar si inicia con 15
                        
                            if orden_limpio.startswith('15') and len(orden_limpio) == 9:
                                print(f"[DEBUG] Orden inicia con 15 (9 dígitos)")
                                
                                # ORDEN 15: Validar Indicador impuestos
                                aprobados_indicador = []
                                
                                for d in ListaIndicador:
                                    if validar_indicador_servicios_orden15(d):
                                        aprobados_indicador.append('SI')
                                    else:
                                        aprobados_indicador.append('NO')
                                        indicador_valido = False
                                
                                if not indicador_valido:
                                    if all(campo_vacio(ind) for ind in ListaIndicador):
                                        observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero campo "Indicador impuestos" NO se encuentra diligenciado, {registro['ObservacionesFase_4_dp']}'
                                    else:
                                        observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero campo "Indicador impuestos" NO corresponde alguna de las opciones H4, H5, H6, H7, VP, CO, IC, CR, {registro['ObservacionesFase_4_dp']}'
                                    hay_novedad = True
                                    resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                    
                                    campos_novedad_ind = {
                                        'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                        'ObservacionesFase_4': truncar_observacion(observacion),
                                        'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                    }
                                    
                                    actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                                    
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Observaciones',
                                        valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                    
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Indicador impuestos',
                                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                                    
                                    actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                    
                                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                    break
                                else:
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Indicador impuestos',
                                        valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                                    
                                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                
                                # ORDEN 15: Centro de coste debe estar VACÍO
                                aprobados_centro = []
                                indicador_valido = True
                                
                                for d in ListaCentroCoste:
                                    if campo_vacio(d):
                                        aprobados_centro.append('SI')
                                    else:
                                        aprobados_centro.append('NO')
                                        indicador_valido = False
                                
                                if not indicador_valido:
                                    observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo "Centro de coste" se encuentra diligenciado cuando NO debe estarlo, {registro['ObservacionesFase_4_dp']}'
                                    hay_novedad = True
                                    resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                    
                                    campos_novedad_centro = {
                                        'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                        'ObservacionesFase_4': truncar_observacion(observacion),
                                        'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                                    }
                                    
                                    actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)

                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='CentroCoste',
                                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                                    
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Observaciones',
                                        valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                    
                                    actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                    
                                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                    break
                                else:
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='CentroCoste',
                                        valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                                    
                                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                    
                                
                                # ORDEN 15: Cuenta debe ser 5199150001
                                aprobados_cuenta = []
                                cuenta_valida = True
                                
                                for d in ListaCuenta:
                                    if d.strip() == '5199150001':
                                        aprobados_cuenta.append('SI')
                                    else:
                                        aprobados_cuenta.append('NO')
                                        cuenta_valida = False
                                
                                if not cuenta_valida:
                                    observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo "Cuenta" es diferente a 5199150001, {registro['ObservacionesFase_4_dp']}'
                                    hay_novedad = True
                                    resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                    
                                    campos_novedad_cuenta = {
                                        'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                        'ObservacionesFase_4': truncar_observacion(observacion),
                                        'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                    }
                                    actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                                    
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Cuenta',
                                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['Cuenta_hoc'])
                                    
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Observaciones',
                                        valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                    
                                    actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                    
                                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                    break
                                
                                else:
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Cuenta',
                                        valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Cuenta_hoc'])
                                    
                                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                
                                # ORDEN 15: Clase orden según indicador
                                aprobados_clase = []
                                clase_valida = True
                                
                                for indicador, clase_ord in zip_longest(ListaIndicador, ListaClaseOrden):

                                    # Validamos usando .strip() para limpiar posibles espacios en blanco
                                    es_valido, msg = validar_clase_orden(indicador.strip(), clase_ord.strip())
                                    
                                    if es_valido:
                                        aprobados_clase.append('SI')
                                    else:
                                        aprobados_clase.append('NO')
                                        clase_valida = False
                                
                                if not clase_valida:
                                    clases = [safe_str(d) for d in ListaClaseOrden]
                                    if all(campo_vacio(c) for c in clases):
                                        observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo "Clase orden" NO se encuentra diligenciado, {registro['ObservacionesFase_4_dp']}'
                                    else:
                                        observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo "Clase orden" NO se encuentra aplicado correctamente segun reglas "H4 y H5 = ZINV", "H6 y H7 = ZADM" o "VP, CO, CR o IC = ZINV o ZADM", {registro['ObservacionesFase_4_dp']}'
                                    hay_novedad = True
                                    resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                    
                                    campos_novedad_clase = {
                                        'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                        'ObservacionesFase_4': truncar_observacion(observacion),
                                        'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                    }
                                    actualizar_bd_cxp(cx, registro_id, campos_novedad_clase)
                                    
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Cuenta',
                                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['Cuenta_hoc'])
                                    
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Observaciones',
                                        valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                    
                                    actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                    
                                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                    break
                                
                                else:
                                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Cuenta',
                                        valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Cuenta_hoc'])
                                    
                                    marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                    
                                #########################################################################################################################################################################
                                    
                            
                            else:
                                # ORDEN NO INICIA CON 15
                                # Determinar si es 53 (ESTADÍSTICAS) o diferente
                                if orden_limpio.startswith('53') and len(orden_limpio) == 8:
                                    print(f"[DEBUG] Orden inicia con 53 (ESTADISTICAS)")
                                    
                                    # ORDEN 53: Centro de coste debe CONTENER VALOR
                                    aprobados_centro = []
                                    centro_valido = True
                                    
                                    for d in ListaCentroCoste:
                                        if campo_vacio(d):
                                            aprobados_centro.append('SI')
                                            indicador_valido = False
                                        else:
                                            aprobados_centro.append('NO')
                                            
                                    
                                    if not centro_valido:
                                        observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 53, pero Campo "Centro de coste" se encuentra vacio para pedidos ESTADISTICAS, {registro['ObservacionesFase_4_dp']}'
                                        hay_novedad = True
                                        
                                        campos_novedad_centro = {
                                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                            'ObservacionesFase_4': truncar_observacion(observacion),
                                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                        }
                                        actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                                        
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='CentroCoste',
                                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                                    
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Observaciones',
                                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                        
                                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                        break
                                    else:
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='CentroCoste',
                                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                                        
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                
                                else:
                                    print(f"[DEBUG] Orden diferente a 15 y 53 (NO ESTADISTICAS)")
                                    
                                    # ORDEN DIFERENTE: Centro de coste debe estar VACÍO
                                    aprobados_centro = []
                                    centro_valido = True
                                    
                                    for d in ListaCentroCoste:
                                        if campo_vacio(d):
                                            aprobados_centro.append('SI')
                                            
                                        else:
                                            aprobados_centro.append('NO')
                                            indicador_valido = False
                                    
                                    if not centro_valido:
                                        observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo "Centro de coste" se encuentra diligenciado para pedidos NO ESTADISTICAS, {registro['ObservacionesFase_4_dp']}'
                                        hay_novedad = True
                                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                        
                                        campos_novedad_centro = {
                                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                            'ObservacionesFase_4': truncar_observacion(observacion),
                                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                        }
                                        actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                                        
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='CentroCoste',
                                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                                    
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Observaciones',
                                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                        
                                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                        break
                                    else:
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='CentroCoste',
                                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                                        
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                
                                    
                                    # ORDEN DIFERENTE: Cuenta debe ser 5299150099 O iniciar con 7 y 10 dígitos
                                    aprobados_cuenta = []
                                    cuenta_valida = True
                                    
                                    for d in ListaCuenta:
                                        cuenta = safe_str(d).strip()
                                        if validar_cuenta_orden_no_15(cuenta):
                                            aprobados_cuenta.append('SI')
                                        else:
                                            aprobados_cuenta.append('NO')
                                            cuenta_valida = False
                                    
                                    if not cuenta_valida:
                                        observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo "Cuenta" es diferente a 5299150099 y/o NO cumple regla "inicia con "7" y tiene 10 digitos", {registro['ObservacionesFase_4_dp']}'
                                        hay_novedad = True
                                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                        
                                        campos_novedad_cuenta = {
                                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                            'ObservacionesFase_4': truncar_observacion(observacion),
                                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                        }
                                        actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                                        
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Cuenta',
                                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['Cuenta_hoc'])
                                    
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Observaciones',
                                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                        
                                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                        break
                                    
                                    else:
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                            nombre_item='Cuenta',
                                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Cuenta_hoc'])
                                        
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                                    
                
                # =========================================================
                # RUTA B: TIENE ELEMENTO PEP (y no tiene Orden)
                # =========================================================
                if tiene_elemento_pep:
                    print(f"[DEBUG] Validando Elemento PEP")
                    
                    # Indicador impuestos: H4, H5, H6, H7, VP, CO, IC, CR
                    aprobados_indicador = []
                    indicador_valido = True
                    
                    for d in ListaIndicador:
                        if validar_indicador_servicios_orden15(d):
                            aprobados_indicador.append('SI')
                        else:
                            aprobados_indicador.append('NO')
                            indicador_valido = False
                    
                    if not indicador_valido:
                        indicadores_actual = ListaIndicador
                        if all(campo_vacio(ind) for ind in indicadores_actual):
                            observacion = f'Pedido corresponde a {tipo_pedido} con Elemento PEP, pero campo "Indicador impuestos" NO se encuentra diligenciado, {registro['ObservacionesFase_4_dp']}'
                        else:
                            observacion = f'Pedido corresponde a {tipo_pedido} con Elemento PEP, pero campo "Indicador impuestos" NO corresponde alguna de las opciones H4, H5, H6, H7, VP, CO, IC, CR, {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        
                        campos_novedad_ind = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Observaciones',
                                        valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Indicador impuestos',
                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        break
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Indicador impuestos',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                
                    
                    # Centro de coste debe estar VACÍO
                    aprobados_centro = []
                    centro_valido = True
                    
                    for d in ListaCentroCoste:
                        if campo_vacio(d):
                            aprobados_centro.append('SI')
                            
                        else:
                            aprobados_centro.append('NO')
                            indicador_valido = False
                    
                    if not centro_valido:
                        observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo "Centro de coste" se encuentra diligenciado para pedidos NO ESTADISTICAS, {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        
                        campos_novedad_centro = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                        nombre_item='CentroCoste',
                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Observaciones',
                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        break
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='CentroCoste',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    
                    # Cuenta debe ser 5199150001
                    aprobados_cuenta = []
                    cuenta_valida = True
                    
                    for d in ListaCuenta:
                        cuenta = safe_str(d).strip()
                        if cuenta == '5199150001':
                            aprobados_cuenta.append('SI')
                        else:
                            aprobados_cuenta.append('NO')
                            cuenta_valida = False
                    
                    if not cuenta_valida:
                        observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo "Cuenta" es diferente a 5299150099 y/o NO cumple regla "inicia con "7" y tiene 10 digitos", {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        
                        campos_novedad_cuenta = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                        nombre_item='Cuenta',
                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['Cuenta_hoc'])
                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Observaciones',
                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        break
                    
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Cuenta',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Cuenta_hoc'])
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    
                    # Emplazamiento según indicador
                    aprobados_empl = []
                    empl_valido = True
                    
                    for emplazamiento, indicador in zip_longest(ListaEmplazamiento, ListaIndicador):
                        es_valido, msg = validar_emplazamiento(indicador, emplazamiento)
                        if es_valido:
                            aprobados_empl.append('SI')
                        else:
                            aprobados_empl.append('NO')
                            empl_valido = False
                    
                    if not empl_valido:
                        empls = ListaEmplazamiento
                        if all(campo_vacio(e) for e in empls):
                            observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Elemento PEP, pero Campo "Emplazamiento" NO se encuentra diligenciado, {registro['ObservacionesFase_4_dp']}'
                        else:
                            observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Elemento PEP, pero Campo "Emplazamiento" NO se encuentra aplicado correctamente segun reglas "H4 y H5 = DCTO_01", "H6 y H7 = GTO_02" o "VP, CO, CR o IC = DCTO_01 o GTO_02", {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        
                        campos_novedad_empl = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_empl)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                        nombre_item='Emplazamiento',
                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['Emplazamiento_hoc'])
                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Observaciones',
                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        break
                    
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Emplazamiento',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Emplazamiento_hoc'])
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    
                
                # =========================================================
                # RUTA C: TIENE ACTIVO FIJO (y no tiene Orden ni Elemento PEP)
                # =========================================================
                if tiene_activo_fijo:
                    print(f"[DEBUG] Validando Activo Fijo")
                    
                    es_diferido = True
                    aprobadosdiferido = []
                    # Determinar si es DIFERIDO (inicia con 2000 y 10 dígitos)
                    for activofijo in ListaActivoFijo:
                        activo_limpio = re.sub(r'\D', '', activofijo)
                        if activo_limpio.startswith('2000') and len(activo_limpio) == 10:
                            aprobadosdiferido.append('SI')
                        else:
                            aprobadosdiferido.append('NO')
                            es_diferido = False
                    
                    if es_diferido:
                        print(f"[DEBUG] Activo Fijo DIFERIDO (2000)")
                        
                        # DIFERIDO: Indicador impuestos: C1, FA, VP, CO, CR
                        aprobados_indicador = []
                        indicador_valido = True
                        
                        for d in ListaIndicador:
                            if validar_indicador_diferido(d):
                                aprobados_indicador.append('SI')
                            else:
                                aprobados_indicador.append('NO')
                                indicador_valido = False
                        
                        if not indicador_valido:
                            if all(campo_vacio(ind) for ind in indicadores_actual):
                                observacion = f'Pedido corresponde a {tipo_pedido} con Activo Fijo, pero campo "Indicador impuestos" NO se encuentra diligenciado para pedido DIFERIDO, {registro['ObservacionesFase_4_dp']}'
                            else:
                                observacion = f'Pedido corresponde a {tipo_pedido} con Activo Fijo, pero campo "Indicador impuestos" NO corresponde alguna de las opciones "C1", "FA", "VP", "CO" o "CR" para pedido DIFERIDO, {registro['ObservacionesFase_4_dp']}'
                            hay_novedad = True
                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                            
                            campos_novedad_ind = {
                                'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                            
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='IndicadorImpuestos',
                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                        
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                nombre_item='Observaciones',
                                valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                            
                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
            
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                            break
                        
                        else:
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                nombre_item='IndicadorImpuestos',
                                valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                            
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        
                        
                        # DIFERIDO: Centro de coste debe estar VACÍO
                        aprobados_centro = []
                        centro_valido = True
                        
                        for d in ListaCentroCoste:
                            if campo_vacio(d):
                                aprobados_centro.append('SI')
                            else:
                                aprobados_centro.append('NO')
                                centro_valido = False
                        
                        if not centro_valido:
                            observacion = f'Pedido corresponde a {tipo_pedido} con Activo Fijo, pero Campo "Centro de coste" se encuentra diligenciado cuando NO debe estarlo para pedido DIFERIDO, {registro['ObservacionesFase_4_dp']}'
                            hay_novedad = True
                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                            
                            campos_novedad_centro = {
                                'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                            
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='CentroCoste',
                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                        
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                nombre_item='Observaciones',
                                valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                            
                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
            
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                            break
                        else:
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                nombre_item='CentroCoste',
                                valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                            
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        
                        
                        # DIFERIDO: Cuenta debe estar VACÍO
                        aprobados_cuenta = []
                        cuenta_valida = True
                        
                        for d in ListaCuenta:
                            if campo_vacio(d):
                                aprobados_cuenta.append('SI')
                            else:
                                aprobados_cuenta.append('NO')
                                cuenta_valida = False
                        
                        if not cuenta_valida:
                            observacion = f'Pedido corresponde a {tipo_pedido} con Activo Fijo, pero Campo "Cuenta" se encuentra diligenciado cuando NO debe estarlo para pedido DIFERIDO, {registro['ObservacionesFase_4_dp']}'
                            hay_novedad = True
                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                            
                            campos_novedad_cuenta = {
                                'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                            
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Cuenta',
                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['Cuenta_hoc'])
                        
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                nombre_item='Observaciones',
                                valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                            
                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
            
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                            break
                        
                        else:
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                nombre_item='Cuenta',
                                valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Cuenta_hoc'])
                            
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                
                # =========================================================
                # RUTA D: NO TIENE ORDEN, ELEMENTO PEP NI ACTIVO FIJO (GENERALES)
                # =========================================================
                else:
                    print(f"[DEBUG] Validando como GENERALES (sin Orden, Elemento PEP ni Activo Fijo)")
                    
                    # GENERALES: Cuenta debe estar diligenciada
                    aprobados_cuenta = []
                    cuenta_valida = True
                    
                    for d in ListaCuenta:
                        if campo_con_valor(d):
                            aprobados_cuenta.append('SI')
                        else:
                            aprobados_cuenta.append('NO')
                            cuenta_valida = False
                    
                    if not cuenta_valida:
                        observacion = f'Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero Campo "Cuenta" NO se encuentra diligenciado cuando debe estarlo para pedido GENERALES, {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        
                        campos_novedad_cuenta = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                        nombre_item='Cuenta',
                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['Cuenta_hoc'])
                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Observaciones',
                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        break
                    
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Cuenta',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Cuenta_hoc'])
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    
                    # GENERALES: Indicador impuestos debe estar diligenciado
                    aprobados_indicador = []
                    indicador_valido = True
                    
                    for d in ListaIndicador:
                        if campo_con_valor(d):
                            aprobados_indicador.append('SI')
                        else:
                            aprobados_indicador.append('NO')
                            indicador_valido = False
                    
                    if not indicador_valido:
                        observacion = f'Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero campo "Indicador impuestos" NO se encuentra diligenciado para pedido GENERALES, {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        
                        campos_novedad_ind = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Observaciones',
                                        valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Indicador impuestos',
                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        break
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Indicador impuestos',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    
                    # GENERALES: Centro de coste debe estar diligenciado
                    aprobados_centro = []
                    centro_valido = True
                    
                    for d in ListaCentroCoste:
                        if campo_con_valor(d):
                            aprobados_centro.append('SI')
                        else:
                            aprobados_centro.append('NO')
                            centro_valido = False
                    
                    if not centro_valido:
                        observacion = f'Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero Campo "Centro de coste" NO se encuentra diligenciado cuando debe estarlo para pedido GENERALES, {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        
                        campos_novedad_centro = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                        nombre_item='CentroCoste',
                        valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                    
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Observaciones',
                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        break
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='CentroCoste',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['CentroDeCoste_hoc'])
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    
                    
                    # GENERALES: Validar indicador contra archivo Impuestos especiales
                    if indicador_valido and centro_valido and mapeo_ceco_impuestos:
                        aprobados_indicador_ceco = []
                        indicador_ceco_valido = True
                        indicadores_fallidos_detalle = set() # Usamos set para evitar duplicados en el mensaje
                        
                        # Usamos zip simple si las listas DEBEN medir lo mismo. Si usas zip_longest, asegúrate del manejo de None.
                        for centro, indicador in zip(ListaCentroCoste, ListaIndicador):
                            centro = safe_str(centro).upper()
                            indicador = safe_str(indicador).upper()
                            
                            if centro in mapeo_ceco_impuestos:
                                indicadores_permitidos = mapeo_ceco_impuestos[centro]
                                if indicador in indicadores_permitidos:
                                    aprobados_indicador_ceco.append('SI')
                                else:
                                    aprobados_indicador_ceco.append('NO')
                                    indicador_ceco_valido = False
                                    # Capturamos el error EXACTO de la línea que falló
                                    inds_str = ', '.join(indicadores_permitidos) if indicadores_permitidos else 'N/A'
                                    indicadores_fallidos_detalle.add(f"CECO {centro}: ({inds_str})")
                            else:
                                # Regla de negocio: Si no existe en el archivo, se omite validación (SI)
                                aprobados_indicador_ceco.append('SI')

                        if not indicador_ceco_valido:
                            # Armar mensaje con los indicadores reales que fallaron
                            detalle_indicadores = " | ".join(indicadores_fallidos_detalle)
                            
                            observacion = f'Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero campo "Indicador impuestos" NO se encuentra diligenciado correctamente segun los indicadores: {detalle_indicadores}, {registro["ObservacionesFase_4_dp"]}'
                            hay_novedad = True
                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                            
                            campos_novedad_ind_ceco = {
                                'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_ind_ceco)
                            
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                        nombre_item='Observaciones',
                                        valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                    
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                nombre_item='Indicador impuestos',
                                valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                            
                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
            
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                            break
                        else:
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                nombre_item='Indicador impuestos',
                                valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                        
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                
                # =========================================================
                # 12. FINALIZAR REGISTRO
                # =========================================================
                marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                
                campos_exitoso = {
                    'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                    'ResultadoFinalAntesEventos': f"PROCESADO {sufijo_contado}"
                }
                actualizar_bd_cxp(cx, registro_id, campos_exitoso)
                
                print(f"[SUCCESS] Registro {registro_id} procesado exitosamente")
                registros_exitosos += 1
                
                registros_procesados += 1
        
        # Fin del procesamiento
        tiempo_total = time.time() - t_inicio
        
        print("")
        print("=" * 80)
        print("[FIN] Procesamiento ZPSA/ZPSS/43 - Pedidos de Servicios completado")
        print("=" * 80)
        print("[ESTADISTICAS]")
        print(f"  Total registros procesados: {registros_procesados}")
        print(f"  Exitosos: {registros_exitosos}")
        print(f"  Con novedad: {registros_con_novedad}")
        print(f"  Tiempo total: {round(tiempo_total, 2)}s")
        print("=" * 80)
        
        resumen = f"Procesados {registros_procesados} registros ZPSA/ZPSS/43. Exitosos: {registros_exitosos}, Con novedad: {registros_con_novedad}"
        
        #SetVar("vLocStrResultadoSP", "True")
        #SetVar("vLocStrResumenSP", resumen)
        
    except Exception as e:
        print("")
        print("=" * 80)
        print("[ERROR CRITICO] La funcion ZPSA_ZPSS_ValidarServicios fallo")
        print("=" * 80)
        print(f"[ERROR] Mensaje: {str(e)}")
        print(traceback.format_exc())
        print("=" * 80)
        
        #SetVar("vGblStrDetalleError", traceback.format_exc())
        #SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        #SetVar("vLocStrResultadoSP", "False")


ZPSA_ZPSS_ValidarServicios()
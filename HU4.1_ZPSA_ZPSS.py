def ZPSA_ZPSS_ValidarServicios():
    """
    Función para procesar las validaciones de ZPSA/ZPSS/43 (Pedidos de Servicios).
    
    VERSIÓN: 1.0 - 12 Enero 2026
    
    FLUJO PRINCIPAL:
        1. Lee registros de [CxP].[Trans_Candidatos_HU41] con ClaseDePedido_hoc IN ('ZPSA', 'ZPSS', '43')
        2. Para cada registro:
           a. Determina si es USD o no (campo Moneda_hoc)
           b. Si USD: compara VlrPagarCop vs PorCalcular_hoc
           c. Si NO USD: compara Valor de la Compra LEA vs PorCalcular_hoc
           d. Valida TRM (5 decimales, manejo punto/coma)
           e. Valida Nombre Emisor
           f. Valida según campo Orden:
              - Si tiene Orden 15: Indicador, Centro coste vacío, Cuenta=5199150001, Clase orden
              - Si tiene Orden 53: Centro coste con valor (ESTADÍSTICAS)
              - Si tiene Orden diferente: Centro coste vacío, Cuenta=5299150099 o inicia con 7
           g. Si no tiene Orden, valida Elemento PEP:
              - Indicador, Centro coste vacío, Cuenta=5199150001, Emplazamiento
           h. Si no tiene Elemento PEP, valida Activo Fijo:
              - Si inicia con 2000 (DIFERIDO): Indicador C1/FA/VP/CO/CR, Centro/Cuenta vacíos
              - Si no tiene Activo Fijo (GENERALES): Cuenta e Indicador diligenciados,
                Centro coste diligenciado, validar contra archivo Impuestos especiales
        3. Actualiza [CxP].[DocumentsProcessing] con estados y observaciones
        4. Genera trazabilidad en [CxP].[CxP_Comparativa]
    
    NOTA IMPORTANTE SOBRE PaymentMeans:
        - Si PaymentMeans = '01', se agrega ' CONTADO' al resultado final
    
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
    from datetime import datetime
    from contextlib import contextmanager
    import time
    import warnings
    import re
    from itertools import combinations
    import os
    
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

        usuario = GetVar("vGblStrUsuarioBaseDatos")
        contrasena = GetVar("vGblStrClaveBaseDatos")
        
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={cfg['ServidorBaseDatos']};"
            f"DATABASE={cfg['NombreBaseDatos']};"
            f"UID={usuario};"
            f"PWD={contrasena};"
            "autocommit=False;"
        )
        
        cx = None
        for attempt in range(max_retries):
            try:
                cx = pyodbc.connect(conn_str, timeout=30)
                cx.autocommit = False
                print(f"[DEBUG] Conexion SQL abierta (intento {attempt + 1})")
                break
            except pyodbc.Error as e:
                if attempt < max_retries - 1:
                    print(f"[WARNING] Intento {attempt + 1} fallido, reintentando...")
                    time.sleep(1 * (attempt + 1))
                    continue
                raise
        
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
    
    def convertir_nombre_persona(nombre_completo):
        """Convierte el orden del nombre de persona."""
        if pd.isna(nombre_completo) or nombre_completo == "":
            return ""
        
        partes = safe_str(nombre_completo).strip().split()
        
        if len(partes) >= 3:
            apellidos = partes[-2:]
            nombres = partes[:-2]
            return " ".join(apellidos + nombres)
        
        return nombre_completo
    
    def comparar_nombres_proveedor(nombre_xml, nombre_sap):
        """Compara nombres de proveedores aplicando todas las reglas."""
        if pd.isna(nombre_xml) or pd.isna(nombre_sap):
            return False
        
        nombre_xml_empresa = normalizar_nombre_empresa(nombre_xml)
        nombre_sap_empresa = normalizar_nombre_empresa(nombre_sap)
        
        if nombre_xml_empresa == nombre_sap_empresa:
            return True
        
        nombre_xml_persona = normalizar_nombre_empresa(convertir_nombre_persona(nombre_xml))
        nombre_sap_persona = normalizar_nombre_empresa(convertir_nombre_persona(nombre_sap))
        
        if nombre_xml_persona == nombre_sap_empresa or nombre_xml_empresa == nombre_sap_persona:
            return True
        
        if nombre_xml_empresa == nombre_sap_persona:
            return True
        
        return False
    
    # =========================================================================
    # FUNCIONES DE VALIDACIÓN
    # =========================================================================
    
    def validar_tolerancia_numerica(valor1, valor2, tolerancia=500):
        """Valida si dos valores numéricos están dentro del rango de tolerancia."""
        try:
            val1 = normalizar_decimal(valor1)
            val2 = normalizar_decimal(valor2)
            return abs(val1 - val2) <= tolerancia
        except:
            return False
    
    def comparar_trm_5_decimales(trm_xml, trm_sap):
        """
        Compara TRM con precisión de 5 decimales.
        Maneja conversión de coma a punto.
        """
        try:
            val_xml = normalizar_decimal(trm_xml)
            val_sap = normalizar_decimal(trm_sap)
            
            # Redondear a 5 decimales para comparación
            val_xml_r = round(val_xml, 5)
            val_sap_r = round(val_sap, 5)
            
            return abs(val_xml_r - val_sap_r) < 0.00001
        except:
            return False
    
    def encontrar_combinacion_posiciones(valores_por_calcular, valor_objetivo, tolerancia=500):
        """Encuentra una combinación de posiciones cuya suma coincida con el valor objetivo."""
        valor_objetivo = normalizar_decimal(valor_objetivo)
        
        if valor_objetivo <= 0:
            return False, [], 0
        
        for r in range(1, len(valores_por_calcular) + 1):
            for combo in combinations(range(len(valores_por_calcular)), r):
                suma = sum(normalizar_decimal(valores_por_calcular[i][1]) for i in combo)
                if abs(suma - valor_objetivo) <= tolerancia:
                    posiciones_usadas = [valores_por_calcular[i][0] for i in combo]
                    return True, posiciones_usadas, suma
        
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
    
    def actualizar_items_comparativa(id_reg, cx, nit, factura, nombre_item, valores_lista,
                                     actualizar_valor_xml=False, valor_xml=None,
                                     actualizar_aprobado=False, valor_aprobado=None):
        """Actualiza o inserta items en [CxP].[CxP_Comparativa]."""
        cur = cx.cursor()
        
        query_count = """
        SELECT COUNT(*) FROM [CxP].[CxP_Comparativa]
        WHERE NIT = ? AND Factura = ? AND Item = ?
        """
        cur.execute(query_count, (nit, factura, nombre_item))
        count_actual = cur.fetchone()[0]
        
        count_necesario = len(valores_lista)
        
        if isinstance(valor_aprobado, list):
            aprobados_lista = valor_aprobado
        else:
            aprobados_lista = [valor_aprobado] * count_necesario
        
        if count_actual == 0:
            for i, valor in enumerate(valores_lista):
                insert_query = """
                INSERT INTO [CxP].[CxP_Comparativa] (
                    ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra,
                    Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                vxml = valor_xml if actualizar_valor_xml else None
                vaprob = aprobados_lista[i] if actualizar_aprobado and i < len(aprobados_lista) else None
                cur.execute(insert_query, (id_reg, nit, factura, nombre_item, valor, vxml, vaprob))
        
        elif count_actual < count_necesario:
            for i in range(count_actual):
                update_query = "UPDATE [CxP].[CxP_Comparativa] SET Valor_Orden_de_Compra = ?"
                params = [valores_lista[i]]
                
                if actualizar_valor_xml:
                    update_query += ", Valor_XML = ?"
                    params.append(valor_xml)
                if actualizar_aprobado:
                    update_query += ", Aprobado = ?"
                    params.append(aprobados_lista[i] if i < len(aprobados_lista) else None)
                
                update_query += """
                WHERE NIT = ? AND Factura = ? AND Item = ?
                  AND ID_registro IN (
                    SELECT TOP 1 ID_registro FROM [CxP].[CxP_Comparativa]
                    WHERE NIT = ? AND Factura = ? AND Item = ?
                    ORDER BY ID_registro OFFSET ? ROWS
                  )
                """
                params.extend([nit, factura, nombre_item, nit, factura, nombre_item, i])
                cur.execute(update_query, params)
            
            for i in range(count_actual, count_necesario):
                insert_query = """
                INSERT INTO [CxP].[CxP_Comparativa] (
                    ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra,
                    Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                vxml = valor_xml if actualizar_valor_xml else None
                vaprob = aprobados_lista[i] if actualizar_aprobado and i < len(aprobados_lista) else None
                cur.execute(insert_query, (id_reg, nit, factura, nombre_item, valores_lista[i], vxml, vaprob))
        
        else:
            for i, valor in enumerate(valores_lista):
                update_query = "UPDATE [CxP].[CxP_Comparativa] SET Valor_Orden_de_Compra = ?"
                params = [valor]
                
                if actualizar_valor_xml:
                    update_query += ", Valor_XML = ?"
                    params.append(valor_xml)
                if actualizar_aprobado:
                    update_query += ", Aprobado = ?"
                    params.append(aprobados_lista[i] if i < len(aprobados_lista) else None)
                
                update_query += """
                WHERE NIT = ? AND Factura = ? AND Item = ?
                  AND ID_registro IN (
                    SELECT TOP 1 ID_registro FROM [CxP].[CxP_Comparativa]
                    WHERE NIT = ? AND Factura = ? AND Item = ?
                    ORDER BY ID_registro OFFSET ? ROWS
                  )
                """
                params.extend([nit, factura, nombre_item, nit, factura, nombre_item, i])
                cur.execute(update_query, params)
        
        cur.close()
    
    def actualizar_estado_comparativa(cx, nit, factura, estado):
        """Actualiza el Estado_validacion_antes_de_eventos en CxP_Comparativa."""
        cur = cx.cursor()
        update_sql = """
        UPDATE [CxP].[CxP_Comparativa]
        SET Estado_validacion_antes_de_eventos = ?
        WHERE NIT = ? AND Factura = ?
        """
        cur.execute(update_sql, (estado, nit, factura))
        cur.close()
    
    def marcar_posiciones_procesadas(cx, doc_compra):
        """Marca posiciones en Trans_Candidatos_HU41 como PROCESADO."""
        cur = cx.cursor()
        update_sql = """
        UPDATE [CxP].[Trans_Candidatos_HU41]
        SET Marca_hoc = 'PROCESADO'
        WHERE DocCompra_hoc = ?
        """
        cur.execute(update_sql, (doc_compra,))
        cur.close()
        print(f"[UPDATE] Marcado como PROCESADO - OC {doc_compra}")
    
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
    # FUNCIONES DE TRAZABILIDAD
    # =========================================================================
    
    def generar_trazabilidad_base(cx, registro_id, nit, factura, posiciones_usadas,
                                  datos_posiciones, valor_xml, valor_sap, es_usd):
        """Genera la trazabilidad base en CxP_Comparativa."""
        datos_filtrados = [d for d in datos_posiciones if d['Posicion'] in posiciones_usadas]
        
        nombre_campo_valor = 'VlrPagarCop' if es_usd else 'LineExtensionAmount'
        
        # Valor principal
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=factura,
            nombre_item=nombre_campo_valor,
            valores_lista=[str(valor_sap)],
            actualizar_valor_xml=True, valor_xml=str(valor_xml),
            actualizar_aprobado=True, valor_aprobado='SI'
        )
        
        # Posiciones
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=factura,
            nombre_item='Posicion',
            valores_lista=[d['Posicion'] for d in datos_filtrados],
            actualizar_aprobado=True, valor_aprobado='SI'
        )
        
        # Valor PorCalcular_hoc SAP
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=factura,
            nombre_item='Valor PorCalcular_hoc SAP',
            valores_lista=[str(normalizar_decimal(d['PorCalcular'])) for d in datos_filtrados],
            actualizar_aprobado=True, valor_aprobado='SI'
        )
        
        # Campos básicos del histórico
        campos_basicos = [
            ('Tipo NIF', 'TipoNif'),
            ('Acreedor', 'Acreedor'),
            ('Fec.Doc', 'FecDoc'),
            ('Fec.Reg', 'FecReg'),
            ('Fecha. cont gasto', 'FecContGasto'),
            ('Texto breve', 'TextoBreve'),
            ('Clase de impuesto', 'ClaseDeImpuesto'),
            ('DOC.FI.ENTRADA', 'DocFiEntrada'),
            ('CTA 26', 'Cuenta26'),
        ]
        
        for nombre_item, campo_historico in campos_basicos:
            actualizar_items_comparativa(
                id_reg=registro_id, cx=cx, nit=nit, factura=factura,
                nombre_item=nombre_item,
                valores_lista=[safe_str(d.get(campo_historico, '')) for d in datos_filtrados],
                actualizar_aprobado=True, valor_aprobado='SI'
            )
        
        # Campos específicos de servicios
        campos_servicios = [
            ('Poblacion Servicio', 'PoblacionServicio'),
            ('Activo fijo', 'ActivoFijo'),
            ('Orden', 'Orden'),
            ('Centro de coste', 'CentroCoste'),
            ('Clase orden', 'ClaseOrden'),
            ('Elemento PEP', 'ElementoPEP'),
            ('Emplazamiento', 'Emplazamiento'),
        ]
        
        for nombre_item, campo_historico in campos_servicios:
            actualizar_items_comparativa(
                id_reg=registro_id, cx=cx, nit=nit, factura=factura,
                nombre_item=nombre_item,
                valores_lista=[safe_str(d.get(campo_historico, '')) for d in datos_filtrados],
                actualizar_aprobado=True, valor_aprobado='SI'
            )
    
    def generar_trazabilidad_sin_coincidencia(cx, registro_id, nit, factura, valor_xml, es_usd, observacion):
        """Genera la trazabilidad cuando NO hay coincidencia de valores."""
        nombre_campo_valor = 'VlrPagarCop' if es_usd else 'LineExtensionAmount'
        
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=factura,
            nombre_item=nombre_campo_valor,
            valores_lista=['NO ENCONTRADO'],
            actualizar_valor_xml=True, valor_xml=str(valor_xml),
            actualizar_aprobado=True, valor_aprobado='NO'
        )
        
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=factura,
            nombre_item='Observaciones',
            valores_lista=[''],
            actualizar_valor_xml=True, valor_xml=observacion
        )
    
    # =========================================================================
    # FUNCIONES DE CARGA DE ARCHIVO IMPUESTOS ESPECIALES
    # =========================================================================
    
    def cargar_archivo_impuestos_especiales(ruta_archivo):
        """
        Carga el archivo Impuestos especiales CXP.xlsx y extrae la hoja IVA_CECO.
        
        Returns:
            dict: {centro_coste: [lista_indicadores_permitidos]}
        """
        try:
            if not os.path.exists(ruta_archivo):
                print(f"[WARNING] Archivo Impuestos especiales no encontrado: {ruta_archivo}")
                return None
            
            df_iva_ceco = pd.read_excel(ruta_archivo, sheet_name='IVA_CECO')
            
            # Normalizar nombres de columnas
            df_iva_ceco.columns = df_iva_ceco.columns.str.strip()
            
            # Buscar columnas requeridas
            col_ceco = None
            col_codigo_iva = None
            
            for col in df_iva_ceco.columns:
                col_upper = col.upper()
                if 'CECO' in col_upper and 'NOMBRE' not in col_upper:
                    col_ceco = col
                if 'CODIGO' in col_upper and 'IVA' in col_upper and 'APLICABLE' in col_upper:
                    col_codigo_iva = col
            
            if not col_ceco or not col_codigo_iva:
                print(f"[WARNING] Columnas requeridas no encontradas en IVA_CECO")
                return None
            
            # Crear diccionario de mapeo
            mapeo_ceco = {}
            
            for _, row in df_iva_ceco.iterrows():
                ceco = safe_str(row[col_ceco])
                codigo_iva = safe_str(row[col_codigo_iva])
                
                if ceco and codigo_iva:
                    # Separar los códigos por guión
                    indicadores = [ind.strip().upper() for ind in codigo_iva.replace('-', ',').split(',') if ind.strip()]
                    mapeo_ceco[ceco.upper()] = indicadores
            
            print(f"[INFO] Archivo Impuestos especiales cargado: {len(mapeo_ceco)} CECOs")
            return mapeo_ceco
            
        except Exception as e:
            print(f"[ERROR] Error cargando archivo Impuestos especiales: {str(e)}")
            return None
    
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
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[INFO] Configuracion cargada exitosamente")
        
        required_config = ['ServidorBaseDatos', 'NombreBaseDatos']
        missing_config = [k for k in required_config if not cfg.get(k)]
        if missing_config:
            raise ValueError(f"Faltan parametros de configuracion: {', '.join(missing_config)}")
        
        # Cargar archivo de impuestos especiales si está configurado
        ruta_impuestos = cfg.get('RutaImpuestosEspeciales', '')
        mapeo_ceco_impuestos = None
        if ruta_impuestos:
            mapeo_ceco_impuestos = cargar_archivo_impuestos_especiales(ruta_impuestos)
        
        # 2. Conectar a base de datos
        with crear_conexion_db(cfg) as cx:
            print("[INFO] Obteniendo registros ZPSA/ZPSS/43 para procesar...")
            
            query_zpsa = """
                SELECT * FROM [CxP].[Trans_Candidatos_HU41]
                WHERE [ClaseDePedido_hoc] IN ('ZPSA', 'ZPSS', '43')
                  AND (Marca_hoc IS NULL OR Marca_hoc <> 'PROCESADO')
                ORDER BY [executionDate_dp] DESC
            """
            
            df_registros = pd.read_sql(query_zpsa, cx)
            
            print(f"[INFO] Obtenidos {len(df_registros)} registros ZPSA/ZPSS/43 para procesar")
            
            if len(df_registros) == 0:
                print("[INFO] No hay registros ZPSA/ZPSS/43 pendientes de procesar")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros ZPSA/ZPSS/43 pendientes de procesar")
                return
            
            # Variables de conteo
            registros_procesados = 0
            registros_con_novedad = 0
            registros_exitosos = 0
            
            # 3. Procesar cada registro
            for idx, registro in df_registros.iterrows():
                try:
                    registro_id = safe_str(registro.get('ID_dp', ''))
                    numero_oc = safe_str(registro.get('numero_de_liquidacion_u_orden_de_compra_dp', ''))
                    numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
                    payment_means = safe_str(registro.get('forma_de_pago_dp', ''))
                    nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
                    clase_pedido = safe_str(registro.get('ClaseDePedido_hoc', '')).upper()
                    
                    # Determinar etiqueta para mensajes (ZPSA o ZPSS)
                    tipo_pedido = 'ZPSA' if clase_pedido in ('ZPSA', '43') else 'ZPSS'
                    
                    print(f"\n[PROCESO] Registro {registros_procesados + 1}/{len(df_registros)}: OC {numero_oc}, Factura {numero_factura}, Tipo {tipo_pedido}")
                    
                    sufijo_contado = " CONTADO" if payment_means == "01" else ""
                    
                    # 4. Expandir posiciones del histórico
                    datos_posiciones = expandir_posiciones_historico(registro)
                    
                    if not datos_posiciones:
                        print(f"[WARNING] No se encontraron posiciones en el historico para OC {numero_oc}")
                        observacion = "No se encuentran posiciones en el historico de ordenes de compra"
                        resultado_final = f"CON NOVEDAD{sufijo_contado}"
                        
                        campos_novedad = {
                            'EstadoFinalFase_4': 'Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': resultado_final
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad)
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                        registros_con_novedad += 1
                        registros_procesados += 1
                        continue
                    
                    # 5. Determinar si es USD
                    moneda = safe_str(datos_posiciones[0].get('Moneda', '')).upper()
                    es_usd = moneda == 'USD'
                    
                    # 6. Obtener valor a comparar según moneda
                    if es_usd:
                        valor_xml = normalizar_decimal(registro.get('VlrPagarCop_dp', 0))
                    else:
                        valor_xml = normalizar_decimal(registro.get('Valor de la Compra LEA_ddp', 0))
                    
                    # 7. Buscar combinación de posiciones
                    valores_por_calcular = [(d['Posicion'], d['PorCalcular']) for d in datos_posiciones]
                    
                    coincidencia_encontrada, posiciones_usadas, suma_encontrada = encontrar_combinacion_posiciones(
                        valores_por_calcular, valor_xml, tolerancia=500
                    )
                    
                    if not coincidencia_encontrada:
                        print(f"[INFO] No se encuentra coincidencia del valor a pagar para OC {numero_oc}")
                        observacion = "No se encuentra coincidencia del Valor a pagar de la factura"
                        resultado_final = f"CON NOVEDAD{sufijo_contado}"
                        
                        campos_novedad = {
                            'EstadoFinalFase_4': 'Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': resultado_final
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad)
                        
                        generar_trazabilidad_sin_coincidencia(
                            cx, registro_id, nit, numero_factura, valor_xml, es_usd, observacion
                        )
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                        registros_con_novedad += 1
                        registros_procesados += 1
                        continue
                    
                    print(f"[DEBUG] Coincidencia encontrada con posiciones: {posiciones_usadas}")
                    
                    # Filtrar datos de posiciones usadas
                    datos_posiciones_usadas = [d for d in datos_posiciones if d['Posicion'] in posiciones_usadas]
                    
                    # 8. Generar trazabilidad base
                    generar_trazabilidad_base(
                        cx, registro_id, nit, numero_factura, posiciones_usadas,
                        datos_posiciones, valor_xml, suma_encontrada, es_usd
                    )
                    
                    hay_novedad = False
                    
                    # 9. Validar TRM (con 5 decimales)
                    trm_xml = normalizar_decimal(registro.get('CalculationRate_dp', 0))
                    trm_sap = normalizar_decimal(datos_posiciones_usadas[0].get('Trm', 0))
                    
                    if trm_xml > 0 or trm_sap > 0:
                        trm_coincide = comparar_trm_5_decimales(trm_xml, trm_sap)
                        
                        if not trm_coincide:
                            print(f"[INFO] TRM no coincide: XML {trm_xml} vs SAP {trm_sap}")
                            observacion = "No se encuentra coincidencia en el campo TRM de la factura vs la informacion reportada en SAP"
                            hay_novedad = True
                            
                            campos_novedad_trm = {
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_trm)
                            
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='TRM',
                                valores_lista=[str(trm_sap)] * len(posiciones_usadas),
                                actualizar_valor_xml=True, valor_xml=str(trm_xml),
                                actualizar_aprobado=True, valor_aprobado='NO'
                            )
                        else:
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='TRM',
                                valores_lista=[str(trm_sap)] * len(posiciones_usadas),
                                actualizar_valor_xml=True, valor_xml=str(trm_xml),
                                actualizar_aprobado=True, valor_aprobado='SI'
                            )
                    
                    # 10. Validar Nombre Emisor
                    nombre_emisor_xml = safe_str(registro.get('nombre_emisor_dp', ''))
                    nombre_proveedor_sap = safe_str(datos_posiciones_usadas[0].get('NProveedor', ''))
                    
                    nombres_coinciden = comparar_nombres_proveedor(nombre_emisor_xml, nombre_proveedor_sap)
                    
                    if not nombres_coinciden:
                        print(f"[INFO] Nombre emisor no coincide")
                        observacion = "No se encuentra coincidencia en Nombre Emisor de la factura vs la informacion reportada en SAP"
                        hay_novedad = True
                        
                        campos_novedad_nombre = {
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_nombre)
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Nombre emisor',
                            valores_lista=[nombre_proveedor_sap],
                            actualizar_valor_xml=True, valor_xml=nombre_emisor_xml,
                            actualizar_aprobado=True, valor_aprobado='NO'
                        )
                    else:
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Nombre emisor',
                            valores_lista=[nombre_proveedor_sap],
                            actualizar_valor_xml=True, valor_xml=nombre_emisor_xml,
                            actualizar_aprobado=True, valor_aprobado='SI'
                        )
                    
                    # =========================================================
                    # 11. VALIDACIONES ESPECÍFICAS SEGÚN ORDEN/ELEMENTO PEP/ACTIVO FIJO
                    # =========================================================
                    
                    # Determinar qué ruta de validación seguir
                    tiene_orden = any(campo_con_valor(d.get('Orden', '')) for d in datos_posiciones_usadas)
                    tiene_elemento_pep = any(campo_con_valor(d.get('ElementoPEP', '')) for d in datos_posiciones_usadas)
                    tiene_activo_fijo = any(campo_con_valor(d.get('ActivoFijo', '')) for d in datos_posiciones_usadas)
                    
                    print(f"[DEBUG] Tiene Orden: {tiene_orden}, Elemento PEP: {tiene_elemento_pep}, Activo Fijo: {tiene_activo_fijo}")
                    
                    # =========================================================
                    # RUTA A: TIENE ORDEN
                    # =========================================================
                    if tiene_orden:
                        # Obtener primer valor de Orden para determinar tipo
                        orden_valor = safe_str(datos_posiciones_usadas[0].get('Orden', ''))
                        orden_limpio = re.sub(r'\D', '', orden_valor)  # Solo dígitos
                        
                        print(f"[DEBUG] Orden valor: {orden_valor}, limpio: {orden_limpio}")
                        
                        # Determinar si inicia con 15
                        if orden_limpio.startswith('15') and len(orden_limpio) == 9:
                            print(f"[DEBUG] Orden inicia con 15 (9 dígitos)")
                            
                            # ORDEN 15: Validar Indicador impuestos
                            aprobados_indicador = []
                            indicador_valido = True
                            
                            for d in datos_posiciones_usadas:
                                indicador = d.get('IndicadorImpuestos', '')
                                if validar_indicador_servicios_orden15(indicador):
                                    aprobados_indicador.append('SI')
                                else:
                                    aprobados_indicador.append('NO')
                                    indicador_valido = False
                            
                            if not indicador_valido:
                                indicadores_actual = [safe_str(d.get('IndicadorImpuestos', '')) for d in datos_posiciones_usadas]
                                if all(campo_vacio(ind) for ind in indicadores_actual):
                                    observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero campo "Indicador impuestos" NO se encuentra diligenciado'
                                else:
                                    observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero campo "Indicador impuestos" NO corresponde alguna de las opciones H4, H5, H6, H7, VP, CO, IC, CR'
                                hay_novedad = True
                                
                                campos_novedad_ind = {
                                    'ObservacionesFase_4': truncar_observacion(observacion),
                                    'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                }
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                            
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='Indicador impuestos',
                                valores_lista=[safe_str(d.get('IndicadorImpuestos', '')) for d in datos_posiciones_usadas],
                                actualizar_aprobado=True, valor_aprobado=aprobados_indicador
                            )
                            
                            # ORDEN 15: Centro de coste debe estar VACÍO
                            aprobados_centro = []
                            centro_valido = True
                            
                            for d in datos_posiciones_usadas:
                                centro = d.get('CentroCoste', '')
                                if campo_vacio(centro):
                                    aprobados_centro.append('SI')
                                else:
                                    aprobados_centro.append('NO')
                                    centro_valido = False
                            
                            if not centro_valido:
                                observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo "Centro de coste" se encuentra diligenciado cuando NO debe estarlo'
                                hay_novedad = True
                                
                                campos_novedad_centro = {
                                    'ObservacionesFase_4': truncar_observacion(observacion),
                                    'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                }
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                            
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='Centro de coste',
                                valores_lista=[safe_str(d.get('CentroCoste', '')) for d in datos_posiciones_usadas],
                                actualizar_aprobado=True, valor_aprobado=aprobados_centro
                            )
                            
                            # ORDEN 15: Cuenta debe ser 5199150001
                            aprobados_cuenta = []
                            cuenta_valida = True
                            
                            for d in datos_posiciones_usadas:
                                cuenta = safe_str(d.get('Cuenta', '')).strip()
                                if cuenta == '5199150001':
                                    aprobados_cuenta.append('SI')
                                else:
                                    aprobados_cuenta.append('NO')
                                    cuenta_valida = False
                            
                            if not cuenta_valida:
                                observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo "Cuenta" es diferente a 5199150001'
                                hay_novedad = True
                                
                                campos_novedad_cuenta = {
                                    'ObservacionesFase_4': truncar_observacion(observacion),
                                    'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                }
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                            
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='Cuenta',
                                valores_lista=[safe_str(d.get('Cuenta', '')) for d in datos_posiciones_usadas],
                                actualizar_aprobado=True, valor_aprobado=aprobados_cuenta
                            )
                            
                            # ORDEN 15: Clase orden según indicador
                            aprobados_clase = []
                            clase_valida = True
                            
                            for d in datos_posiciones_usadas:
                                indicador = d.get('IndicadorImpuestos', '')
                                clase_ord = d.get('ClaseOrden', '')
                                es_valido, msg = validar_clase_orden(indicador, clase_ord)
                                if es_valido:
                                    aprobados_clase.append('SI')
                                else:
                                    aprobados_clase.append('NO')
                                    clase_valida = False
                            
                            if not clase_valida:
                                clases = [safe_str(d.get('ClaseOrden', '')) for d in datos_posiciones_usadas]
                                if all(campo_vacio(c) for c in clases):
                                    observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo "Clase orden" NO se encuentra diligenciado'
                                else:
                                    observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo "Clase orden" NO se encuentra aplicado correctamente segun reglas "H4 y H5 = ZINV", "H6 y H7 = ZADM" o "VP, CO, CR o IC = ZINV o ZADM"'
                                hay_novedad = True
                                
                                campos_novedad_clase = {
                                    'ObservacionesFase_4': truncar_observacion(observacion),
                                    'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                }
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_clase)
                            
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='Clase orden',
                                valores_lista=[safe_str(d.get('ClaseOrden', '')) for d in datos_posiciones_usadas],
                                actualizar_aprobado=True, valor_aprobado=aprobados_clase
                            )
                        
                        else:
                            # ORDEN NO INICIA CON 15
                            # Determinar si es 53 (ESTADÍSTICAS) o diferente
                            if orden_limpio.startswith('53') and len(orden_limpio) == 8:
                                print(f"[DEBUG] Orden inicia con 53 (ESTADISTICAS)")
                                
                                # ORDEN 53: Centro de coste debe CONTENER VALOR
                                aprobados_centro = []
                                centro_valido = True
                                
                                for d in datos_posiciones_usadas:
                                    centro = d.get('CentroCoste', '')
                                    if campo_con_valor(centro):
                                        aprobados_centro.append('SI')
                                    else:
                                        aprobados_centro.append('NO')
                                        centro_valido = False
                                
                                if not centro_valido:
                                    observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden 53, pero Campo "Centro de coste" se encuentra vacio para pedidos ESTADISTICAS'
                                    hay_novedad = True
                                    
                                    campos_novedad_centro = {
                                        'ObservacionesFase_4': truncar_observacion(observacion),
                                        'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                    }
                                    actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                                
                                actualizar_items_comparativa(
                                    id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                    nombre_item='Centro de coste',
                                    valores_lista=[safe_str(d.get('CentroCoste', '')) for d in datos_posiciones_usadas],
                                    actualizar_aprobado=True, valor_aprobado=aprobados_centro
                                )
                            
                            else:
                                print(f"[DEBUG] Orden diferente a 15 y 53 (NO ESTADISTICAS)")
                                
                                # ORDEN DIFERENTE: Centro de coste debe estar VACÍO
                                aprobados_centro = []
                                centro_valido = True
                                
                                for d in datos_posiciones_usadas:
                                    centro = d.get('CentroCoste', '')
                                    if campo_vacio(centro):
                                        aprobados_centro.append('SI')
                                    else:
                                        aprobados_centro.append('NO')
                                        centro_valido = False
                                
                                if not centro_valido:
                                    observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo "Centro de coste" se encuentra diligenciado para pedidos NO ESTADISTICAS'
                                    hay_novedad = True
                                    
                                    campos_novedad_centro = {
                                        'ObservacionesFase_4': truncar_observacion(observacion),
                                        'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                    }
                                    actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                                
                                actualizar_items_comparativa(
                                    id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                    nombre_item='Centro de coste',
                                    valores_lista=[safe_str(d.get('CentroCoste', '')) for d in datos_posiciones_usadas],
                                    actualizar_aprobado=True, valor_aprobado=aprobados_centro
                                )
                                
                                # ORDEN DIFERENTE: Cuenta debe ser 5299150099 O iniciar con 7 y 10 dígitos
                                aprobados_cuenta = []
                                cuenta_valida = True
                                
                                for d in datos_posiciones_usadas:
                                    cuenta = safe_str(d.get('Cuenta', '')).strip()
                                    if validar_cuenta_orden_no_15(cuenta):
                                        aprobados_cuenta.append('SI')
                                    else:
                                        aprobados_cuenta.append('NO')
                                        cuenta_valida = False
                                
                                if not cuenta_valida:
                                    observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo "Cuenta" es diferente a 5299150099 y/o NO cumple regla "inicia con "7" y tiene 10 digitos"'
                                    hay_novedad = True
                                    
                                    campos_novedad_cuenta = {
                                        'ObservacionesFase_4': truncar_observacion(observacion),
                                        'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                    }
                                    actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                                
                                actualizar_items_comparativa(
                                    id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                    nombre_item='Cuenta',
                                    valores_lista=[safe_str(d.get('Cuenta', '')) for d in datos_posiciones_usadas],
                                    actualizar_aprobado=True, valor_aprobado=aprobados_cuenta
                                )
                    
                    # =========================================================
                    # RUTA B: TIENE ELEMENTO PEP (y no tiene Orden)
                    # =========================================================
                    elif tiene_elemento_pep:
                        print(f"[DEBUG] Validando Elemento PEP")
                        
                        # Indicador impuestos: H4, H5, H6, H7, VP, CO, IC, CR
                        aprobados_indicador = []
                        indicador_valido = True
                        
                        for d in datos_posiciones_usadas:
                            indicador = d.get('IndicadorImpuestos', '')
                            if validar_indicador_servicios_orden15(indicador):
                                aprobados_indicador.append('SI')
                            else:
                                aprobados_indicador.append('NO')
                                indicador_valido = False
                        
                        if not indicador_valido:
                            indicadores_actual = [safe_str(d.get('IndicadorImpuestos', '')) for d in datos_posiciones_usadas]
                            if all(campo_vacio(ind) for ind in indicadores_actual):
                                observacion = f'Pedido corresponde a {tipo_pedido} con Elemento PEP, pero campo "Indicador impuestos" NO se encuentra diligenciado'
                            else:
                                observacion = f'Pedido corresponde a {tipo_pedido} con Elemento PEP, pero campo "Indicador impuestos" NO corresponde alguna de las opciones H4, H5, H6, H7, VP, CO, IC, CR'
                            hay_novedad = True
                            
                            campos_novedad_ind = {
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Indicador impuestos',
                            valores_lista=[safe_str(d.get('IndicadorImpuestos', '')) for d in datos_posiciones_usadas],
                            actualizar_aprobado=True, valor_aprobado=aprobados_indicador
                        )
                        
                        # Centro de coste debe estar VACÍO
                        aprobados_centro = []
                        centro_valido = True
                        
                        for d in datos_posiciones_usadas:
                            centro = d.get('CentroCoste', '')
                            if campo_vacio(centro):
                                aprobados_centro.append('SI')
                            else:
                                aprobados_centro.append('NO')
                                centro_valido = False
                        
                        if not centro_valido:
                            observacion = f'Pedido corresponde a {tipo_pedido} con Elemento PEP, pero Campo "Centro de coste" se encuentra diligenciado cuando NO debe estarlo'
                            hay_novedad = True
                            
                            campos_novedad_centro = {
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Centro de coste',
                            valores_lista=[safe_str(d.get('CentroCoste', '')) for d in datos_posiciones_usadas],
                            actualizar_aprobado=True, valor_aprobado=aprobados_centro
                        )
                        
                        # Cuenta debe ser 5199150001
                        aprobados_cuenta = []
                        cuenta_valida = True
                        
                        for d in datos_posiciones_usadas:
                            cuenta = safe_str(d.get('Cuenta', '')).strip()
                            if cuenta == '5199150001':
                                aprobados_cuenta.append('SI')
                            else:
                                aprobados_cuenta.append('NO')
                                cuenta_valida = False
                        
                        if not cuenta_valida:
                            observacion = f'Pedido corresponde a {tipo_pedido} con Elemento PEP, pero Campo "Cuenta" es diferente a 5199150001'
                            hay_novedad = True
                            
                            campos_novedad_cuenta = {
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Cuenta',
                            valores_lista=[safe_str(d.get('Cuenta', '')) for d in datos_posiciones_usadas],
                            actualizar_aprobado=True, valor_aprobado=aprobados_cuenta
                        )
                        
                        # Emplazamiento según indicador
                        aprobados_empl = []
                        empl_valido = True
                        
                        for d in datos_posiciones_usadas:
                            indicador = d.get('IndicadorImpuestos', '')
                            empl = d.get('Emplazamiento', '')
                            es_valido, msg = validar_emplazamiento(indicador, empl)
                            if es_valido:
                                aprobados_empl.append('SI')
                            else:
                                aprobados_empl.append('NO')
                                empl_valido = False
                        
                        if not empl_valido:
                            empls = [safe_str(d.get('Emplazamiento', '')) for d in datos_posiciones_usadas]
                            if all(campo_vacio(e) for e in empls):
                                observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Elemento PEP, pero Campo "Emplazamiento" NO se encuentra diligenciado'
                            else:
                                observacion = f'Pedido corresponde a {tipo_pedido} y cuenta con Elemento PEP, pero Campo "Emplazamiento" NO se encuentra aplicado correctamente segun reglas "H4 y H5 = DCTO_01", "H6 y H7 = GTO_02" o "VP, CO, CR o IC = DCTO_01 o GTO_02"'
                            hay_novedad = True
                            
                            campos_novedad_empl = {
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_empl)
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Emplazamiento',
                            valores_lista=[safe_str(d.get('Emplazamiento', '')) for d in datos_posiciones_usadas],
                            actualizar_aprobado=True, valor_aprobado=aprobados_empl
                        )
                    
                    # =========================================================
                    # RUTA C: TIENE ACTIVO FIJO (y no tiene Orden ni Elemento PEP)
                    # =========================================================
                    elif tiene_activo_fijo:
                        print(f"[DEBUG] Validando Activo Fijo")
                        
                        # Determinar si es DIFERIDO (inicia con 2000 y 10 dígitos)
                        activo_fijo_valor = safe_str(datos_posiciones_usadas[0].get('ActivoFijo', ''))
                        activo_limpio = re.sub(r'\D', '', activo_fijo_valor)
                        
                        es_diferido = activo_limpio.startswith('2000') and len(activo_limpio) == 10
                        
                        if es_diferido:
                            print(f"[DEBUG] Activo Fijo DIFERIDO (2000)")
                            
                            # DIFERIDO: Indicador impuestos: C1, FA, VP, CO, CR
                            aprobados_indicador = []
                            indicador_valido = True
                            
                            for d in datos_posiciones_usadas:
                                indicador = d.get('IndicadorImpuestos', '')
                                if validar_indicador_diferido(indicador):
                                    aprobados_indicador.append('SI')
                                else:
                                    aprobados_indicador.append('NO')
                                    indicador_valido = False
                            
                            if not indicador_valido:
                                indicadores_actual = [safe_str(d.get('IndicadorImpuestos', '')) for d in datos_posiciones_usadas]
                                if all(campo_vacio(ind) for ind in indicadores_actual):
                                    observacion = f'Pedido corresponde a {tipo_pedido} con Activo Fijo, pero campo "Indicador impuestos" NO se encuentra diligenciado para pedido DIFERIDO'
                                else:
                                    observacion = f'Pedido corresponde a {tipo_pedido} con Activo Fijo, pero campo "Indicador impuestos" NO corresponde alguna de las opciones "C1", "FA", "VP", "CO" o "CR" para pedido DIFERIDO'
                                hay_novedad = True
                                
                                campos_novedad_ind = {
                                    'ObservacionesFase_4': truncar_observacion(observacion),
                                    'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                }
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                            
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='Indicador impuestos',
                                valores_lista=[safe_str(d.get('IndicadorImpuestos', '')) for d in datos_posiciones_usadas],
                                actualizar_aprobado=True, valor_aprobado=aprobados_indicador
                            )
                            
                            # DIFERIDO: Centro de coste debe estar VACÍO
                            aprobados_centro = []
                            centro_valido = True
                            
                            for d in datos_posiciones_usadas:
                                centro = d.get('CentroCoste', '')
                                if campo_vacio(centro):
                                    aprobados_centro.append('SI')
                                else:
                                    aprobados_centro.append('NO')
                                    centro_valido = False
                            
                            if not centro_valido:
                                observacion = f'Pedido corresponde a {tipo_pedido} con Activo Fijo, pero Campo "Centro de coste" se encuentra diligenciado cuando NO debe estarlo para pedido DIFERIDO'
                                hay_novedad = True
                                
                                campos_novedad_centro = {
                                    'ObservacionesFase_4': truncar_observacion(observacion),
                                    'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                }
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                            
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='Centro de coste',
                                valores_lista=[safe_str(d.get('CentroCoste', '')) for d in datos_posiciones_usadas],
                                actualizar_aprobado=True, valor_aprobado=aprobados_centro
                            )
                            
                            # DIFERIDO: Cuenta debe estar VACÍO
                            aprobados_cuenta = []
                            cuenta_valida = True
                            
                            for d in datos_posiciones_usadas:
                                cuenta = d.get('Cuenta', '')
                                if campo_vacio(cuenta):
                                    aprobados_cuenta.append('SI')
                                else:
                                    aprobados_cuenta.append('NO')
                                    cuenta_valida = False
                            
                            if not cuenta_valida:
                                observacion = f'Pedido corresponde a {tipo_pedido} con Activo Fijo, pero Campo "Cuenta" se encuentra diligenciado cuando NO debe estarlo para pedido DIFERIDO'
                                hay_novedad = True
                                
                                campos_novedad_cuenta = {
                                    'ObservacionesFase_4': truncar_observacion(observacion),
                                    'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                }
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                            
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='Cuenta',
                                valores_lista=[safe_str(d.get('Cuenta', '')) for d in datos_posiciones_usadas],
                                actualizar_aprobado=True, valor_aprobado=aprobados_cuenta
                            )
                    
                    # =========================================================
                    # RUTA D: NO TIENE ORDEN, ELEMENTO PEP NI ACTIVO FIJO (GENERALES)
                    # =========================================================
                    else:
                        print(f"[DEBUG] Validando como GENERALES (sin Orden, Elemento PEP ni Activo Fijo)")
                        
                        # GENERALES: Cuenta debe estar diligenciada
                        aprobados_cuenta = []
                        cuenta_valida = True
                        
                        for d in datos_posiciones_usadas:
                            cuenta = d.get('Cuenta', '')
                            if campo_con_valor(cuenta):
                                aprobados_cuenta.append('SI')
                            else:
                                aprobados_cuenta.append('NO')
                                cuenta_valida = False
                        
                        if not cuenta_valida:
                            observacion = f'Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero Campo "Cuenta" NO se encuentra diligenciado cuando debe estarlo para pedido GENERALES'
                            hay_novedad = True
                            
                            campos_novedad_cuenta = {
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Cuenta',
                            valores_lista=[safe_str(d.get('Cuenta', '')) for d in datos_posiciones_usadas],
                            actualizar_aprobado=True, valor_aprobado=aprobados_cuenta
                        )
                        
                        # GENERALES: Indicador impuestos debe estar diligenciado
                        aprobados_indicador = []
                        indicador_valido = True
                        
                        for d in datos_posiciones_usadas:
                            indicador = d.get('IndicadorImpuestos', '')
                            if campo_con_valor(indicador):
                                aprobados_indicador.append('SI')
                            else:
                                aprobados_indicador.append('NO')
                                indicador_valido = False
                        
                        if not indicador_valido:
                            observacion = f'Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero campo "Indicador impuestos" NO se encuentra diligenciado para pedido GENERALES'
                            hay_novedad = True
                            
                            campos_novedad_ind = {
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Indicador impuestos',
                            valores_lista=[safe_str(d.get('IndicadorImpuestos', '')) for d in datos_posiciones_usadas],
                            actualizar_aprobado=True, valor_aprobado=aprobados_indicador
                        )
                        
                        # GENERALES: Centro de coste debe estar diligenciado
                        aprobados_centro = []
                        centro_valido = True
                        
                        for d in datos_posiciones_usadas:
                            centro = d.get('CentroCoste', '')
                            if campo_con_valor(centro):
                                aprobados_centro.append('SI')
                            else:
                                aprobados_centro.append('NO')
                                centro_valido = False
                        
                        if not centro_valido:
                            observacion = f'Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero Campo "Centro de coste" NO se encuentra diligenciado cuando debe estarlo para pedido GENERALES'
                            hay_novedad = True
                            
                            campos_novedad_centro = {
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Centro de coste',
                            valores_lista=[safe_str(d.get('CentroCoste', '')) for d in datos_posiciones_usadas],
                            actualizar_aprobado=True, valor_aprobado=aprobados_centro
                        )
                        
                        # GENERALES: Validar indicador contra archivo Impuestos especiales
                        if indicador_valido and centro_valido and mapeo_ceco_impuestos:
                            aprobados_indicador_ceco = []
                            indicador_ceco_valido = True
                            
                            for d in datos_posiciones_usadas:
                                centro = safe_str(d.get('CentroCoste', '')).upper()
                                indicador = safe_str(d.get('IndicadorImpuestos', '')).upper()
                                
                                if centro in mapeo_ceco_impuestos:
                                    indicadores_permitidos = mapeo_ceco_impuestos[centro]
                                    if indicador in indicadores_permitidos:
                                        aprobados_indicador_ceco.append('SI')
                                    else:
                                        aprobados_indicador_ceco.append('NO')
                                        indicador_ceco_valido = False
                                else:
                                    # Si el CECO no está en el archivo, se considera válido
                                    aprobados_indicador_ceco.append('SI')
                            
                            if not indicador_ceco_valido:
                                # Obtener indicadores permitidos para el mensaje
                                centro_ejemplo = safe_str(datos_posiciones_usadas[0].get('CentroCoste', '')).upper()
                                inds_permitidos = mapeo_ceco_impuestos.get(centro_ejemplo, [])
                                inds_str = ', '.join(inds_permitidos) if inds_permitidos else 'N/A'
                                
                                observacion = f'Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero campo "Indicador impuestos" NO se encuentra diligenciado correctamente segun los indicadores ({inds_str})'
                                hay_novedad = True
                                
                                campos_novedad_ind_ceco = {
                                    'ObservacionesFase_4': truncar_observacion(observacion),
                                    'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                                }
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_ind_ceco)
                                
                                # Actualizar aprobación de indicador con resultado de validación CECO
                                actualizar_items_comparativa(
                                    id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                    nombre_item='Indicador impuestos',
                                    valores_lista=[safe_str(d.get('IndicadorImpuestos', '')) for d in datos_posiciones_usadas],
                                    actualizar_aprobado=True, valor_aprobado=aprobados_indicador_ceco
                                )
                    
                    # =========================================================
                    # 12. FINALIZAR REGISTRO
                    # =========================================================
                    if hay_novedad:
                        actualizar_estado_comparativa(cx, nit, numero_factura, f"CON NOVEDAD{sufijo_contado}")
                        registros_con_novedad += 1
                    else:
                        doc_compra = safe_str(registro.get('DocCompra_hoc', ''))
                        marcar_posiciones_procesadas(cx, doc_compra)
                        
                        campos_exitoso = {
                            'EstadoFinalFase_4': 'Exitoso',
                            'ResultadoFinalAntesEventos': f"PROCESADO{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_exitoso)
                        actualizar_estado_comparativa(cx, nit, numero_factura, f"PROCESADO{sufijo_contado}")
                        
                        print(f"[SUCCESS] Registro {registro_id} procesado exitosamente")
                        registros_exitosos += 1
                    
                    registros_procesados += 1
                    
                except Exception as e:
                    print(f"[ERROR] Error procesando registro {idx}: {str(e)}")
                    print(traceback.format_exc())
                    
                    try:
                        registro_id = safe_str(registro.get('ID_dp', ''))
                        campos_error = {
                            'EstadoFinalFase_4': 'Error',
                            'ObservacionesFase_4': f"Error en procesamiento: {str(e)[:500]}",
                            'ResultadoFinalAntesEventos': 'ERROR TECNICO'
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_error)
                    except:
                        pass
                    
                    registros_con_novedad += 1
                    registros_procesados += 1
                    continue
        
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
        
        SetVar("vLocStrResultadoSP", "True")
        SetVar("vLocStrResumenSP", resumen)
        
    except Exception as e:
        print("")
        print("=" * 80)
        print("[ERROR CRITICO] La funcion ZPSA_ZPSS_ValidarServicios fallo")
        print("=" * 80)
        print(f"[ERROR] Mensaje: {str(e)}")
        print(traceback.format_exc())
        print("=" * 80)
        
        SetVar("vGblStrDetalleError", str(e))
        SetVar("vGblStrSystemError", traceback.format_exc())
        SetVar("vLocStrResultadoSP", "False")


# Mock para pruebas locales
if __name__ == "__main__":
    _mock_vars = {}
    def GetVar(name):
        return _mock_vars.get(name, "")
    def SetVar(name, value):
        _mock_vars[name] = value
        print(f"[SETVAR] {name} = {value}")
    
    _mock_vars["vLocDicConfig"] = '{"ServidorBaseDatos":"localhost","NombreBaseDatos":"NotificationsPaddy"}'
    _mock_vars["vGblStrUsuarioBaseDatos"] = "sa"
    _mock_vars["vGblStrClaveBaseDatos"] = "password"
    
    print("Ejecutando prueba local...")
    # ZPSA_ZPSS_ValidarServicios()
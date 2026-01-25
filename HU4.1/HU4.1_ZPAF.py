def ZPAF_ValidarActivosFijos():
    """
    Función para procesar las validaciones de ZPAF/41 (Pedidos Activos Fijos).
    
    VERSIÓN: 1.0 - 12 Enero 2026
    
    FLUJO PRINCIPAL:
        1. Lee registros de [CxP].[Trans_Candidatos_HU41] con ClaseDePedido_hoc IN ('ZPAF', '41')
        2. Para cada registro:
           a. Determina si es USD o no (campo Moneda_hoc)
           b. Si USD: compara VlrPagarCop vs PorCalcular_hoc
           c. Si NO USD: compara Valor de la Compra LEA vs PorCalcular_hoc
           d. Valida TRM
           e. Valida Nombre Emisor
           f. Valida Activo fijo (9 dígitos)
           g. Valida Capitalizado el (NUNCA diligenciado)
           h. Valida Indicador impuestos (H4/H5/VP o H6/H7/VP sin mezclar)
           i. Valida Criterio clasif. 2 (según indicador)
           j. Valida Cuenta (debe ser 2695950020)
        3. Actualiza [CxP].[DocumentsProcessing] con estados y observaciones
        4. Genera trazabilidad en [dbo].[CxP.Comparativa]
    
    NOTA IMPORTANTE SOBRE PaymentMeans:
        - Si PaymentMeans = '01', se agrega ' CONTADO' al resultado final
        - Ejemplo: 'CON NOVEDAD CONTADO'
    
    ESTRUCTURA DE DATOS:
        - Campos _dp: datos del XML (DocumentsProcessing)
        - Campos _hoc: datos del histórico de órdenes de compra
        - Campos _ddp: datos del detalle de documento
        - DocumentsProcessing NO tiene sufijos (nombres base)
    
    Returns:
        None: Actualiza variables globales en RocketBot
            - vLocStrResultadoSP: 'True' si exitoso, 'False' si error
            - vLocStrResumenSP: Resumen del procesamiento
            - vGblStrDetalleError: Detalle del error (si aplica)
            - vGblStrSystemError: Traceback completo (si aplica)
    """
    
    # =========================================================================
    # IMPORTS - Todas las librerías dentro de la función
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
    
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    # =========================================================================
    # FUNCIONES AUXILIARES BÁSICAS
    # =========================================================================
    
    def safe_str(v):
        """
        Convierte un valor a string de manera segura.
        
        Args:
            v: Valor a convertir (cualquier tipo)
            
        Returns:
            str: Valor convertido a string, vacío si es None o NaN
        """
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
        """
        Trunca observación para prevenir overflow en BD.
        
        Args:
            obs: Observación a truncar
            max_len: Longitud máxima permitida
            
        Returns:
            str: Observación truncada si excede max_len
        """
        if not obs:
            return ""
        obs_str = safe_str(obs)
        if len(obs_str) > max_len:
            return obs_str[:max_len - 3] + "..."
        return obs_str
    
    def parse_config(raw):
        """
        Parsea la configuración desde RocketBot.
        
        Args:
            raw: Configuración en formato dict, JSON string o literal Python
            
        Returns:
            dict: Configuración parseada
            
        Raises:
            ValueError: Si la configuración está vacía o es inválida
        """
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
        """
        Normaliza valores decimales con punto o coma.
        
        Args:
            valor: Valor numérico (puede ser string con coma/punto)
            
        Returns:
            float: Valor normalizado, 0.0 si es inválido
        """
        if pd.isna(valor) or valor == '' or valor is None:
            return 0.0
        if isinstance(valor, (int, float)):
            if np.isnan(valor) if isinstance(valor, float) else False:
                return 0.0
            return float(valor)
        valor_str = str(valor).strip()
        # Reemplazar coma por punto para decimales
        valor_str = valor_str.replace(',', '.')
        # Eliminar espacios y caracteres no numéricos excepto punto y signo
        valor_str = re.sub(r'[^\d.\-]', '', valor_str)
        try:
            return float(valor_str)
        except:
            return 0.0
    
    # =========================================================================
    # FUNCIONES DE CONEXIÓN A BASE DE DATOS
    # =========================================================================
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        """
        Crea conexión a la base de datos con reintentos y manejo de transacciones.
        
        Args:
            cfg: Diccionario de configuración con ServidorBaseDatos y NombreBaseDatos
            max_retries: Número máximo de reintentos
            
        Yields:
            pyodbc.Connection: Conexión a la base de datos
            
        Raises:
            ValueError: Si faltan parámetros de configuración
            pyodbc.Error: Si falla la conexión después de todos los reintentos
        """
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            raise ValueError(f"Parametros faltantes: {', '.join(missing)}")

        usuario = cfg['UsuarioBaseDatos']
        contrasena = cfg['ClaveBaseDatos']
        
        # Estrategia de conexion:
        # 1. Intentar con Usuario y Contraseña (para Produccion)
        # 2. Si falla, intentar con Trusted Connection (para Desarrollo/Windows Auth)

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
        ########################
        print("[DEBUG] Intentando conexion con Usuario/Contraseña...")
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

        # Intento 2: Trusted Connection (si fallo el anterior)
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
    # FUNCIONES DE NORMALIZACIÓN Y COMPARACIÓN DE NOMBRES
    # =========================================================================
    
    def normalizar_nombre_empresa(nombre):
        """
        Normaliza nombres de empresas según las reglas de la HU.
        
        Variaciones manejadas:
            - SAS: S.A.S., S. A. S., S A S, etc.
            - LTDA: Limitada, Ltda., Ltda
            - S EN C: S. EN C., Comandita
            - Mayúsculas/minúsculas
            - Puntuación y espacios
        
        Args:
            nombre: Nombre de la empresa
            
        Returns:
            str: Nombre normalizado en mayúsculas sin puntuación
        """
        if pd.isna(nombre) or nombre == "":
            return ""
        
        nombre = safe_str(nombre).upper().strip()
        
        # Eliminar puntuación y espacios para comparación
        nombre_limpio = re.sub(r'[,.\s]', '', nombre)
        
        # Normalizar variantes de tipo de sociedad
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
        que contengan exactamente las mismas palabras sin importar el orden.
        """
        if pd.isna(nombre_xml) or pd.isna(nombre_sap):
            return False
        
        # 1. Normalizar y limpiar los textos (asumo que tu función los deja en minúsculas y sin puntuación)
        nombre_xml_limpio = normalizar_nombre_empresa(str(nombre_xml))
        nombre_sap_limpio = normalizar_nombre_empresa(str(nombre_sap))
        
        # 2. Dividir por espacios para crear las listas de palabras
        lista_xml = nombre_xml_limpio.split()
        lista_sap = nombre_sap_limpio.split()
        
        # 3. Regla 1: La cantidad de items en ambos listados debe ser la misma
        if len(lista_xml) != len(lista_sap):
            return False
            
        # 4. Regla 2: Todos los items deben coincidir sin importar la posición.
        # Usar sorted() organiza las listas alfabéticamente. 
        # Si tienen los mismos elementos, las listas ordenadas serán idénticas.
        return sorted(lista_xml) == sorted(lista_sap)
    
    # =========================================================================
    # FUNCIONES DE VALIDACION DE DATOS
    # =========================================================================

    def comparar_suma_total(valores_por_calcular, valor_objetivo, tolerancia=500):
        """
        Suma TODOS los valores de la lista y compara si el total coincide 
        con el valor objetivo dentro de un rango de tolerancia.
        
        Args:
            valores_por_calcular: Lista de tuplas (posicion, valor)
            valor_objetivo: Valor a buscar
            tolerancia: Tolerancia permitida (default: 500)
            
        Returns:
            tuple: (coincide, lista_todas_posiciones, suma_total)
        """
        valor_objetivo = normalizar_decimal(valor_objetivo)
        
        if valor_objetivo <= 0 or not valores_por_calcular:
            return False, [], 0
            
        # 1. Sumar TODOS los valores de la lista
        suma_total = sum(normalizar_decimal(valor) for posicion, valor in valores_por_calcular)
        
        # 2. Comparar esa suma total con el objetivo (+/- tolerancia)
        if abs(suma_total - valor_objetivo) <= tolerancia:
            # Extraer todas las posiciones, ya que usamos todos los valores
            todas_las_posiciones = [posicion for posicion, valor in valores_por_calcular]
            return True, todas_las_posiciones, suma_total
            
        return False, [], 0
    
    def validar_activo_fijo(valor):
        """
        Valida que el campo Activo fijo esté diligenciado con 9 dígitos.
        
        Args:
            valor: Valor del campo Activo fijo
            
        Returns:
            bool: True si es válido (9 dígitos)
        """
        valor_str = safe_str(valor)
        if not valor_str:
            return False
        # Limpiar y verificar que sean exactamente 9 dígitos
        valor_limpio = re.sub(r'\D', '', valor_str)
        return len(valor_limpio) == 9
    
    def validar_capitalizado_el(valor):
        """
        Valida que el campo Capitalizado el NO esté diligenciado.
        
        Args:
            valor: Valor del campo Capitalizado el
            
        Returns:
            bool: True si NO está diligenciado (correcto)
        """
        valor_str = safe_str(valor)
        return valor_str == "" or valor_str.lower() == "null" or valor_str.lower() == "none"
    
    def validar_indicador_impuestos(indicadores_lista):
        """
        Valida que los indicadores de impuestos sean válidos y no mezclen grupos.
        
        Grupo 1 (productores): H4, H5, VP
        Grupo 2 (no productores): H6, H7, VP
        
        Args:
            indicadores_lista: Lista de indicadores de todas las posiciones
            
        Returns:
            tuple: (es_valido, mensaje_error, grupo_detectado)
        """
        indicadores_validos_g1 = {'H4', 'H5', 'VP'}  # Productores
        indicadores_validos_g2 = {'H6', 'H7', 'VP'}  # No productores
        
        indicadores_limpios = set()
        for ind in indicadores_lista:
            ind_str = safe_str(ind).upper().strip()
            if ind_str:
                indicadores_limpios.add(ind_str)
        
        if not indicadores_limpios:
            return False, "NO se encuentra diligenciado", None
        
        # Verificar que todos sean indicadores válidos
        todos_validos = indicadores_limpios.issubset(indicadores_validos_g1.union(indicadores_validos_g2))
        if not todos_validos:
            invalidos = indicadores_limpios - indicadores_validos_g1.union(indicadores_validos_g2)
            return False, f"NO corresponde alguna de las opciones 'H4', 'H5', 'H6', 'H7' o 'VP' en pedido de Activos fijos", None
        
        # Determinar el grupo
        tiene_g1 = bool(indicadores_limpios.intersection({'H4', 'H5'}))
        tiene_g2 = bool(indicadores_limpios.intersection({'H6', 'H7'}))
        
        if tiene_g1 and tiene_g2:
            return False, "NO se encuentra aplicado correctamente", None
        
        grupo = 'G1' if tiene_g1 else ('G2' if tiene_g2 else 'VP_ONLY')
        
        return True, "", grupo
    
    def validar_criterio_clasif_2(indicador, criterio):
        """
        Valida que Criterio clasif. 2 corresponda según el Indicador impuestos.
        
        Reglas:
            - H4/H5 → 0001
            - H6/H7 → 0000
            - VP → 0001 o 0000
        
        Args:
            indicador: Valor del campo Indicador impuestos
            criterio: Valor del campo Criterio clasif. 2
            
        Returns:
            tuple: (es_valido, mensaje_error)
        """
        indicador_str = safe_str(indicador).upper().strip()
        criterio_str = safe_str(criterio).strip()
        
        if not criterio_str:
            return False, "NO se encuentra diligenciado"
        
        if indicador_str in ('H4', 'H5'):
            if criterio_str == '0001':
                return True, ""
            else:
                return False, f"NO se encuentra aplicado correctamente segun reglas 'H4 y H5 = 0001', 'H6 y H7 = 0000' o 'VP = 0001 o 0000'"
        
        elif indicador_str in ('H6', 'H7'):
            if criterio_str == '0000':
                return True, ""
            else:
                return False, f"NO se encuentra aplicado correctamente segun reglas 'H4 y H5 = 0001', 'H6 y H7 = 0000' o 'VP = 0001 o 0000'"
        
        elif indicador_str == 'VP':
            if criterio_str in ('0001', '0000'):
                return True, ""
            else:
                return False, f"NO se encuentra aplicado correctamente segun reglas 'H4 y H5 = 0001', 'H6 y H7 = 0000' o 'VP = 0001 o 0000'"
        
        return False, "Indicador impuestos no reconocido"
    
    def validar_cuenta_zpaf(cuenta):
        """
        Valida que el campo Cuenta sea igual a 2695950020.
        
        Args:
            cuenta: Valor del campo Cuenta
            
        Returns:
            bool: True si es igual a 2695950020
        """
        cuenta_str = safe_str(cuenta).strip()
        return cuenta_str == '2695950020'
    
    # =========================================================================
    # FUNCIONES DE ACTUALIZACIÓN DE BASE DE DATOS
    # =========================================================================
    
    def actualizar_bd_cxp(cx, registro_id, campos_actualizar):
        """
        Actualiza campos en [CxP].[DocumentsProcessing].
        
        IMPORTANTE: DocumentsProcessing NO tiene sufijos _dp en sus columnas.
        
        Para ObservacionesFase_4: Conserva observaciones previas separadas por coma,
        pero prima la última observación.
        
        Args:
            cx: Conexión a la base de datos
            registro_id: ID del registro a actualizar
            campos_actualizar: Diccionario {nombre_campo: valor}
        """
        try:
            sets = []
            parametros = []
            
            for campo, valor in campos_actualizar.items():
                if valor is not None:
                    # Manejo especial para ObservacionesFase_4 (concatenar)
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
                affected_rows = cur.rowcount
                cur.close()
                
                if affected_rows > 0:
                    print(f"[UPDATE] DocumentsProcessing actualizada - ID {registro_id}")
                else:
                    print(f"[WARNING] No se encontro registro ID {registro_id} en DocumentsProcessing")
            
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
        """
        Expande valores separados por | o comas.
        
        Args:
            valor_string: String con valores separados
            separador: Separador principal (default: |)
            
        Returns:
            list: Lista de valores individuales
        """
        if pd.isna(valor_string) or valor_string == '' or valor_string is None:
            return []
        
        valor_str = safe_str(valor_string)
        
        # Intentar con separador |
        if '|' in valor_str:
            return [v.strip() for v in valor_str.split('|') if v.strip()]
        
        # Intentar con coma
        if ',' in valor_str:
            return [v.strip() for v in valor_str.split(',') if v.strip()]
        
        # Sin separador, devolver como lista de un elemento
        return [valor_str.strip()]
    
    def expandir_posiciones_historico(registro):
        """
        Expande las posiciones del histórico que están concatenadas en el registro.
        
        Los campos del histórico terminan en _hoc y pueden tener múltiples valores
        separados por | o coma.
        
        Args:
            registro: Registro de Trans_Candidatos_HU41 (dict o Series)
            
        Returns:
            list: Lista de diccionarios con datos de cada posición
        """
        try:
            # Expandir posiciones
            posiciones = expandir_posiciones_string(registro.get('Posicion_hoc', ''))
            
            if not posiciones:
                return []
            
            # Expandir valores correspondientes
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
            ciudad_prov_list = expandir_posiciones_string(registro.get('CiudadProveedor_hoc', ''))
            doc_fi_entrada_list = expandir_posiciones_string(registro.get('DocFiEntrada_hoc', ''))
            cuenta26_list = expandir_posiciones_string(registro.get('Cuenta26_hoc', ''))
            activo_fijo_list = expandir_posiciones_string(registro.get('ActivoFijo_hoc', ''))
            capitalizado_el_list = expandir_posiciones_string(registro.get('CapitalizadoEl_hoc', ''))
            criterio_clasif2_list = expandir_posiciones_string(registro.get('CriterioClasif2_hoc', ''))
            moneda_list = expandir_posiciones_string(registro.get('Moneda_hoc', ''))
            
            # Datos comunes (usualmente no varían por posición)
            n_proveedor = safe_str(registro.get('NProveedor_hoc', ''))
            
            # Crear lista de datos por posición
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
                    'CiudadProveedor': ciudad_prov_list[i] if i < len(ciudad_prov_list) else (ciudad_prov_list[0] if ciudad_prov_list else ''),
                    'DocFiEntrada': doc_fi_entrada_list[i] if i < len(doc_fi_entrada_list) else (doc_fi_entrada_list[0] if doc_fi_entrada_list else ''),
                    'Cuenta26': cuenta26_list[i] if i < len(cuenta26_list) else (cuenta26_list[0] if cuenta26_list else ''),
                    'ActivoFijo': activo_fijo_list[i] if i < len(activo_fijo_list) else (activo_fijo_list[0] if activo_fijo_list else ''),
                    'CapitalizadoEl': capitalizado_el_list[i] if i < len(capitalizado_el_list) else (capitalizado_el_list[0] if capitalizado_el_list else ''),
                    'CriterioClasif2': criterio_clasif2_list[i] if i < len(criterio_clasif2_list) else (criterio_clasif2_list[0] if criterio_clasif2_list else ''),
                    'Moneda': moneda_list[i] if i < len(moneda_list) else (moneda_list[0] if moneda_list else '')
                }
                datos_posiciones.append(datos_pos)
            
            return datos_posiciones
            
        except Exception as e:
            print(f"[ERROR] Error expandiendo posiciones del historico: {str(e)}")
            return []

    # =========================================================================
    # INICIO DEL PROCESAMIENTO PRINCIPAL
    # =========================================================================
    
    try:
        print("")
        print("=" * 80)
        print("[INICIO] Procesamiento ZPAF/41 - Activos Fijos")
        print("=" * 80)
        
        t_inicio = time.time()
        
        # 1. Obtener y validar configuración
        #cfg = parse_config(GetVar("vLocDicConfig"))
        #############################
        cfg = {}
        cfg['ServidorBaseDatos'] = 'localhost\SQLEXPRESS'
        cfg['NombreBaseDatos'] = 'NotificationsPaddy'
        cfg['UsuarioBaseDatos'] = 'aa'
        cfg['ClaveBaseDatos'] = 'aa'
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
        # cfg[''] = 
        
        
        #----------------------
        print("[INFO] Configuracion cargada exitosamente")
        
        # Parámetros requeridos (solo BD, no hay archivos maestros)
        required_config = ['ServidorBaseDatos', 'NombreBaseDatos']
        
        missing_config = [k for k in required_config if not cfg.get(k)]
        if missing_config:
            raise ValueError(f"Faltan parametros de configuracion: {', '.join(missing_config)}")
        
        # 2. Conectar a base de datos y obtener registros ZPAF/41
        with crear_conexion_db(cfg) as cx:
            print("[INFO] Obteniendo registros ZPAF/41 para procesar...")
            
            # Query para obtener registros ZPAF y 41 desde Trans_Candidatos_HU41
            query_zpaf = """
                SELECT * FROM [CxP].[HU41_CandidatosValidacion]
                WHERE [ClaseDePedido_hoc] IN ('ZPAF', '41')
            """
            
            df_registros = pd.read_sql(query_zpaf, cx)
            
            print(f"[INFO] Obtenidos {len(df_registros)} registros ZPAF/41 para procesar")
            
            if len(df_registros) == 0:
                print("[INFO] No hay registros ZPAF/41 pendientes de procesar")
                #SetVar("vLocStrResultadoSP", "True")
                #SetVar("vLocStrResumenSP", "No hay registros ZPAF/41 pendientes de procesar")
                return
            
            # Variables de conteo
            registros_procesados = 0
            registros_con_novedad = 0
            registros_exitosos = 0
            ##########################################
            # 3. Procesar cada registro
            for idx, registro in df_registros.iterrows():
                try:
                    registro_id = safe_str(registro.get('ID_dp', ''))
                    numero_oc = safe_str(registro.get('numero_de_liquidacion_u_orden_de_compra_dp', ''))
                    numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
                    payment_means = safe_str(registro.get('forma_de_pago_dp', ''))
                    nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
                    
                    print(f"\n[PROCESO] Registro {registros_procesados + 1}/{len(df_registros)}: OC {numero_oc}, Factura {numero_factura}")
                    
                    # Determinar sufijo CONTADO según PaymentMeans
                    sufijo_contado = " CONTADO" if payment_means in ['1', '01'] else ""
                    
                    # 4. Expandir posiciones del histórico
                    datos_posiciones = expandir_posiciones_historico(registro)

                    # 5. Determinar si es USD
                    moneda = safe_str(datos_posiciones[0].get('Moneda', '')).upper()
                    es_usd = moneda == 'USD'
                    
                    print(f"[DEBUG] Moneda: {moneda}, Es USD: {es_usd}")
                    
                    # 6. Obtener valor a comparar según moneda
                    if es_usd:
                        valor_xml = normalizar_decimal(registro.get('VlrPagarCop_dp', 0))
                        campo_valor_nombre = 'VlrPagarCop'
                    else:
                        valor_xml = normalizar_decimal(registro.get('Valor de la Compra LEA_ddp', 0))
                        campo_valor_nombre = 'Valor de la Compra LEA (LineExtensionAmount)'
                    
                    print(f"[DEBUG] Campo valor: {campo_valor_nombre}, Valor XML: {valor_xml}")
                    
                    # 7. Preparar valores para búsqueda de combinación
                    valores_por_calcular = [(d['Posicion'], d['PorCalcular']) for d in datos_posiciones]
                    
                    print(f"[DEBUG] Posiciones disponibles: {len(valores_por_calcular)}")
                    
                    # 8. Buscar combinación de posiciones que coincida
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
                        
                        print(f"[DEBUG] Coincidencia encontrada con posiciones: {posiciones_usadas}, Suma: {suma_encontrada}")
                    
                        # Filtrar datos de posiciones usadas
                        datos_posiciones_usadas = [d for d in datos_posiciones if d['Posicion'] in posiciones_usadas]

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
                    
                    # 10. Validar TRM
                    print(f"[DEBUG] Validando TRM...")
                    
                    trm_xml = normalizar_decimal(registro.get('CalculationRate_dp', 0))
                    trm_sap = normalizar_decimal(datos_posiciones_usadas[0].get('Trm', 0))
                    
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
                    
                    # 11. Validar Nombre Emisor
                    print(f"[DEBUG] Validando nombre emisor...")
                    
                    nombre_emisor_xml = safe_str(registro.get('nombre_emisor_dp', ''))
                    nombre_proveedor_sap = safe_str(datos_posiciones_usadas[0].get('NProveedor', ''))
                    
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
                    
                    # 12. Validar Activo Fijo (9 dígitos)
                    print(f"[DEBUG] Validando Activo fijo...")
                    
                    aprobados_activo_fijo = []
                    activo_fijo_valido = True
                    listado_activoFijo = registro['ActivoFijo_hoc'].split('|')
                    
                    for d in listado_activoFijo:
                        if not validar_activo_fijo(d):
                            activo_fijo_valido = False
                    
                    if not activo_fijo_valido:
                        print(f"[INFO] Activo fijo no valido en alguna posicion")
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        observacion = f'Pedido corresponde a ZPAF pero campo "Activo fijo" NO se encuentra diligenciado y/o NO corresponde a un dato de 9 digitos, {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        
                        campos_novedad_af = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_af)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='ActivoFijo',
                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['ActivoFijo_hoc'])
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Observaciones',
                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='ActivoFijo',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['ActivoFijo_hoc'])
                        
                    # 13. Validar Capitalizado el (NUNCA debe estar diligenciado)
                    print(f"[DEBUG] Validando Capitalizado el...")
                    
                    aprobados_capitalizado = []
                    capitalizado_valido = True
                    listado_capitalizado = registro['CapitalizadoEl_hoc'].split('|')
                    for d in listado_capitalizado:
                        if not validar_capitalizado_el(d):
                            capitalizado_valido = False
                    
                    if not capitalizado_valido:
                        print(f"[INFO] Capitalizado el esta diligenciado cuando NO deberia")
                        observacion = f'Pedido corresponde a ZPAF (Activo fijo) pero campo "Capitalizado el" se encuentra diligenciado cuando NUNCA debe estarlo, {registro["ObservacionesFase_4_dp"]}'
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        
                        campos_novedad_cap = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_cap)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Observaciones',
                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='CapitalizadoEl',
                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['CapitalizadoEl_hoc'])
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='CapitalizadoEl',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['CapitalizadoEl_hoc'])
                        
                    # 14. Validar Indicador impuestos
                    print(f"[DEBUG] Validando Indicador impuestos...")
                    
                    listado_indicador = registro['IndicadorImpuestos_hoc'].split('|')
                    indicador_valido, msg_indicador, grupo_indicador = validar_indicador_impuestos(listado_indicador)
                    
                    aprobados_indicador = []
                    if not indicador_valido:
                        print(f"[INFO] Indicador impuestos no valido: {msg_indicador}")
                        observacion = f'Pedido corresponde a ZPAF pero campo "Indicador impuestos" {msg_indicador} , {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        
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
                            nombre_item='IndicadorImpuestos',
                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='IndicadorImpuestos',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['IndicadorImpuestos_hoc'])
                    
                    # 15. Validar Criterio clasif. 2
                    print(f"[DEBUG] Validando Criterio clasif. 2...")
                    
                    aprobados_criterio = []
                    criterio_valido = True
                    listado_indicador = registro['IndicadorImpuestos_hoc'].split('|')
                    listado_clasif2 = registro['CriterioClasif2_hoc'].split('|')
                    
                    for indicador, criterio in zip(listado_indicador, listado_clasif2):
    
                        # Es buena práctica hacer .strip() por si quedaron espacios al hacer el split
                        es_valido_crit, msg_crit = validar_criterio_clasif_2(indicador.strip(), criterio.strip())
                        
                        if es_valido_crit:
                            aprobados_criterio.append('SI')
                        else:
                            aprobados_criterio.append('NO')
                            criterio_valido = False
                    
                    if not criterio_valido:
                        print(f"[INFO] Criterio clasif. 2 no valido")
                        # Determinar mensaje según el error
                        criterios = [d.get('CriterioClasif2', '') for d in datos_posiciones_usadas]
                        if all(not safe_str(c) for c in criterios):
                            observacion = f'Pedido corresponde a ZPAF pero campo "Criterio clasif." 2 NO se encuentra diligenciado, {registro['ObservacionesFase_4_dp']}'
                        else:
                            observacion = f'Pedido corresponde a ZPAF pero campo "Criterio clasif." 2 NO se encuentra aplicado correctamente segun reglas "H4 y H5 = 0001", "H6 y H7 = 0000" o "VP = 0001 o 0000", {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        
                        campos_novedad_crit = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_crit)
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='CriterioClasif2',
                            valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro['CriterioClasif2_hoc'])
                        
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Observaciones',
                            valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='CriterioClasif2',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['CriterioClasif2_hoc'])
                    
                    # 16. Validar Cuenta (debe ser 2695950020)
                    print(f"[DEBUG] Validando Cuenta...")
                    
                    aprobados_cuenta = []
                    cuenta_valida = True
                    listado_cuenta = registro['Cuenta_hoc'].split('|')
                    
                    for d in listado_cuenta:
                        if not validar_cuenta_zpaf(d):
                            cuenta_valida = False
                    
                    if not cuenta_valida:
                        print(f"[INFO] Cuenta no es igual a 2695950020")
                        observacion = f'Pedido corresponde a ZPAF, pero Campo "Cuenta" NO corresponde a 2695950020, {registro['ObservacionesFase_4_dp']}'
                        hay_novedad = True
                        
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
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                            nombre_item='Cuenta',
                            valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro['Cuenta_hoc'])
                    
                    
                    # 17. Finalizar registro
                    if hay_novedad:
                        actualizar_estado_comparativa(cx, nit, numero_factura, f"CON NOVEDAD {sufijo_contado}")
                        registros_con_novedad += 1
                    else:
                        # Marcar como PROCESADO
                        doc_compra = safe_str(registro.get('DocCompra_hoc', ''))
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro['Posicion_hoc']))
                        
                        campos_exitoso = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACIÓN: Exitoso',
                            'ResultadoFinalAntesEventos': f"PROCESADO {sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_exitoso)
                        
                        print(f"[SUCCESS] Registro {registro_id} procesado exitosamente")
                        registros_exitosos += 1
                    
                    registros_procesados += 1
                    
                except Exception as e:
                    print(f"[ERROR] Error procesando registro {idx}: {str(e)}")
                    print(traceback.format_exc())
                    
                    # SetVar("vGblStrDetalleError", traceback.format_exc())
                    # SetVar("vGblStrSystemError", "ErrorHU4_4.1")
                    
                    registros_con_novedad += 1
                    registros_procesados += 1
                    continue
        
        # Fin del procesamiento
        tiempo_total = time.time() - t_inicio
        
        print("")
        print("=" * 80)
        print("[FIN] Procesamiento ZPAF/41 - Activos Fijos completado")
        print("=" * 80)
        print("[ESTADISTICAS]")
        print(f"  Total registros procesados: {registros_procesados}")
        print(f"  Exitosos: {registros_exitosos}")
        print(f"  Con novedad: {registros_con_novedad}")
        print(f"  Tiempo total: {round(tiempo_total, 2)}s")
        print("=" * 80)
        
        resumen = f"Procesados {registros_procesados} registros ZPAF/41. Exitosos: {registros_exitosos}, Con novedad: {registros_con_novedad}"
        
        # SetVar("vLocStrResultadoSP", "True")
        # SetVar("vLocStrResumenSP", resumen)
        
    except Exception as e:
        exc_type = type(e).__name__
        print("")
        print("=" * 80)
        print("[ERROR CRITICO] La funcion ZPAF_ValidarActivosFijos fallo")
        print("=" * 80)
        print(f"[ERROR] Tipo de error: {exc_type}")
        print(f"[ERROR] Mensaje: {str(e)}")
        print("[ERROR] Traceback completo:")
        print(traceback.format_exc())
        print("=" * 80)
        
        #SetVar("vGblStrDetalleError", traceback.format_exc())
        #SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        #SetVar("vLocStrResultadoSP", "False")



ZPAF_ValidarActivosFijos()
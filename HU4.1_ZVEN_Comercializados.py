def ZVEN_ValidarComercializados():
    """
    Función para procesar las validaciones de ZVEN/50 (Pedidos Comercializados).
    
    VERSIÓN: 1.0 - 12 Enero 2026
    
    FLUJO PRINCIPAL:
        1. Lee registros de [CxP].[Trans_Candidatos_HU41] con ClaseDePedido_hoc IN ('ZVEN', '50')
        2. Valida archivos maestros (Maestro de comercializados.xlsx y Asociación cuenta indicador.xlsx)
        3. Para cada registro:
           - Busca OC y Factura en el maestro de comercializados
           - Si NO existe: marca EN ESPERA y mueve insumos a carpeta destino
           - Si EXISTE: valida posiciones contra histórico, TRM, cantidad/precio, nombre emisor
        4. Actualiza [CxP].[DocumentsProcessing] con estados y observaciones
        5. Genera trazabilidad en [dbo].[CxP.Comparativa]
    
    NOTA IMPORTANTE SOBRE PaymentMeans:
        - Si PaymentMeans = '01', se agrega ' CONTADO' al resultado final
        - Ejemplo: 'CON NOVEDAD - COMERCIALIZADOS CONTADO'
    
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
    import os
    import shutil
    import re
    from decimal import Decimal, ROUND_HALF_UP
    
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
    # FUNCIONES DE VALIDACIÓN DE ARCHIVOS MAESTROS
    # =========================================================================
    
    def validar_archivo_maestro_comercializados(ruta_archivo):
        """
        Valida la estructura del archivo Maestro de comercializados.xlsx.
        
        Columnas requeridas según HU:
            - OC
            - FACTURA
            - VALOR TOTAL OC
            - POSICION
            - PorCalcular_hoc (VALOR UNITARIO)
            - PorCalcular_hoc (ME)
        
        Args:
            ruta_archivo: Ruta completa al archivo Excel
            
        Returns:
            tuple: (es_valido, mensaje, dataframe)
        """
        try:
            if not os.path.exists(ruta_archivo):
                return False, f"No existe el archivo: {ruta_archivo}", None
            
            df = pd.read_excel(ruta_archivo)
            
            # Limpiar nombres de columnas (quitar espacios extra)
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
            
            # Convertir OC y FACTURA a string para comparaciones
            df['OC'] = df['OC'].astype(str).str.strip()
            df['FACTURA'] = df['FACTURA'].astype(str).str.strip()
            df['POSICION'] = df['POSICION'].astype(str).str.strip()
            
            print(f"[DEBUG] Maestro de comercializados validado - {len(df)} registros")
            return True, "Estructura valida", df
            
        except Exception as e:
            return False, f"Error validando Maestro de comercializados: {str(e)}", None
    
    def validar_archivo_asociacion_cuenta(ruta_archivo):
        """
        Valida la estructura del archivo Asociación cuenta indicador.xlsx.
        
        Hoja requerida: 'grupo cuentas agrupacion provee' (o 'Grupo cuentas prove')
        Columnas requeridas:
            - Cta Mayor
            - Nombre cuenta
            - TIPO RET.
            - IND.RETENCION
            - Descripcion ind.Ret. (o similar)
            - Agrupacion codigo
            - Nombre codigo
        
        Args:
            ruta_archivo: Ruta completa al archivo Excel
            
        Returns:
            tuple: (es_valido, mensaje, dataframe)
        """
        try:
            if not os.path.exists(ruta_archivo):
                return False, f"No existe el archivo: {ruta_archivo}", None
            
            # Intentar con diferentes nombres de hoja
            xl = pd.ExcelFile(ruta_archivo)
            hojas_posibles = ['Grupo cuentas prove', 'grupo cuentas agrupacion provee', 
                           'Grupo cuentas agrupacion provee']
            
            hoja_encontrada = None
            for hoja in hojas_posibles:
                if hoja in xl.sheet_names:
                    hoja_encontrada = hoja
                    break
            
            if not hoja_encontrada:
                # Usar la primera hoja que contenga 'cuentas' en el nombre
                for hoja in xl.sheet_names:
                    if 'cuentas' in hoja.lower():
                        hoja_encontrada = hoja
                        break
            
            if not hoja_encontrada:
                return False, f"No se encontro hoja de cuentas en: {xl.sheet_names}", None
            
            df = pd.read_excel(ruta_archivo, sheet_name=hoja_encontrada)
            
            # Verificar columnas (permitir variaciones menores)
            columnas_requeridas_base = ['Cta Mayor', 'Nombre cuenta', 'TIPO RET.', 'IND.RETENCION']
            
            columnas_faltantes = []
            for col in columnas_requeridas_base:
                if col not in df.columns:
                    # Buscar variación
                    encontrado = False
                    for df_col in df.columns:
                        if col.lower().replace('.', '').replace(' ', '') in df_col.lower().replace('.', '').replace(' ', ''):
                            encontrado = True
                            break
                    if not encontrado:
                        columnas_faltantes.append(col)
            
            if columnas_faltantes:
                return False, f"Columnas faltantes en Asociacion cuenta indicador: {columnas_faltantes}", None
            
            print(f"[DEBUG] Asociacion cuenta indicador validado - {len(df)} registros")
            return True, "Estructura valida", df
            
        except Exception as e:
            return False, f"Error validando Asociacion cuenta indicador: {str(e)}", None
    
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
    
    def convertir_nombre_persona(nombre_completo):
        """
        Convierte el orden del nombre de persona según la HU.
        
        XML: Nombres + Apellidos -> SAP: Apellidos + Nombres
        
        Ejemplo:
            'ALEXANDER LOZANO CALDERON' -> 'LOZANO CALDERON ALEXANDER'
        
        Args:
            nombre_completo: Nombre en formato Nombres + Apellidos
            
        Returns:
            str: Nombre en formato Apellidos + Nombres
        """
        if pd.isna(nombre_completo) or nombre_completo == "":
            return ""
        
        partes = safe_str(nombre_completo).strip().split()
        
        if len(partes) >= 3:
            # Los últimos 2 son apellidos, el resto son nombres
            apellidos = partes[-2:]
            nombres = partes[:-2]
            return " ".join(apellidos + nombres)
        
        return nombre_completo
    
    def comparar_nombres_proveedor(nombre_xml, nombre_sap):
        """
        Compara nombres de proveedores aplicando todas las reglas de la HU.
        
        Aplica:
            1. Normalización de empresas (SAS, LTDA, etc.)
            2. Conversión de orden de nombres de persona
            3. Comparación case-insensitive sin puntuación
        
        Args:
            nombre_xml: Nombre del XML (DocumentsProcessing)
            nombre_sap: Nombre del SAP (HistoricoOrdenesCompra)
            
        Returns:
            bool: True si los nombres coinciden
        """
        if pd.isna(nombre_xml) or pd.isna(nombre_sap):
            return False
        
        # Primero intentar como empresa
        nombre_xml_empresa = normalizar_nombre_empresa(nombre_xml)
        nombre_sap_empresa = normalizar_nombre_empresa(nombre_sap)
        
        if nombre_xml_empresa == nombre_sap_empresa:
            return True
        
        # Intentar con conversión de nombre de persona
        nombre_xml_persona = normalizar_nombre_empresa(convertir_nombre_persona(nombre_xml))
        nombre_sap_persona = normalizar_nombre_empresa(convertir_nombre_persona(nombre_sap))
        
        if nombre_xml_persona == nombre_sap_empresa or nombre_xml_empresa == nombre_sap_persona:
            return True
        
        # Intentar también SAP convertido
        if nombre_xml_empresa == nombre_sap_persona:
            return True
        
        return False
    
    # =========================================================================
    # FUNCIONES DE VALIDACIÓN DE DATOS
    # =========================================================================
    
    def validar_tolerancia_numerica(valor1, valor2, tolerancia=500):
        """
        Valida si dos valores numéricos están dentro del rango de tolerancia.
        
        Args:
            valor1: Primer valor
            valor2: Segundo valor
            tolerancia: Diferencia máxima permitida (default: 500)
            
        Returns:
            bool: True si la diferencia es <= tolerancia
        """
        try:
            val1 = normalizar_decimal(valor1)
            val2 = normalizar_decimal(valor2)
            return abs(val1 - val2) <= tolerancia
        except:
            return False
    
    def validar_cantidad_precio_tolerancia(cantidad_xml, precio_xml, cantidad_sap, precio_sap, valor_total_factura):
        """
        Valida cantidad y precio con tolerancia de 1 unidad/peso.
        
        La tolerancia de 1 NO debe hacer que el valor calculado exceda el valor total.
        
        Args:
            cantidad_xml: Cantidad del XML
            precio_xml: Precio unitario del XML
            cantidad_sap: Cantidad del SAP
            precio_sap: Precio unitario del SAP
            valor_total_factura: Valor total de la factura
            
        Returns:
            tuple: (cantidad_ok, precio_ok)
        """
        try:
            cantidad_xml = normalizar_decimal(cantidad_xml)
            precio_xml = normalizar_decimal(precio_xml)
            cantidad_sap = normalizar_decimal(cantidad_sap)
            precio_sap = normalizar_decimal(precio_sap)
            valor_total = normalizar_decimal(valor_total_factura)
            
            cantidad_ok = abs(cantidad_xml - cantidad_sap) <= 1
            precio_ok = abs(precio_xml - precio_sap) <= 1
            
            # Verificar que no exceda valor total
            if cantidad_ok and precio_ok:
                valor_calculado = cantidad_xml * precio_xml
                if valor_calculado > valor_total + 500:  # Con tolerancia
                    return False, False
            
            return cantidad_ok, precio_ok
            
        except Exception:
            return False, False
    
    # =========================================================================
    # FUNCIONES DE MANEJO DE ARCHIVOS
    # =========================================================================
    
    def copiar_insumos_a_carpeta_destino(ruta_origen, nombre_archivo, ruta_destino):
        """
        Copia insumos XML/PDF a la carpeta destino EN ESPERA - COMERCIALIZADOS.
        
        Args:
            ruta_origen: Carpeta donde está el archivo original
            nombre_archivo: Nombre del archivo a copiar
            ruta_destino: Carpeta destino
            
        Returns:
            tuple: (exito, ruta_nueva_o_mensaje_error)
        """
        try:
            # Crear carpeta destino si no existe
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
    
    def actualizar_items_comparativa(id_reg, cx, nit, factura, nombre_item, valores_lista,
                                     valores_comercializados=None,
                                     actualizar_valor_xml=False, valor_xml=None,
                                     actualizar_aprobado=False, valor_aprobado=None):
        """
        Actualiza o inserta items en [dbo].[CxP.Comparativa].
        
        Maneja:
            - Creación de nuevos items si no existen
            - Actualización de items existentes
            - Múltiples valores por item (posiciones)
        
        Args:
            id_reg: ID del registro
            cx: Conexión a BD
            nit: NIT del proveedor
            factura: Número de factura
            nombre_item: Nombre del item (ej: 'Posicion', 'TRM')
            valores_lista: Lista de valores para Valor_Orden_de_Compra
            valores_comercializados: Lista de valores para Valor_Orden_de_Compra_Comercializados
            actualizar_valor_xml: Si actualizar columna Valor_XML
            valor_xml: Valor para Valor_XML
            actualizar_aprobado: Si actualizar columna Aprobado
            valor_aprobado: Valor para Aprobado ('SI', 'NO')
        """
        cur = cx.cursor()
        
        # Contar items existentes
        query_count = """
        SELECT COUNT(*)
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ? AND Factura = ? AND Item = ?
        """
        cur.execute(query_count, (nit, factura, nombre_item))
        count_actual = cur.fetchone()[0]
        
        count_necesario = len(valores_lista)
        
        # Normalizar valores_comercializados
        if valores_comercializados is None:
            valores_comercializados = [''] * count_necesario
        elif len(valores_comercializados) < count_necesario:
            valores_comercializados = valores_comercializados + [''] * (count_necesario - len(valores_comercializados))
        
        if count_actual == 0:
            # INSERT nuevos registros
            for i, valor in enumerate(valores_lista):
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra,
                    Valor_Orden_de_Compra_Comercializados, Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                vxml = valor_xml if actualizar_valor_xml else None
                vaprob = valor_aprobado if actualizar_aprobado else None
                vcom = valores_comercializados[i] if i < len(valores_comercializados) and valores_comercializados[i] != '' else None
                cur.execute(insert_query, (id_reg, nit, factura, nombre_item, valor, vcom, vxml, vaprob))
        
        elif count_actual < count_necesario:
            # UPDATE existentes + INSERT faltantes
            for i in range(count_actual):
                update_query = "UPDATE [dbo].[CxP.Comparativa] SET Valor_Orden_de_Compra = ?, Valor_Orden_de_Compra_Comercializados = ?"
                params = [valores_lista[i], valores_comercializados[i] if valores_comercializados[i] != '' else None]
                
                if actualizar_valor_xml:
                    update_query += ", Valor_XML = ?"
                    params.append(valor_xml)
                if actualizar_aprobado:
                    update_query += ", Aprobado = ?"
                    params.append(valor_aprobado)
                
                update_query += """
                WHERE NIT = ? AND Factura = ? AND Item = ?
                  AND ID_registro IN (
                    SELECT TOP 1 ID_registro FROM [dbo].[CxP.Comparativa]
                    WHERE NIT = ? AND Factura = ? AND Item = ?
                    ORDER BY ID_registro
                    OFFSET ? ROWS
                  )
                """
                params.extend([nit, factura, nombre_item, nit, factura, nombre_item, i])
                cur.execute(update_query, params)
            
            # INSERT faltantes
            for i in range(count_actual, count_necesario):
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra,
                    Valor_Orden_de_Compra_Comercializados, Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                vxml = valor_xml if actualizar_valor_xml else None
                vaprob = valor_aprobado if actualizar_aprobado else None
                vcom = valores_comercializados[i] if i < len(valores_comercializados) and valores_comercializados[i] != '' else None
                cur.execute(insert_query, (id_reg, nit, factura, nombre_item, valores_lista[i], vcom, vxml, vaprob))
        
        else:
            # UPDATE existentes
            for i, valor in enumerate(valores_lista):
                update_query = "UPDATE [dbo].[CxP.Comparativa] SET Valor_Orden_de_Compra = ?, Valor_Orden_de_Compra_Comercializados = ?"
                params = [valor, valores_comercializados[i] if valores_comercializados[i] != '' else None]
                
                if actualizar_valor_xml:
                    update_query += ", Valor_XML = ?"
                    params.append(valor_xml)
                if actualizar_aprobado:
                    update_query += ", Aprobado = ?"
                    params.append(valor_aprobado)
                
                update_query += """
                WHERE NIT = ? AND Factura = ? AND Item = ?
                  AND ID_registro IN (
                    SELECT TOP 1 ID_registro FROM [dbo].[CxP.Comparativa]
                    WHERE NIT = ? AND Factura = ? AND Item = ?
                    ORDER BY ID_registro
                    OFFSET ? ROWS
                  )
                """
                params.extend([nit, factura, nombre_item, nit, factura, nombre_item, i])
                cur.execute(update_query, params)
        
        cur.close()
        print(f"[UPDATE] Item '{nombre_item}' actualizado - {count_necesario} valor(es)")
    
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
    
    def marcar_posiciones_procesadas(cx, doc_compra, posiciones):
        """
        Marca posiciones en Trans_Candidatos_HU41 como PROCESADO.
        
        Args:
            cx: Conexión a BD
            doc_compra: Número de orden de compra
            posiciones: Lista de posiciones a marcar
        """
        try:
            cur = cx.cursor()
            
            update_sql = """
            UPDATE [CxP].[Trans_Candidatos_HU41]
            SET Marca_hoc = 'PROCESADO'
            WHERE DocCompra_hoc = ?
            """
            cur.execute(update_sql, (doc_compra,))
            print(f"[UPDATE] Marcado como PROCESADO - OC {doc_compra}")
            
            cur.close()
            
        except Exception as e:
            print(f"[ERROR] Error marcando posiciones: {str(e)}")
            raise
    
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
            dict: {posicion: {datos}} para cada posición
        """
        try:
            # Expandir posiciones
            posiciones = expandir_posiciones_string(registro.get('Posicion_hoc', ''))
            
            if not posiciones:
                return {}
            
            # Expandir valores correspondientes
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
            
            # Datos comunes (usualmente no varían por posición)
            n_proveedor = safe_str(registro.get('NProveedor_hoc', ''))
            
            # Crear diccionario por posición
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
    
    # =========================================================================
    # FUNCIONES DE PROCESAMIENTO DE CASOS ESPECÍFICOS
    # =========================================================================
    
    def procesar_registro_sin_datos_maestro(cx, registro, cfg, sufijo_contado):
        """
        Procesa un registro cuando NO se encuentran datos en el maestro de comercializados.
        
        Flujo:
            1. Intenta copiar insumos a carpeta EN ESPERA
            2. Actualiza DocumentsProcessing con estado EN ESPERA - COMERCIALIZADOS
            3. Genera trazabilidad en CxP_Comparativa
        
        Args:
            cx: Conexión a BD
            registro: Registro de Trans_Candidatos_HU41
            cfg: Configuración
            sufijo_contado: ' CONTADO' si PaymentMeans = '01', '' si no
            
        Returns:
            str: 'EN_ESPERA'
        """
        registro_id = safe_str(registro.get('ID_dp', ''))
        nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
        numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
        
        # Obtener ruta de archivo
        ruta, nombre = os.path.split(safe_str(registro.get('RutaArchivo_dp', '')))
        carpeta_destino = cfg.get('CarpetaDestinoComercializados', '')
        
        if ruta and nombre and carpeta_destino:
            # Intentar copiar insumos
            copiado, resultado_copia = copiar_insumos_a_carpeta_destino(
                ruta, nombre, carpeta_destino
            )
            
            if copiado:
                observacion = f"No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados"
                campos_actualizar = {
                    'EstadoFinalFase_4': 'Exitoso',
                    'ObservacionesFase_4': truncar_observacion(observacion),
                    'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}",
                    'RutaArchivo': carpeta_destino
                }
            else:
                observacion = f"No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados - No se logran mover insumos a carpeta COMERCIALIZADOS"
                campos_actualizar = {
                    'EstadoFinalFase_4': 'Exitoso',
                    'ObservacionesFase_4': truncar_observacion(observacion),
                    'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}"
                }
        else:
            observacion = f"No se encuentran datos de la orden de compra y factura en el archivo Maestro de comercializados - No se logran identificar insumos"
            campos_actualizar = {
                'EstadoFinalFase_4': 'Exitoso',
                'ObservacionesFase_4': truncar_observacion(observacion),
                'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}"
            }
        
        # Actualizar DocumentsProcessing
        actualizar_bd_cxp(cx, registro_id, campos_actualizar)
        
        # Actualizar CxP_Comparativa - Observaciones
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Observaciones',
            valores_lista=[observacion],
            actualizar_valor_xml=True, valor_xml=observacion
        )
        
        # Actualizar estado
        actualizar_estado_comparativa(cx, nit, numero_factura, f"EN ESPERA - COMERCIALIZADOS{sufijo_contado}")
        
        return 'EN_ESPERA'
    
    def procesar_sin_coincidencia_valores(cx, registro, posiciones_maestro, valores_unitario, valores_me, sufijo_contado):
        """
        Procesa cuando NO hay coincidencia de valores entre maestro e histórico.
        
        Args:
            cx: Conexión a BD
            registro: Registro de Trans_Candidatos_HU41
            posiciones_maestro: Lista de posiciones del maestro
            valores_unitario: Lista de valores unitarios del maestro
            valores_me: Lista de valores ME del maestro
            sufijo_contado: Sufijo CONTADO si aplica
        """
        registro_id = safe_str(registro.get('ID_dp', ''))
        nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
        numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
        valor_a_pagar = normalizar_decimal(registro.get('valor_a_pagar_dp', 0))
        vlr_pagar_cop = normalizar_decimal(registro.get('VlrPagarCop_dp', 0))
        
        observacion = f"No se encuentra coincidencia del Valor a pagar de la factura"
        resultado_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
        
        campos_novedad = {
            'EstadoFinalFase_4': 'Exitoso',
            'ObservacionesFase_4': truncar_observacion(observacion),
            'ResultadoFinalAntesEventos': resultado_final
        }
        actualizar_bd_cxp(cx, registro_id, campos_novedad)
        
        # Actualizar items en comparativa
        # LineExtensionAmount
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='LineExtensionAmount',
            valores_lista=['NO ENCONTRADO'],
            actualizar_valor_xml=True, valor_xml=str(valor_a_pagar),
            actualizar_aprobado=True, valor_aprobado='NO'
        )
        
        # VlrPagarCop
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='VlrPagarCop',
            valores_lista=['NO ENCONTRADO'],
            actualizar_valor_xml=True, valor_xml=str(vlr_pagar_cop),
            actualizar_aprobado=True, valor_aprobado='NO'
        )
        
        # Observaciones
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Observaciones',
            valores_lista=[''],
            actualizar_valor_xml=True, valor_xml=observacion
        )
        
        # Nombre emisor
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Nombre emisor',
            valores_lista=['NO ENCONTRADO'],
            actualizar_aprobado=True, valor_aprobado='NO'
        )
        
        # Posiciones
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Posicion',
            valores_lista=['NO ENCONTRADO'] * len(posiciones_maestro),
            valores_comercializados=[str(p) for p in posiciones_maestro],
            actualizar_aprobado=True, valor_aprobado='NO'
        )
        
        # Valor PorCalcular_hoc de la posición
        valores_calc_comercializados = []
        for i in range(len(posiciones_maestro)):
            if valores_me[i] > 0:
                valores_calc_comercializados.append(str(valores_me[i]))
            else:
                valores_calc_comercializados.append(str(valores_unitario[i]))
        
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Valor PorCalcular_hoc de la posicion',
            valores_lista=['NO ENCONTRADO'] * len(posiciones_maestro),
            valores_comercializados=valores_calc_comercializados,
            actualizar_aprobado=True, valor_aprobado='NO'
        )
        
        # Fec.Doc y Fec.Reg
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Fec.Doc',
            valores_lista=['NO ENCONTRADO'] * len(posiciones_maestro),
            actualizar_aprobado=True, valor_aprobado='NO'
        )
        
        actualizar_items_comparativa(
            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
            nombre_item='Fec.Reg',
            valores_lista=['NO ENCONTRADO'] * len(posiciones_maestro),
            actualizar_aprobado=True, valor_aprobado='NO'
        )
        
        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
    
    # =========================================================================
    # INICIO DEL PROCESAMIENTO PRINCIPAL
    # =========================================================================
    
    try:
        print("")
        print("=" * 80)
        print("[INICIO] Procesamiento ZVEN/50 - Comercializados")
        print("=" * 80)
        
        t_inicio = time.time()
        
        # 1. Obtener y validar configuración
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[INFO] Configuracion cargada exitosamente")
        
        # Parámetros requeridos
        required_config = [
            'RutaInsumosComercializados',   # Ruta al Maestro de comercializados.xlsx
            'RutaInsumoAsociacion',          # Ruta a Asociación cuenta indicador.xlsx
            'CarpetaDestinoComercializados', # Carpeta destino EN ESPERA
            'ServidorBaseDatos',
            'NombreBaseDatos'
        ]
        
        missing_config = [k for k in required_config if not cfg.get(k)]
        if missing_config:
            raise ValueError(f"Faltan parametros de configuracion: {', '.join(missing_config)}")
        
        # 2. Validar archivos maestros
        ruta_maestro = cfg.get('RutaInsumosComercializados', '')
        ruta_asociacion = cfg.get('RutaInsumoAsociacion', '')
        
        print("[INFO] Validando archivos maestros...")
        
        es_valido_maestro, msg_maestro, df_maestro = validar_archivo_maestro_comercializados(ruta_maestro)
        if not es_valido_maestro:
            raise FileNotFoundError(msg_maestro)
        
        es_valido_asociacion, msg_asociacion, df_asociacion = validar_archivo_asociacion_cuenta(ruta_asociacion)
        if not es_valido_asociacion:
            raise FileNotFoundError(msg_asociacion)
        
        print("[INFO] Archivos maestros validados exitosamente")
        
        # 3. Conectar a base de datos y obtener registros ZVEN/50
        with crear_conexion_db(cfg) as cx:
            print("[INFO] Obteniendo registros ZVEN/50 para procesar...")
            
            # Query para obtener registros ZVEN y 50 desde Trans_Candidatos_HU41
            query_zven = """
                SELECT * FROM [CxP].[HU41_CandidatosValidacion]
                WHERE [ClaseDePedido_hoc] IN ('ZVEN', '50')
                ORDER BY [executionDate_dp] DESC
            """
            
            df_registros = pd.read_sql(query_zven, cx)
            
            print(f"[INFO] Obtenidos {len(df_registros)} registros ZVEN/50 para procesar")
            
            if len(df_registros) == 0:
                print("[INFO] No hay registros ZVEN/50 pendientes de procesar")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros ZVEN/50 pendientes de procesar")
                return
            
            # Variables de conteo
            registros_procesados = 0
            registros_con_novedad = 0
            registros_en_espera = 0
            registros_exitosos = 0
            #################################################################################
            # 4. Procesar cada registro
            for idx, registro in df_registros.iterrows():
                try:
                    registro_id = safe_str(registro.get('ID_dp', ''))
                    numero_oc = safe_str(registro.get('numero_de_liquidacion_u_orden_de_compra_dp', ''))
                    numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
                    payment_means = safe_str(registro.get('forma_de_pago_dp', ''))
                    
                    print(f"\n[PROCESO] Registro {registros_procesados + 1}/{len(df_registros)}: OC {numero_oc}, Factura {numero_factura}")
                    
                    # Determinar sufijo CONTADO según PaymentMeans
                    sufijo_contado = " CONTADO" if payment_means == "01" else ""
                    
                    # 5. Buscar en maestro de comercializados
                    registros_maestro = df_maestro[
                        (df_maestro['OC'] == numero_oc) &
                        (df_maestro['FACTURA'] == numero_factura)
                    ]
                    
                    if len(registros_maestro) == 0:
                        print(f"[INFO] No se encuentran datos de OC {numero_oc} y Factura {numero_factura} en maestro")
                        procesar_registro_sin_datos_maestro(cx, registro, cfg, sufijo_contado)
                        registros_en_espera += 1
                        registros_procesados += 1
                        continue
                    
                    # 6. Datos encontrados en el maestro - extraer
                    print(f"[DEBUG] Encontrados {len(registros_maestro)} registros en maestro")
                    
                    posiciones_maestro = registros_maestro['POSICION'].astype(str).tolist()
                    valores_unitario = [normalizar_decimal(v) for v in registros_maestro['PorCalcular_hoc (VALOR UNITARIO)'].tolist()]
                    valores_me = [normalizar_decimal(v) for v in registros_maestro['PorCalcular_hoc (ME)'].tolist()]
                    
                    nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
                    
                    # Actualizar campos comercializado en DocumentsProcessing
                    campos_comercializado = {
                        'Posicion_Comercializado': ','.join(posiciones_maestro),
                        'Valor_a_pagar_Comercializado': ','.join(map(str, valores_unitario)),
                        'Valor_a_pagar_Comercializado_ME': ','.join(map(str, valores_me))
                    }
                    actualizar_bd_cxp(cx, registro_id, campos_comercializado)
                    ########################################################################
                    # 7. Expandir datos del histórico y validar coincidencias
                    print(f"[DEBUG] Validando coincidencia de posiciones con historico...")
                    
                    datos_historico_por_posicion = expandir_posiciones_historico(registro)
                    
                    if not datos_historico_por_posicion:
                        print(f"[WARNING] No se pudieron expandir datos del historico para OC {numero_oc}")
                        observacion = f"No se encuentran datos del historico en el registro"
                        resultado_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                        
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
                    
                    # Validar coincidencia de posiciones y valores
                    coincidencias_encontradas = True
                    detalles_validacion = []
                    
                    for i, posicion in enumerate(posiciones_maestro):
                        datos_pos = datos_historico_por_posicion.get(posicion)
                        
                        if datos_pos is None:
                            coincidencias_encontradas = False
                            detalles_validacion.append(f"Posicion {posicion} no encontrada en historico")
                            break
                        
                        valor_historico = normalizar_decimal(datos_pos.get('PorCalcular', 0))
                        
                        # Comparar según si hay valor ME
                        if valores_me[i] > 0:
                            if abs(valor_historico - valores_me[i]) > 0.01:
                                coincidencias_encontradas = False
                                detalles_validacion.append(f"Posicion {posicion}: ME maestro {valores_me[i]} vs historico {valor_historico}")
                                break
                        else:
                            if abs(valor_historico - valores_unitario[i]) > 0.01:
                                coincidencias_encontradas = False
                                detalles_validacion.append(f"Posicion {posicion}: Unitario maestro {valores_unitario[i]} vs historico {valor_historico}")
                                break
                            
                    if not coincidencias_encontradas:
                        print(f"[INFO] No hay coincidencia de valores para OC {numero_oc}: {'; '.join(detalles_validacion)}")
                        procesar_sin_coincidencia_valores(cx, registro, posiciones_maestro, valores_unitario, valores_me, sufijo_contado)
                        registros_con_novedad += 1
                        registros_procesados += 1
                        continue
                    ##############################################################
                    
                    # 8. Validar sumas totales con tolerancia de 500
                    print(f"[DEBUG] Validando sumas con tolerancia...")
                    
                    valor_a_pagar = normalizar_decimal(registro.get('Valor de la Compra LEA_ddp', 0))
                    vlr_pagar_cop = normalizar_decimal(registro.get('VlrPagarCop_dp', 0))
                    
                    suma_valores_unitario = sum(valores_unitario)
                    suma_valores_me = sum(valores_me)
                    
                    validacion_suma_exitosa = True
                    mensaje_validacion_suma = ""
                    
                    # Si hay valores ME, validar ambos
                    if any(v > 0 for v in valores_me):
                        if not validar_tolerancia_numerica(suma_valores_unitario, valor_a_pagar, 500):
                            validacion_suma_exitosa = False
                            mensaje_validacion_suma += f"Suma unitarios {suma_valores_unitario} vs valor a pagar {valor_a_pagar}. "
                        
                        if vlr_pagar_cop > 0 and not validar_tolerancia_numerica(suma_valores_me, vlr_pagar_cop, 500):
                            validacion_suma_exitosa = False
                            mensaje_validacion_suma += f"Suma ME {suma_valores_me} vs VlrPagarCop {vlr_pagar_cop}. "
                    else:
                        if not validar_tolerancia_numerica(suma_valores_unitario, valor_a_pagar, 500):
                            validacion_suma_exitosa = False
                            mensaje_validacion_suma = f"Suma unitarios {suma_valores_unitario} vs valor a pagar {valor_a_pagar}"
                    
                    if not validacion_suma_exitosa:
                        print(f"[INFO] Valores fuera del rango de tolerancia para OC {numero_oc}: {mensaje_validacion_suma}")
                        observacion = f"No se encuentra coincidencia del Valor a pagar de la factura"
                        resultado_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                        
                        campos_novedad = {
                            'EstadoFinalFase_4': 'Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': resultado_final
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad)
                        
                        # Actualizar comparativa con valores
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='LineExtensionAmount',
                            valores_lista=[str(suma_valores_unitario)],
                            valores_comercializados=[str(suma_valores_unitario)],
                            actualizar_valor_xml=True, valor_xml=str(valor_a_pagar),
                            actualizar_aprobado=True, valor_aprobado='NO'
                        )
                        
                        if any(v > 0 for v in valores_me):
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='VlrPagarCop',
                                valores_lista=[str(suma_valores_me)],
                                valores_comercializados=[str(suma_valores_me)],
                                actualizar_valor_xml=True, valor_xml=str(vlr_pagar_cop),
                                actualizar_aprobado=True, valor_aprobado='NO'
                            )
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Observaciones',
                            valores_lista=[''],
                            actualizar_valor_xml=True, valor_xml=observacion
                        )
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                        registros_con_novedad += 1
                        registros_procesados += 1
                        continue
                    
                    # 9. Validar TRM
                    print(f"[DEBUG] Validando TRM...")
                    
                    if registro.get('DocumentCurrencyCode_dp') == 'COP':
                        print('No se requiere validar TRM, el valor es COP')
                    else:
                        trm_xml = normalizar_decimal(registro.get('CalculationRate_dp', 0))
                        primera_posicion = posiciones_maestro[0]
                        datos_primera_pos = datos_historico_por_posicion.get(primera_posicion, {})
                        trm_sap = normalizar_decimal(datos_primera_pos.get('Trm', 0))
                        
                        # Solo validar TRM si hay valor en el XML
                        trm_coincide = True
                        if trm_xml > 0 or trm_sap > 0:
                            trm_coincide = abs(trm_xml - trm_sap) < 0.01
                        
                        if not trm_coincide:
                            print(f"[INFO] TRM no coincide para OC {numero_oc}: XML {trm_xml} vs SAP {trm_sap}")
                            observacion = f"No se encuentra coincidencia en el campo TRM de la factura vs la informacion reportada en SAP"
                            resultado_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                            
                            campos_novedad_trm = {
                                'EstadoFinalFase_4': 'Exitoso',
                                'ObservacionesFase_4': truncar_observacion(observacion),
                                'ResultadoFinalAntesEventos': resultado_final
                            }
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_trm)
                            
                            # Actualizar TRM en comparativa
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='TRM',
                                valores_lista=[str(trm_sap)] * len(posiciones_maestro),
                                actualizar_valor_xml=True, valor_xml=str(trm_xml),
                                actualizar_aprobado=True, valor_aprobado='NO'
                            )
                            
                            actualizar_items_comparativa(
                                id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                                nombre_item='Observaciones',
                                valores_lista=[''],
                                actualizar_valor_xml=True, valor_xml=observacion
                            )
                            
                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                            
                            registros_con_novedad += 1
                            registros_procesados += 1
                            continue
                    ########################################
                    # 10. Validar cantidad y precio unitario
                    print(f"[DEBUG] Validando cantidad y precio unitario...")
                    
                    cantidad_xml = normalizar_decimal(registro.get('Cantidad de producto_ddp', 0))
                    precio_xml = normalizar_decimal(registro.get('Precio Unitario del producto_ddp', 0))
                    
                    todas_posiciones_cantidad_precio_ok = True
                    posiciones_con_error_cantidad = []
                    posiciones_con_error_precio = []
                    
                    for i, posicion in enumerate(posiciones_maestro):
                        datos_pos = datos_historico_por_posicion.get(posicion, {})
                        cantidad_sap = normalizar_decimal(datos_pos.get('CantPedido', 0))
                        precio_sap = normalizar_decimal(datos_pos.get('PrecioUnitario', 0))
                        
                        cantidad_ok, precio_ok = validar_cantidad_precio_tolerancia(
                            cantidad_xml, precio_xml, cantidad_sap, precio_sap, valor_a_pagar
                        )
                        
                        if not cantidad_ok:
                            posiciones_con_error_cantidad.append(posicion)
                            todas_posiciones_cantidad_precio_ok = False
                        if not precio_ok:
                            posiciones_con_error_precio.append(posicion)
                            todas_posiciones_cantidad_precio_ok = False
                    
                    if not todas_posiciones_cantidad_precio_ok:
                        print(f"[INFO] Cantidad/precio no coinciden para OC {numero_oc}")
                        observacion = f"No se encuentra coincidencia en cantidad y/o precio unitario de la factura vs la informacion reportada en SAP"
                        resultado_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                        
                        campos_novedad_cp = {
                            'EstadoFinalFase_4': 'Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': resultado_final
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_cp)
                        
                        # Actualizar comparativa
                        n_pos = len(posiciones_maestro)
                        
                        valores_cantidad_sap = [str(normalizar_decimal(datos_historico_por_posicion.get(p, {}).get('CantPedido', 0))) for p in posiciones_maestro]
                        valores_precio_sap = [str(normalizar_decimal(datos_historico_por_posicion.get(p, {}).get('PrecioUnitario', 0))) for p in posiciones_maestro]
                        
                        aprobados_cantidad = ['NO' if p in posiciones_con_error_cantidad else 'SI' for p in posiciones_maestro]
                        aprobados_precio = ['NO' if p in posiciones_con_error_precio else 'SI' for p in posiciones_maestro]
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Cantidad de producto',
                            valores_lista=valores_cantidad_sap,
                            actualizar_valor_xml=True, valor_xml=str(cantidad_xml),
                            actualizar_aprobado=True, valor_aprobado=aprobados_cantidad[0] if n_pos == 1 else None
                        )
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Precio Unitario del producto',
                            valores_lista=valores_precio_sap,
                            actualizar_valor_xml=True, valor_xml=str(precio_xml),
                            actualizar_aprobado=True, valor_aprobado=aprobados_precio[0] if n_pos == 1 else None
                        )
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Observaciones',
                            valores_lista=[''],
                            actualizar_valor_xml=True, valor_xml=observacion
                        )
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                        registros_con_novedad += 1
                        registros_procesados += 1
                        continue
                    
                    else:
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Precio Unitario del producto',
                            valores_lista=[str(normalizar_decimal(datos_historico_por_posicion.get(p, {}).get('PrecioUnitario', 0)))
                                        for p in posiciones_maestro],
                            actualizar_valor_xml=True, valor_xml=str(precio_xml),
                            actualizar_aprobado=True, valor_aprobado='NO'
                        )
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Cantidad de producto',
                            valores_lista=[str(normalizar_decimal(datos_historico_por_posicion.get(p, {}).get('CantPedido', 0)))
                                        for p in posiciones_maestro],
                            actualizar_valor_xml=True, valor_xml=str(cantidad_xml),
                            actualizar_aprobado=True, valor_aprobado='NO'
                        )
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                    #####################################################
                    # 11. Validar nombre emisor
                    print(f"[DEBUG] Validando nombre emisor...")
                    
                    nombre_emisor_xml = safe_str(registro.get('nombre_emisor_dp', ''))
                    nombre_proveedor_sap = safe_str(datos_primera_pos.get('Acreedor', ''))
                    
                    nombres_coinciden = comparar_nombres_proveedor(nombre_emisor_xml, nombre_proveedor_sap)
                    
                    if not nombres_coinciden:
                        print(f"[INFO] Nombre emisor no coincide para OC {numero_oc}: XML '{nombre_emisor_xml}' vs SAP '{nombre_proveedor_sap}'")
                        observacion = f"No se encuentra coincidencia en Nombre Emisor de la factura vs la informacion reportada en SAP"
                        resultado_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                        
                        campos_novedad_nombre = {
                            'EstadoFinalFase_4': 'Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': resultado_final
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_nombre)
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Nombre emisor',
                            valores_lista=[nombre_proveedor_sap],
                            actualizar_valor_xml=True, valor_xml=nombre_emisor_xml,
                            actualizar_aprobado=True, valor_aprobado='NO'
                        )
                        
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Observaciones',
                            valores_lista=[''],
                            actualizar_valor_xml=True, valor_xml=observacion
                        )
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        
                        registros_con_novedad += 1
                        registros_procesados += 1
                        continue
                    
                    ###########################################################
                    # 12. TODAS LAS VALIDACIONES EXITOSAS
                    print(f"[SUCCESS] Registro {registro_id} procesado exitosamente")
                    
                    # Marcar posiciones como procesadas
                    doc_compra = safe_str(registro.get('DocCompra_hoc', ''))
                    marcar_posiciones_procesadas(cx, doc_compra, posiciones_maestro)
                    
                    campos_exitoso = {
                        'EstadoFinalFase_4': 'Exitoso',
                        'ResultadoFinalAntesEventos': f"PROCESADO{sufijo_contado}"
                    }
                    actualizar_bd_cxp(cx, registro_id, campos_exitoso)
                    
                    # Actualizar tabla comparativa con todos los datos exitosos
                    n_posiciones = len(posiciones_maestro)
                    
                    # LineExtensionAmount
                    actualizar_items_comparativa(
                        id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                        nombre_item='LineExtensionAmount',
                        valores_lista=[str(suma_valores_unitario)],
                        valores_comercializados=[str(suma_valores_unitario)],
                        actualizar_valor_xml=True, valor_xml=str(valor_a_pagar),
                        actualizar_aprobado=True, valor_aprobado='SI'
                    )
                    
                    # VlrPagarCop
                    vlr_pagar_final = suma_valores_me if suma_valores_me > 0 else suma_valores_unitario
                    actualizar_items_comparativa(
                        id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                        nombre_item='VlrPagarCop',
                        valores_lista=[str(vlr_pagar_final)],
                        valores_comercializados=[str(vlr_pagar_final)],
                        actualizar_valor_xml=True, valor_xml=str(vlr_pagar_cop if vlr_pagar_cop > 0 else valor_a_pagar),
                        actualizar_aprobado=True, valor_aprobado='SI'
                    )
                    
                    # Nombre emisor
                    actualizar_items_comparativa(
                        id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                        nombre_item='Nombre emisor',
                        valores_lista=[nombre_proveedor_sap],
                        actualizar_valor_xml=True, valor_xml=nombre_emisor_xml,
                        actualizar_aprobado=True, valor_aprobado='SI'
                    )
                    
                    # Posiciones
                    actualizar_items_comparativa(
                        id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                        nombre_item='Posicion',
                        valores_lista=[str(p) for p in posiciones_maestro],
                        valores_comercializados=[str(p) for p in posiciones_maestro],
                        actualizar_aprobado=True, valor_aprobado='SI'
                    )
                    
                    # Valor PorCalcular_hoc de la posición
                    actualizar_items_comparativa(
                        id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                        nombre_item='Valor PorCalcular_hoc de la posicion',
                        valores_lista=[str(valores_unitario[i]) for i in range(n_posiciones)],
                        valores_comercializados=[str(valores_unitario[i]) for i in range(n_posiciones)],
                        actualizar_aprobado=True, valor_aprobado='SI'
                    )
                    
                    # Valor PorCalcular_hoc ME de la posición (si aplica)
                    if any(v > 0 for v in valores_me):
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item='Valor PorCalcular_hoc ME de la posicion',
                            valores_lista=[str(valores_me[i]) for i in range(n_posiciones) if valores_me[i] > 0],
                            valores_comercializados=[str(valores_me[i]) for i in range(n_posiciones) if valores_me[i] > 0],
                            actualizar_aprobado=True, valor_aprobado='SI'
                        )
                    
                    # TRM
                    actualizar_items_comparativa(
                        id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                        nombre_item='TRM',
                        valores_lista=[str(trm_sap)] * n_posiciones,
                        actualizar_valor_xml=True, valor_xml=str(trm_xml),
                        actualizar_aprobado=True, valor_aprobado='SI'
                    )
                    
                    # Valor PorCalcular_hoc SAP
                    actualizar_items_comparativa(
                        id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                        nombre_item='Valor PorCalcular_hoc SAP',
                        valores_lista=[str(normalizar_decimal(datos_historico_por_posicion.get(p, {}).get('PorCalcular', 0))) 
                                      for p in posiciones_maestro],
                        actualizar_aprobado=True, valor_aprobado='SI'
                    )
                    
                    # Campos adicionales del histórico (uno por cada posición)
                    campos_historico = [
                        ('Tipo NIF', 'TipoNif'),
                        ('Acreedor', 'Acreedor'),
                        ('Fec.Doc', 'FecDoc'),
                        ('Fec.Reg', 'FecReg'),
                        ('Fecha. cont gasto', 'FecContGasto'),
                        ('Indicador impuestos', 'IndicadorImpuestos'),
                        ('Texto breve', 'TextoBreve'),
                        ('Clase de impuesto', 'ClaseDeImpuesto'),
                        ('Cuenta', 'Cuenta'),
                        ('Ciudad proveedor', 'CiudadProveedor'),
                        ('DOC.FI.ENTRADA', 'DocFiEntrada'),
                        ('CTA 26', 'Cuenta26')
                    ]
                    
                    for nombre_item, campo_historico in campos_historico:
                        actualizar_items_comparativa(
                            id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                            nombre_item=nombre_item,
                            valores_lista=[safe_str(datos_historico_por_posicion.get(p, {}).get(campo_historico, ''))
                                          for p in posiciones_maestro],
                            actualizar_aprobado=True, valor_aprobado='SI'
                        )
                    
                    # Cantidad y precio
                    actualizar_items_comparativa(
                        id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                        nombre_item='Precio Unitario del producto',
                        valores_lista=[str(normalizar_decimal(datos_historico_por_posicion.get(p, {}).get('PrecioUnitario', 0)))
                                      for p in posiciones_maestro],
                        actualizar_valor_xml=True, valor_xml=str(precio_xml),
                        actualizar_aprobado=True, valor_aprobado='SI'
                    )
                    
                    actualizar_items_comparativa(
                        id_reg=registro_id, cx=cx, nit=nit, factura=numero_factura,
                        nombre_item='Cantidad de producto',
                        valores_lista=[str(normalizar_decimal(datos_historico_por_posicion.get(p, {}).get('CantPedido', 0)))
                                      for p in posiciones_maestro],
                        actualizar_valor_xml=True, valor_xml=str(cantidad_xml),
                        actualizar_aprobado=True, valor_aprobado='SI'
                    )
                    
                    actualizar_estado_comparativa(cx, nit, numero_factura, f"PROCESADO{sufijo_contado}")
                    
                    registros_exitosos += 1
                    registros_procesados += 1
                    
                except Exception as e:
                    print(f"[ERROR] Error procesando registro {idx}: {str(e)}")
                    print(traceback.format_exc())
                    
                    registros_con_novedad += 1
                    registros_procesados += 1
                    continue
        
        # Fin del procesamiento
        tiempo_total = time.time() - t_inicio
        
        print("")
        print("=" * 80)
        print("[FIN] Procesamiento ZVEN/50 - Comercializados completado")
        print("=" * 80)
        print("[ESTADISTICAS]")
        print(f"  Total registros procesados: {registros_procesados}")
        print(f"  Exitosos: {registros_exitosos}")
        print(f"  Con novedad: {registros_con_novedad}")
        print(f"  En espera: {registros_en_espera}")
        print(f"  Tiempo total: {round(tiempo_total, 2)}s")
        print("=" * 80)
        
        resumen = f"Procesados {registros_procesados} registros ZVEN/50. Exitosos: {registros_exitosos}, Con novedad: {registros_con_novedad}, En espera: {registros_en_espera}"
        
        SetVar("vLocStrResultadoSP", "True")
        SetVar("vLocStrResumenSP", resumen)
        
    except Exception as e:
        exc_type = type(e).__name__
        print("")
        print("=" * 80)
        print("[ERROR CRITICO] La funcion ZVEN_ValidarComercializados fallo")
        print("=" * 80)
        print(f"[ERROR] Tipo de error: {exc_type}")
        print(f"[ERROR] Mensaje: {str(e)}")
        print("[ERROR] Traceback completo:")
        print(traceback.format_exc())
        print("=" * 80)
        
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        SetVar("vLocStrResultadoSP", "False")



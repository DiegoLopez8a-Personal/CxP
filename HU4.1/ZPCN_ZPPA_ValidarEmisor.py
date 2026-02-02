"""
================================================================================
SCRIPT: ZPRE_ValidarEmisor.py
================================================================================

Descripcion General:
--------------------
    Valida el nombre del emisor para pedidos ZPRE/45 (Prepagos).
    Compara el nombre del emisor en el XML de factura contra el nombre
    del acreedor registrado en el historico de ordenes de compra de SAP.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Plataforma: RocketBot RPA

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |               ZPRE_ValidarEmisor()                          |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Obtener configuracion desde vLocDicConfig                  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Conectar a base de datos SQL Server                        |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Consultar [CxP].[HU41_CandidatosValidacion]                |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Filtrar registros:                                         |
    |  - ClaseDePedido contiene ZPRE o 45                         |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Para cada registro:                                        |
    |  +-------------------------------------------------------+  |
    |  |  Normalizar nombre_emisor_dp (XML)                    |  |
    |  |  Normalizar Acreedor_hoc (SAP) - puede tener varios   |  |
    |  +-------------------------------------------------------+  |
    |  |  Comparar nombre normalizado contra lista acreedores  |  |
    |  +-------------------------------------------------------+  |
    |  |  SI coincide con alguno:                              |  |
    |  |    -> Aprobado                                        |  |
    |  +-------------------------------------------------------+  |
    |  |  SI no coincide con ninguno:                          |  |
    |  |    -> CON NOVEDAD                                     |  |
    |  |    -> Actualizar DocumentsProcessing                  |  |
    |  |    -> Actualizar Comparativa                          |  |
    |  +-------------------------------------------------------+  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar estadisticas y configurar variables RocketBot     |
    +-------------------------------------------------------------+

================================================================================
VARIABLES DE ENTRADA (RocketBot)
================================================================================

    vLocDicConfig : str | dict
        Configuracion JSON con parametros:
        - ServidorBaseDatos: Servidor SQL Server
        - NombreBaseDatos: Nombre de la base de datos

    vGblStrUsuarioBaseDatos : str
        Usuario para conexion SQL Server

    vGblStrClaveBaseDatos : str
        Contrasena para conexion SQL Server

================================================================================
VARIABLES DE SALIDA (RocketBot)
================================================================================

    vLocStrResultadoSP : str
        "True" si exitoso, "False" si error

    vLocStrResumenSP : str
        Resumen: "OK. Total:X"

    vGblStrDetalleError : str
        Traceback en caso de error

================================================================================
NORMALIZACION DE NOMBRES
================================================================================

La funcion normalizar() aplica:
    1. Conversion a mayusculas
    2. Eliminacion de tildes/acentos (NFD + filtro Mn)
    3. Eliminacion de caracteres no alfanumericos (excepto espacios)
    4. Eliminacion de espacios extremos

Ejemplo:
    "Café & Más S.A.S." -> "CAFE MAS SAS"

================================================================================
CRITERIOS DE FILTRADO
================================================================================

El script procesa solo registros que cumplan:

    ClaseDePedido contiene: ZPRE o 45

================================================================================
VALIDACION REALIZADA
================================================================================

    Nombre Emisor:
        - nombre_emisor_dp (XML) normalizado
        - Acreedor_hoc (SAP) - lista separada por |, cada uno normalizado
        - Coincidencia exacta despues de normalizacion
        
    Si no coincide con ningun acreedor:
        - Estado: CON NOVEDAD o CON NOVEDAD - CONTADO
        - Observacion: "No coincide nombre del emisor"

================================================================================
TABLAS ACTUALIZADAS
================================================================================

    [CxP].[DocumentsProcessing]
        - EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
        - ObservacionesFase_4 = Observacion concatenada
        - ResultadoFinalAntesEventos = Estado final

    [dbo].[CxP.Comparativa]
        - Valor_XML = Observacion (Item = 'Observaciones')
        - Estado_validacion_antes_de_eventos = Estado final

================================================================================
EJEMPLOS DE USO
================================================================================

    # Configurar variables en RocketBot
    SetVar("vLocDicConfig", json.dumps({
        "ServidorBaseDatos": "servidor.ejemplo.com",
        "NombreBaseDatos": "NotificationsPaddy"
    }))
    
    # Ejecutar funcion
    ZPRE_ValidarEmisor()
    
    # Verificar resultado
    resultado = GetVar("vLocStrResultadoSP")  # "True"

================================================================================
NOTAS TECNICAS
================================================================================

    - Normalizacion elimina diferencias por tildes, mayusculas, puntuacion
    - Multiples acreedores en HOC se comparan individualmente
    - Observaciones se truncan a 3900 caracteres
    - Errores por registro no detienen el proceso

================================================================================
"""

def ZPCN_ZPPA_ValidarEmisor():
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
    import unicodedata
    
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    print("=" * 80)
    print("[INICIO] Funcion ZPCN_ZPPA_ValidarEmisor() iniciada")
    print("[INICIO] Timestamp: " + str(datetime.now()))
    print("=" * 80)
    
    # ========================================================================
    # FUNCIONES AUXILIARES
    # ========================================================================
    
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
            if isinstance(v, float) and np.isnan(v):
                return ""
            return str(v)
        try:
            return str(v).strip()
        except:
            return ""
    
    # CORRECCIÓN: Función para truncar observaciones
    def truncar_observacion(obs):
        """Truncar observación a 3900 caracteres para prevenir overflow"""
        if not obs:
            return ""
        obs_str = safe_str(obs)
        if len(obs_str) > 3900:
            return obs_str[:3900]
        return obs_str
    
    def parse_config(raw):
        if isinstance(raw, dict):
            if not raw:
                raise ValueError("Config empty")
            return raw
        text = safe_str(raw)
        if not text:
            raise ValueError("vLocDicConfig empty")
        try:
            config = json.loads(text)
            if not config:
                raise ValueError("Config empty JSON")
            return config
        except json.JSONDecodeError:
            pass
        try:
            config = ast.literal_eval(text)
            if not config:
                raise ValueError("Config empty literal")
            return config
        except (ValueError, SyntaxError) as e:
            raise ValueError("Invalid config: " + str(e))
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            raise ValueError("Missing params: " + ', '.join(missing))
        
        usuario = GetVar("vGblStrUsuarioBaseDatos")
        contrasena = GetVar("vGblStrClaveBaseDatos")        
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=" + cfg['ServidorBaseDatos'] + ";"
            "DATABASE=" + cfg['NombreBaseDatos'] + ";"
            f"UID={usuario};"
            f"PWD={contrasena};"
            "autocommit=False;"
        )

        
        cx = None
        for attempt in range(max_retries):
            try:
                cx = pyodbc.connect(conn_str, timeout=30)
                cx.autocommit = False
                print("[DEBUG] Conexion SQL abierta (intento " + str(attempt + 1) + ")")
                break
            except pyodbc.Error:
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))
                    continue
                raise
        try:
            yield cx
            if cx:
                cx.commit()
                print("[DEBUG] Commit final de conexion exitoso")
        except Exception as e:
            if cx:
                cx.rollback()
                print("[ERROR] Rollback por error: " + str(e))
            raise
        finally:
            if cx:
                try:
                    cx.close()
                    print("[DEBUG] Conexion cerrada")
                except:
                    pass
    
    def split_valores(valor_str):
        """Dividir string por | y retornar lista de valores"""
        if not valor_str or valor_str == "" or pd.isna(valor_str):
            return []
        valores = str(valor_str).split('|')
        return [v.strip() for v in valores if v.strip()]
    
    def contiene_valor(campo, valor_buscado):
        """Verificar si campo contiene valor buscado"""
        valores = split_valores(campo)
        return valor_buscado in valores
    
    def quitar_tildes(texto):
        """
        Quitar tildes y acentos de un texto
        
        Ejemplos:
        - INTERES -> INTERES
        - JOSE -> JOSE
        - Maria -> MARIA
        """
        if not texto:
            return ""
        
        # Normalizar a NFD (descomponer caracteres con tildes)
        nfd = unicodedata.normalize('NFD', texto)
        
        # Filtrar solo caracteres que NO sean marcas diacriticas
        sin_tildes = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
        
        # Re-normalizar a NFC (forma canonica compuesta)
        return unicodedata.normalize('NFC', sin_tildes)
    
    def normalizar_nombre(nombre):
        """
        Normalizar nombre de empresa segun reglas especificas
        
        MEJORAS V3:
        1. Quitar tildes/acentos (INTERES -> INTERES)
        2. Normalizar '&' a 'Y' (ANGEL & DG -> ANGEL Y DG)
        3. Eliminar 'Y' que esten solas
        
        Normalizaciones completas:
        - S.A.S. = SAS
        - S.A.S = SAS
        - S. A. S. = SAS
        - S A S = SAS
        - S. A. = SA
        - S.A = SA
        - Limitada = LTDA
        - Ltda = LTDA
        - S. EN C. = SENC
        - & = Y (se convierte y luego se elimina si esta sola)
        """
        if not nombre or nombre == "":
            return ""
        
        # Convertir a string y hacer copia para trabajar
        texto = safe_str(nombre)
        
        # PASO 1: Quitar tildes ANTES de convertir a mayusculas
        texto = quitar_tildes(texto)
        
        # PASO 2: Convertir a mayusculas
        texto = texto.upper()
        
        # PASO 2.5: Reemplazar '&' por 'Y' ANTES de otras normalizaciones
        # Esto normaliza "ANGEL & DG" -> "ANGEL Y DG"
        # Luego la 'Y' se eliminara si esta sola
        texto = texto.replace('&', 'Y')
        
        # PASO 3: Aplicar normalizaciones especificas ANTES de quitar caracteres especiales
        # IMPORTANTE: Orden de mas especifico a mas general
        normalizaciones = [
            # SAS - todas las variantes
            ('S. A. S.', 'SAS'),  # S. A. S.
            ('S. A. S', 'SAS'),   # Sin punto final
            ('S.A.S.', 'SAS'),    # S.A.S.
            ('S.A.S', 'SAS'),     # S.A.S (sin punto final)
            ('S, A. S.', 'SAS'),  # S, A. S.
            ('S. A, S.', 'SAS'),  # S. A, S.
            ('S,A.S', 'SAS'),     # S,A.S
            ('S A S', 'SAS'),     # S A S (con espacios)
            # SA - variantes (NUEVO para S.A, S.A.)
            ('S. A.', 'SA'),      # S. A.
            ('S.A.', 'SA'),       # S.A.
            ('S. A', 'SA'),       # S. A (sin punto final)
            ('S.A', 'SA'),        # S.A (sin punto final)
            # LTDA - todas las variantes
            ('LIMITADA', 'LTDA'),
            ('LTDA.', 'LTDA'),
            ('LTDA,', 'LTDA'),
            ('LTDA', 'LTDA'),  # Ya normalizado
            # SENC - todas las variantes
            ('S. EN C A', 'SENC'),
            ('S. EN C.', 'SENC'),
            ('S. EN C', 'SENC'),
            ('S EN C A', 'SENC'),
            ('S EN C', 'SENC')
        ]
        
        for patron, reemplazo in normalizaciones:
            texto = texto.replace(patron, reemplazo)
        
        # PASO 4: Quitar caracteres especiales, solo dejar letras, numeros y espacios
        texto = re.sub(r'[^A-Z0-9\s]', '', texto)
        
        # PASO 5: Normalizar espacios multiples a uno solo
        texto = re.sub(r'\s+', ' ', texto)
        
        # PASO 6: Eliminar 'Y' que esten solas (palabras completas)
        # Esto elimina " Y " pero no "YUCA" ni "YESO"
        # Usar word boundaries \b para asegurar que sea una palabra completa
        texto = re.sub(r'\bY\b', '', texto)
        
        # PASO 7: Limpiar espacios nuevamente (pueden quedar espacios dobles despues de eliminar Y)
        texto = re.sub(r'\s+', ' ', texto)
        
        return texto.strip()
    
    def comparar_nombres(nombre1, nombre2):
        """
        Comparar dos nombres normalizados segun reglas especificas
        
        Pasos:
        1. Normalizar ambos nombres
        2. Separar por espacios
        3. Verificar que tengan EXACTAMENTE las mismas palabras
        
        IMPORTANTE: NO se permiten subconjuntos
        "FERRICENTROS SAS" != "FERRICENTROS SAS COLOMBIA"
        
        Retorna: (coincide: bool, nombre1_norm: str, nombre2_norm: str, detalle: str)
        """
        # Normalizar
        norm1 = normalizar_nombre(nombre1)
        norm2 = normalizar_nombre(nombre2)
        
        print("[DEBUG] Nombre 1 original: '" + safe_str(nombre1) + "'")
        print("[DEBUG] Nombre 1 normalizado: '" + norm1 + "'")
        print("[DEBUG] Nombre 2 original: '" + safe_str(nombre2) + "'")
        print("[DEBUG] Nombre 2 normalizado: '" + norm2 + "'")
        
        # Separar por espacios
        items1 = [item for item in norm1.split(' ') if item]
        items2 = [item for item in norm2.split(' ') if item]
        
        print("[DEBUG] Items nombre 1: " + str(items1))
        print("[DEBUG] Items nombre 2: " + str(items2))
        
        # Crear sets de palabras
        items1_set = set(items1)
        items2_set = set(items2)
        
        # CORRECCION: Solo coincide si los sets son EXACTAMENTE iguales
        # NO se permiten subconjuntos
        if items1_set == items2_set:
            detalle = "Coincidencia exacta - mismas palabras"
            coincide = True
        else:
            # Calcular diferencias
            faltantes_en_2 = items1_set - items2_set
            faltantes_en_1 = items2_set - items1_set
            
            if faltantes_en_2 and not faltantes_en_1:
                detalle = "NO COINCIDE: Nombre 2 le falta: " + str(faltantes_en_2)
            elif faltantes_en_1 and not faltantes_en_2:
                detalle = "NO COINCIDE: Nombre 1 le falta: " + str(faltantes_en_1)
            else:
                detalle = "NO COINCIDE: Diferentes palabras. Falta en 2: " + str(faltantes_en_2) + ", Falta en 1: " + str(faltantes_en_1)
            
            coincide = False
        
        print("[DEBUG] Detalle comparacion: " + detalle)
        
        return coincide, norm1, norm2, detalle
    
    # ========================================================================
    # INICIO DE PROCESO
    # ========================================================================
    
    try:
        print("[DEBUG] Obteniendo configuracion...")
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[DEBUG] Configuracion obtenida OK")
        print("[DEBUG] Servidor: " + cfg.get('ServidorBaseDatos', 'N/A'))
        print("[DEBUG] Base de datos: " + cfg.get('NombreBaseDatos', 'N/A'))
        
        stats = {
            'total_registros': 0,
            'aprobados': 0,
            'con_novedad': 0,
            'errores': 0,
            'tiempo_total': 0
        }
        
        t_inicio = time.time()
        
        with crear_conexion_db(cfg) as cx:
            
            # ================================================================
            # PASO 1: Consultar candidatos de HU41_CandidatosValidacion
            # ================================================================
            
            print("")
            print("[PASO 1] Consultando tabla HU41_CandidatosValidacion...")
            
            query_candidatos = """
            SELECT 
                ID_dp,
                nit_emisor_o_nit_del_proveedor_dp,
                numero_de_factura_dp,
                numero_de_liquidacion_u_orden_de_compra_dp,
                forma_de_pago_dp,
                nombre_emisor_dp,
                Acreedor_hoc,
                ClaseDePedido_hoc,
                DocCompra_hoc,
                NitCedula_hoc,
                PorCalcular_hoc,
                TextoBreve_hoc
            FROM [CxP].[HU41_CandidatosValidacion] WITH (NOLOCK)
            WHERE 1=1
            """
            
            df_candidatos = pd.read_sql(query_candidatos, cx)
            print("[DEBUG] Registros consultados: " + str(len(df_candidatos)))
            
            if df_candidatos.empty:
                print("[INFO] No hay registros en HU41_CandidatosValidacion")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros para procesar")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay registros para procesar", None, stats
            
            # ================================================================
            # PASO 2: Aplicar filtros
            # ================================================================
            
            print("[PASO 2] Aplicando filtros...")
            
            # Filtro: ClaseDePedido_hoc contiene 'ZPPA', 'ZPCN' o '42'
            mask_clase = df_candidatos['ClaseDePedido_hoc'].apply(
                lambda x: contiene_valor(x, 'ZPPA') or contiene_valor(x, 'ZPCN') or contiene_valor(x, '42') if pd.notna(x) else False
            )
            
            print("[DEBUG] Registros con ClaseDePedido = ZPPA, ZPCN o 42: " + str(mask_clase.sum()))
            
            # Aplicar filtro
            df_filtrado = df_candidatos[mask_clase].copy()
            
            print("[DEBUG] Registros despues de filtros: " + str(len(df_filtrado)))
            
            if df_filtrado.empty:
                print("[INFO] No hay registros que cumplan los filtros")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros con ClaseDePedido ZPPA/ZPCN/42")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay registros que cumplan filtros", None, stats
            
            stats['total_registros'] = len(df_filtrado)
            
            # ================================================================
            # PASO 3: Procesar cada registro - VALIDACION NOMBRE EMISOR
            # ================================================================
            
            print("")
            print("[PASO 3] Procesando validacion: Nombre Emisor...")
            
            for idx, row in df_filtrado.iterrows():
                try:
                    print("")
                    print("[REGISTRO " + str(idx + 1) + "/" + str(len(df_filtrado)) + "]")
                    
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    forma_pago = safe_str(row['forma_de_pago_dp'])
                    
                    print("[DEBUG] NIT: " + nit)
                    print("[DEBUG] Factura: " + factura)
                    print("[DEBUG] OC: " + oc)
                    
                    # Obtener valores
                    nombre_emisor = safe_str(row['nombre_emisor_dp'])
                    acreedor_completo = safe_str(row['Acreedor_hoc'])
                    
                    # Obtener PRIMER valor de Acreedor_hoc
                    valores_acreedor = split_valores(acreedor_completo)
                    primer_acreedor = valores_acreedor[0] if valores_acreedor else ""
                    
                    print("")
                    print("[VALIDACION] Comparando Nombre Emisor vs Acreedor...")
                    print("[DEBUG] Nombre Emisor (dp): '" + nombre_emisor + "'")
                    print("[DEBUG] Acreedor completo (hoc): '" + acreedor_completo + "'")
                    print("[DEBUG] Primer Acreedor (a comparar): '" + primer_acreedor + "'")
                    
                    # ========================================================
                    # COMPARAR NOMBRES CON NORMALIZACION
                    # ========================================================
                    
                    coincide, norm1, norm2, detalle = comparar_nombres(nombre_emisor, primer_acreedor)
                    
                    print("[RESULTADO] " + ("COINCIDEN" if coincide else "NO COINCIDEN"))
                    print("[DETALLE] " + detalle)
                    
                    # ========================================================
                    # DECISION: COINCIDEN O NO?
                    # ========================================================
                    
                    if coincide:
                        # ====================================================
                        # CASO: NOMBRES COINCIDEN (APROBADO)
                        # ====================================================
                        
                        print("")
                        print("[RESULTADO FINAL] APROBADO (nombres coinciden)")
                        stats['aprobados'] += 1
                        
                        # ====================================================
                        # 2. ACTUALIZAR [dbo].[CxP.Comparativa] - SOLO ESTO
                        # ====================================================
                        
                        print("[UPDATE] Actualizando tabla CxP.Comparativa...")
                        
                        cur = cx.cursor()
                        
                        # 2.1.1: Actualizar Valor_XML
                        update_xml = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Valor_XML = ?
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'NombreEmisor'
                        """
                        cur.execute(update_xml, (nombre_emisor, nit, factura))
                        
                        # 2.1.3: Actualizar Aprobado = 'SI'
                        update_aprobado = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Aprobado = 'SI'
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'NombreEmisor'
                        """
                        cur.execute(update_aprobado, (nit, factura))
                        
                        # 2.1.4: Actualizar Valor_Orden_de_Compra
                        update_voc = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Valor_Orden_de_Compra = ?
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'NombreEmisor'
                        """
                        cur.execute(update_voc, (primer_acreedor, nit, factura))
                        cx.commit()
                        cur.close()
                        print("[UPDATE] Tabla CxP.Comparativa actualizada OK (APROBADO)")
                        
                    else:
                        # ====================================================
                        # CASO: NOMBRES NO COINCIDEN (CON NOVEDAD)
                        # ====================================================
                        
                        print("")
                        print("[RESULTADO FINAL] CON NOVEDAD (nombres NO coinciden)")
                        stats['con_novedad'] += 1
                        
                        # Determinar estado segun forma de pago
                        if forma_pago == '1' or forma_pago == '01':
                            estado_final = 'CON NOVEDAD - CONTADO'
                        else:
                            estado_final = 'CON NOVEDAD'
                        
                        cur = cx.cursor()
                        
                        # ====================================================
                        # 3.1 ACTUALIZAR [CxP].[DocumentsProcessing]
                        # ====================================================
                        
                        print("[UPDATE] Actualizando tabla DocumentsProcessing...")
                        
                        update_fase4 = """
                        UPDATE [CxP].[DocumentsProcessing]
                        SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
                        WHERE nit_emisor_o_nit_del_proveedor = ?
                          AND numero_de_factura = ?
                          AND numero_de_liquidacion_u_orden_de_compra = ?
                        """
                        cur.execute(update_fase4, (nit, factura, oc))
                        
                        select_obs = """
                        SELECT ObservacionesFase_4
                        FROM [CxP].[DocumentsProcessing]
                        WHERE nit_emisor_o_nit_del_proveedor = ?
                          AND numero_de_factura = ?
                          AND numero_de_liquidacion_u_orden_de_compra = ?
                        """
                        cur.execute(select_obs, (nit, factura, oc))
                        result_obs = cur.fetchone()
                        
                        obs_actual = safe_str(result_obs[0]) if result_obs and result_obs[0] else ""
                        nueva_obs = "No se encuentra coincidencia en Nombre Emisor de la factura vs la informacion reportada en SAP"
                        
                        if obs_actual:
                            obs_final = nueva_obs + ", " + obs_actual
                        else:
                            obs_final = nueva_obs
                        
                        # CORRECCIÓN: Truncar antes de UPDATE
                        obs_final = truncar_observacion(obs_final)
                        
                        update_obs = """
                        UPDATE [CxP].[DocumentsProcessing]
                        SET ObservacionesFase_4 = ?
                        WHERE nit_emisor_o_nit_del_proveedor = ?
                          AND numero_de_factura = ?
                          AND numero_de_liquidacion_u_orden_de_compra = ?
                        """
                        cur.execute(update_obs, (obs_final, nit, factura, oc))
                        
                        update_resultado = """
                        UPDATE [CxP].[DocumentsProcessing]
                        SET ResultadoFinalAntesEventos = ?
                        WHERE nit_emisor_o_nit_del_proveedor = ?
                          AND numero_de_factura = ?
                          AND numero_de_liquidacion_u_orden_de_compra = ?
                        """
                        cur.execute(update_resultado, (estado_final, nit, factura, oc))
                        
                        print("[DEBUG] DocumentsProcessing actualizado OK")
                        
                        # ====================================================
                        # 3.2 ACTUALIZAR [dbo].[CxP.Comparativa]
                        # ====================================================
                        
                        print("[UPDATE] Actualizando tabla CxP.Comparativa...")
                        
                        # 3.2.1: Actualizar Valor_XML NombreEmisor
                        update_xml = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Valor_XML = ?
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'NombreEmisor'
                        """
                        cur.execute(update_xml, (nombre_emisor, nit, factura))
                        
                        # 3.2.3: Actualizar Aprobado = 'NO'
                        update_aprobado = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Aprobado = 'NO'
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'NombreEmisor'
                        """
                        cur.execute(update_aprobado, (nit, factura))
                        
                        # 3.2.4: Actualizar Valor_Orden_de_Compra
                        update_voc = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Valor_Orden_de_Compra = ?
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'NombreEmisor'
                        """
                        cur.execute(update_voc, (primer_acreedor, nit, factura))
                        
                        # 3.2.2: Actualizar Observaciones
                        select_obs_comp = """
                        SELECT Valor_XML
                        FROM [dbo].[CxP.Comparativa]
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'Observaciones'
                        """
                        cur.execute(select_obs_comp, (nit, factura))
                        result_obs_comp = cur.fetchone()
                        
                        obs_comp_actual = safe_str(result_obs_comp[0]) if result_obs_comp and result_obs_comp[0] else ""
                        
                        if obs_comp_actual:
                            obs_comp_final = nueva_obs + ", " + obs_comp_actual
                        else:
                            obs_comp_final = nueva_obs
                        
                        # CORRECCIÓN: Truncar antes de UPDATE
                        obs_comp_final = truncar_observacion(obs_comp_final)
                        
                        update_obs_comp = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Valor_XML = ?
                        WHERE NIT = ?
                          AND Factura = ?
                          AND Item = 'Observaciones'
                        """
                        cur.execute(update_obs_comp, (obs_comp_final, nit, factura))
                        
                        # 3.2.5: Actualizar Estado_validacion_antes_de_eventos (TODOS LOS ITEMS)
                        update_estado_todos = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Estado_validacion_antes_de_eventos = ?
                        WHERE NIT = ?
                          AND Factura = ?
                        """
                        cur.execute(update_estado_todos, (estado_final, nit, factura))
                        
                        print("[DEBUG] CxP.Comparativa actualizado OK")
                        
                        # ====================================================
                        # 3.3 ACTUALIZAR [CxP].[HistoricoOrdenesCompra]
                        # ====================================================
                        
                        print("[UPDATE] Actualizando tabla HistoricoOrdenesCompra...")
                        
                        valores_doccompra = split_valores(row['DocCompra_hoc'])
                        valores_nitcedula = split_valores(row['NitCedula_hoc'])
                        valores_porcalcular = split_valores(row['PorCalcular_hoc'])
                        valores_textobreve = split_valores(row['TextoBreve_hoc'])
                        
                        num_actualizados = 0
                        for i in range(max(len(valores_doccompra), len(valores_nitcedula), 
                                          len(valores_porcalcular), len(valores_textobreve))):
                            
                            doccompra_val = valores_doccompra[i] if i < len(valores_doccompra) else ""
                            nitcedula_val = valores_nitcedula[i] if i < len(valores_nitcedula) else ""
                            porcalcular_val = valores_porcalcular[i] if i < len(valores_porcalcular) else ""
                            textobreve_val = valores_textobreve[i] if i < len(valores_textobreve) else ""
                            
                            if doccompra_val and nitcedula_val:
                                update_marca = """
                                UPDATE [CxP].[HistoricoOrdenesCompra]
                                SET Marca = 'PROCESADO'
                                WHERE DocCompra = ?
                                  AND NitCedula = ?
                                  AND PorCalcular = ?
                                  AND TextoBreve = ?
                                """
                                cur.execute(update_marca, (doccompra_val, nitcedula_val, porcalcular_val, textobreve_val))
                                num_actualizados += 1
                        
                        print("[DEBUG] HistoricoOrdenesCompra actualizado: " + str(num_actualizados) + " registros")
                        cx.commit()
                        cur.close()
                        print("[UPDATE] Todas las tablas actualizadas OK (CON NOVEDAD)")
                    
                except Exception as e_row:
                    print("[ERROR] Error procesando registro " + str(idx) + ": " + str(e_row))
                    stats['errores'] += 1
                    continue
            
            # ================================================================
            # FIN DE PROCESO
            # ================================================================
            
            stats['tiempo_total'] = time.time() - t_inicio
            
            print("")
            print("=" * 80)
            print("[FIN] Proceso completado")
            print("=" * 80)
            print("[ESTADISTICAS]")
            print("  Total registros: " + str(stats['total_registros']))
            print("  Aprobados: " + str(stats['aprobados']))
            print("  Con novedad: " + str(stats['con_novedad']))
            print("  Errores: " + str(stats['errores']))
            print("  Tiempo total: " + str(round(stats['tiempo_total'], 2)) + "s")
            print("=" * 80)
            
            msg = ("Proceso OK. Total:" + str(stats['total_registros']) + 
                   " Aprobados:" + str(stats['aprobados']) + 
                   " ConNovedad:" + str(stats['con_novedad']))
            
            SetVar("vLocStrResultadoSP", "True")
            SetVar("vLocStrResumenSP", msg)
            SetVar("vLocDicEstadisticas", str(stats))
            
            return True, msg, None, stats
    
    except Exception as e:
        exc_type = type(e).__name__
        print("")
        print("=" * 80)
        print("[ERROR CRITICO] La funcion fallo")
        print("=" * 80)
        print("[ERROR] Tipo de error: " + exc_type)
        print("[ERROR] Mensaje: " + str(e))
        print("[ERROR] Traceback completo:")
        print(traceback.format_exc())
        print("=" * 80)
        
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        SetVar("vLocStrResultadoSP", "False")
        
        return False, str(e), None, {}

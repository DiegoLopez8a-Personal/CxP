"""
================================================================================
SCRIPT: HU4_1_ZPSA_ZPSS.py
================================================================================

Descripcion General:
--------------------
    Valida pedidos de Servicios (Clases ZPSA/ZPSS/43) implementando multiples
    rutas de validacion segun las caracteristicas del pedido: tiene Orden,
    tiene Elemento PEP, tiene Activo Fijo, o es General.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0
Plataforma: RocketBot RPA

================================================================================
RUTAS DE VALIDACION
================================================================================

    RUTA A - TIENE ORDEN:
    ---------------------
        Orden 15:
            - Indicador impuestos: H4, H5, H6, H7, VP, CO, IC, CR
            - Centro de coste: debe estar VACIO
            - Cuenta: debe ser 5199150001
            - Clase orden segun indicador (H4/H5->ZINV, H6/H7->ZADM)
            
        Orden 53:
            - Centro de coste: debe estar DILIGENCIADO (estadisticas)
            
        Orden diferente (!=15, !=53):
            - Centro de coste: debe estar VACIO
            - Cuenta: 5299150099 o inicia con 7 (10 digitos)

    RUTA B - TIENE ELEMENTO PEP (sin Orden):
    ----------------------------------------
        - Indicador impuestos: H4, H5, H6, H7, VP, CO, IC, CR
        - Centro de coste: debe estar VACIO
        - Cuenta: debe ser 5199150001
        - Emplazamiento segun indicador:
            * H4/H5 -> DCTO_01
            * H6/H7 -> GTO_02
            * VP/CO/CR/IC -> DCTO_01 o GTO_02

    RUTA C - TIENE ACTIVO FIJO (sin Orden ni PEP):
    ----------------------------------------------
        Activo Diferido (inicia con 2000, 10 digitos):
            - Indicador impuestos: C1, FA, VP, CO, CR
            - Centro de coste: debe estar VACIO
            - Cuenta: debe estar VACIA
            
        No diferido:
            - Aplica validaciones generales

    RUTA D - GENERALES (sin Orden, PEP ni Activo Fijo):
    --------------------------------------------------
        - Cuenta: debe estar DILIGENCIADA
        - Indicador impuestos: debe estar DILIGENCIADO
        - Centro de coste: debe estar DILIGENCIADO
        - Cruce indicador-CECO segun archivo impuestos especiales

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |           ZPSA_ZPSS_ValidarServicios()                      |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  1. Cargar archivo Impuestos Especiales (opcional)          |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  2. Consultar candidatos ZPSA/ZPSS/43                       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  3. Para cada registro, determinar RUTA:                    |
    |  +-------------------------------------------------------+  |
    |  |  Tiene Orden?                                         |  |
    |  |    SI -> RUTA A (validaciones segun tipo orden)       |  |
    |  +-------------------------------------------------------+  |
    |  |  Tiene ElementoPEP? (y no Orden)                      |  |
    |  |    SI -> RUTA B (validaciones PEP)                    |  |
    |  +-------------------------------------------------------+  |
    |  |  Tiene ActivoFijo? (y no Orden ni PEP)                |  |
    |  |    SI -> RUTA C (validaciones Activo)                 |  |
    |  +-------------------------------------------------------+  |
    |  |  ELSE                                                 |  |
    |  |    -> RUTA D (validaciones Generales)                 |  |
    |  +-------------------------------------------------------+  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  4. Actualizar resultados en BD                             |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  5. Retornar estadisticas a RocketBot                       |
    +-------------------------------------------------------------+

================================================================================
ARCHIVO IMPUESTOS ESPECIALES (Opcional)
================================================================================

    Ruta: RutaImpuestosEspeciales en config
    
    Hojas requeridas:
        - TRIBUTO
        - TARIFAS ESPECIALES
        - IVA CECO
        
    Uso: Mapeo CECO a indicadores IVA permitidos para RUTA D

================================================================================
INDICADORES DE IMPUESTOS VALIDOS
================================================================================

    Por Ruta:
        - Orden 15 / Elemento PEP: H4, H5, H6, H7, VP, CO, IC, CR
        - Activo Fijo Diferido: C1, FA, VP, CO, CR

    Reglas de Clase Orden (Orden 15):
        - H4/H5 -> ZINV
        - H6/H7 -> ZADM
        - VP/CO/CR/IC -> ZINV o ZADM

    Reglas de Emplazamiento (Elemento PEP):
        - H4/H5 -> DCTO_01
        - H6/H7 -> GTO_02
        - VP/CO/CR/IC -> DCTO_01 o GTO_02

================================================================================
VARIABLES DE ENTRADA (RocketBot)
================================================================================

    vLocDicConfig : str | dict
        - ServidorBaseDatos: Servidor SQL Server
        - NombreBaseDatos: Base de datos
        - UsuarioBaseDatos: Usuario SQL
        - ClaveBaseDatos: Contrasena SQL
        - RutaImpuestosEspeciales: Ruta al archivo Excel (opcional)

    vGblStrUsuarioBaseDatos : str
        Usuario alternativo para conexion

    vGblStrClaveBaseDatos : str
        Contrasena alternativa para conexion

================================================================================
VARIABLES DE SALIDA (RocketBot)
================================================================================

    vLocStrResultadoSP : str
        "True" si exitoso, "False" si error critico

    vLocStrResumenSP : str
        "Procesados X registros ZPSA/ZPSS. Exitosos: Y, Con novedad: Z"

    vGblStrDetalleError : str
        Traceback en caso de error critico

    vGblStrSystemError : str
        "ErrorHU4_4.1" en caso de error

================================================================================
ESTADOS FINALES POSIBLES
================================================================================

    - PROCESADO: Validacion exitosa sin novedades
    - PROCESADO CONTADO: Validacion exitosa, forma de pago contado
    - CON NOVEDAD: Se encontraron discrepancias
    - CON NOVEDAD CONTADO: Discrepancias con forma de pago contado

================================================================================
TABLAS INVOLUCRADAS
================================================================================

    Lectura:
        - [CxP].[HU41_CandidatosValidacion]: Candidatos a validar
        
    Escritura:
        - [CxP].[DocumentsProcessing]: Estado y observaciones
        - [dbo].[CxP.Comparativa]: Trazabilidad de validaciones
        - [CxP].[HistoricoOrdenesCompra]: Marca de ordenes procesadas

================================================================================
EJEMPLOS DE USO
================================================================================

    # Configurar variables en RocketBot
    SetVar("vLocDicConfig", json.dumps({
        "ServidorBaseDatos": "sqlserver.empresa.com",
        "NombreBaseDatos": "CxP_Produccion",
        "RutaImpuestosEspeciales": "C:/Insumos/ImpuestosEspeciales.xlsx"
    }))
    
    # Ejecutar la validacion
    ZPSA_ZPSS_ValidarServicios()
    
    # Verificar resultado
    resultado = GetVar("vLocStrResultadoSP")
    resumen = GetVar("vLocStrResumenSP")

================================================================================
NOTAS TECNICAS
================================================================================

    - Errores individuales por registro NO detienen el proceso
    - Solo errores criticos de infraestructura detienen el bot
    - Tolerancia para montos: $500 COP
    - Archivo de impuestos especiales es opcional pero recomendado
    - Procesa posicion por posicion (valores separados por |)
    - Observaciones se truncan a 3900 caracteres

================================================================================
"""

def ZPSA_ZPSS_ValidarServicios():
    """
    Funcion principal para procesar las validaciones de pedidos ZPSA/ZPSS/43 (Pedidos de Servicios).

    Esta funcion orquesta el flujo completo de validacion para pedidos de servicios en SAP,
    implementando multiples rutas de validacion segun las caracteristicas del pedido
    (tiene Orden, tiene Elemento PEP, tiene Activo Fijo, o es General).

    El proceso realiza cruces de informacion entre los datos del XML (DocumentsProcessing)
    y el historico de ordenes de compra (HistoricoOrdenesCompra), aplicando reglas de
    negocio especificas segun el tipo de pedido detectado.

    Rutas de validacion implementadas:

        **RUTA A - TIENE ORDEN**:
            - Orden 15: Valida indicador impuestos (H4-H7, VP, CO, IC, CR),
              centro de coste vacio, cuenta 5199150001, clase orden segun indicador.
            - Orden 53: Valida centro de coste diligenciado (estadisticas).
            - Orden diferente: Valida centro coste vacio, cuenta (5299150099 o inicia con 7).

        **RUTA B - TIENE ELEMENTO PEP** (sin Orden):
            - Valida indicador impuestos (H4-H7, VP, CO, IC, CR).
            - Centro de coste debe estar vacio.
            - Cuenta debe ser 5199150001.
            - Emplazamiento segun indicador (DCTO_01 o GTO_02).

        **RUTA C - TIENE ACTIVO FIJO** (sin Orden ni PEP):
            - Diferido (activo inicia con 2000, 10 digitos): Indicador C1/FA/VP/CO/CR,
              centro coste vacio, cuenta vacia.
            - No diferido: Aplica validaciones generales.

        **RUTA D - GENERALES** (sin Orden, PEP ni Activo Fijo):
            - Valida cuenta diligenciada.
            - Valida indicador impuestos diligenciado.
            - Valida centro de coste diligenciado.
            - Valida cruce indicador-CECO segun archivo impuestos especiales.

    Tablas involucradas:
        - **[CxP].[HU41_CandidatosValidacion]** (Vista): Fuente de registros candidatos.
        - **[CxP].[DocumentsProcessing]** (Tabla): Estado y observaciones del documento.
        - **[dbo].[CxP.Comparativa]** (Tabla): Trazabilidad detallada de validaciones.
        - **[CxP].[HistoricoOrdenesCompra]** (Tabla): Marca de ordenes procesadas.

    Archivos externos requeridos:
        - **Archivo Impuestos Especiales** (Excel, opcional): Contiene mapeo CECO a
          indicadores IVA permitidos. Hojas requeridas: TRIBUTO, TARIFAS ESPECIALES, IVA CECO.

    Indicadores de impuestos validos:
        - **Orden 15 / Elemento PEP**: H4, H5, H6, H7, VP, CO, IC, CR
        - **Activo Fijo Diferido**: C1, FA, VP, CO, CR

    Reglas de Clase Orden (para Orden 15):
        - H4/H5 -> ZINV
        - H6/H7 -> ZADM
        - VP/CO/CR/IC -> ZINV o ZADM

    Reglas de Emplazamiento (para Elemento PEP):
        - H4/H5 -> DCTO_01
        - H6/H7 -> GTO_02
        - VP/CO/CR/IC -> DCTO_01 o GTO_02

    Reglas de Cuenta:
        - Orden 15: 5199150001
        - Orden != 15: 5299150099 o inicia con 7 (10 digitos)
        - Elemento PEP: 5199150001

    Variables de entrada (RocketBot):
        - ``vLocDicConfig`` (str | dict): Configuracion JSON con parametros:
            - ServidorBaseDatos (str): Hostname o IP del servidor SQL.
            - NombreBaseDatos (str): Nombre de la base de datos.
            - UsuarioBaseDatos (str): Usuario para autenticacion SQL.
            - ClaveBaseDatos (str): Contrasena del usuario SQL.
            - RutaImpuestosEspeciales (str, opcional): Ruta al archivo Excel de impuestos.

    Variables de salida (RocketBot):
        - ``vLocStrResultadoSP`` (str): "True" si finalizo correctamente, "False" si hubo error.
        - ``vLocStrResumenSP`` (str): Resumen estadistico del procesamiento.
        - ``vGblStrDetalleError`` (str): Traceback completo en caso de excepcion.
        - ``vGblStrSystemError`` (str): Identificador del error del sistema.

    Estados finales posibles:
        - ``PROCESADO``: Validacion exitosa sin novedades.
        - ``PROCESADO CONTADO``: Validacion exitosa, forma de pago contado.
        - ``CON NOVEDAD``: Se encontraron discrepancias.
        - ``CON NOVEDAD CONTADO``: Discrepancias con forma de pago contado.

    Returns:
        None: Los resultados se comunican via variables de RocketBot.

    Raises:
        ValueError: Si faltan parametros obligatorios en la configuracion.
        pyodbc.Error: Si hay errores de conexion a la base de datos.
        Exception: Cualquier error critico no manejado.

    Note:
        - Los errores individuales por registro NO detienen el procesamiento.
        - La tolerancia para comparacion de montos es de $500 COP.
        - El archivo de impuestos especiales es opcional pero recomendado.

    Example:
        Configuracion tipica en RocketBot::

            SetVar("vLocDicConfig", json.dumps({
                "ServidorBaseDatos": "sqlserver.empresa.com",
                "NombreBaseDatos": "CxP_Produccion",
                "UsuarioBaseDatos": "app_user",
                "ClaveBaseDatos": "SecurePass123",
                "RutaImpuestosEspeciales": "C:/Insumos/ImpuestosEspeciales.xlsx"
            }))

            ZPSA_ZPSS_ValidarServicios()

            resultado = GetVar("vLocStrResultadoSP")
            # "True" o "False"

    Version:
        1.0

    Author:
        Diego Ivan Lopez Ochoa
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
    
    # Suprimir advertencias de pandas sobre SQLAlchemy
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    # =========================================================================
    # FUNCIONES AUXILIARES BASICAS
    # =========================================================================
    
    def safe_str(v):
        """
        Convierte cualquier tipo de entrada a una cadena de texto limpia y segura.

        Maneja valores nulos (None), flotantes (NaN), bytes codificados y espacios
        en blanco, garantizando siempre una salida de tipo string sin errores.

        Args:
            v (Any): El valor a convertir. Tipos soportados:
                - None: Retorna cadena vacia.
                - str: Retorna el string sin espacios extremos.
                - bytes: Decodifica usando latin-1 con manejo de errores.
                - int/float: Convierte a string, manejando NaN como vacio.
                - Otros: Intenta conversion estandar a string.

        Returns:
            str: La representacion en cadena del valor, sin espacios al inicio/final.
                Siempre retorna un string valido (nunca None).

        Examples:
            >>> safe_str(None)
            ''
            >>> safe_str("  Texto  ")
            'Texto'
            >>> safe_str(12345)
            '12345'
            >>> safe_str(float('nan'))
            ''
        """
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
        """
        Trunca una cadena de texto para evitar errores de desbordamiento en SQL Server.

        Args:
            obs (str | Any): El texto de la observacion a guardar.
            max_len (int, optional): La longitud maxima permitida. Por defecto 3900.

        Returns:
            str: El texto truncado con "..." al final si excedia la longitud,
                o el texto original si era menor al limite.

        Examples:
            >>> truncar_observacion("Texto corto")
            'Texto corto'
            >>> truncar_observacion("A" * 5000, max_len=100)
            'AAA...AAA...'
        """
        if not obs: return ""
        obs_str = safe_str(obs)
        if len(obs_str) > max_len: return obs_str[:max_len - 3] + "..."
        return obs_str
    
    def parse_config(raw):
        """
        Analiza y convierte la configuracion de entrada a un diccionario Python.

        Intenta parsear como JSON primero, luego como literal de Python.

        Args:
            raw (str | dict): La configuracion cruda proveniente de RocketBot.

        Returns:
            dict: Un diccionario con las claves de configuracion.

        Raises:
            ValueError: Si la configuracion esta vacia o tiene formato invalido.

        Examples:
            >>> parse_config({"clave": "valor"})
            {'clave': 'valor'}
            >>> parse_config('{"servidor": "localhost"}')
            {'servidor': 'localhost'}
        """
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
        """
        Normaliza una entrada numerica o de texto a un valor flotante estandar.

        Soporta formatos con coma decimal (1.000,50) o punto decimal (1000.50).
        Elimina caracteres no numericos excepto el signo menos y el separador decimal.

        Args:
            valor (str | float | int | None): El valor a normalizar.

        Returns:
            float: El valor numerico. Retorna 0.0 si la conversion falla.

        Examples:
            >>> normalizar_decimal("1.234,56")
            1234.56
            >>> normalizar_decimal(None)
            0.0
            >>> normalizar_decimal("$1,000.00")
            1000.0
        """
        if pd.isna(valor) or valor == '' or valor is None: return 0.0
        if isinstance(valor, (int, float)):
            if np.isnan(valor) if isinstance(valor, float) else False: return 0.0
            return float(valor)
        valor_str = str(valor).strip().replace(',', '.')
        valor_str = re.sub(r'[^\d.\-]', '', valor_str)
        try: return float(valor_str)
        except: return 0.0
    
    # =========================================================================
    # CONEXION A BASE DE DATOS
    # =========================================================================
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        """
        Context Manager para establecer una conexion segura y resiliente a SQL Server.

        Implementa una estrategia de reintentos y prueba dos metodos de autenticacion:
        1. Autenticacion SQL (Usuario/Contrasena).
        2. Autenticacion de Windows (Trusted Connection).

        Args:
            cfg (dict): Diccionario con credenciales de conexion.
                Claves requeridas:
                    - ServidorBaseDatos (str): Hostname o IP del servidor.
                    - NombreBaseDatos (str): Nombre de la base de datos.
                Claves opcionales:
                    - UsuarioBaseDatos (str): Usuario para autenticacion SQL.
                    - ClaveBaseDatos (str): Contrasena del usuario.
            max_retries (int, optional): Numero maximo de intentos por metodo. Default 3.

        Yields:
            pyodbc.Connection: Objeto de conexion activo con autocommit deshabilitado.

        Raises:
            ValueError: Si faltan parametros obligatorios.
            pyodbc.Error: Si no se logra conectar tras agotar reintentos.

        Examples:
            >>> with crear_conexion_db(cfg) as cx:
            ...     cursor = cx.cursor()
            ...     cursor.execute("SELECT * FROM Tabla")

        Note:
            - El commit se realiza automaticamente al salir del contexto sin errores.
            - El rollback se realiza automaticamente si ocurre una excepcion.
        """
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing: raise ValueError(f"Parametros faltantes: {', '.join(missing)}")

        usuario = cfg['UsuarioBaseDatos']
        contrasena = cfg['ClaveBaseDatos']
        
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

        # Fase 1: Autenticacion SQL
        for attempt in range(max_retries):
            try:
                cx = pyodbc.connect(conn_str_auth, timeout=30)
                cx.autocommit = False
                conectado = True
                break
            except pyodbc.Error as e:
                excepcion_final = e
                if attempt < max_retries - 1: time.sleep(1)

        # Fase 2: Trusted Connection (si fallo la anterior)
        if not conectado:
            for attempt in range(max_retries):
                try:
                    cx = pyodbc.connect(conn_str_trusted, timeout=30)
                    cx.autocommit = False
                    conectado = True
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
    # FUNCIONES DE NORMALIZACION DE NOMBRES
    # =========================================================================
    
    def normalizar_nombre_empresa(nombre):
        """
        Normaliza nombres de empresas eliminando variantes de tipo societario y puntuacion.

        Args:
            nombre (str | Any): Nombre de la empresa a normalizar.

        Returns:
            str: Nombre normalizado en MAYUSCULAS sin puntuacion.
                Tipos societarios estandarizados:
                    - S.A.S., S.A.S, S A S -> SAS
                    - LIMITADA, LTDA. -> LTDA
                    - S.ENC., S.EN.C. -> SENC
                    - S.A., S.A -> SA

        Examples:
            >>> normalizar_nombre_empresa("Empresa S.A.S.")
            'EMPRESASAS'
            >>> normalizar_nombre_empresa("ACME LIMITADA")
            'ACMELTDA'
        """
        if pd.isna(nombre) or nombre == "": return ""
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
        Compara nombres de proveedores usando tecnica bag of words.

        Args:
            nombre_xml (str): Nombre proveniente del XML de la factura.
            nombre_sap (str): Nombre proveniente de SAP.

        Returns:
            bool: True si contienen las mismas palabras normalizadas.

        Examples:
            >>> comparar_nombres_proveedor("ACME S.A.S.", "ACME SAS")
            True
            >>> comparar_nombres_proveedor("Empresa ABC", "ABC Empresa")
            True
        """
        if pd.isna(nombre_xml) or pd.isna(nombre_sap): return False
        nombre_xml_limpio = normalizar_nombre_empresa(str(nombre_xml))
        nombre_sap_limpio = normalizar_nombre_empresa(str(nombre_sap))
        lista_xml = nombre_xml_limpio.split()
        lista_sap = nombre_sap_limpio.split()
        if len(lista_xml) != len(lista_sap): return False
        return sorted(lista_xml) == sorted(lista_sap)
    
    # =========================================================================
    # FUNCIONES DE VALIDACION
    # =========================================================================
    
    def validar_tolerancia_numerica(valor1, valor2, tolerancia=500):
        """
        Verifica si dos valores numericos estan dentro de una tolerancia.

        Args:
            valor1 (Any): Primer valor a comparar.
            valor2 (Any): Segundo valor a comparar.
            tolerancia (float, optional): Diferencia maxima permitida. Default 500.

        Returns:
            bool: True si la diferencia absoluta es menor o igual a la tolerancia.

        Examples:
            >>> validar_tolerancia_numerica(1000, 1400, tolerancia=500)
            True
            >>> validar_tolerancia_numerica(1000, 2000, tolerancia=500)
            False
        """
        try:
            val1 = normalizar_decimal(valor1)
            val2 = normalizar_decimal(valor2)
            return abs(val1 - val2) <= tolerancia
        except: return False

    def comparar_suma_total(valores_por_calcular, valor_objetivo, tolerancia=500):
        """
        Verifica si la suma total de las posiciones coincide con el valor objetivo.

        Args:
            valores_por_calcular (list[tuple]): Lista de tuplas (posicion, valor).
            valor_objetivo (float | str): Valor a buscar (monto de factura).
            tolerancia (float, optional): Diferencia maxima permitida. Default 500.

        Returns:
            tuple[bool, list, float]: Tupla con:
                - coincide (bool): True si la suma esta dentro de la tolerancia.
                - lista_posiciones (list): Posiciones usadas si coincide.
                - suma_total (float): Suma calculada.

        Examples:
            >>> valores = [('00010', '1000'), ('00020', '2000')]
            >>> comparar_suma_total(valores, 3000, tolerancia=500)
            (True, ['00010', '00020'], 3000.0)
        """
        valor_objetivo = normalizar_decimal(valor_objetivo)
        if valor_objetivo <= 0 or not valores_por_calcular: return False, [], 0
        suma_total = sum(normalizar_decimal(valor) for posicion, valor in valores_por_calcular)
        if abs(suma_total - valor_objetivo) <= tolerancia:
            todas_las_posiciones = [posicion for posicion, valor in valores_por_calcular]
            return True, todas_las_posiciones, suma_total
        return False, [], 0
    
    def validar_indicador_servicios_orden15(indicador):
        """
        Valida que el indicador de impuestos sea valido para Orden 15 o Elemento PEP.

        Indicadores validos: H4, H5, H6, H7, VP, CO, IC, CR

        Args:
            indicador (str): Indicador de impuestos a validar.

        Returns:
            bool: True si el indicador es valido.

        Examples:
            >>> validar_indicador_servicios_orden15('H4')
            True
            >>> validar_indicador_servicios_orden15('XX')
            False
        """
        indicadores_validos = {'H4', 'H5', 'H6', 'H7', 'VP', 'CO', 'IC', 'CR'}
        return safe_str(indicador).upper().strip() in indicadores_validos
    
    def validar_indicador_diferido(indicador):
        """
        Valida que el indicador sea valido para pedidos con Activo Fijo Diferido.

        Indicadores validos: C1, FA, VP, CO, CR

        Args:
            indicador (str): Indicador de impuestos a validar.

        Returns:
            bool: True si el indicador es valido para diferido.

        Examples:
            >>> validar_indicador_diferido('C1')
            True
            >>> validar_indicador_diferido('H4')
            False
        """
        indicadores_validos = {'C1', 'FA', 'VP', 'CO', 'CR'}
        return safe_str(indicador).upper().strip() in indicadores_validos
    
    def validar_clase_orden(indicador, clase_orden):
        """
        Valida que la Clase de Orden corresponda al Indicador de Impuestos.

        Reglas:
            - H4/H5 -> ZINV
            - H6/H7 -> ZADM
            - VP/CO/CR/IC -> ZINV o ZADM

        Args:
            indicador (str): Indicador de impuestos de la posicion.
            clase_orden (str): Clase de orden a validar.

        Returns:
            tuple[bool, str]: (es_valido, mensaje_error)

        Examples:
            >>> validar_clase_orden('H4', 'ZINV')
            (True, '')
            >>> validar_clase_orden('H6', 'ZINV')
            (False, 'NO se encuentra aplicado correctamente...')
        """
        indicador_str = safe_str(indicador).upper().strip()
        clase_str = safe_str(clase_orden).upper().strip()
        if not clase_str: return False, "NO se encuentra diligenciado"
        if indicador_str in ('H4', 'H5'):
            return (True, "") if clase_str == 'ZINV' else (False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = ZINV", "H6 y H7 = ZADM" o "VP, CO, CR o IC = ZINV o ZADM"')
        elif indicador_str in ('H6', 'H7'):
            return (True, "") if clase_str == 'ZADM' else (False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = ZINV", "H6 y H7 = ZADM" o "VP, CO, CR o IC = ZINV o ZADM"')
        elif indicador_str in ('VP', 'CO', 'CR', 'IC'):
            return (True, "") if clase_str in ('ZINV', 'ZADM') else (False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = ZINV", "H6 y H7 = ZADM" o "VP, CO, CR o IC = ZINV o ZADM"')
        return False, "Indicador impuestos no reconocido"
    
    def validar_emplazamiento(indicador, emplazamiento):
        """
        Valida que el Emplazamiento corresponda al Indicador de Impuestos.

        Reglas:
            - H4/H5 -> DCTO_01
            - H6/H7 -> GTO_02
            - VP/CO/CR/IC -> DCTO_01 o GTO_02

        Args:
            indicador (str): Indicador de impuestos de la posicion.
            emplazamiento (str): Emplazamiento a validar.

        Returns:
            tuple[bool, str]: (es_valido, mensaje_error)

        Examples:
            >>> validar_emplazamiento('H4', 'DCTO_01')
            (True, '')
            >>> validar_emplazamiento('H6', 'DCTO_01')
            (False, 'NO se encuentra aplicado correctamente...')
        """
        indicador_str = safe_str(indicador).upper().strip()
        empl_str = safe_str(emplazamiento).upper().strip()
        if not empl_str: return False, "NO se encuentra diligenciado"
        if indicador_str in ('H4', 'H5'):
            return (True, "") if empl_str == 'DCTO_01' else (False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = DCTO_01", "H6 y H7 = GTO_02" o "VP, CO, CR o IC = DCTO_01 o GTO_02"')
        elif indicador_str in ('H6', 'H7'):
            return (True, "") if empl_str == 'GTO_02' else (False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = DCTO_01", "H6 y H7 = GTO_02" o "VP, CO, CR o IC = DCTO_01 o GTO_02"')
        elif indicador_str in ('VP', 'CO', 'CR', 'IC'):
            return (True, "") if empl_str in ('DCTO_01', 'GTO_02') else (False, 'NO se encuentra aplicado correctamente segun reglas "H4 y H5 = DCTO_01", "H6 y H7 = GTO_02" o "VP, CO, CR o IC = DCTO_01 o GTO_02"')
        return False, "Indicador impuestos no reconocido"
    
    def validar_cuenta_orden_no_15(cuenta):
        """
        Valida que la cuenta sea valida para ordenes diferentes a 15.

        Cuentas validas:
            - 5299150099 (exacta)
            - Cualquier cuenta que inicie con 7 y tenga 10 digitos

        Args:
            cuenta (str): Numero de cuenta a validar.

        Returns:
            bool: True si la cuenta es valida.

        Examples:
            >>> validar_cuenta_orden_no_15('5299150099')
            True
            >>> validar_cuenta_orden_no_15('7123456789')
            True
            >>> validar_cuenta_orden_no_15('5199150001')
            False
        """
        cuenta_str = safe_str(cuenta).strip()
        if cuenta_str == '5299150099': return True
        if cuenta_str.startswith('7') and len(cuenta_str) == 10 and cuenta_str.isdigit(): return True
        return False
    
    def campo_vacio(valor):
        """
        Verifica si un campo esta vacio o contiene valores nulos.

        Args:
            valor (Any): Valor a verificar.

        Returns:
            bool: True si el campo esta vacio, es null, none o nan.

        Examples:
            >>> campo_vacio('')
            True
            >>> campo_vacio('null')
            True
            >>> campo_vacio('valor')
            False
        """
        valor_str = safe_str(valor)
        return valor_str == "" or valor_str.lower() in ('null', 'none', 'nan')
    
    def campo_con_valor(valor):
        """
        Verifica si un campo tiene un valor valido (no vacio).

        Args:
            valor (Any): Valor a verificar.

        Returns:
            bool: True si el campo tiene un valor valido.

        Examples:
            >>> campo_con_valor('texto')
            True
            >>> campo_con_valor('')
            False
        """
        return not campo_vacio(valor)
    
    # =========================================================================
    # FUNCIONES DE ACTUALIZACION DE BD
    # =========================================================================
    
    def actualizar_bd_cxp(cx, registro_id, campos_actualizar):
        """
        Actualiza campos en la tabla principal [CxP].[DocumentsProcessing].

        Args:
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            registro_id (str): Identificador unico del registro (campo [ID]).
            campos_actualizar (dict): Diccionario con campos y valores a actualizar.
                El campo 'ObservacionesFase_4' tiene tratamiento especial para
                concatenar observaciones existentes.

        Returns:
            None: Los cambios se confirman con commit dentro de la funcion.

        Raises:
            Exception: Si ocurre un error durante la actualizacion.

        Note:
            - La tabla destino es [CxP].[DocumentsProcessing].
            - Se usa parametrizacion para prevenir SQL injection.
        """
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
            print(f"[ERROR] Error actualizando DocumentsProcessing: {str(e)}")
            raise
    
    def actualizar_items_comparativa(registro, cx, nit, factura, nombre_item,
                                 actualizar_valor_xml=True, valor_xml=None,
                                 actualizar_aprobado=True, valor_aprobado=None,
                                 actualizar_orden_compra=True, val_orden_de_compra=None):
        """
        Actualiza o inserta items detallados en la tabla de trazabilidad.

        Gestiona la tabla [dbo].[CxP.Comparativa] donde se almacena el detalle
        de cada validacion realizada.

        Args:
            registro (dict | pd.Series): Registro del documento siendo procesado.
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            nit (str): NIT del emisor/proveedor.
            factura (str): Numero de factura.
            nombre_item (str): Nombre del item de validacion.
            actualizar_valor_xml (bool, optional): Si actualizar Valor_XML. Default True.
            valor_xml (str | None, optional): Valor del XML.
            actualizar_aprobado (bool, optional): Si actualizar Aprobado. Default True.
            valor_aprobado (str | list | None, optional): Resultado: 'SI', 'NO', o lista.
            actualizar_orden_compra (bool, optional): Si actualizar Valor_Orden_de_Compra.
            val_orden_de_compra (str | None, optional): Valor de SAP.

        Returns:
            None: Los cambios se confirman con commit dentro de la funcion.

        Note:
            - Para valores con separador '|', se crea un registro por cada valor.
            - La tabla destino es [dbo].[CxP.Comparativa].
        """
        cur = cx.cursor()
        def safe_db_val(v):
            if v is None: return None
            s = str(v).strip()
            if not s or s.lower() == 'none' or s.lower() == 'null': return None
            return s

        query_count = """SELECT COUNT(*) FROM [dbo].[CxP.Comparativa] WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?"""
        cur.execute(query_count, (nit, factura, nombre_item, registro.get('ID_dp','')))
        count_existentes = cur.fetchone()[0]

        lista_compra = val_orden_de_compra.split('|') if val_orden_de_compra else []
        lista_xml = valor_xml.split('|') if valor_xml else []
        lista_aprob = valor_aprobado.split('|') if valor_aprobado else []
        
        if isinstance(valor_aprobado, list): lista_aprob = valor_aprobado

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
                UPDATE CTE SET {", ".join(set_clauses)} WHERE rn = ?
                """
                final_params = params + [nit, factura, nombre_item, registro.get('ID_dp',''), i + 1]
                cur.execute(update_query, final_params)
            else:
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra,
                    Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(insert_query, (registro.get('Fecha_de_retoma_antes_de_contabilizacion_dp',''),registro.get('documenttype_dp',''),registro.get('numero_de_liquidacion_u_orden_de_compra_dp',''),registro.get('nombre_emisor_dp',''), registro.get('ID_dp',''), nit, factura, nombre_item, val_compra, val_xml, val_aprob))
        cx.commit()
        cur.close()
    
    def actualizar_estado_comparativa(cx, nit, factura, estado):
        """
        Actualiza el estado de validacion para todos los registros de una factura.

        Args:
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            nit (str): NIT del emisor/proveedor.
            factura (str): Numero de factura.
            estado (str): Estado final de la validacion.

        Returns:
            None: Los cambios se confirman con commit.

        Note:
            - La tabla destino es [dbo].[CxP.Comparativa].
            - Afecta TODOS los registros de la factura especificada.
        """
        cur = cx.cursor()
        update_sql = "UPDATE [dbo].[CxP.Comparativa] SET Estado_validacion_antes_de_eventos = ? WHERE NIT = ? AND Factura = ?"
        cur.execute(update_sql, (estado, nit, factura))
        cx.commit()
        cur.close()
    
    def marcar_orden_procesada(cx, oc_numero, posiciones_string):
        """
        Marca las posiciones de una orden de compra como procesadas.

        Args:
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            oc_numero (str): Numero del documento de compra.
            posiciones_string (str): Posiciones separadas por pipe (|).

        Returns:
            None: Los cambios se confirman con commit.

        Note:
            - La tabla destino es [CxP].[HistoricoOrdenesCompra].
            - El valor de marca es 'PROCESADO'.
        """
        cur = cx.cursor()
        lista_posiciones = posiciones_string.split('|')
        update_query = "UPDATE [CxP].[HistoricoOrdenesCompra] SET Marca = 'PROCESADO' WHERE DocCompra = ? AND Posicion = ?"
        for posicion in lista_posiciones:
            pos = posicion.strip() 
            if pos: cur.execute(update_query, (oc_numero, pos))
        cx.commit() 
        cur.close()
    
    # =========================================================================
    # FUNCIONES DE PROCESAMIENTO DE POSICIONES
    # =========================================================================
    
    def expandir_posiciones_string(valor_string, separador='|'):
        """
        Expande una cadena delimitada en una lista de valores individuales.

        Args:
            valor_string (str | Any): Cadena con valores separados.
            separador (str, optional): Separador principal. Default '|'.

        Returns:
            list[str]: Lista de valores individuales sin espacios extremos.

        Examples:
            >>> expandir_posiciones_string("10|20|30")
            ['10', '20', '30']
            >>> expandir_posiciones_string("100,200")
            ['100', '200']
        """
        if pd.isna(valor_string) or valor_string == '' or valor_string is None: return []
        valor_str = safe_str(valor_string)
        if '|' in valor_str: return [v.strip() for v in valor_str.split('|') if v.strip()]
        if ',' in valor_str: return [v.strip() for v in valor_str.split(',') if v.strip()]
        return [valor_str.strip()]
    
    def expandir_posiciones_historico(registro):
        """
        Desglosa la informacion concatenada del historico en diccionarios por posicion.

        Transforma los campos concatenados del registro (separados por '|') en una
        lista de diccionarios, donde cada diccionario representa una posicion
        individual con todos sus atributos de SAP.

        Args:
            registro (dict | pd.Series): Registro con campos concatenados del historico.
                Campos procesados incluyen: Posicion_hoc, PorCalcular_hoc, Trm_hoc,
                IndicadorImpuestos_hoc, Cuenta_hoc, CentroCoste_hoc, Orden_hoc,
                ElementoPEP_hoc, ActivoFijo_hoc, ClaseOrden_hoc, Emplazamiento_hoc, etc.

        Returns:
            list[dict]: Lista de diccionarios, uno por cada posicion.
                Lista vacia si no hay posiciones o si ocurre un error.

        Examples:
            >>> registro = {'Posicion_hoc': '00010|00020', 'PorCalcular_hoc': '1000|2000'}
            >>> resultado = expandir_posiciones_historico(registro)
            >>> len(resultado)
            2
        """
        try:
            posiciones = expandir_posiciones_string(registro.get('Posicion_hoc', ''))
            if not posiciones: return []
            
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
        Carga y procesa el archivo Excel de Impuestos Especiales.

        Este archivo contiene el mapeo entre Centros de Costo (CECO) y los
        indicadores de IVA permitidos para cada uno.

        Args:
            ruta_archivo (str): Ruta completa al archivo Excel.
                Hojas requeridas: TRIBUTO, TARIFAS ESPECIALES, IVA CECO

        Returns:
            dict: Diccionario donde las claves son CECOs (string) y los valores
                son listas de indicadores IVA permitidos.
                Ejemplo: {'CECO001': ['H4', 'H5'], 'CECO002': ['H6', 'H7']}

        Raises:
            Exception: Si el archivo no existe, tiene estructura invalida,
                o faltan columnas requeridas.

        Examples:
            >>> mapeo = cargar_archivo_impuestos_especiales('C:/Insumos/Impuestos.xlsx')
            >>> 'CECO001' in mapeo
            True

        Note:
            La hoja 'IVA CECO' debe contener columnas:
            - CECO (o similar): Codigo del centro de costo
            - CODIGO IND. IVA APLICABLE (o similar): Indicadores permitidos
        """
        try:
            if not os.path.exists(ruta_archivo):
                print(f"[WARNING] Archivo no encontrado: {ruta_archivo}")
                raise Exception(f'Archivo no encontrado: {ruta_archivo}') 
            
            xls = pd.ExcelFile(ruta_archivo)
            hojas_requeridas = ['TRIBUTO', 'TARIFAS ESPECIALES', 'IVA CECO']
            hojas_faltantes = [h for h in hojas_requeridas if h not in xls.sheet_names]
            
            if hojas_faltantes:
                print(f"[WARNING] Estructura invalida. Hojas faltantes: {hojas_faltantes}")
                raise Exception(f'Estructura invalida. Hojas faltantes: {hojas_faltantes}') 
            
            df_iva_ceco = pd.read_excel(xls, sheet_name='IVA CECO')
            df_iva_ceco.columns = df_iva_ceco.columns.str.strip()
            
            col_ceco = None
            col_codigo_iva = None
            
            for col in df_iva_ceco.columns:
                col_upper = col.upper()
                if 'CECO' in col_upper and 'NOMBRE' not in col_upper:
                    col_ceco = col
                if 'CODIGO IND. IVA APLICABLE' in col_upper or ('CODIGO' in col_upper and 'IVA' in col_upper and 'APLICABLE' in col_upper):
                    col_codigo_iva = col
            
            if not col_ceco or not col_codigo_iva:
                print("[WARNING] Columnas requeridas no encontradas en IVA_CECO")
                raise Exception(f'Columnas requeridas no encontradas en IVA_CECO')
                
            mapeo_ceco = {}
            for _, row in df_iva_ceco.iterrows():
                ceco = safe_str(row[col_ceco])
                codigo_iva = safe_str(row[col_codigo_iva])
                
                if ceco and codigo_iva:
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
        print("[INFO] Procesamiento ZPSA/ZPSS/43 - Pedidos de Servicios")
        print("=" * 80)
        
        t_inicio = time.time()
        
        # 1. Obtener y validar configuracion
        cfg = parse_config(GetVar("vLocDicConfig"))
        
        print("[INFO] Configuracion cargada exitosamente")
        
        required_config = ['ServidorBaseDatos', 'NombreBaseDatos']
        missing_config = [k for k in required_config if not cfg.get(k)]
        if missing_config:
            raise ValueError(f"Faltan parametros de configuracion: {', '.join(missing_config)}")
        
        # Cargar archivo de impuestos especiales si esta configurado
        ruta_impuestos = cfg.get('RutaImpuestosEspeciales', '')
        mapeo_ceco_impuestos = None
        if ruta_impuestos:
            try:
                mapeo_ceco_impuestos = cargar_archivo_impuestos_especiales(ruta_impuestos)
            except Exception as e:
                SetVar("vGblStrDetalleError", traceback.format_exc())
                SetVar("vGblStrSystemError", "ErrorHU4_4.1")
                SetVar("vLocStrResultadoSP", "False")
                raise e 
        
        # 2. Conectar a base de datos
        with crear_conexion_db(cfg) as cx:
            print("[INFO] Obteniendo registros ZPSA/ZPSS/43 para procesar...")
            
            query_zpsa = """SELECT * FROM [CxP].[HU41_CandidatosValidacion] 
                    WHERE CAST([ClaseDePedido_hoc] AS NVARCHAR(MAX)) LIKE '%ZPSA%' OR CAST([ClaseDePedido_hoc] AS NVARCHAR(MAX)) LIKE '%ZPSS%'"""
            
            df_registros = pd.read_sql(query_zpsa, cx)
            
            print(f"[INFO] Obtenidos {len(df_registros)} registros ZPSA/ZPSS/43 para procesar")
            
            if len(df_registros) == 0:
                print("[INFO] No hay registros ZPSA/ZPSS/43 pendientes de procesar")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros ZPSA/ZPSS/43 pendientes de procesar")
                return
            
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
                
                tipo_pedido = 'ZPSA' if clase_pedido in ('ZPSA', '43') else 'ZPSS'
                
                print(f"\n[INFO] Registro {registros_procesados + 1}/{len(df_registros)}: OC {numero_oc}, Factura {numero_factura}, Tipo {tipo_pedido}")
                
                sufijo_contado = " CONTADO" if payment_means in ["01", "1"] else ""
                
                # 4. Expandir posiciones del historico
                datos_posiciones = expandir_posiciones_historico(registro)
                
                # 5. Determinar si es USD
                es_usd = True if 'USD' in safe_str(registro.get('Moneda_hoc','')).upper() else False
                
                # 6. Obtener valor a comparar segun moneda
                if es_usd:
                    valor_xml = normalizar_decimal(registro.get('VlrPagarCop_dp', 0))
                else:
                    valor_xml = normalizar_decimal(registro.get('Valor de la Compra LEA_ddp', 0))
                
                # 7. Buscar combinacion de posiciones
                valores_por_calcular = [(d['Posicion'], d['PorCalcular']) for d in datos_posiciones]
                
                coincidencia_encontrada, posiciones_usadas, suma_encontrada = comparar_suma_total(
                    valores_por_calcular, valor_xml, tolerancia=500
                )
                
                if not coincidencia_encontrada:
                    print(f"[INFO] No se encuentra coincidencia del valor a pagar para OC {numero_oc}")
                    observacion = f"No se encuentra coincidencia del Valor a pagar de la factura, {registro.get('ObservacionesFase_4_dp','')}"
                    resultado_final = f"CON NOVEDAD {sufijo_contado}"
                    
                    campos_novedad = {
                        'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso',
                        'ObservacionesFase_4': truncar_observacion(observacion),
                        'ResultadoFinalAntesEventos': resultado_final
                    }
                    actualizar_bd_cxp(cx, registro_id, campos_novedad)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='LineExtensionAmount',
                                                valor_xml=registro.get('valor_a_pagar_dp',''), valor_aprobado=None, val_orden_de_compra=None)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Observaciones',
                                                valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                    
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='VlrPagarCop',
                                                valor_xml=registro.get('VlrPagarCop_dp',''), valor_aprobado='NO', val_orden_de_compra='NO ENCONTRADO')
                    
                    actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                    
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                    
                    registros_con_novedad += 1
                    registros_procesados += 1
                    continue
                
                else:
                    # Registrar campos base en comparativa
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='LineExtensionAmount',
                                                valor_xml=registro.get('valor_a_pagar_dp',''), valor_aprobado=None, val_orden_de_compra=None)
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Posicion',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('Posicion_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='ValorPorCalcularSAP',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('PorCalcular_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TipoNIF',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('TipoNif_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Acreedor',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('Acreedor_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='FecDoc',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('FecDoc_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='FecReg',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('FecReg_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='FechaContGasto',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('FecContGasto_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='IndicadorImpuestos',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='TextoBreve',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('Texto_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='ClaseImpuesto',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('ClaseDeImpuesto_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='Cuenta',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('Cuenta_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CiudadProveedor',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('CiudadProveedor_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='DocFIEntrada',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('DocFiEntrada_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CTA26',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('Cuenta26_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='ActivoFijo',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('ActivoFijo_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CapitalizadoEl',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('CapitalizadoEl_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='CriterioClasif2',
                                                valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('CriterioClasif2_hoc',''))
                    actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                nombre_item='LineExtensionAmount',
                                                valor_xml=None, valor_aprobado='SI', val_orden_de_compra=None)
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                    
                    hay_novedad = False
                    
                    # Variable de control para rutas especificas
                    rutas_especificas_ejecutadas = False
                
                    # =========================================================
                    # 11. VALIDACIONES ESPECIFICAS
                    # =========================================================
                    
                    ListaOrden = registro.get('Orden_hoc','').split('|') if registro.get('Orden_hoc','') else []
                    ListaPeP = registro.get('ElementoPEP_hoc','').split('|') if registro.get('ElementoPEP_hoc','') else []
                    ListaActivoFijo = registro.get('ActivoFijo_hoc','').split('|') if registro.get('ActivoFijo_hoc','') else []
                    ListaIndicador = registro.get('IndicadorImpuestos_hoc','').split('|') if registro.get('IndicadorImpuestos_hoc','') else []
                    ListaCentroCoste = registro.get('CentroDeCoste_hoc','').split('|') if registro.get('CentroDeCoste_hoc','') else []
                    ListaCuenta = registro.get('Cuenta_hoc','').split('|') if registro.get('Cuenta_hoc','') else []
                    ListaClaseOrden = registro.get('ClaseDeOrden_hoc','').split('|') if registro.get('ClaseDeOrden_hoc','') else []
                    ListaEmplazamiento = registro.get('Emplazamiento_hoc','').split('|') if registro.get('Emplazamiento_hoc','') else []
                    
                    tiene_orden = any(campo_con_valor(d) for d in ListaOrden)
                    tiene_elemento_pep = any(campo_con_valor(d) for d in ListaPeP)
                    tiene_activo_fijo = any(campo_con_valor(d) for d in ListaActivoFijo)
                    
                    # ---------------------------------------------------------
                    # RUTA A: TIENE ORDEN
                    # ---------------------------------------------------------
                    if tiene_orden:
                        rutas_especificas_ejecutadas = True
                        indicador_valido = True
                        for item in ListaOrden:
                            if indicador_valido:
                                orden_valor = safe_str(item)
                                orden_limpio = re.sub(r'\D', '', orden_valor)
                                
                                if orden_limpio.startswith('15') and len(orden_limpio) == 9:
                                    # ORDEN 15
                                    aprobados_indicador = []
                                    for d in ListaIndicador:
                                        if validar_indicador_servicios_orden15(d): aprobados_indicador.append('SI')
                                        else:
                                            aprobados_indicador.append('NO')
                                            indicador_valido = False
                                    
                                    if not indicador_valido:
                                        if all(campo_vacio(ind) for ind in ListaIndicador):
                                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero campo 'Indicador impuestos' NO se encuentra diligenciado, {registro.get('ObservacionesFase_4_dp','')}"
                                        else:
                                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero campo 'Indicador impuestos' NO corresponde alguna de las opciones H4, H5, H6, H7, VP, CO, IC, CR, {registro.get('ObservacionesFase_4_dp','')}"
                                        hay_novedad = True
                                        
                                        campos_novedad_ind = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                        actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Indicador impuestos',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                        registros_con_novedad += 1
                                        break 
                                    else:
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Indicador impuestos',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                    
                                    aprobados_centro = []
                                    indicador_valido = True
                                    for d in ListaCentroCoste:
                                        if campo_vacio(d): aprobados_centro.append('SI')
                                        else:
                                            aprobados_centro.append('NO')
                                            indicador_valido = False
                                    
                                    if not indicador_valido:
                                        observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo 'Centro de coste' se encuentra diligenciado cuando NO debe estarlo, {registro.get('ObservacionesFase_4_dp','')}"
                                        hay_novedad = True
                                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                        campos_novedad_centro = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"}
                                        actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                        registros_con_novedad += 1
                                        break
                                    else:
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                    
                                    aprobados_cuenta = []
                                    cuenta_valida = True
                                    for d in ListaCuenta:
                                        if d.strip() == '5199150001': aprobados_cuenta.append('SI')
                                        else:
                                            aprobados_cuenta.append('NO')
                                            cuenta_valida = False
                                    
                                    if not cuenta_valida:
                                        observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo 'Cuenta' es diferente a 5199150001, {registro.get('ObservacionesFase_4_dp','')}"
                                        hay_novedad = True
                                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                        campos_novedad_cuenta = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                        actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                        registros_con_novedad += 1
                                        break
                                    else:
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                    
                                    aprobados_clase = []
                                    clase_valida = True
                                    for indicador, clase_ord in zip_longest(ListaIndicador, ListaClaseOrden, fillvalue=''):
                                        es_valido, msg = validar_clase_orden(indicador.strip(), clase_ord.strip())
                                        if es_valido: aprobados_clase.append('SI')
                                        else:
                                            aprobados_clase.append('NO')
                                            clase_valida = False
                                    
                                    if not clase_valida:
                                        clases = [safe_str(d) for d in ListaClaseOrden]
                                        if all(campo_vacio(c) for c in clases):
                                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo 'Clase orden' NO se encuentra diligenciado, {registro.get('ObservacionesFase_4_dp','')}"
                                        else:
                                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden 15, pero Campo 'Clase orden' NO se encuentra aplicado correctamente segun reglas 'H4 y H5 = ZINV', 'H6 y H7 = ZADM' o 'VP, CO, CR o IC = ZINV o ZADM', {registro.get('ObservacionesFase_4_dp','')}"
                                        hay_novedad = True
                                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                        campos_novedad_clase = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                        actualizar_bd_cxp(cx, registro_id, campos_novedad_clase)
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                        registros_con_novedad += 1
                                        break
                                    else:
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                
                                else:
                                    # ORDEN NO INICIA CON 15
                                    if orden_limpio.startswith('53') and len(orden_limpio) == 8:
                                        # ORDEN 53
                                        aprobados_centro = []
                                        centro_valido = True
                                        for d in ListaCentroCoste:
                                            if campo_vacio(d):
                                                aprobados_centro.append('SI')
                                                indicador_valido = False
                                            else: aprobados_centro.append('NO')
                                        
                                        if not centro_valido:
                                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden 53, pero Campo 'Centro de coste' se encuentra vacio para pedidos ESTADISTICAS, {registro.get('ObservacionesFase_4_dp','')}"
                                            hay_novedad = True
                                            campos_novedad_centro = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                            actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                            registros_con_novedad += 1
                                            break
                                        else:
                                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                    else:
                                        # ORDEN DIFERENTE
                                        aprobados_centro = []
                                        centro_valido = True
                                        for d in ListaCentroCoste:
                                            if campo_vacio(d): aprobados_centro.append('SI')
                                            else:
                                                aprobados_centro.append('NO')
                                                indicador_valido = False
                                        
                                        if not centro_valido:
                                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo 'Centro de coste' se encuentra diligenciado para pedidos NO ESTADISTICAS, {registro.get('ObservacionesFase_4_dp','')}"
                                            hay_novedad = True
                                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                            campos_novedad_centro = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                            actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                            registros_con_novedad += 1
                                            break
                                        else:
                                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                        
                                        aprobados_cuenta = []
                                        cuenta_valida = True
                                        for d in ListaCuenta:
                                            cuenta = safe_str(d).strip()
                                            if validar_cuenta_orden_no_15(cuenta): aprobados_cuenta.append('SI')
                                            else:
                                                aprobados_cuenta.append('NO')
                                                cuenta_valida = False
                                        
                                        if not cuenta_valida:
                                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo 'Cuenta' es diferente a 5299150099 y/o NO cumple regla 'inicia con 7 y tiene 10 digitos', {registro.get('ObservacionesFase_4_dp','')}"
                                            hay_novedad = True
                                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                                            campos_novedad_cuenta = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                            actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                            registros_con_novedad += 1
                                            break
                                        else:
                                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                    
                    if hay_novedad:
                        registros_procesados += 1
                        continue

                    # ---------------------------------------------------------
                    # RUTA B: TIENE ELEMENTO PEP (y no tiene Orden)
                    # ---------------------------------------------------------
                    if tiene_elemento_pep:
                        rutas_especificas_ejecutadas = True
                        aprobados_indicador = []
                        indicador_valido = True
                        for d in ListaIndicador:
                            if validar_indicador_servicios_orden15(d): aprobados_indicador.append('SI')
                            else:
                                aprobados_indicador.append('NO')
                                indicador_valido = False
                        
                        if not indicador_valido:
                            indicadores_actual = ListaIndicador
                            if all(campo_vacio(ind) for ind in indicadores_actual):
                                observacion = f"Pedido corresponde a {tipo_pedido} con Elemento PEP, pero campo 'Indicador impuestos' NO se encuentra diligenciado, {registro.get('ObservacionesFase_4_dp','')}"
                            else:
                                observacion = f"Pedido corresponde a {tipo_pedido} con Elemento PEP, pero campo 'Indicador impuestos' NO corresponde alguna de las opciones H4, H5, H6, H7, VP, CO, IC, CR, {registro.get('ObservacionesFase_4_dp','')}"
                            hay_novedad = True
                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                            campos_novedad_ind = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Indicador impuestos',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                            registros_con_novedad += 1
                            continue
                        else:
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Indicador impuestos',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                    
                        aprobados_centro = []
                        centro_valido = True
                        for d in ListaCentroCoste:
                            if campo_vacio(d): aprobados_centro.append('SI')
                            else:
                                aprobados_centro.append('NO')
                                indicador_valido = False
                        
                        if not centro_valido:
                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo 'Centro de coste' se encuentra diligenciado para pedidos NO ESTADISTICAS, {registro.get('ObservacionesFase_4_dp','')}"
                            hay_novedad = True
                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                            campos_novedad_centro = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                            registros_con_novedad += 1
                            continue
                        else:
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                        
                        if centro_valido:
                            aprobados_cuenta = []
                            cuenta_valida = True
                            for d in ListaCuenta:
                                cuenta = safe_str(d).strip()
                                if cuenta == '5199150001': aprobados_cuenta.append('SI')
                                else:
                                    aprobados_cuenta.append('NO')
                                    cuenta_valida = False
                            
                            if not cuenta_valida:
                                observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Orden diferente a 53, pero Campo 'Cuenta' es diferente a 5299150099 y/o NO cumple regla 'inicia con 7 y tiene 10 digitos', {registro.get('ObservacionesFase_4_dp','')}"
                                hay_novedad = True
                                campos_novedad_cuenta = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                registros_con_novedad += 1
                                continue
                            else:
                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                            
                                if cuenta_valida:
                                    aprobados_empl = []
                                    empl_valido = True
                                    for emplazamiento, indicador in zip_longest(ListaEmplazamiento, ListaIndicador, fillvalue=''):
                                        es_valido, msg = validar_emplazamiento(indicador, emplazamiento)
                                        if es_valido: aprobados_empl.append('SI')
                                        else:
                                            aprobados_empl.append('NO')
                                            empl_valido = False
                                    
                                    if not empl_valido:
                                        empls = ListaEmplazamiento
                                        if all(campo_vacio(e) for e in empls):
                                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Elemento PEP, pero Campo 'Emplazamiento' NO se encuentra diligenciado, {registro.get('ObservacionesFase_4_dp','')}"
                                        else:
                                            observacion = f"Pedido corresponde a {tipo_pedido} y cuenta con Elemento PEP, pero Campo 'Emplazamiento' NO se encuentra aplicado correctamente segun reglas 'H4 y H5 = DCTO_01', 'H6 y H7 = GTO_02' o 'VP, CO, CR o IC = DCTO_01 o GTO_02', {registro.get('ObservacionesFase_4_dp','')}"
                                        hay_novedad = True
                                        campos_novedad_empl = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                        actualizar_bd_cxp(cx, registro_id, campos_novedad_empl)
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Emplazamiento',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('Emplazamiento_hoc',''))
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                        registros_con_novedad += 1
                                        continue
                                    else:
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Emplazamiento',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('Emplazamiento_hoc',''))
                        
                    # ---------------------------------------------------------
                    # RUTA C: TIENE ACTIVO FIJO (y no tiene Orden ni Elemento PEP)
                    # ---------------------------------------------------------
                    if tiene_activo_fijo:
                        rutas_especificas_ejecutadas = True
                        es_diferido = True
                        aprobadosdiferido = []
                        for activofijo in ListaActivoFijo:
                            activo_limpio = re.sub(r'\D', '', activofijo)
                            if activo_limpio.startswith('2000') and len(activo_limpio) == 10: aprobadosdiferido.append('SI')
                            else:
                                aprobadosdiferido.append('NO')
                                es_diferido = False
                        
                        if es_diferido:
                            aprobados_indicador = []
                            indicador_valido = True
                            for d in ListaIndicador:
                                if validar_indicador_diferido(d): aprobados_indicador.append('SI')
                                else:
                                    aprobados_indicador.append('NO')
                                    indicador_valido = False
                            
                            if not indicador_valido:
                                if all(campo_vacio(ind) for ind in ListaIndicador):
                                    observacion = f"Pedido corresponde a {tipo_pedido} con Activo Fijo, pero campo 'Indicador impuestos' NO se encuentra diligenciado para pedido DIFERIDO, {registro.get('ObservacionesFase_4_dp','')}"
                                else:
                                    observacion = f"Pedido corresponde a {tipo_pedido} con Activo Fijo, pero campo 'Indicador impuestos' NO corresponde alguna de las opciones 'C1', 'FA', 'VP', 'CO' o 'CR' para pedido DIFERIDO, {registro.get('ObservacionesFase_4_dp','')}"
                                hay_novedad = True
                                campos_novedad_ind = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='IndicadorImpuestos',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                registros_con_novedad += 1
                                continue
                            else:
                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='IndicadorImpuestos',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                                marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                            
                                if indicador_valido:
                                    aprobados_centro = []
                                    centro_valido = True
                                    for d in ListaCentroCoste:
                                        if campo_vacio(d): aprobados_centro.append('SI')
                                        else:
                                            aprobados_centro.append('NO')
                                            centro_valido = False
                                    
                                    if not centro_valido:
                                        observacion = f"Pedido corresponde a {tipo_pedido} con Activo Fijo, pero Campo 'Centro de coste' se encuentra diligenciado cuando NO debe estarlo para pedido DIFERIDO, {registro.get('ObservacionesFase_4_dp','')}"
                                        hay_novedad = True
                                        campos_novedad_centro = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                        actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                        registros_con_novedad += 1
                                        continue
                                    else:
                                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                                        
                                        if centro_valido:
                                            aprobados_cuenta = []
                                            cuenta_valida = True
                                            for d in ListaCuenta:
                                                if campo_vacio(d): aprobados_cuenta.append('SI')
                                                else:
                                                    aprobados_cuenta.append('NO')
                                                    cuenta_valida = False
                                            
                                            if not cuenta_valida:
                                                observacion = f"Pedido corresponde a {tipo_pedido} con Activo Fijo, pero Campo 'Cuenta' se encuentra diligenciado cuando NO debe estarlo para pedido DIFERIDO, {registro.get('ObservacionesFase_4_dp','')}"
                                                hay_novedad = True
                                                campos_novedad_cuenta = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                                                actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                                                actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                                                marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                                                registros_con_novedad += 1
                                                continue
                                            else:
                                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                                                marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                    
                # ---------------------------------------------------------
                # RUTA D: GENERALES (Si no entro a ninguna ruta especifica)
                # ---------------------------------------------------------
                if not rutas_especificas_ejecutadas:
                    aprobados_cuenta = []
                    cuenta_valida = True
                    for d in ListaCuenta:
                        if campo_con_valor(d): aprobados_cuenta.append('SI')
                        else:
                            aprobados_cuenta.append('NO')
                            cuenta_valida = False
                    
                    if not cuenta_valida:
                        observacion = f"Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero Campo 'Cuenta' NO se encuentra diligenciado cuando debe estarlo para pedido GENERALES, {registro.get('ObservacionesFase_4_dp','')}"
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        campos_novedad_cuenta = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_cuenta)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                        registros_con_novedad += 1
                        continue
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Cuenta',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                    
                    aprobados_indicador = []
                    indicador_valido = True
                    for d in ListaIndicador:
                        if campo_con_valor(d): aprobados_indicador.append('SI')
                        else:
                            aprobados_indicador.append('NO')
                            indicador_valido = False
                    
                    if not indicador_valido:
                        observacion = f"Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero campo 'Indicador impuestos' NO se encuentra diligenciado para pedido GENERALES, {registro.get('ObservacionesFase_4_dp','')}"
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        campos_novedad_ind = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_ind)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Indicador impuestos',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                        registros_con_novedad += 1
                        continue
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Indicador impuestos',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                    
                    aprobados_centro = []
                    centro_valido = True
                    for d in ListaCentroCoste:
                        if campo_con_valor(d): aprobados_centro.append('SI')
                        else:
                            aprobados_centro.append('NO')
                            centro_valido = False
                    
                    if not centro_valido:
                        observacion = f"Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero Campo 'Centro de coste' NO se encuentra diligenciado cuando debe estarlo para pedido GENERALES, {registro.get('ObservacionesFase_4_dp','')}"
                        hay_novedad = True
                        resultado_final = f"CON NOVEDAD {sufijo_contado}"
                        campos_novedad_centro = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                        actualizar_bd_cxp(cx, registro_id, campos_novedad_centro)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                        actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                        registros_con_novedad += 1
                        continue
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='CentroCoste',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('CentroDeCoste_hoc',''))
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                    
                    if indicador_valido and centro_valido and mapeo_ceco_impuestos:
                        aprobados_indicador_ceco = []
                        indicador_ceco_valido = True
                        indicadores_fallidos_detalle = set()
                        for centro, indicador in zip_longest(ListaCentroCoste, ListaIndicador, fillvalue=''):
                            centro = safe_str(centro).upper()
                            indicador = safe_str(indicador).upper()
                            if centro in mapeo_ceco_impuestos:
                                indicadores_permitidos = mapeo_ceco_impuestos[centro]
                                if indicador in indicadores_permitidos: aprobados_indicador_ceco.append('SI')
                                else:
                                    aprobados_indicador_ceco.append('NO')
                                    indicador_ceco_valido = False
                                    inds_str = ', '.join(indicadores_permitidos) if indicadores_permitidos else 'N/A'
                                    indicadores_fallidos_detalle.add(f"CECO {centro}: ({inds_str})")
                            else: aprobados_indicador_ceco.append('SI')

                        if not indicador_ceco_valido:
                            detalle_indicadores = " | ".join(indicadores_fallidos_detalle)
                            observacion = f"Pedido corresponde a {tipo_pedido} sin Activo Fijo, pero campo 'Indicador impuestos' NO se encuentra diligenciado correctamente segun los indicadores: {detalle_indicadores}, {registro.get('ObservacionesFase_4_dp','')}"
                            hay_novedad = True
                            resultado_final = f"CON NOVEDAD {sufijo_contado}"
                            campos_novedad_ind_ceco = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ObservacionesFase_4': truncar_observacion(observacion),'ResultadoFinalAntesEventos': f"CON NOVEDAD{sufijo_contado}"}
                            actualizar_bd_cxp(cx, registro_id, campos_novedad_ind_ceco)
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Observaciones',valor_xml=truncar_observacion(observacion), valor_aprobado=None, val_orden_de_compra=None)
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Indicador impuestos',valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                            actualizar_estado_comparativa(cx, nit, numero_factura, resultado_final)
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                            registros_con_novedad += 1
                            continue
                        else:
                            actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, nombre_item='Indicador impuestos',valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                            marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
            
                # 12. FINALIZAR REGISTRO EXITOSO
                if not hay_novedad:
                    marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                    campos_exitoso = {'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso','ResultadoFinalAntesEventos': f"APROBADO {sufijo_contado}"}
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
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        SetVar("vLocStrResultadoSP", "False")
        raise e
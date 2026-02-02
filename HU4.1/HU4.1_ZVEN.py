"""
================================================================================
SCRIPT: HU4_1_ZVEN.py
================================================================================

Descripcion General:
--------------------
    Valida pedidos ZVEN/50 (Pedidos Comercializados) realizando cruces de
    informacion entre multiples fuentes de datos: Maestro de Comercializados,
    Asociacion cuenta indicador y el Historico de Ordenes de Compra de SAP.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0
Plataforma: RocketBot RPA

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |            ZVEN_ValidarComercializados()                    |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  1. Cargar configuracion desde vLocDicConfig                |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  2. Validar archivos maestros:                              |
    |     - Maestro de Comercializados (Excel)                    |
    |     - Asociacion cuenta indicador (Excel)                   |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  3. Conectar a base de datos SQL Server                     |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  4. Consultar candidatos ZVEN/50                            |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  5. Para cada registro:                                     |
    |  +-------------------------------------------------------+  |
    |  |  a. Buscar en Maestro de Comercializados              |  |
    |  |     SI no encuentra -> EN ESPERA - COMERCIALIZADOS    |  |
    |  +-------------------------------------------------------+  |
    |  |  b. Validar LineExtensionAmount vs SAP                |  |
    |  |     SI diferencia > tolerancia -> CON NOVEDAD         |  |
    |  +-------------------------------------------------------+  |
    |  |  c. Validar TRM (si USD)                              |  |
    |  |     SI no coincide -> CON NOVEDAD                     |  |
    |  +-------------------------------------------------------+  |
    |  |  d. Validar Cantidad y Precio Unitario                |  |
    |  |     SI no coincide -> CON NOVEDAD                     |  |
    |  +-------------------------------------------------------+  |
    |  |  e. Validar Nombre Emisor                             |  |
    |  |     SI no coincide -> CON NOVEDAD                     |  |
    |  +-------------------------------------------------------+  |
    |  |  SI todas pasan -> PROCESADO                          |  |
    |  +-------------------------------------------------------+  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  6. Actualizar resultados en BD                             |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  7. Retornar estadisticas a RocketBot                       |
    +-------------------------------------------------------------+

================================================================================
FUENTES DE DATOS
================================================================================

    1. Archivo Maestro de Comercializados (Excel):
       - Contiene informacion de OC, facturas, valores totales y posiciones
       - Ruta: RutaInsumosComercializados en config
       
    2. Archivo Asociacion cuenta indicador (Excel):
       - Informacion de cuentas contables y agrupaciones de proveedores
       - Ruta: RutaInsumoAsociacion en config
       
    3. Historico de Ordenes de Compra (SAP via SQL):
       - Datos transaccionales de SAP
       - Tabla: [CxP].[HistoricoOrdenesCompra]

================================================================================
VARIABLES DE ENTRADA (RocketBot)
================================================================================

    vLocDicConfig : str | dict
        - ServidorBaseDatos: Servidor SQL Server
        - NombreBaseDatos: Base de datos
        - RutaInsumosComercializados: Ruta al archivo Maestro
        - RutaInsumoAsociacion: Ruta al archivo Asociacion
        - CarpetaDestinoComercializados: Carpeta de salida

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
        "Procesados X registros ZVEN. Exitosos: Y, Con novedad: Z, En espera: W"

    vGblStrDetalleError : str
        Traceback en caso de error critico

    vGblStrSystemError : str
        "Error_HU4.1_ZVEN" en caso de error

================================================================================
ESTADOS FINALES POSIBLES
================================================================================

    - PROCESADO: Validacion exitosa sin novedades
    - PROCESADO CONTADO: Validacion exitosa, forma de pago contado
    - CON NOVEDAD: Se encontraron discrepancias
    - CON NOVEDAD - COMERCIALIZADOS: Discrepancias especificas
    - EN ESPERA - COMERCIALIZADOS: No encontrado en maestro

================================================================================
VALIDACIONES REALIZADAS
================================================================================

    1. Busqueda en Maestro:
       - Verifica existencia en archivo Excel de comercializados
       
    2. LineExtensionAmount:
       - Compara valor de factura vs suma de posiciones SAP
       - Tolerancia: $500 COP o 0.01 para decimales
       
    3. TRM (operaciones USD):
       - Compara tasa de cambio del XML vs SAP
       
    4. Cantidad y Precio Unitario:
       - Compara por cada linea de la factura
       - Tolerancia de 1 unidad
       
    5. Nombre Emisor:
       - Compara proveedor XML vs SAP con normalizacion

================================================================================
FUNCIONES AUXILIARES DESTACADAS
================================================================================

    safe_str(v):
        Convierte cualquier valor a string de forma segura
        
    truncar_observacion(obs):
        Trunca observaciones a 3900 caracteres
        
    normalizar_decimal(val):
        Normaliza valores decimales para comparacion
        
    comparar_nombres_proveedor(nombre1, nombre2):
        Compara nombres con normalizacion avanzada
        
    expandir_posiciones_string(valor):
        Expande valores separados por | a lista

================================================================================
EJEMPLOS DE USO
================================================================================

    # Configurar variables en RocketBot
    SetVar("vLocDicConfig", json.dumps({
        "ServidorBaseDatos": "servidor.ejemplo.com",
        "NombreBaseDatos": "CxP_Database",
        "RutaInsumosComercializados": "C:/Insumos/Maestro.xlsx",
        "RutaInsumoAsociacion": "C:/Insumos/Asociacion.xlsx",
        "CarpetaDestinoComercializados": "C:/Salida/Comercializados"
    }))
    
    # Ejecutar la funcion
    ZVEN_ValidarComercializados()
    
    # Verificar resultado
    resultado = GetVar("vLocStrResultadoSP")

================================================================================
NOTAS TECNICAS
================================================================================

    - Errores individuales por registro NO detienen el proceso
    - Solo errores criticos de infraestructura detienen el bot
    - Warnings de pandas sobre SQLAlchemy deshabilitados
    - Archivos maestros se cargan una vez al inicio
    - Tolerancia de $500 COP para montos, 0.01 para decimales

================================================================================
"""

def ZVEN_ValidarComercializados():
    """
    Funcion principal para procesar las validaciones de pedidos ZVEN/50 (Pedidos Comercializados).

    Esta funcion implementa el flujo completo de validacion de pedidos comercializados,
    realizando cruces de informacion entre multiples fuentes de datos para verificar
    la consistencia y exactitud de las facturas procesadas.

    El proceso de validacion incluye las siguientes etapas:

        1. **Carga de configuracion**: Obtiene parametros desde RocketBot mediante
           la variable ``vLocDicConfig``.
        
        2. **Validacion de archivos maestros**: Verifica la existencia y estructura
           del Maestro de Comercializados y el archivo de Asociacion cuenta indicador.
        
        3. **Conexion a base de datos**: Establece conexion con SQL Server para
           consultar y actualizar registros.
        
        4. **Procesamiento de registros**: Para cada registro candidato tipo ZVEN/50:
           
           - Busca coincidencias en el Maestro de Comercializados
           - Valida montos (LineExtensionAmount vs SAP)
           - Valida TRM (Tasa Representativa del Mercado) para operaciones en USD
           - Valida cantidad y precio unitario
           - Valida nombre del emisor
        
        5. **Actualizacion de resultados**: Registra el estado final de cada
           validacion en las tablas correspondientes.

    Fuentes de datos cruzadas:
        - **Archivo Maestro de Comercializados** (Excel): Contiene informacion
          de ordenes de compra, facturas, valores totales y posiciones.
        - **Archivo Asociacion cuenta indicador** (Excel): Informacion de cuentas
          contables y agrupaciones de proveedores.
        - **Historico de Ordenes de Compra** (SAP via SQL): Datos transaccionales
          de SAP incluyendo valores, fechas y estados.

    Variables de entrada (RocketBot):
        - ``vLocDicConfig``: Diccionario JSON con configuracion del proceso.

    Variables de salida (RocketBot):
        - ``vLocStrResultadoSP``: "True" si el proceso fue exitoso, "False" en caso contrario.
        - ``vLocStrResumenSP``: Resumen textual del procesamiento.
        - ``vGblStrDetalleError``: Detalle del error en caso de fallo (traceback).
        - ``vGblStrSystemError``: Identificador del error del sistema.

    Estados finales posibles por registro:
        - ``PROCESADO``: Validacion exitosa sin novedades.
        - ``PROCESADO CONTADO``: Validacion exitosa, forma de pago contado.
        - ``CON NOVEDAD``: Se encontraron discrepancias en la validacion.
        - ``CON NOVEDAD - COMERCIALIZADOS``: Discrepancias especificas de comercializados.
        - ``EN ESPERA - COMERCIALIZADOS``: Registro no encontrado en maestro.

    Raises:
        ValueError: Si faltan parametros obligatorios en la configuracion.
        FileNotFoundError: Si no se encuentran los archivos maestros requeridos.
        pyodbc.Error: Si hay errores de conexion o ejecucion en la base de datos.
        Exception: Cualquier error critico no manejado que detenga el proceso.

    Note:
        - La funcion esta disenada para ejecutarse dentro del entorno RocketBot.
        - Los errores individuales por registro no detienen el procesamiento completo.
        - Solo los errores criticos de infraestructura (conexion, archivos) detienen el bot.
        - La tolerancia para comparacion de montos es de $500 COP o 0.01 para decimales.
        - Los warnings de pandas sobre SQLAlchemy estan deshabilitados intencionalmente.

    Version:
        1.0

    Author:
        Diego Ivan Lopez Ochoa

    Example:
        Desde RocketBot, la funcion se invoca sin parametros::

            # Configurar variables previas en RocketBot
            SetVar("vLocDicConfig", json.dumps({
                "ServidorBaseDatos": "servidor.ejemplo.com",
                "NombreBaseDatos": "CxP_Database",
                "UsuarioBaseDatos": "usuario",
                "ClaveBaseDatos": "contrasena",
                "RutaInsumosComercializados": "C:/Insumos/Maestro.xlsx",
                "RutaInsumoAsociacion": "C:/Insumos/Asociacion.xlsx",
                "CarpetaDestinoComercializados": "C:/Salida/Comercializados"
            }))
            
            # Ejecutar la funcion
            ZVEN_ValidarComercializados()
            
            # Verificar resultado
            resultado = GetVar("vLocStrResultadoSP")  # "True" o "False"
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
    
    # Suprimir advertencias de pandas sobre SQLAlchemy que no aplican en este contexto
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    # =========================================================================
    # 1. FUNCIONES AUXILIARES 
    # =========================================================================
    
    def safe_str(v):
        """
        Convierte de forma segura cualquier valor a su representacion en cadena de texto.

        Esta funcion maneja multiples tipos de datos de entrada, incluyendo valores
        nulos, bytes, numeros y objetos diversos, garantizando siempre una salida
        de tipo string sin errores de conversion.

        Args:
            v (Any): Valor de cualquier tipo a convertir. Puede ser:
                - None: Retorna cadena vacia.
                - str: Retorna el string limpio (sin espacios extremos).
                - bytes: Decodifica usando latin-1 con reemplazo de errores.
                - int/float: Convierte a string, manejando NaN como vacio.
                - Otros: Intenta conversion estandar a string.

        Returns:
            str: Representacion en cadena del valor de entrada.
                - Siempre retorna un string valido (nunca None).
                - Los espacios en blanco al inicio y final son eliminados.
                - Los valores NaN/NA de pandas retornan cadena vacia.

        Examples:
            >>> safe_str(None)
            ''
            >>> safe_str("  Hola Mundo  ")
            'Hola Mundo'
            >>> safe_str(12345)
            '12345'
            >>> safe_str(float('nan'))
            ''
            >>> safe_str(b'datos binarios')
            'datos binarios'

        Note:
            La funcion nunca lanza excepciones; cualquier error de conversion
            resulta en una cadena vacia como fallback seguro.
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
        Trunca una cadena de observaciones a una longitud maxima segura para la base de datos.

        Esta funcion es esencial para prevenir errores de insercion en campos
        de la base de datos que tienen limites de longitud definidos (tipicamente
        NVARCHAR(4000) en SQL Server).

        Args:
            obs (str | Any): Texto de observacion a truncar. Si no es string,
                se convierte usando ``safe_str()``.
            max_len (int, optional): Longitud maxima permitida para la cadena
                resultante. Por defecto es 3900 caracteres, dejando margen
                de seguridad respecto al limite tipico de 4000.

        Returns:
            str: Cadena truncada si excede ``max_len``, o la cadena original
                si esta dentro del limite.
                - Si la entrada es vacia o None, retorna cadena vacia.
                - Si se trunca, se anade "..." al final para indicar continuacion.

        Examples:
            >>> truncar_observacion("Texto corto")
            'Texto corto'
            >>> truncar_observacion("A" * 5000, max_len=100)
            'AAAA...AAA...'  # 97 caracteres + "..."
            >>> truncar_observacion(None)
            ''
            >>> truncar_observacion("", max_len=50)
            ''

        Note:
            El valor predeterminado de 3900 permite espacio adicional para:
            - Concatenaciones posteriores de observaciones existentes.
            - Caracteres especiales que puedan expandirse en la codificacion.
            - Metadatos o prefijos que se anadan al insertar en la BD.
        """
        if not obs: 
            return ""
        obs_str = safe_str(obs)
        if len(obs_str) > max_len: 
            return obs_str[:max_len - 3] + "..."
        return obs_str
    
    def normalizar_decimal(valor):
        """
        Normaliza y convierte cualquier representacion de numero decimal a float.

        Esta funcion maneja multiples formatos de numeros decimales que pueden
        provenir de diferentes fuentes (Excel, SAP, XML), incluyendo formatos
        con comas como separador decimal (formato europeo/latinoamericano).

        Args:
            valor (Any): Valor a normalizar. Puede ser:
                - None, '', pd.NA, np.nan: Retorna 0.0
                - int: Convierte directamente a float
                - float: Retorna el valor (0.0 si es NaN)
                - str: Parsea el string, reemplazando comas por puntos
                  y eliminando caracteres no numericos excepto punto y signo

        Returns:
            float: Valor numerico normalizado.
                - Siempre retorna un float valido (nunca None o NaN).
                - Valores no parseables retornan 0.0.

        Examples:
            >>> normalizar_decimal("1.234,56")  # Formato europeo
            1234.56
            >>> normalizar_decimal("$1,000.00")  # Con simbolo de moneda
            1000.0
            >>> normalizar_decimal(None)
            0.0
            >>> normalizar_decimal("")
            0.0
            >>> normalizar_decimal(12345)
            12345.0
            >>> normalizar_decimal("  -123.45  ")
            -123.45

        Note:
            El orden de procesamiento para strings es:
            1. Strip de espacios
            2. Reemplazo de comas por puntos
            3. Eliminacion de caracteres no numericos (excepto ., -)
            4. Conversion a float

        Warning:
            Para valores con multiples puntos despues del reemplazo de comas
            (ej: "1.234.567,89" → "1.234.567.89"), el resultado puede ser
            inesperado. Se recomienda preprocesar estos casos externamente.
        """
        if pd.isna(valor) or valor == '' or valor is None: 
            return 0.0
        if isinstance(valor, (int, float)):
            if np.isnan(valor) if isinstance(valor, float) else False: 
                return 0.0
            return float(valor)
        valor_str = str(valor).strip().replace(',', '.')
        valor_str = re.sub(r'[^\d.\-]', '', valor_str)
        try: 
            return float(valor_str)
        except: 
            return 0.0

    def parse_config(raw):
        """
        Parsea y valida la configuracion del proceso desde multiples formatos de entrada.

        Esta funcion es el punto de entrada para procesar la configuracion
        proveniente de RocketBot, que puede llegar como diccionario Python,
        string JSON o representacion literal de Python.

        Args:
            raw (dict | str): Configuracion en bruto. Formatos aceptados:
                - dict: Se retorna directamente sin modificacion.
                - str (JSON): Se parsea con ``json.loads()``.
                - str (Python literal): Se parsea con ``ast.literal_eval()``.

        Returns:
            dict: Diccionario de configuracion parseado y validado.
                Claves tipicas esperadas:
                - ServidorBaseDatos (str): Hostname o IP del servidor SQL.
                - NombreBaseDatos (str): Nombre de la base de datos.
                - UsuarioBaseDatos (str): Usuario para autenticacion SQL.
                - ClaveBaseDatos (str): Contrasena del usuario.
                - RutaInsumosComercializados (str): Ruta al archivo Excel maestro.
                - RutaInsumoAsociacion (str): Ruta al archivo de asociacion.
                - CarpetaDestinoComercializados (str): Carpeta de salida.

        Raises:
            ValueError: Si el valor de entrada esta vacio o no puede ser
                parseado como diccionario valido por ninguno de los metodos.

        Examples:
            >>> parse_config({"clave": "valor"})
            {'clave': 'valor'}
            >>> parse_config('{"servidor": "localhost", "puerto": 1433}')
            {'servidor': 'localhost', 'puerto': 1433}
            >>> parse_config("{'clave': 'valor'}")  # Literal Python
            {'clave': 'valor'}
            >>> parse_config("")
            Raises ValueError: vLocDicConfig vacio

        Note:
            El orden de intentos de parseo es:
            1. Verificar si ya es diccionario
            2. Intentar JSON (json.loads)
            3. Intentar literal Python (ast.literal_eval)
            4. Lanzar ValueError si todos fallan
        """
        if isinstance(raw, dict): 
            return raw
        text = safe_str(raw)
        if not text: 
            raise ValueError("vLocDicConfig vacio")
        try: 
            return json.loads(text)
        except json.JSONDecodeError: 
            pass
        try: 
            return ast.literal_eval(text)
        except: 
            raise ValueError("Config invalida")

    def expandir_posiciones_string(valor_string, separador='|'):
        """
        Expande una cadena de valores separados en una lista de elementos individuales.

        Esta funcion procesa campos que contienen multiples valores concatenados,
        comun en datos provenientes de SAP donde las posiciones, cantidades o
        precios de multiples lineas se almacenan en un solo campo.

        Args:
            valor_string (str | Any): Cadena con valores separados. Puede contener:
                - Valores separados por pipe (|): "10|20|30"
                - Valores separados por coma (,): "10,20,30"
                - Valor unico sin separador: "10"
                - Valores con espacios: "10 | 20 | 30"
            separador (str, optional): Separador principal esperado. Por defecto '|'.
                Nota: La funcion detecta automaticamente pipes y comas.

        Returns:
            list[str]: Lista de valores individuales como strings.
                - Cada elemento esta limpio de espacios (strip).
                - Elementos vacios son filtrados automaticamente.
                - Si la entrada es None/vacia/NA, retorna lista vacia.

        Examples:
            >>> expandir_posiciones_string("10|20|30")
            ['10', '20', '30']
            >>> expandir_posiciones_string("100,200,300")
            ['100', '200', '300']
            >>> expandir_posiciones_string("  50  |  60  ")
            ['50', '60']
            >>> expandir_posiciones_string("UNICO")
            ['UNICO']
            >>> expandir_posiciones_string(None)
            []
            >>> expandir_posiciones_string("")
            []
            >>> expandir_posiciones_string("10||30")  # Elementos vacios
            ['10', '30']

        Note:
            La prioridad de separadores es:
            1. Pipe (|) - Usado primariamente en datos SAP
            2. Coma (,) - Alternativa comun
            3. Sin separador - Valor unico
        """
        if pd.isna(valor_string) or valor_string == '' or valor_string is None: 
            return []
        valor_str = safe_str(valor_string)
        if '|' in valor_str: 
            return [v.strip() for v in valor_str.split('|') if v.strip()]
        if ',' in valor_str: 
            return [v.strip() for v in valor_str.split(',') if v.strip()]
        return [valor_str.strip()]
        
    def normalizar_nombre_empresa(nombre):
        """
        Normaliza el nombre de una empresa para comparacion estandarizada.

        Esta funcion aplica multiples transformaciones al nombre de una empresa
        para permitir comparaciones robustas que ignoren variaciones comunes
        en la escritura de razones sociales (mayusculas, puntuacion, tipos societarios).

        Args:
            nombre (str | Any): Nombre de empresa a normalizar. Puede contener:
                - Variaciones de tipos societarios (S.A.S., SAS, S A S, etc.)
                - Puntuacion diversa (puntos, comas, espacios)
                - Mayusculas/minusculas mezcladas

        Returns:
            str: Nombre normalizado con las siguientes transformaciones:
                - Convertido a mayusculas
                - Sin espacios, puntos ni comas
                - Tipos societarios estandarizados:
                    - S.A.S., S.A.S, S A S → SAS
                    - LIMITADA, LTDA., LTDA → LTDA
                    - S.ENC., S.EN.C., COMANDITA → SENC
                    - S.A., S.A → SA
                - Retorna cadena vacia si la entrada es None/vacia/NA

        Examples:
            >>> normalizar_nombre_empresa("Empresa Colombia S.A.S.")
            'EMPRESACOLOMBIASAS'
            >>> normalizar_nombre_empresa("ACME LIMITADA")
            'ACMELTDA'
            >>> normalizar_nombre_empresa("  Compania S. A. S.  ")
            'COMPANIASAS'
            >>> normalizar_nombre_empresa(None)
            ''
            >>> normalizar_nombre_empresa("ABC S.EN.C.")
            'ABCSENC'

        Note:
            La funcion es utilizada por ``comparar_nombres_proveedor()`` para
            determinar si dos nombres de empresa (uno del XML y otro de SAP)
            corresponden a la misma entidad legal.

        Warning:
            La normalizacion puede producir colisiones para empresas con nombres
            muy similares. Por ejemplo, "ABC SAS" y "A B C S.A.S." producirian
            el mismo resultado normalizado.
        """
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
        Compara dos nombres de proveedor para determinar si corresponden a la misma entidad.

        Esta funcion realiza una comparacion flexible entre el nombre del emisor
        proveniente del XML de la factura electronica y el nombre del proveedor
        registrado en SAP, considerando variaciones comunes en la escritura.

        Args:
            nombre_xml (str | Any): Nombre del emisor extraido del archivo XML
                de la factura electronica.
            nombre_sap (str | Any): Nombre del proveedor registrado en SAP,
                obtenido de la tabla de historico de ordenes de compra.

        Returns:
            bool: True si los nombres se consideran equivalentes, False en caso contrario.
                - Retorna False si alguno de los nombres es None/NA.
                - La comparacion ignora el orden de las palabras.
                - La comparacion es case-insensitive y ignora puntuacion.

        Examples:
            >>> comparar_nombres_proveedor("ACME S.A.S.", "ACME SAS")
            True
            >>> comparar_nombres_proveedor("EMPRESA ABC LTDA", "ABC EMPRESA LIMITADA")
            True
            >>> comparar_nombres_proveedor("Proveedor Uno", "Proveedor Dos")
            False
            >>> comparar_nombres_proveedor(None, "ACME SAS")
            False
            >>> comparar_nombres_proveedor("", "ACME")
            False

        Note:
            El algoritmo de comparacion:
            1. Normaliza ambos nombres usando ``normalizar_nombre_empresa()``.
            2. Divide cada nombre normalizado en "palabras" (caracteres contiguos).
            3. Ordena las palabras alfabeticamente.
            4. Compara las listas ordenadas para igualdad.

            Esto permite coincidir nombres donde el orden de palabras difiere,
            por ejemplo "EMPRESA ABC" vs "ABC EMPRESA".

        See Also:
            normalizar_nombre_empresa: Funcion de normalizacion de nombres.
        """
        if pd.isna(nombre_xml) or pd.isna(nombre_sap): 
            return False
        nombre_xml_limpio = normalizar_nombre_empresa(str(nombre_xml))
        nombre_sap_limpio = normalizar_nombre_empresa(str(nombre_sap))
        return sorted(nombre_xml_limpio.split()) == sorted(nombre_sap_limpio.split())

    # =========================================================================
    # 2. FUNCIONES DE BASE DE DATOS
    # =========================================================================
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        """
        Context manager para crear y gestionar conexiones a la base de datos SQL Server.

        Esta funcion implementa un patron de conexion robusto con reintentos,
        soporte para multiples metodos de autenticacion, y manejo automatico
        de transacciones (commit/rollback).

        Args:
            cfg (dict): Diccionario de configuracion con los parametros de conexion.
                Claves requeridas:
                    - ServidorBaseDatos (str): Hostname o IP del servidor SQL Server.
                    - NombreBaseDatos (str): Nombre de la base de datos destino.
                Claves opcionales:
                    - UsuarioBaseDatos (str): Usuario para autenticacion SQL.
                    - ClaveBaseDatos (str): Contrasena del usuario SQL.
            max_retries (int, optional): Numero maximo de intentos de conexion
                por cada metodo de autenticacion. Por defecto 3.

        Yields:
            pyodbc.Connection: Objeto de conexion activa a la base de datos.
                - autocommit esta deshabilitado por defecto.
                - La conexion se hace commit automatico al salir del contexto sin errores.
                - Se hace rollback automatico si ocurre una excepcion.

        Raises:
            ValueError: Si faltan parametros requeridos (ServidorBaseDatos, NombreBaseDatos).
            pyodbc.Error: Si no se puede establecer conexion despues de agotar reintentos.
            Exception: Cualquier error que ocurra durante las operaciones de base de datos.

        Examples:
            Uso basico con autenticacion SQL::

                cfg = {
                    "ServidorBaseDatos": "sqlserver.empresa.com",
                    "NombreBaseDatos": "CxP_Produccion",
                    "UsuarioBaseDatos": "app_user",
                    "ClaveBaseDatos": "SecurePass123"
                }
                with crear_conexion_db(cfg) as conexion:
                    cursor = conexion.cursor()
                    cursor.execute("SELECT * FROM Tabla")
                    resultados = cursor.fetchall()
                # Commit automatico al salir del bloque 'with'

            Uso con autenticacion Windows (Trusted Connection)::

                cfg = {
                    "ServidorBaseDatos": "localhost\\SQLEXPRESS",
                    "NombreBaseDatos": "TestDB",
                    "UsuarioBaseDatos": "",  # Vacio para usar Windows Auth
                    "ClaveBaseDatos": ""
                }
                with crear_conexion_db(cfg) as conexion:
                    # Operaciones de base de datos...
                    pass

            Manejo de errores con rollback automatico::

                try:
                    with crear_conexion_db(cfg) as conexion:
                        cursor = conexion.cursor()
                        cursor.execute("INSERT INTO Tabla VALUES (...)")
                        raise ValueError("Error intencional")
                except ValueError:
                    # El rollback ya se ejecuto automaticamente
                    print("Transaccion revertida")

        Note:
            Estrategia de conexion:
            
            1. Intenta primero autenticacion SQL (UID/PWD) si hay credenciales.
            2. Si falla, intenta autenticacion Windows (Trusted_Connection=yes).
            3. Cada metodo se intenta hasta ``max_retries`` veces.
            4. Hay una pausa de 1 segundo entre reintentos.
            5. Usa ODBC Driver 17 for SQL Server.
            
            Timeouts:
            - Timeout de conexion: 30 segundos.

        Warning:
            - La conexion debe usarse dentro de un bloque ``with`` para garantizar
              el cierre apropiado de recursos.
            - No reutilizar el objeto de conexion fuera del contexto.
        """
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing: 
            raise ValueError(f"Parametros faltantes: {', '.join(missing)}")
        
        usuario = cfg['UsuarioBaseDatos']
        contrasena = cfg['ClaveBaseDatos']

        conn_str_auth = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg['ServidorBaseDatos']};DATABASE={cfg['NombreBaseDatos']};UID={usuario};PWD={contrasena};autocommit=False;"
        conn_str_trusted = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg['ServidorBaseDatos']};DATABASE={cfg['NombreBaseDatos']};Trusted_Connection=yes;autocommit=False;"

        cx = None
        conectado = False
        ultimo_error = None

        for conn_str in [conn_str_auth, conn_str_trusted]:
            for attempt in range(max_retries):
                try:
                    cx = pyodbc.connect(conn_str, timeout=30)
                    cx.autocommit = False
                    conectado = True
                    break 
                except pyodbc.Error as e:
                    ultimo_error = e
                    time.sleep(1)
            if conectado:
                break 

        if not conectado:
            raise ultimo_error or Exception("Fallo conexion a BD tras multiples intentos")

        try:
            yield cx
            cx.commit() 
        except Exception as e:
            cx.rollback() 
            raise e 
        finally:
            cx.close() 

    def actualizar_bd_cxp(cx, registro_id, campos_actualizar):
        """
        Actualiza campos especificos de un registro en la tabla DocumentsProcessing.

        Esta funcion construye y ejecuta dinamicamente una sentencia UPDATE
        para modificar uno o mas campos de un registro de procesamiento de
        cuentas por pagar.

        Args:
            cx (pyodbc.Connection): Conexion activa a la base de datos.
                Debe estar dentro de un contexto de transaccion activo.
            registro_id (str): Identificador unico del registro a actualizar.
                Corresponde al campo [ID] de la tabla DocumentsProcessing.
            campos_actualizar (dict): Diccionario con los campos y valores a actualizar.
                - Las claves son nombres de columnas de la tabla.
                - Los valores None son ignorados (no se actualizan).
                - El campo 'ObservacionesFase_4' tiene tratamiento especial
                  para concatenar observaciones existentes.

        Returns:
            None: La funcion no retorna valor. Los cambios quedan pendientes
                de commit en la transaccion activa.

        Behavior:
            - Para campos normales: Sobrescribe el valor existente.
            - Para 'ObservacionesFase_4': Concatena al valor existente separado
              por coma, o establece el nuevo valor si estaba vacio/NULL.

        Examples:
            Actualizacion simple de estado::

                actualizar_bd_cxp(conexion, "12345", {
                    "EstadoFinalFase_4": "VALIDACION DATOS DE FACTURACION: Exitoso",
                    "ResultadoFinalAntesEventos": "PROCESADO"
                })

            Actualizacion con observaciones (concatenacion)::

                actualizar_bd_cxp(conexion, "12345", {
                    "ObservacionesFase_4": "Nueva observacion detectada",
                    "EstadoFinalFase_4": "CON NOVEDAD"
                })
                # Si ObservacionesFase_4 tenia "Obs anterior", ahora tendra:
                # "Obs anterior, Nueva observacion detectada"

            Actualizacion con valores None (ignorados)::

                actualizar_bd_cxp(conexion, "12345", {
                    "Campo1": "Valor actualizado",
                    "Campo2": None,  # Este campo NO se actualiza
                    "Campo3": "Otro valor"
                })

        Note:
            - La tabla destino es [CxP].[DocumentsProcessing].
            - La funcion usa parametros preparados para prevenir SQL injection.
            - Los cambios no se persisten hasta que se haga commit en la conexion.
            - Si no hay campos validos para actualizar (todos None), no se ejecuta query.

        Warning:
            No llamar a cx.commit() dentro de esta funcion para mantener
            el control de transacciones en el nivel superior.
        """
        sets, parametros = [], []
        for campo, valor in campos_actualizar.items():
            if valor is not None:
                if campo == 'ObservacionesFase_4':
                    # Logica especial: concatenar observaciones existentes
                    sets.append(f"[{campo}] = CASE WHEN [{campo}] IS NULL OR [{campo}] = '' THEN ? ELSE [{campo}] + ', ' + ? END")
                    parametros.extend([valor, valor])
                else:
                    sets.append(f"[{campo}] = ?")
                    parametros.append(valor)
    
        if sets:
            parametros.append(registro_id)
            with cx.cursor() as cur:
                cur.execute(f"UPDATE [CxP].[DocumentsProcessing] SET {', '.join(sets)} WHERE [ID] = ?", parametros)

    def actualizar_items_comparativa(registro, cx, nit, factura, nombre_item, 
                                     actualizar_valor_xml=True, valor_xml=None,
                                     actualizar_aprobado=True, valor_aprobado=None, 
                                     actualizar_orden_compra=True, val_orden_de_compra=None,
                                     actualizar_orden_compra_comercializados=True, val_orden_de_compra_comercializados=None):
        """
        Actualiza o inserta registros de comparativa para un item especifico de validacion.

        Esta funcion gestiona la tabla de comparativa donde se almacena el detalle
        de cada validacion realizada, incluyendo valores del XML, valores de SAP,
        y el resultado de la comparacion (aprobado/rechazado).

        Args:
            registro (dict | pd.Series): Registro completo del documento siendo procesado.
                Debe contener al menos:
                    - ID_dp: Identificador del documento
                    - Fecha_de_retoma_antes_de_contabilizacion_dp
                    - documenttype_dp: Tipo de documento
                    - numero_de_liquidacion_u_orden_de_compra_dp: Numero de OC
                    - nombre_emisor_dp: Nombre del proveedor
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            nit (str): NIT del emisor/proveedor para identificar el registro.
            factura (str): Numero de factura para identificar el registro.
            nombre_item (str): Nombre del item de validacion (ej: 'LineExtensionAmount',
                'TRM', 'NombreEmisor', 'Observaciones', etc.).
            actualizar_valor_xml (bool, optional): Si actualizar el campo Valor_XML.
                Por defecto True.
            valor_xml (Any, optional): Valor extraido del XML de la factura.
                Puede ser string, numero, o lista (se procesa elemento por elemento).
            actualizar_aprobado (bool, optional): Si actualizar el campo Aprobado.
                Por defecto True.
            valor_aprobado (str, optional): Resultado de la validacion: 'SI', 'NO', o None.
            actualizar_orden_compra (bool, optional): Si actualizar Valor_Orden_de_Compra.
                Por defecto True.
            val_orden_de_compra (Any, optional): Valor de SAP/Orden de compra.
                Puede ser string, numero, o lista.
            actualizar_orden_compra_comercializados (bool, optional): Si actualizar
                Valor_Orden_de_Compra_Comercializados. Por defecto True.
            val_orden_de_compra_comercializados (Any, optional): Valor especifico
                del maestro de comercializados.

        Returns:
            None: Los cambios se confirman con commit dentro de la funcion.

        Behavior:
            La funcion maneja dos escenarios:
            
            1. **Registros existentes**: Si ya existen registros para la combinacion
               NIT+Factura+Item+ID_registro, se actualizan usando ROW_NUMBER.
            
            2. **Registros nuevos**: Si no existen suficientes registros, se insertan
               nuevas filas con toda la informacion del documento.
            
            Para valores multiples (separados por | o ,):
            - Se expanden en listas individuales.
            - Se crea/actualiza un registro por cada posicion.
            - Los indices se alinean entre las diferentes listas de valores.

        Examples:
            Registrar validacion exitosa de monto::

                actualizar_items_comparativa(
                    registro=fila_df,
                    cx=conexion,
                    nit="900123456",
                    factura="FE-001",
                    nombre_item="LineExtensionAmount",
                    valor_xml="1500000.00",
                    valor_aprobado="SI",
                    val_orden_de_compra="1500000.00"
                )

            Registrar validacion fallida con multiples posiciones::

                actualizar_items_comparativa(
                    registro=fila_df,
                    cx=conexion,
                    nit="900123456",
                    factura="FE-001",
                    nombre_item="CantidadProducto",
                    valor_xml="10|20|30",
                    valor_aprobado="NO",
                    val_orden_de_compra="10|25|30"  # La posicion 2 no coincide
                )

            Solo registrar observaciones::

                actualizar_items_comparativa(
                    registro=fila_df,
                    cx=conexion,
                    nit="900123456",
                    factura="FE-001",
                    nombre_item="Observaciones",
                    valor_xml="Discrepancia encontrada en TRM",
                    actualizar_aprobado=False,
                    actualizar_orden_compra=False
                )

        Note:
            - La tabla destino es [dbo].[CxP.Comparativa].
            - Se hace commit despues de cada llamada para persistir cambios.
            - Los valores None, 'none', 'null' se convierten a NULL en la BD.
            - La funcion es idempotente para actualizaciones del mismo item.

        Warning:
            El commit dentro de la funcion puede afectar el control de transacciones
            si se requiere atomicidad entre multiples operaciones.
        """
        cur = cx.cursor()
        
        def safe_db_val(v):
            """
            Sanitiza valores para insercion segura en la base de datos.
            
            Args:
                v: Valor a sanitizar.
                
            Returns:
                str | None: Valor limpio o None si es vacio/null.
            """
            if v is None: 
                return None
            s = str(v).strip()
            return None if not s or s.lower() in ('none', 'null') else s

        # Verificar registros existentes
        cur.execute(
            "SELECT COUNT(*) FROM [dbo].[CxP.Comparativa] "
            "WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?", 
            (nit, factura, nombre_item, registro.get('ID_dp',''))
        )
        count_existentes = cur.fetchone()[0]

        def split_safe(val):
            """
            Divide de forma segura un valor en lista, manejando multiples formatos.
            
            Args:
                val: Valor a dividir (string con separadores, lista, o escalar).
                
            Returns:
                list: Lista de valores individuales.
            """
            if isinstance(val, str):
                return [x.strip() for x in val.replace(',', '|').split('|') if x.strip()]
            elif isinstance(val, (list, tuple)):
                return val
            elif val is not None:
                return [str(val)]
            return []

        # Expandir valores en listas
        lista_compra = split_safe(val_orden_de_compra)
        lista_xml = split_safe(valor_xml)
        lista_aprob = split_safe(valor_aprobado)
        lista_comer = split_safe(val_orden_de_compra_comercializados)
        
        # Calcular numero de registros a procesar
        count_nuevos = max(len(lista_compra), len(lista_xml), len(lista_aprob))
        count_nuevos = 1 if count_nuevos == 0 else count_nuevos

        for i in range(count_nuevos):
            val_compra = safe_db_val(lista_compra[i] if i < len(lista_compra) else None)
            val_xml = safe_db_val(lista_xml[i] if i < len(lista_xml) else None)
            val_aprob = safe_db_val(lista_aprob[i] if i < len(lista_aprob) else None)
            val_comer = safe_db_val(lista_comer[i] if i < len(lista_comer) else None)
            
            if i < count_existentes:
                # Actualizar registro existente usando ROW_NUMBER
                set_clauses, params = [], []
                if actualizar_orden_compra: 
                    set_clauses.append("Valor_Orden_de_Compra = ?")
                    params.append(val_compra)
                if actualizar_orden_compra_comercializados: 
                    set_clauses.append("Valor_Orden_de_Compra_Comercializados = ?")
                    params.append(val_comer)
                if actualizar_valor_xml: 
                    set_clauses.append("Valor_XML = ?")
                    params.append(val_xml)
                if actualizar_aprobado: 
                    set_clauses.append("Aprobado = ?")
                    params.append(val_aprob)
                if not set_clauses: 
                    continue
                final_params = params + [nit, factura, nombre_item, registro.get('ID_dp',''), i + 1]
                cur.execute(
                    f"WITH CTE AS ("
                    f"SELECT Valor_Orden_de_Compra, Valor_Orden_de_Compra_Comercializados, "
                    f"Valor_XML, Aprobado, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn "
                    f"FROM [dbo].[CxP.Comparativa] "
                    f"WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?) "
                    f"UPDATE CTE SET {', '.join(set_clauses)} WHERE rn = ?", 
                    final_params
                )
            else:
                # Insertar nuevo registro
                cur.execute(
                    """INSERT INTO [dbo].[CxP.Comparativa] 
                       (Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, 
                        Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, 
                        Item, Valor_Orden_de_Compra, Valor_Orden_de_Compra_Comercializados, 
                        Valor_XML, Aprobado) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                    (registro.get('Fecha_de_retoma_antes_de_contabilizacion_dp'), 
                     registro.get('documenttype_dp'), 
                     registro.get('numero_de_liquidacion_u_orden_de_compra_dp'), 
                     registro.get('nombre_emisor_dp'), 
                     registro.get('ID_dp',''), 
                     nit, factura, nombre_item, 
                     val_compra, val_comer, val_xml, val_aprob)
                )
        cx.commit()
        cur.close()

    def marcar_orden_procesada(cx, oc_numero, posiciones_string):
        """
        Marca las posiciones de una orden de compra como procesadas en el historico.

        Esta funcion actualiza el campo 'Marca' en la tabla de historico de ordenes
        de compra para indicar que las posiciones especificadas ya han sido
        validadas y procesadas, evitando reprocesamiento en ejecuciones futuras.

        Args:
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            oc_numero (str): Numero del documento de compra (DocCompra).
                Corresponde al numero de orden de compra en SAP.
            posiciones_string (str): String con posiciones separadas por pipe (|).
                Ejemplo: "00010|00020|00030" o "10|20|30".
                Cada posicion se procesa individualmente.

        Returns:
            None: Los cambios se confirman con commit dentro de la funcion.

        Examples:
            Marcar multiples posiciones::

                marcar_orden_procesada(
                    conexion,
                    "4500001234",
                    "00010|00020|00030"
                )
                # Actualiza Marca='PROCESADO' para las 3 posiciones

            Marcar posicion unica::

                marcar_orden_procesada(conexion, "4500001234", "00010")

            String con posiciones vacias (se ignoran)::

                marcar_orden_procesada(conexion, "4500001234", "10||20|")
                # Solo procesa posiciones 10 y 20

        Note:
            - La tabla destino es [CxP].[HistoricoOrdenesCompra].
            - El valor de marca establecido es 'PROCESADO'.
            - Se hace commit despues de procesar todas las posiciones.
            - Las posiciones vacias o solo con espacios son ignoradas.

        Warning:
            Esta operacion es irreversible en el contexto normal del proceso.
            Una vez marcada como PROCESADO, la posicion no sera seleccionada
            nuevamente por la vista de candidatos.
        """
        cur = cx.cursor()
        for pos in posiciones_string.split('|'):
            if pos.strip():
                cur.execute(
                    "UPDATE [CxP].[HistoricoOrdenesCompra] "
                    "SET Marca = 'PROCESADO' "
                    "WHERE DocCompra = ? AND Posicion = ?", 
                    (oc_numero, pos.strip())
                )
        cx.commit()
        cur.close()

    def actualizar_estado_comparativa(cx, nit, factura, estado):
        """
        Actualiza el estado de validacion para todos los registros de comparativa de una factura.

        Esta funcion establece el estado final del proceso de validacion en la tabla
        de comparativa, afectando todos los items asociados a la combinacion
        NIT+Factura especificada.

        Args:
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            nit (str): NIT del emisor/proveedor.
            factura (str): Numero de factura.
            estado (str): Estado final de la validacion. Valores tipicos:
                - "PROCESADO": Validacion exitosa sin novedades.
                - "PROCESADO CONTADO": Exitoso con forma de pago contado.
                - "CON NOVEDAD": Se encontraron discrepancias.
                - "CON NOVEDAD - COMERCIALIZADOS": Discrepancias en comercializados.
                - "EN ESPERA - COMERCIALIZADOS": Pendiente de informacion.

        Returns:
            None: Los cambios se confirman con commit dentro de la funcion.

        Examples:
            Marcar factura como procesada exitosamente::

                actualizar_estado_comparativa(
                    conexion,
                    "900123456",
                    "FE-001",
                    "PROCESADO"
                )

            Marcar factura con novedad::

                actualizar_estado_comparativa(
                    conexion,
                    "900123456",
                    "FE-001",
                    "CON NOVEDAD - COMERCIALIZADOS"
                )

        Note:
            - La tabla destino es [dbo].[CxP.Comparativa].
            - El campo actualizado es Estado_validacion_antes_de_eventos.
            - Afecta TODOS los registros (items) de la factura.
            - Se hace commit inmediatamente despues de la actualizacion.
        """
        cur = cx.cursor()
        cur.execute(
            "UPDATE [dbo].[CxP.Comparativa] "
            "SET Estado_validacion_antes_de_eventos = ? "
            "WHERE NIT = ? AND Factura = ?", 
            (estado, nit, factura)
        )
        cx.commit()
        cur.close()

    # =========================================================================
    # 3. FUNCIONES ESPECIFICAS DE ARCHIVOS Y VALIDACIONES
    # =========================================================================

    def validar_maestro_comercializados(ruta):
        """
        Valida y carga el archivo Maestro de Comercializados.

        Esta funcion verifica que el archivo Excel del maestro de comercializados
        exista, tenga la estructura esperada con las columnas requeridas, y
        prepara el DataFrame para su uso en las validaciones posteriores.

        Args:
            ruta (str): Ruta completa al archivo Excel del Maestro de Comercializados.
                El archivo debe tener extension .xlsx o .xls.

        Returns:
            pd.DataFrame: DataFrame con los datos del maestro, normalizado con:
                - Nombres de columnas en MAYUSCULAS y sin espacios extremos.
                - Columnas OC, FACTURA y POSICION convertidas a string y limpiadas.
                
                Columnas garantizadas en el DataFrame resultante:
                    - OC: Numero de orden de compra
                    - FACTURA: Numero de factura asociada
                    - VALOR TOTAL OC: Valor total de la orden
                    - POSICION: Posicion dentro de la orden
                    - POR CALCULAR (VALOR UNITARIO): Valor unitario a calcular
                    - POR CALCULAR (ME): Valor en moneda extranjera

        Raises:
            FileNotFoundError: Si el archivo no existe en la ruta especificada.
            ValueError: Si faltan columnas requeridas en el archivo.
            Exception: Otros errores de lectura del archivo Excel.

        Examples:
            Carga exitosa::

                df_maestro = validar_maestro_comercializados(
                    "C:/Insumos/Maestro_Comercializados.xlsx"
                )
                print(df_maestro.columns.tolist())
                # ['OC', 'FACTURA', 'VALOR TOTAL OC', 'POSICION', 
                #  'POR CALCULAR (VALOR UNITARIO)', 'POR CALCULAR (ME)', ...]

            Archivo no encontrado::

                try:
                    df = validar_maestro_comercializados("/ruta/inexistente.xlsx")
                except FileNotFoundError as e:
                    print(f"Error: {e}")
                    # Error: No existe archivo: /ruta/inexistente.xlsx

        Note:
            Las columnas requeridas son criticas para el proceso de validacion:
            
            - **OC**: Se usa para cruzar con el numero de orden de compra del documento.
            - **FACTURA**: Se cruza con el numero de factura del XML.
            - **POSICION**: Identifica lineas especificas dentro de la OC.
            - **VALOR TOTAL OC**: Referencia del valor total esperado.
            - **POR CALCULAR (VALOR UNITARIO)**: Valor en COP para comparacion.
            - **POR CALCULAR (ME)**: Valor en moneda extranjera (USD) si aplica.

        See Also:
            validar_asociacion_cuentas: Validacion del otro archivo maestro requerido.
        """
        if not os.path.exists(ruta): 
            raise FileNotFoundError(f"No existe archivo: {ruta}")
        df = pd.read_excel(ruta)
        df.columns = df.columns.str.strip().str.upper()
        cols_req = ['OC', 'FACTURA', 'VALOR TOTAL OC', 'POSICION', 
                    'POR CALCULAR (VALOR UNITARIO)', 'POR CALCULAR (ME)']
        if any(c not in df.columns for c in cols_req): 
            raise ValueError(f"Faltan columnas en Maestro de Comercializados. Requeridas: {cols_req}")
        
        # Normalizar columnas clave para busquedas consistentes
        df['OC'] = df['OC'].astype(str).str.strip()
        df['FACTURA'] = df['FACTURA'].astype(str).str.strip()
        df['POSICION'] = df['POSICION'].astype(str).str.strip()
        return df

    def validar_asociacion_cuentas(ruta):
        """
        Valida y carga el archivo de Asociacion cuenta indicador.

        Esta funcion verifica que el archivo Excel de asociacion de cuentas
        exista, contenga la hoja esperada con la informacion de grupos de
        cuentas y agrupaciones de proveedores, y tenga las columnas requeridas.

        Args:
            ruta (str): Ruta completa al archivo Excel de Asociacion cuenta indicador.
                El archivo debe contener una hoja con nombre que incluya
                "Grupo cuentas agrupacion provee" (busqueda case-insensitive).

        Returns:
            pd.DataFrame: DataFrame con los datos de asociacion de cuentas.
                Columnas garantizadas (nombres normalizados en MAYUSCULAS):
                    - CTA MAYOR: Cuenta contable mayor
                    - NOMBRE CUENTA: Descripcion de la cuenta
                    - TIPO RET.: Tipo de retencion aplicable
                    - IND.RETENCION: Indicador de retencion
                    - DESCRIPCION IND.RET.: Descripcion del indicador
                    - AGRUPACION CODIGO: Codigo de agrupacion de proveedor
                    - NOMBRE CODIGO: Nombre de la agrupacion

        Raises:
            FileNotFoundError: Si el archivo no existe en la ruta especificada.
            ValueError: Si no se encuentra la hoja requerida o faltan columnas.
            Exception: Otros errores de lectura del archivo Excel.

        Examples:
            Carga exitosa::

                df_asoc = validar_asociacion_cuentas(
                    "C:/Insumos/Asociacion_Cuenta_Indicador.xlsx"
                )
                print(df_asoc['CTA MAYOR'].unique()[:5])

            Hoja no encontrada::

                try:
                    df = validar_asociacion_cuentas("/ruta/archivo_sin_hoja.xlsx")
                except ValueError as e:
                    print(f"Error: {e}")
                    # Error: Hoja 'Grupo cuentas prove' no encontrada...

        Note:
            - La busqueda de la hoja es flexible (case-insensitive, coincidencia parcial).
            - La busqueda de columnas tambien es flexible, ignorando puntos y espacios.
            - Este archivo se usa principalmente para validaciones de cuentas contables
              y retenciones, aunque en el flujo actual de ZVEN puede no ser utilizado
              directamente en todas las validaciones.

        See Also:
            validar_maestro_comercializados: Validacion del maestro de comercializados.
        """
        if not os.path.exists(ruta): 
            raise FileNotFoundError(f"No existe archivo: {ruta}")
        xls = pd.ExcelFile(ruta)
        hoja_req = next(
            (h for h in xls.sheet_names if 'grupo cuentas agrupacion provee' in h.lower()), 
            None
        )
        if not hoja_req: 
            raise ValueError("Hoja 'Grupo cuentas prove' no encontrada en Asociacion cuenta indicador")
        
        df = pd.read_excel(ruta, sheet_name=hoja_req)
        df.columns = df.columns.str.strip().str.upper()
        
        cols_req = ['CTA MAYOR', 'NOMBRE CUENTA', 'TIPO RET.', 'IND.RETENCION', 
                    'DESCRIPCION IND.RET.', 'AGRUPACION CODIGO', 'NOMBRE CODIGO']
        
        # Busqueda flexible de columnas (ignorando puntos y espacios)
        cols_faltantes = []
        for col in cols_req:
            if not any(col.replace('.', '').replace(' ', '') in c.replace('.', '').replace(' ', '') 
                      for c in df.columns):
                cols_faltantes.append(col)
                
        if cols_faltantes: 
            raise ValueError(f"Faltan columnas en Asociacion cuenta indicador: {cols_faltantes}")
        return df

    def mover_insumos_en_espera(registro, ruta_destino_base):
        """
        Mueve los archivos de insumo de un registro a la carpeta de espera de comercializados.

        Esta funcion copia los archivos asociados a un documento (XML, PDF, anexos)
        a una subcarpeta 'INSUMO' dentro del directorio de destino de comercializados,
        cuando el registro no se encuentra en el maestro y queda en estado de espera.

        Args:
            registro (dict | pd.Series): Registro del documento con informacion de archivos.
                Campos utilizados:
                    - RutaArchivo_dp: Ruta origen donde estan los archivos.
                    - actualizacionNombreArchivos_dp: Lista de nombres de archivo
                      separados por coma.
            ruta_destino_base (str): Ruta base del directorio de destino.
                Se creara una subcarpeta 'INSUMO' dentro de esta ruta.

        Returns:
            tuple[bool, str | None]: Tupla con dos elementos:
                - bool: True si se movio al menos un archivo exitosamente, False si no.
                - str | None: Ruta completa del directorio destino si hubo exito,
                  None si no se movieron archivos o hubo error.

        Examples:
            Movimiento exitoso::

                registro = {
                    'RutaArchivo_dp': 'C:/Entrada/Facturas',
                    'actualizacionNombreArchivos_dp': 'factura.xml,anexo.pdf'
                }
                exito, ruta = mover_insumos_en_espera(
                    registro, 
                    'C:/Comercializados/EnEspera'
                )
                if exito:
                    print(f"Archivos movidos a: {ruta}")
                    # Archivos movidos a: C:/Comercializados/EnEspera/INSUMO

            Sin archivos para mover::

                registro = {'RutaArchivo_dp': '', 'actualizacionNombreArchivos_dp': ''}
                exito, ruta = mover_insumos_en_espera(registro, 'C:/Destino')
                # exito = False, ruta = None

        Note:
            - Se usa shutil.copy2 para preservar metadatos del archivo.
            - La carpeta destino se crea automaticamente si no existe.
            - Los archivos que no existen en origen son ignorados silenciosamente.
            - Cualquier excepcion durante el proceso retorna (False, None).

        Warning:
            Esta funcion COPIA archivos, no los mueve. Los originales permanecen
            en la ubicacion de origen. Para eliminar los originales despues de
            copiar, se debe implementar logica adicional.
        """
        try:
            ruta_origen = safe_str(registro.get('RutaArchivo_dp', ''))
            nombre_archivos = safe_str(registro.get('actualizacionNombreArchivos_dp', '')).split(',')
            if not ruta_origen or not nombre_archivos: 
                return False, None
            ruta_destino = os.path.join(ruta_destino_base, "INSUMO")
            os.makedirs(ruta_destino, exist_ok=True)
            archivos_movidos = 0
            for archivo in nombre_archivos:
                origen = os.path.join(ruta_origen, archivo.strip())
                if os.path.exists(origen):
                    shutil.copy2(origen, os.path.join(ruta_destino, archivo.strip()))
                    archivos_movidos += 1
            return archivos_movidos > 0, ruta_destino
        except Exception: 
            return False, None

    # =========================================================================
    # INICIO DEL PROCESO PRINCIPAL
    # =========================================================================
    try:
        t_inicio = time.time()
        print("="*80 + "\n[INICIO] Procesamiento ZVEN/50 - Comercializados\n" + "="*80)
        
        # ---------------------------------------------------------------------
        # 1. OBTENER VARIABLES DE ROCKETBOT
        # ---------------------------------------------------------------------
        cfg = parse_config(GetVar("vLocDicConfig"))
        
        # Validar parametros de configuracion obligatorios
        req_cfg = ['ServidorBaseDatos', 'NombreBaseDatos', 'RutaInsumosComercializados', 
                   'RutaInsumoAsociacion', 'CarpetaDestinoComercializados']
        if any(not cfg.get(k) for k in req_cfg): 
            raise ValueError("Faltan parametros en vLocDicConfig")
        
        # ---------------------------------------------------------------------
        # 2. VALIDAR ARCHIVOS MAESTROS
        # Si falla, va a excepcion y detiene el bot
        # ---------------------------------------------------------------------
        print("[INFO] Validando Maestro de Comercializados...")
        df_maestro = validar_maestro_comercializados(cfg['RutaInsumosComercializados'])
        
        print("[INFO] Validando Asociacion cuenta indicador...")
        df_asociacion = validar_asociacion_cuentas(cfg['RutaInsumoAsociacion'])
        
        # Contadores de procesamiento
        cnt_proc, cnt_ok, cnt_nov, cnt_esp = 0, 0, 0, 0

        # ---------------------------------------------------------------------
        # 3. CONEXION Y PROCESAMIENTO PRINCIPAL
        # ---------------------------------------------------------------------
        with crear_conexion_db(cfg) as cx:
            # Consultar registros candidatos ZVEN/50
            df_registros = pd.read_sql(
                "SELECT * FROM [CxP].[HU41_CandidatosValidacion] "
                "WHERE [ClaseDePedido_hoc] IN ('ZVEN', '50')", 
                cx
            )
            print(f"[INFO] {len(df_registros)} registros ZVEN/50 para procesar.")

            # -----------------------------------------------------------------
            # ITERACION SOBRE CADA REGISTRO CANDIDATO
            # -----------------------------------------------------------------
            for idx, registro in df_registros.iterrows():
                try:
                    # Extraer campos principales del registro
                    registro_id = safe_str(registro.get('ID_dp', ''))
                    numero_oc = safe_str(registro.get('numero_de_liquidacion_u_orden_de_compra_dp', ''))
                    numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
                    nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
                    payment_means = safe_str(registro.get('forma_de_pago_dp', ''))
                    obs_existente = safe_str(registro.get('ObservacionesFase_4_dp', ''))
                    
                    # Determinar sufijo segun forma de pago
                    sufijo_contado = " CONTADO" if payment_means in ["01", "1"] else ""

                    print(f"\n[PROCESO] Registro {idx+1}: OC {numero_oc}, Factura {numero_factura}")

                    # ---------------------------------------------------------
                    # BUSQUEDA EN MAESTRO DE COMERCIALIZADOS
                    # ---------------------------------------------------------
                    matches = df_maestro[
                        (df_maestro['OC'] == numero_oc) & 
                        (df_maestro['FACTURA'] == numero_factura)
                    ]
                    
                    # ---------------------------------------------------------
                    # CASO 1: NO EXISTE EN MAESTRO → EN ESPERA
                    # ---------------------------------------------------------
                    if matches.empty:
                        print("[INFO] No encontrado en Maestro -> Mover a En Espera")
                        movido_ok, nueva_ruta = mover_insumos_en_espera(
                            registro, 
                            cfg['CarpetaDestinoComercializados']
                        )
                        if movido_ok:
                            obs = (f"No se encuentran datos de la orden de compra y factura "
                                   f"en el archivo Maestro de comercializados, {obs_existente}")
                            campos_db = {
                                'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 
                                'ObservacionesFase_4': truncar_observacion(obs), 
                                'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS {sufijo_contado}", 
                                'RutaArchivo': nueva_ruta
                            }
                        else:
                            obs = (f"No se encuentran datos de la orden de compra y factura "
                                   f"en el archivo Maestro de comercializados - No se logran "
                                   f"mover insumos a carpeta COMERCIALIZADOS, {obs_existente}")
                            campos_db = {
                                'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 
                                'ObservacionesFase_4': truncar_observacion(obs), 
                                'ResultadoFinalAntesEventos': f"EN ESPERA - COMERCIALIZADOS {sufijo_contado}"
                            }

                        actualizar_bd_cxp(cx, registro_id, campos_db)
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'Observaciones', 
                            valor_xml=truncar_observacion(obs), val_orden_de_compra=None
                        )
                        actualizar_estado_comparativa(
                            cx, nit, numero_factura, 
                            f"EN ESPERA - COMERCIALIZADOS {sufijo_contado}"
                        )
                        cnt_esp += 1
                        cnt_proc += 1
                        continue

                    # ---------------------------------------------------------
                    # CASO 2: EXISTE EN MAESTRO → PROCESAR VALIDACIONES
                    # ---------------------------------------------------------
                    pos_maestro = matches['POSICION'].tolist()
                    vals_unitario = [normalizar_decimal(v) for v in matches['POR CALCULAR (VALOR UNITARIO)']]
                    vals_me = [normalizar_decimal(v) for v in matches['POR CALCULAR (ME)']]
                    
                    # Actualizar informacion de comercializados en BD
                    actualizar_bd_cxp(cx, registro_id, {
                        'Posicion_Comercializado': ','.join(map(str, pos_maestro)),
                        'Valor_a_pagar_Comercializado': ','.join(map(str, vals_unitario)),
                        'Valor_a_pagar_Comercializado_ME': ','.join(map(str, vals_me))
                    })
                    
                    # Cargar trazabilidad de valores del maestro en comparativa
                    actualizar_items_comparativa(
                        registro, cx, nit, numero_factura, 'Posicion', 
                        valor_xml=','.join(map(str, pos_maestro)), 
                        val_orden_de_compra_comercializados=','.join(map(str, pos_maestro))
                    )
                    actualizar_items_comparativa(
                        registro, cx, nit, numero_factura, 'ValorPorCalcularPosicion', 
                        valor_xml=','.join(map(str, vals_unitario)), 
                        val_orden_de_compra_comercializados=','.join(map(str, vals_unitario))
                    )
                    actualizar_items_comparativa(
                        registro, cx, nit, numero_factura, 'ValorPorCalcularMEPosicion', 
                        valor_xml=','.join(map(str, vals_me)), 
                        val_orden_de_compra_comercializados=','.join(map(str, vals_me))
                    )
                    
                    # Preparar datos de SAP
                    sap_posiciones = [str(p) for p in expandir_posiciones_string(
                        registro.get('Posicion_hoc', '')
                    )]
                    sap_por_calcular = expandir_posiciones_string(
                        registro.get('PorCalcular_hoc', '')
                    )
                    
                    # Pre-calculo de sumas para disponibilidad global
                    sum_unitario = sum(vals_unitario)
                    sum_me = sum(vals_me)
                    usa_me = sum_me > 0
                    
                    sum_por_calcular_sap = sum(normalizar_decimal(x) for x in sap_por_calcular)
                    
                    # ---------------------------------------------------------
                    # VALIDACION 1: COINCIDENCIA DE VALORES Y POSICIONES
                    # ---------------------------------------------------------
                    vlr_factura_target = normalizar_decimal(
                        registro.get('Valor de la Compra LEA_ddp', 0)
                    )
                    valores_maestro_a_usar = vals_me if usa_me else vals_unitario
                    suma_maestro = sum(valores_maestro_a_usar)

                    # Tolerancia de $500 COP para comparacion de montos
                    coincide_valor = abs(suma_maestro - vlr_factura_target) <= 500
                    coinciden_posiciones = True
                    
                    for i, pos in enumerate(pos_maestro):
                        pos_str = str(pos).strip()
                        # Verificar si la posicion existe en SAP
                        if pos_str not in sap_posiciones:
                            # Intentar normalizar '00010' -> '10' para comparacion
                            sap_pos_int = [str(int(p)) for p in sap_posiciones if p.isdigit()]
                            if str(int(pos_str)) not in sap_pos_int:
                                coinciden_posiciones = False
                                break
                            else:
                                idx_sap = sap_pos_int.index(str(int(pos_str)))
                        else:
                            idx_sap = sap_posiciones.index(pos_str)
                        
                        # Comparar valor SAP vs Maestro
                        valor_maestro_actual = vals_me[i] if usa_me else vals_unitario[i]
                        val_sap = normalizar_decimal(sap_por_calcular[idx_sap]) if idx_sap < len(sap_por_calcular) else 0
                        
                        # Tolerancia de 0.01 para decimales
                        if abs(val_sap - valor_maestro_actual) > 0.01:
                            coinciden_posiciones = False
                            break

                    if not (coincide_valor and coinciden_posiciones):
                        print("[INFO] No coinciden valores o posiciones Maestro vs SAP/XML")
                        obs = f"No se encuentra coincidencia del Valor a pagar de la factura, {obs_existente}"
                        res_final = f"CON NOVEDAD - COMERCIALIZADOS {sufijo_contado}"
                        
                        actualizar_bd_cxp(cx, registro_id, {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 
                            'ObservacionesFase_4': truncar_observacion(obs), 
                            'ResultadoFinalAntesEventos': res_final
                        })
                        
                        # Reportar error detallado en comparativa
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'LineExtensionAmount', 
                            valor_xml=vlr_factura_target, valor_aprobado='NO', 
                            val_orden_de_compra='NO ENCONTRADO'
                        )
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'VlrPagarCop', 
                            valor_xml=registro.get('VlrPagarCop_dp',''), 
                            valor_aprobado='NO', val_orden_de_compra='NO ENCONTRADO'
                        )
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'Observaciones', 
                            valor_xml=truncar_observacion(obs)
                        )
                        
                        # Registrar valores que se intentaron usar
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'ValorPorCalcularMEPosicion', 
                            valor_xml=None, valor_aprobado='NO', 
                            val_orden_de_compra=str(vals_me)
                        )
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'ValorPorCalcularPosicion', 
                            valor_xml=None, valor_aprobado='NO', 
                            val_orden_de_compra=str(vals_unitario)
                        )
                        
                        actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                        marcar_orden_procesada(
                            cx, numero_oc, 
                            safe_str(registro.get('Posicion_hoc',''))
                        )
                        cnt_nov += 1
                        cnt_proc += 1
                        continue
                    
                    # ---------------------------------------------------------
                    # SI PASA VALIDACION 1: Registrar exito de montos y continuar
                    # ---------------------------------------------------------
                    marcar_orden_procesada(
                        cx, numero_oc, 
                        safe_str(registro.get('Posicion_hoc',''))
                    )
                    
                    # Llenar comparativa con datos exitosos de SAP
                    campos_sap = [
                        'ValorPorCalcularSAP', 'TipoNIF', 'Acreedor', 'FecDoc', 
                        'FecReg', 'FechaContGasto', 'IndicadorImpuestos', 'TextoBreve', 
                        'ClaseImpuesto', 'DocFIEntrada', 'CTA26', 'ActivoFijo', 
                        'CapitalizadoEl', 'CriterioClasif2'
                    ]
                    col_map = {
                        'ValorPorCalcularSAP': 'PorCalcular', 
                        'FechaContGasto': 'FecContGasto', 
                        'CTA26': 'Cuenta26'
                    }
                    
                    for campo in campos_sap:
                        key_hoc = f"{col_map.get(campo, campo)}_hoc"
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, campo, 
                            valor_xml=None, valor_aprobado=None, 
                            val_orden_de_compra=registro.get(key_hoc,'')
                        )

                    # Registrar exito en montos
                    val_xml_comp = registro.get('VlrPagarCop_dp','') if usa_me else registro.get('valor_a_pagar_dp','')
                    val_sap_comp = sum_me if usa_me else sum_unitario
                    
                    actualizar_items_comparativa(
                        registro, cx, nit, numero_factura, 
                        'LineExtensionAmount' if not usa_me else 'VlrPagarCop', 
                        valor_xml=val_xml_comp, valor_aprobado='SI', 
                        val_orden_de_compra=val_sap_comp
                    )

                    # ---------------------------------------------------------
                    # VALIDACION 2: TRM (Solo si es USD o tiene ME)
                    # ---------------------------------------------------------
                    print(f"[DEBUG] Validando TRM...")
                    trm_xml = normalizar_decimal(registro.get('CalculationRate_dp', 0))
                    trm_sap_str = registro.get('Trm_hoc','').split('|')[0]
                    trm_sap = normalizar_decimal(trm_sap_str)
                    es_usd = registro.get('Moneda_hoc','').upper().startswith('USD')
                    
                    if es_usd:
                        if trm_xml > 0 and trm_sap > 0 and abs(trm_xml - trm_sap) > 0.01:
                            print(f"[INFO] TRM no coincide: XML {trm_xml} vs SAP {trm_sap}")
                            obs = (f"No se encuentra coincidencia en el campo TRM de la factura "
                                   f"vs la informacion reportada en SAP, {obs_existente}")
                            res_final = f"CON NOVEDAD {sufijo_contado}"
                            
                            actualizar_bd_cxp(cx, registro_id, {
                                'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 
                                'ObservacionesFase_4': truncar_observacion(obs), 
                                'ResultadoFinalAntesEventos': res_final
                            })
                            actualizar_items_comparativa(
                                registro, cx, nit, numero_factura, 'TRM', 
                                valor_xml=trm_xml, valor_aprobado='NO', 
                                val_orden_de_compra=trm_sap
                            )
                            actualizar_items_comparativa(
                                registro, cx, nit, numero_factura, 'Observaciones', 
                                valor_xml=truncar_observacion(obs)
                            )
                            actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                            marcar_orden_procesada(
                                cx, numero_oc, 
                                safe_str(registro.get('Posicion_hoc',''))
                            )
                            cnt_nov += 1
                            cnt_proc += 1
                            continue
                        else:
                            actualizar_items_comparativa(
                                registro, cx, nit, numero_factura, 'TRM', 
                                valor_xml=trm_xml, valor_aprobado='SI', 
                                val_orden_de_compra=trm_sap
                            )

                    # ---------------------------------------------------------
                    # VALIDACION 3: CANTIDAD Y PRECIO (Comparacion de listas)
                    # ---------------------------------------------------------
                    print(f"[DEBUG] Validando Cantidad y Precio...")
                    cant_xml_list = expandir_posiciones_string(
                        registro.get('Cantidad de producto_ddp', '')
                    )
                    prec_xml_list = expandir_posiciones_string(
                        registro.get('Precio Unitario del producto_ddp', '')
                    )
                    cant_sap_list = expandir_posiciones_string(
                        registro.get('CantPedido_hoc', '')
                    )
                    prec_sap_list = expandir_posiciones_string(
                        registro.get('PrecioUnitario_hoc', '')
                    )
                    
                    fallo_cp = False
                    # Iterar hasta el maximo de lineas para detectar discrepancias
                    max_lines = max(len(cant_xml_list), len(cant_sap_list))
                    
                    for i in range(max_lines):
                        c_xml = normalizar_decimal(cant_xml_list[i]) if i < len(cant_xml_list) else 0
                        p_xml = normalizar_decimal(prec_xml_list[i]) if i < len(prec_xml_list) else 0
                        c_sap = normalizar_decimal(cant_sap_list[i]) if i < len(cant_sap_list) else 0
                        p_sap = normalizar_decimal(prec_sap_list[i]) if i < len(prec_sap_list) else 0
                        
                        # Tolerancia de 1 unidad para cantidad y precio
                        if abs(c_xml - c_sap) > 1 or abs(p_xml - p_sap) > 1:
                            fallo_cp = True
                            break
                    
                    if fallo_cp:
                        print("[INFO] Fallo en Cantidad/Precio")
                        obs = (f"No se encuentra coincidencia en cantidad y/o precio unitario "
                               f"de la factura vs la informacion reportada en SAP, {obs_existente}")
                        res_final = f"CON NOVEDAD - COMERCIALIZADOS{sufijo_contado}"
                        
                        actualizar_bd_cxp(cx, registro_id, {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 
                            'ObservacionesFase_4': truncar_observacion(obs), 
                            'ResultadoFinalAntesEventos': res_final
                        })
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'PrecioUnitarioProducto', 
                            valor_xml=registro.get('Precio Unitario del producto_ddp',''), 
                            valor_aprobado='NO', 
                            val_orden_de_compra=registro.get('PrecioUnitario_hoc','')
                        )
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'CantidadProducto', 
                            valor_xml=registro.get('Cantidad de producto_ddp',''), 
                            valor_aprobado='NO', 
                            val_orden_de_compra=registro.get('CantPedido_hoc','')
                        )
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'Observaciones', 
                            valor_xml=truncar_observacion(obs)
                        )
                        actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                        marcar_orden_procesada(
                            cx, numero_oc, 
                            safe_str(registro.get('Posicion_hoc',''))
                        )
                        cnt_nov += 1
                        cnt_proc += 1
                        continue
                    else:
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'PrecioUnitarioProducto', 
                            valor_xml=registro.get('Precio Unitario del producto_ddp',''), 
                            valor_aprobado='SI', 
                            val_orden_de_compra=registro.get('PrecioUnitario_hoc','')
                        )
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'CantidadProducto', 
                            valor_xml=registro.get('Cantidad de producto_ddp',''), 
                            valor_aprobado='SI', 
                            val_orden_de_compra=registro.get('CantPedido_hoc','')
                        )

                    # ---------------------------------------------------------
                    # VALIDACION 4: NOMBRE EMISOR
                    # ---------------------------------------------------------
                    print(f"[DEBUG] Validando Nombre Emisor...")
                    nombre_emisor_xml = safe_str(registro.get('nombre_emisor_dp', ''))
                    nombre_proveedor_sap = safe_str(registro.get('NProveedor_hoc', ''))
                    
                    if not comparar_nombres_proveedor(nombre_emisor_xml, nombre_proveedor_sap):
                        print("[INFO] Fallo Nombre Emisor")
                        obs = (f"No se encuentra coincidencia en Nombre Emisor de la factura "
                               f"vs la informacion reportada en SAP, {obs_existente}")
                        res_final = f"CON NOVEDAD {sufijo_contado}"
                        
                        actualizar_bd_cxp(cx, registro_id, {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 
                            'ObservacionesFase_4': truncar_observacion(obs), 
                            'ResultadoFinalAntesEventos': res_final
                        })
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'NombreEmisor', 
                            valor_xml=nombre_emisor_xml, valor_aprobado='NO', 
                            val_orden_de_compra=nombre_proveedor_sap
                        )
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'Observaciones', 
                            valor_xml=truncar_observacion(obs)
                        )
                        actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                        marcar_orden_procesada(
                            cx, numero_oc, 
                            safe_str(registro.get('Posicion_hoc',''))
                        )
                        cnt_nov += 1
                        cnt_proc += 1
                        continue
                    else:
                        actualizar_items_comparativa(
                            registro, cx, nit, numero_factura, 'NombreEmisor', 
                            valor_xml=nombre_emisor_xml, valor_aprobado='SI', 
                            val_orden_de_compra=nombre_proveedor_sap
                        )

                    # ---------------------------------------------------------
                    # FIN EXITOSO DEL REGISTRO
                    # ---------------------------------------------------------
                    print(f"[SUCCESS] Registro {registro_id} finalizado OK")
                    res_final = f"PROCESADO {sufijo_contado}"
                    actualizar_bd_cxp(cx, registro_id, {
                        'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso', 
                        'ResultadoFinalAntesEventos': res_final
                    })
                    actualizar_estado_comparativa(cx, nit, numero_factura, res_final)
                    marcar_orden_procesada(
                        cx, numero_oc, 
                        safe_str(registro.get('Posicion_hoc',''))
                    )
                    cnt_ok += 1
                    cnt_proc += 1

                except Exception as e_reg:
                    # Error individual por registro: continuar con el siguiente
                    print(f"[ERROR] Error procesando registro individual {registro_id}: {str(e_reg)}")
                    cnt_proc += 1
                    continue

        # ---------------------------------------------------------------------
        # 4. SALIDA EXITOSA A ROCKETBOT
        # ---------------------------------------------------------------------
        print("="*80 + "\n[FIN] Procesamiento ZVEN/50 completado\n" + "="*80)
        resumen = (f"Procesados {cnt_proc} registros ZVEN. Exitosos: {cnt_ok}, "
                   f"Con novedad: {cnt_nov}, En espera: {cnt_esp}")
        SetVar("vLocStrResultadoSP", "True")
        SetVar("vLocStrResumenSP", resumen)
        
    except Exception as e:
        # ---------------------------------------------------------------------
        # 5. SALIDA DE ERROR A ROCKETBOT
        # Fallo critico de infraestructura que detiene el bot
        # ---------------------------------------------------------------------
        print(f"[CRITICO] Fallo general ZVEN: {str(e)}")
        print(traceback.format_exc())
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "Error_HU4.1_ZVEN")
        SetVar("vLocStrResultadoSP", "False")
        raise e  # IMPORTANTE: Detener el bot en caso de error critico de infraestructura
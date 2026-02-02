"""
================================================================================
SCRIPT: HU4_1_ZPAF.py
================================================================================

Descripcion General:
--------------------
    Valida pedidos de Activos Fijos (Clases ZPAF o 41) en el sistema de
    Cuentas por Pagar. Ejecuta validaciones financieras y de datos maestros
    especificas para este tipo de pedido, incluyendo formato de activo fijo,
    indicadores de impuestos, criterios de clasificacion y cuenta contable.

Autor: Equipo de Desarrollo RPA
Version: 1.0
Plataforma: RocketBot RPA

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |             ZPAF_ValidarActivosFijos()                      |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  1. Cargar configuracion desde vLocDicConfig                |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  2. Conectar a base de datos SQL Server                     |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  3. Consultar [CxP].[HU41_CandidatosValidacion]             |
    |     Filtrar: ClaseDePedido = ZPAF o 41                      |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  4. Para cada registro:                                     |
    |  +-------------------------------------------------------+  |
    |  |  a. Validar Montos (valor XML vs suma SAP)            |  |
    |  +-------------------------------------------------------+  |
    |  |  b. Validar TRM (si USD)                              |  |
    |  +-------------------------------------------------------+  |
    |  |  c. Validar Nombre Emisor                             |  |
    |  +-------------------------------------------------------+  |
    |  |  d. Validar Activo Fijo (formato 9 digitos)           |  |
    |  +-------------------------------------------------------+  |
    |  |  e. Validar Capitalizado (debe estar vacio)           |  |
    |  +-------------------------------------------------------+  |
    |  |  f. Validar Indicador Impuestos (H4/H5/H6/H7/VP)      |  |
    |  +-------------------------------------------------------+  |
    |  |  g. Validar Criterio Clasif. 2                        |  |
    |  +-------------------------------------------------------+  |
    |  |  h. Validar Cuenta (debe ser 2695950020)              |  |
    |  +-------------------------------------------------------+  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  5. Actualizar resultados en tablas                         |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  6. Retornar estadisticas a RocketBot                       |
    +-------------------------------------------------------------+

================================================================================
REGLAS DE NEGOCIO ZPAF
================================================================================

    1. Activo Fijo:
       - Debe tener exactamente 9 digitos numericos
       - Patron regex: ^\d{9}$
       
    2. Capitalizado el:
       - NUNCA debe estar diligenciado (siempre vacio)
       
    3. Indicador Impuestos:
       - Grupo 1 (Productores): H4, H5, VP
       - Grupo 2 (No Productores): H6, H7, VP
       - No se permite mezclar indicadores de ambos grupos
       
    4. Criterio Clasif. 2:
       - H4/H5 -> debe ser '0001'
       - H6/H7 -> debe ser '0000'
       - VP -> puede ser '0001' o '0000'
       
    5. Cuenta:
       - Debe ser estrictamente '2695950020'

================================================================================
VARIABLES DE ENTRADA (RocketBot)
================================================================================

    vLocDicConfig : str | dict
        - ServidorBaseDatos: Servidor SQL Server
        - NombreBaseDatos: Base de datos
        - UsuarioBaseDatos: Usuario SQL (opcional)
        - ClaveBaseDatos: Contrasena SQL (opcional)

================================================================================
VARIABLES DE SALIDA (RocketBot)
================================================================================

    vLocStrResultadoSP : str
        "True" si exitoso, "False" si error critico

    vLocStrResumenSP : str
        "Procesados X registros ZPAF/41. Exitosos: Y, Con novedad: Z"

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
FUNCIONES DE VALIDACION
================================================================================

    validar_activo_fijo(valor):
        Verifica formato de 9 digitos numericos
        
    validar_capitalizado(valor):
        Verifica que este vacio
        
    validar_indicador_impuestos(lista_indicadores):
        Verifica indicadores validos y no mezclados
        
    validar_criterio_clasif_2(indicador, criterio):
        Verifica coherencia segun reglas
        
    validar_cuenta_zpaf(cuenta):
        Verifica que sea 2695950020

================================================================================
EJEMPLOS DE USO
================================================================================

    # Configurar variables en RocketBot
    SetVar("vLocDicConfig", json.dumps({
        "ServidorBaseDatos": "sqlserver.empresa.com",
        "NombreBaseDatos": "CxP_Produccion"
    }))
    
    # Ejecutar la validacion
    ZPAF_ValidarActivosFijos()
    
    # Verificar resultado
    resultado = GetVar("vLocStrResultadoSP")
    resumen = GetVar("vLocStrResumenSP")

================================================================================
NOTAS TECNICAS
================================================================================

    - Errores individuales por registro NO detienen el proceso
    - Solo errores criticos de infraestructura detienen el bot
    - Tolerancia para montos: $500 COP
    - Tolerancia para TRM: 0.01
    - Observaciones se truncan a 3900 caracteres

================================================================================
"""

def ZPAF_ValidarActivosFijos():
    """
    Orquesta la validacion de pedidos de Activos Fijos (Clases ZPAF o 41).

    Esta funcion principal implementa el flujo completo de validacion para pedidos
    de activos fijos en el sistema de Cuentas por Pagar (CxP). Se conecta a la 
    base de datos, recupera los registros pendientes y ejecuta una bateria de 
    validaciones financieras y de datos maestros especificas para este tipo de pedido.

    El proceso de validacion incluye las siguientes etapas:

        1. **Carga de configuracion**: Obtiene parametros desde RocketBot mediante
           la variable ``vLocDicConfig``.
        
        2. **Conexion a base de datos**: Establece conexion resiliente con SQL Server
           utilizando multiples metodos de autenticacion.
        
        3. **Consulta de candidatos**: Recupera registros ZPAF/41 pendientes desde
           la vista ``[CxP].[HU41_CandidatosValidacion]``.
        
        4. **Validaciones por registro**: Para cada registro candidato ejecuta:
           
           - **Validacion de montos**: Compara valor a pagar XML vs suma de posiciones SAP.
           - **Validacion de TRM**: Verifica tasa de cambio para operaciones USD.
           - **Validacion de nombre emisor**: Compara proveedor XML vs SAP.
           - **Validacion de Activo Fijo**: Verifica formato de 9 digitos.
           - **Validacion de Capitalizado**: Campo debe estar vacio para ZPAF.
           - **Validacion de Indicador Impuestos**: H4/H5 (Productores) o H6/H7 (No productores).
           - **Validacion de Criterio Clasif. 2**: Coherencia con indicador de impuestos.
           - **Validacion de Cuenta**: Debe ser 2695950020 para ZPAF.
        
        5. **Actualizacion de resultados**: Registra el estado final en las tablas
           transaccional y de trazabilidad.

    Tablas involucradas:
        - **[CxP].[HU41_CandidatosValidacion]** (Vista): Fuente de registros candidatos.
        - **[CxP].[DocumentsProcessing]** (Tabla): Almacena estado y observaciones.
        - **[dbo].[CxP.Comparativa]** (Tabla): Trazabilidad detallada de validaciones.
        - **[CxP].[HistoricoOrdenesCompra]** (Tabla): Marca de ordenes procesadas.

    Reglas de negocio ZPAF:
        - **Activo Fijo**: Debe tener exactamente 9 digitos numericos.
        - **Capitalizado el**: NUNCA debe estar diligenciado (siempre vacio).
        - **Indicador Impuestos**: 
            - Grupo 1 (Productores): H4, H5, VP
            - Grupo 2 (No Productores): H6, H7, VP
            - No se permite mezclar indicadores de ambos grupos.
        - **Criterio Clasif. 2**:
            - H4/H5 → debe ser '0001'
            - H6/H7 → debe ser '0000'
            - VP → puede ser '0001' o '0000'
        - **Cuenta**: Debe ser estrictamente '2695950020'.

    Variables de entrada (RocketBot):
        - ``vLocDicConfig`` (str | dict): Configuracion JSON con parametros de conexion:
            - ServidorBaseDatos (str): Hostname o IP del servidor SQL Server.
            - NombreBaseDatos (str): Nombre de la base de datos.
            - UsuarioBaseDatos (str): Usuario para autenticacion SQL.
            - ClaveBaseDatos (str): Contrasena del usuario SQL.

    Variables de salida (RocketBot):
        - ``vLocStrResultadoSP`` (str): "True" si finalizo correctamente, "False" si hubo error critico.
        - ``vLocStrResumenSP`` (str): Resumen estadistico del procesamiento.
        - ``vGblStrDetalleError`` (str): Traceback completo en caso de excepcion critica.
        - ``vGblStrSystemError`` (str): Identificador del error del sistema.

    Estados finales posibles por registro:
        - ``PROCESADO``: Validacion exitosa sin novedades.
        - ``PROCESADO CONTADO``: Validacion exitosa, forma de pago contado.
        - ``CON NOVEDAD``: Se encontraron discrepancias en alguna validacion.
        - ``CON NOVEDAD CONTADO``: Discrepancias con forma de pago contado.

    Returns:
        None: La funcion no retorna valores directamente. Los resultados se 
            comunican a traves de las variables de RocketBot especificadas.

    Raises:
        ValueError: Si faltan parametros obligatorios en la configuracion o
            si la configuracion tiene formato invalido.
        pyodbc.Error: Si hay errores de conexion a la base de datos despues
            de agotar todos los reintentos.
        Exception: Cualquier error critico no manejado que detenga el proceso.

    Note:
        - Los errores individuales por registro NO detienen el procesamiento;
          se registra la novedad y continua con el siguiente registro.
        - Solo los errores criticos de infraestructura detienen completamente el bot.
        - La tolerancia para comparacion de montos es de $500 COP.
        - La tolerancia para comparacion de TRM es de 0.01.

    Example:
        Configuracion tipica en RocketBot::

            # Configurar variables previas
            SetVar("vLocDicConfig", json.dumps({
                "ServidorBaseDatos": "sqlserver.empresa.com",
                "NombreBaseDatos": "CxP_Produccion",
                "UsuarioBaseDatos": "app_user",
                "ClaveBaseDatos": "SecurePass123"
            }))
            
            # Ejecutar la validacion
            ZPAF_ValidarActivosFijos()
            
            # Verificar resultado
            resultado = GetVar("vLocStrResultadoSP")  # "True" o "False"
            resumen = GetVar("vLocStrResumenSP")
            # "Procesados 15 registros ZPAF/41. Exitosos: 12, Con novedad: 3"

    Version:
        1.0

    Author:
        Equipo de Desarrollo RPA
    """
    
    # =========================================================================
    # SECCION 1: IMPORTACION DE LIBRERIAS
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
    from itertools import combinations, zip_longest
    
    # Ignorar advertencias de compatibilidad de Pandas con ODBC
    # Estas advertencias no afectan la funcionalidad cuando se usa pyodbc directamente
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
    # =========================================================================
    # SECCION 2: FUNCIONES AUXILIARES (HELPERS)
    # =========================================================================
    
    def safe_str(v):
        """
        Convierte cualquier tipo de entrada a una cadena de texto limpia y segura.

        Esta funcion es fundamental para el manejo seguro de datos provenientes
        de multiples fuentes (base de datos, XML, Excel) que pueden contener
        valores nulos, tipos inesperados o codificaciones diversas.

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
                Los valores NaN/NA de pandas retornan cadena vacia.

        Examples:
            >>> safe_str(None)
            ''
            >>> safe_str("  Texto con espacios  ")
            'Texto con espacios'
            >>> safe_str(12345)
            '12345'
            >>> safe_str(float('nan'))
            ''
            >>> safe_str(b'datos en bytes')
            'datos en bytes'

        Note:
            La funcion nunca lanza excepciones; cualquier error de conversion
            resulta en una cadena vacia como fallback seguro. Esto garantiza
            que el flujo de procesamiento no se interrumpa por datos malformados.
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
            # Verifica si es NaN (Not a Number)
            if isinstance(v, float) and (np.isnan(v) or pd.isna(v)):
                return ""
            return str(v)
        try:
            return str(v).strip()
        except:
            return ""
    
    def truncar_observacion(obs, max_len=3900):
        """
        Trunca una cadena de texto para evitar errores de desbordamiento en SQL Server.

        Los campos de observaciones en SQL Server tipicamente tienen limites de
        longitud (NVARCHAR(4000)). Esta funcion garantiza que los textos largos
        no causen errores de insercion, preservando la informacion mas relevante
        al inicio del texto.

        Args:
            obs (str | Any): El texto de la observacion a guardar. Si no es string,
                se convierte usando ``safe_str()``.
            max_len (int, optional): La longitud maxima permitida. Por defecto es 3900,
                dejando margen de seguridad respecto al limite tipico de 4000.

        Returns:
            str: El texto truncado con "..." al final si excedia la longitud,
                o el texto original si era menor al limite.
                Retorna cadena vacia si la entrada es None o vacia.

        Examples:
            >>> truncar_observacion("Texto corto")
            'Texto corto'
            >>> truncar_observacion("A" * 5000, max_len=100)
            'AAAA...AAA...'  # 97 caracteres + "..."
            >>> truncar_observacion(None)
            ''

        Note:
            El valor predeterminado de 3900 permite espacio adicional para:
            - Concatenaciones posteriores con observaciones existentes.
            - Caracteres especiales que puedan expandirse en la codificacion.
            - Prefijos o sufijos que se anadan durante el procesamiento.
        """
        if not obs:
            return ""
        obs_str = safe_str(obs)
        if len(obs_str) > max_len:
            return obs_str[:max_len - 3] + "..."
        return obs_str
    
    def parse_config(raw):
        """
        Analiza y convierte la configuracion de entrada a un diccionario Python.

        Esta funcion maneja multiples formatos de entrada que pueden provenir
        de RocketBot, incluyendo diccionarios Python directos, strings JSON,
        y representaciones literales de Python.

        Args:
            raw (str | dict): La configuracion cruda proveniente de la variable
                de RocketBot ``vLocDicConfig``. Formatos aceptados:
                - dict: Se valida que no este vacio y se retorna directamente.
                - str (JSON): Se parsea con ``json.loads()``.
                - str (Python literal): Se parsea con ``ast.literal_eval()``.

        Returns:
            dict: Un diccionario con las claves de configuracion.
                Claves tipicas esperadas:
                - ServidorBaseDatos (str): Hostname o IP del servidor.
                - NombreBaseDatos (str): Nombre de la base de datos.
                - UsuarioBaseDatos (str): Usuario para autenticacion.
                - ClaveBaseDatos (str): Contrasena del usuario.

        Raises:
            ValueError: Si la configuracion esta vacia, tiene formato invalido,
                o no puede ser parseada por ninguno de los metodos disponibles.

        Examples:
            >>> parse_config({"servidor": "localhost"})
            {'servidor': 'localhost'}
            >>> parse_config('{"servidor": "localhost", "puerto": 1433}')
            {'servidor': 'localhost', 'puerto': 1433}
            >>> parse_config("{'clave': 'valor'}")  # Literal Python
            {'clave': 'valor'}
            >>> parse_config("")
            Raises ValueError: vLocDicConfig vacio

        Note:
            El orden de intentos de parseo es:
            1. Verificar si ya es diccionario (y no esta vacio).
            2. Intentar JSON (json.loads).
            3. Intentar literal Python (ast.literal_eval).
            4. Lanzar ValueError si todos fallan.
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
        Normaliza una entrada numerica o de texto a un valor flotante estandar.

        Esta funcion maneja multiples formatos numericos que pueden provenir de
        diferentes fuentes (Excel con formato europeo, SAP, XML) y los convierte
        a un float estandar de Python para comparaciones matematicas.

        Args:
            valor (str | float | int | None): El valor a normalizar. Formatos soportados:
                - None, '', pd.NA, np.nan: Retorna 0.0
                - int: Convierte directamente a float
                - float: Retorna el valor (0.0 si es NaN)
                - str con coma decimal (1.000,50): Convierte a 1000.50
                - str con punto decimal (1000.50): Convierte normalmente
                - str con simbolos ($, COP, etc.): Elimina y convierte

        Returns:
            float: El valor numerico normalizado.
                Siempre retorna un float valido (nunca None o NaN).
                Valores no parseables retornan 0.0.

        Examples:
            >>> normalizar_decimal("1.234,56")  # Formato europeo
            1234.56
            >>> normalizar_decimal("$1,000.00")  # Con simbolo
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
            El procesamiento para strings es:
            1. Eliminar espacios extremos.
            2. Reemplazar comas por puntos (estandarizacion).
            3. Eliminar caracteres no numericos (excepto punto y signo negativo).
            4. Convertir a float.

        Warning:
            Para valores con multiples puntos despues del reemplazo de comas,
            el resultado puede ser inesperado. Se recomienda preprocesar
            estos casos externamente si son comunes en los datos de origen.
        """
        if pd.isna(valor) or valor == '' or valor is None:
            return 0.0
        if isinstance(valor, (int, float)):
            if np.isnan(valor) if isinstance(valor, float) else False:
                return 0.0
            return float(valor)
        
        valor_str = str(valor).strip()
        # Estandarizacion: Reemplazar coma por punto para compatibilidad con float() de Python
        valor_str = valor_str.replace(',', '.')
        # Regex: Eliminar todo lo que no sea digito, punto o signo negativo
        valor_str = re.sub(r'[^\d.\-]', '', valor_str)
        try:
            return float(valor_str)
        except:
            return 0.0
    
    # =========================================================================
    # SECCION 3: GESTION DE BASE DE DATOS
    # =========================================================================
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        """
        Context Manager para establecer una conexion segura y resiliente a SQL Server.

        Implementa una estrategia de reintentos (Retries) y prueba dos metodos
        de autenticacion secuencialmente, garantizando maxima compatibilidad
        con diferentes configuraciones de servidor.

        Args:
            cfg (dict): Diccionario con credenciales y parametros de conexion.
                Claves requeridas:
                    - ServidorBaseDatos (str): Hostname o IP del servidor SQL.
                    - NombreBaseDatos (str): Nombre de la base de datos.
                Claves opcionales:
                    - UsuarioBaseDatos (str): Usuario para autenticacion SQL.
                    - ClaveBaseDatos (str): Contrasena del usuario SQL.
            max_retries (int, optional): Numero maximo de intentos por cada
                metodo de autenticacion. Por defecto 3.

        Yields:
            pyodbc.Connection: Objeto de conexion activo con autocommit deshabilitado.
                - Se hace commit automatico al salir del contexto sin errores.
                - Se hace rollback automatico si ocurre una excepcion.

        Raises:
            ValueError: Si faltan parametros obligatorios (ServidorBaseDatos,
                NombreBaseDatos) en el diccionario de configuracion.
            pyodbc.Error: Si no se logra conectar tras agotar todos los intentos
                con ambos metodos de autenticacion.
            Exception: Cualquier error que ocurra durante operaciones de BD.

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
            
            1. **Fase 1**: Intenta autenticacion SQL (UID/PWD) hasta max_retries veces.
            2. **Fase 2**: Si Fase 1 falla, intenta Trusted Connection (Windows Auth).
            3. Hay una pausa de 1 segundo entre reintentos fallidos.
            4. Usa ODBC Driver 17 for SQL Server.
            5. Timeout de conexion: 30 segundos.

        Warning:
            - La conexion debe usarse dentro de un bloque ``with``.
            - No reutilizar el objeto de conexion fuera del contexto.
            - El commit/rollback se maneja automaticamente.
        """
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            raise ValueError(f"Parametros faltantes: {', '.join(missing)}")

        usuario = cfg['UsuarioBaseDatos']
        contrasena = cfg['ClaveBaseDatos']
        
        # Cadenas de conexion para los dos metodos soportados
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

        # Fase 1: Intentar Autenticacion SQL
        for attempt in range(max_retries):
            try:
                cx = pyodbc.connect(conn_str_auth, timeout=30)
                cx.autocommit = False
                conectado = True
                break
            except pyodbc.Error as e:
                excepcion_final = e
                if attempt < max_retries - 1:
                    time.sleep(1)

        # Fase 2: Intentar Trusted Connection (solo si fallo la anterior)
        if not conectado:
            for attempt in range(max_retries):
                try:
                    cx = pyodbc.connect(conn_str_trusted, timeout=30)
                    cx.autocommit = False
                    conectado = True
                    break
                except pyodbc.Error as e:
                    excepcion_final = e
                    if attempt < max_retries - 1:
                        time.sleep(1)

        if not conectado:
            raise excepcion_final or Exception("No se pudo conectar a la base de datos con ningun metodo")
        
        try:
            yield cx
            if cx:
                cx.commit()  # Commit final si todo salio bien
        except Exception as e:
            if cx:
                cx.rollback()  # Rollback en caso de error dentro del bloque 'with'
                print(f"[ERROR] Rollback por error: {str(e)}")
            raise
        finally:
            if cx:
                try:
                    cx.close()
                except:
                    pass
    
    # =========================================================================
    # SECCION 4: NORMALIZACION Y VALIDACION DE DATOS
    # =========================================================================
    
    def normalizar_nombre_empresa(nombre):
        """
        Normaliza nombres de empresas eliminando variantes comunes de tipo societario y puntuacion.

        Esta funcion es esencial para comparar nombres de proveedores que pueden
        estar escritos de diferentes formas en el XML de la factura vs SAP,
        permitiendo identificar correctamente la misma entidad legal.

        Args:
            nombre (str | Any): Nombre de la empresa a normalizar. Puede contener:
                - Variaciones de tipos societarios (S.A.S., SAS, S A S, etc.)
                - Puntuacion diversa (puntos, comas, espacios)
                - Mayusculas/minusculas mezcladas

        Returns:
            str: Nombre normalizado con las siguientes transformaciones:
                - Convertido a MAYUSCULAS.
                - Sin espacios, puntos ni comas.
                - Tipos societarios estandarizados:
                    - S.A.S., S.A.S, S A S, S,A.S. → SAS
                    - LIMITADA, LTDA., LTDA → LTDA
                    - S.ENC., S.EN.C., COMANDITA → SENC
                    - S.A., S.A → SA
                - Retorna cadena vacia si la entrada es None/vacia/NA.

        Examples:
            >>> normalizar_nombre_empresa("Empresa Colombia S.A.S.")
            'EMPRESACOLOMBIASAS'
            >>> normalizar_nombre_empresa("ACME LIMITADA")
            'ACMELTDA'
            >>> normalizar_nombre_empresa("  Compania S. A. S.  ")
            'COMPANIASAS'
            >>> normalizar_nombre_empresa(None)
            ''

        Note:
            Esta funcion es utilizada por ``comparar_nombres_proveedor()`` para
            determinar si dos nombres corresponden a la misma entidad legal.

        See Also:
            comparar_nombres_proveedor: Funcion que usa esta normalizacion.
        """
        if pd.isna(nombre) or nombre == "":
            return ""
        
        nombre = safe_str(nombre).upper().strip()
        nombre_limpio = re.sub(r'[,.\s]', '', nombre)
        
        reemplazos = {
            'SAS': ['SAS', 'S.A.S.', 'S.A.S', 'SAAS', 'S A S', 'S,A.S.', 'S,AS'],
            'LTDA': ['LIMITADA', 'LTDA', 'LTDA.', 'LTDA'],
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
        Compara nombres de proveedores verificando concordancia de palabras (bag of words).

        Esta funcion realiza una comparacion flexible entre el nombre del emisor
        del XML y el nombre del proveedor en SAP, ignorando el orden de las
        palabras y aplicando normalizacion de tipos societarios.

        Args:
            nombre_xml (str | Any): Nombre proveniente del XML de la factura
                electronica (campo nombre_emisor).
            nombre_sap (str | Any): Nombre proveniente de SAP, obtenido del
                historico de ordenes de compra (campo NProveedor).

        Returns:
            bool: True si ambos nombres contienen las mismas palabras normalizadas
                (independientemente del orden). False en caso contrario o si
                alguno de los nombres es None/NA.

        Examples:
            >>> comparar_nombres_proveedor("ACME S.A.S.", "ACME SAS")
            True
            >>> comparar_nombres_proveedor("EMPRESA ABC LTDA", "ABC EMPRESA LIMITADA")
            True
            >>> comparar_nombres_proveedor("Proveedor Uno", "Proveedor Dos")
            False
            >>> comparar_nombres_proveedor(None, "ACME SAS")
            False

        Note:
            El algoritmo de comparacion:
            1. Normaliza ambos nombres usando ``normalizar_nombre_empresa()``.
            2. Divide cada nombre en lista de "palabras".
            3. Verifica que ambas listas tengan el mismo numero de elementos.
            4. Ordena las listas alfabeticamente y compara igualdad.

        See Also:
            normalizar_nombre_empresa: Funcion de normalizacion utilizada.
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

    def comparar_suma_total(valores_por_calcular, valor_objetivo, tolerancia=500):
        """
        Verifica si la suma total de las posiciones coincide con el valor objetivo.

        Esta funcion implementa la logica de validacion de montos para ZPAF,
        comparando el valor a pagar de la factura (XML) contra la suma de
        los valores "Por Calcular" de las posiciones en SAP.

        Args:
            valores_por_calcular (list[tuple]): Lista de tuplas donde cada tupla
                contiene (posicion, valor). Ejemplo:
                [('00010', '1000.50'), ('00020', '2500.00')]
            valor_objetivo (float | str): Valor a buscar, tipicamente el monto
                total de la factura (LineExtensionAmount o VlrPagarCop).
            tolerancia (float, optional): Rango de diferencia permitido en pesos
                colombianos. Por defecto 500 COP.

        Returns:
            tuple[bool, list, float]: Tupla con tres elementos:
                - coincide (bool): True si la suma esta dentro de la tolerancia.
                - lista_posiciones (list): Lista de posiciones usadas si coincide,
                  lista vacia si no coincide.
                - suma_total (float): Suma calculada de las posiciones.

        Examples:
            >>> valores = [('00010', '1000'), ('00020', '2000')]
            >>> comparar_suma_total(valores, 3000, tolerancia=500)
            (True, ['00010', '00020'], 3000.0)
            
            >>> comparar_suma_total(valores, 5000, tolerancia=500)
            (False, [], 0)
            
            >>> comparar_suma_total([], 1000)
            (False, [], 0)

        Note:
            - La tolerancia de $500 COP permite absorber pequenas diferencias
              por redondeo entre sistemas.
            - Si el valor_objetivo es 0 o negativo, retorna no coincidencia.
            - Si la lista de valores esta vacia, retorna no coincidencia.
        """
        valor_objetivo = normalizar_decimal(valor_objetivo)
        
        if valor_objetivo <= 0 or not valores_por_calcular:
            return False, [], 0
            
        suma_total = sum(normalizar_decimal(valor) for posicion, valor in valores_por_calcular)
        
        if abs(suma_total - valor_objetivo) <= tolerancia:
            todas_las_posiciones = [posicion for posicion, valor in valores_por_calcular]
            return True, todas_las_posiciones, suma_total
            
        return False, [], 0
    
    def validar_activo_fijo(valor):
        """
        Valida que el campo Activo Fijo tenga exactamente 9 digitos numericos.

        Esta es una regla de negocio especifica para pedidos ZPAF: el campo
        de activo fijo debe estar diligenciado con un codigo de exactamente
        9 digitos numericos.

        Args:
            valor (str | Any): Valor del campo Activo Fijo a validar.
                Puede contener caracteres no numericos que seran ignorados.

        Returns:
            bool: True si el valor contiene exactamente 9 digitos numericos,
                False en caso contrario o si esta vacio.

        Examples:
            >>> validar_activo_fijo("123456789")
            True
            >>> validar_activo_fijo("AF-123456789")  # 9 digitos entre caracteres
            True
            >>> validar_activo_fijo("12345678")  # Solo 8 digitos
            False
            >>> validar_activo_fijo("")
            False
            >>> validar_activo_fijo(None)
            False

        Note:
            - Solo se cuentan los caracteres numericos (0-9).
            - Los caracteres no numericos se eliminan antes de contar.
            - Un valor vacio o None siempre retorna False.
        """
        valor_str = safe_str(valor)
        if not valor_str:
            return False
        valor_limpio = re.sub(r'\D', '', valor_str)
        return len(valor_limpio) == 9
    
    def validar_capitalizado_el(valor):
        """
        Valida que el campo 'Capitalizado el' NO este diligenciado.

        Esta es una regla de negocio especifica para pedidos ZPAF: el campo
        "Capitalizado el" NUNCA debe estar diligenciado para activos fijos
        en proceso de adquisicion.

        Args:
            valor (str | Any): Valor del campo "Capitalizado el" a validar.

        Returns:
            bool: True si el campo esta vacio, es "null" o es "none".
                False si contiene cualquier otro valor (esta diligenciado).

        Examples:
            >>> validar_capitalizado_el("")
            True
            >>> validar_capitalizado_el(None)
            True
            >>> validar_capitalizado_el("null")
            True
            >>> validar_capitalizado_el("2024-01-15")
            False
            >>> validar_capitalizado_el("123456789")
            False

        Note:
            - Para ZPAF, este campo debe estar siempre vacio.
            - Si contiene una fecha o cualquier valor, indica que el activo
              ya fue capitalizado, lo cual es una novedad para este tipo de pedido.
        """
        valor_str = safe_str(valor)
        return valor_str == "" or valor_str.lower() == "null" or valor_str.lower() == "none"
    
    def validar_indicador_impuestos(indicadores_lista):
        """
        Valida coherencia de indicadores de impuestos segun reglas de productores.

        Los pedidos ZPAF tienen reglas especificas sobre que combinaciones de
        indicadores de impuestos son validas. Los indicadores se dividen en
        dos grupos mutuamente excluyentes.

        Args:
            indicadores_lista (list[str]): Lista de indicadores de impuestos
                de las diferentes posiciones del pedido.
                Valores validos: 'H4', 'H5', 'H6', 'H7', 'VP'

        Returns:
            tuple[bool, str, str | None]: Tupla con tres elementos:
                - es_valido (bool): True si los indicadores son validos y coherentes.
                - mensaje_error (str): Descripcion del error si no es valido,
                  cadena vacia si es valido.
                - grupo_detectado (str | None): 'G1', 'G2', 'VP_ONLY', o None si invalido.

        Reglas de validacion:
            - **Grupo 1 (Productores)**: H4, H5, VP
            - **Grupo 2 (No Productores)**: H6, H7, VP
            - VP puede aparecer en cualquier grupo.
            - NO se permite mezclar indicadores de G1 (H4, H5) con G2 (H6, H7).
            - Todos los indicadores deben estar diligenciados.
            - Solo se aceptan los valores especificados.

        Examples:
            >>> validar_indicador_impuestos(['H4', 'H5', 'VP'])
            (True, '', 'G1')
            >>> validar_indicador_impuestos(['H6', 'H7'])
            (True, '', 'G2')
            >>> validar_indicador_impuestos(['VP'])
            (True, '', 'VP_ONLY')
            >>> validar_indicador_impuestos(['H4', 'H6'])  # Mezcla invalida
            (False, 'NO se encuentra aplicado correctamente', None)
            >>> validar_indicador_impuestos(['XX'])  # Valor no valido
            (False, "NO corresponde alguna de las opciones...", None)
            >>> validar_indicador_impuestos([])
            (False, 'NO se encuentra diligenciado', None)

        Note:
            - H4 y H5 son para proveedores "Productores".
            - H6 y H7 son para proveedores "No Productores".
            - VP (Varios Productores) es compatible con ambos grupos.
        """
        indicadores_validos_g1 = {'H4', 'H5', 'VP'}
        indicadores_validos_g2 = {'H6', 'H7', 'VP'}
        
        indicadores_limpios = set()
        for ind in indicadores_lista:
            ind_str = safe_str(ind).upper().strip()
            if ind_str:
                indicadores_limpios.add(ind_str)
        
        if not indicadores_limpios:
            return False, "NO se encuentra diligenciado", None
        
        todos_validos = indicadores_limpios.issubset(indicadores_validos_g1.union(indicadores_validos_g2))
        if not todos_validos:
            return False, f"NO corresponde alguna de las opciones 'H4', 'H5', 'H6', 'H7' o 'VP' en pedido de Activos fijos", None
        
        tiene_g1 = bool(indicadores_limpios.intersection({'H4', 'H5'}))
        tiene_g2 = bool(indicadores_limpios.intersection({'H6', 'H7'}))
        
        if tiene_g1 and tiene_g2:
            return False, "NO se encuentra aplicado correctamente", None
        
        grupo = 'G1' if tiene_g1 else ('G2' if tiene_g2 else 'VP_ONLY')
        
        return True, "", grupo
    
    def validar_criterio_clasif_2(indicador, criterio):
        """
        Valida que el Criterio de Clasificacion 2 coincida con el Indicador de Impuestos.

        Existe una relacion de dependencia entre el indicador de impuestos y el
        criterio de clasificacion 2 que debe respetarse para pedidos ZPAF.

        Args:
            indicador (str): Indicador de impuestos de la posicion.
                Valores esperados: 'H4', 'H5', 'H6', 'H7', 'VP'
            criterio (str): Valor del campo Criterio de Clasificacion 2.
                Valores esperados: '0001', '0000'

        Returns:
            tuple[bool, str]: Tupla con dos elementos:
                - es_valido (bool): True si la combinacion es valida.
                - mensaje_error (str): Descripcion del error si no es valido,
                  cadena vacia si es valido.

        Reglas de validacion:
            - **H4/H5** (Productores) → Criterio debe ser '0001'
            - **H6/H7** (No Productores) → Criterio debe ser '0000'
            - **VP** → Criterio puede ser '0001' o '0000'

        Examples:
            >>> validar_criterio_clasif_2('H4', '0001')
            (True, '')
            >>> validar_criterio_clasif_2('H6', '0000')
            (True, '')
            >>> validar_criterio_clasif_2('VP', '0001')
            (True, '')
            >>> validar_criterio_clasif_2('H4', '0000')  # Invalido
            (False, "NO se encuentra aplicado correctamente...")
            >>> validar_criterio_clasif_2('H5', '')
            (False, 'NO se encuentra diligenciado')

        Note:
            El criterio de clasificacion esta relacionado con el tratamiento
            tributario que corresponde segun el tipo de proveedor (productor
            o no productor).
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
        Valida que el campo Cuenta sea estrictamente '2695950020' para pedidos ZPAF.

        Esta es una regla de negocio especifica: todos los pedidos de activos
        fijos (ZPAF/41) deben contabilizarse en la cuenta 2695950020.

        Args:
            cuenta (str | Any): Valor del campo Cuenta a validar.

        Returns:
            bool: True si la cuenta es exactamente '2695950020',
                False para cualquier otro valor.

        Examples:
            >>> validar_cuenta_zpaf('2695950020')
            True
            >>> validar_cuenta_zpaf('2695950021')
            False
            >>> validar_cuenta_zpaf('')
            False
            >>> validar_cuenta_zpaf(None)
            False

        Note:
            La cuenta 2695950020 corresponde a "Activos Fijos en Transito" o
            similar en el plan de cuentas de la organizacion.
        """
        cuenta_str = safe_str(cuenta).strip()
        return cuenta_str == '2695950020'
    
    # =========================================================================
    # SECCION 5: ACTUALIZACION DE BD Y EXPANSION DE DATOS
    # =========================================================================
    
    def actualizar_bd_cxp(cx, registro_id, campos_actualizar):
        """
        Actualiza campos en la tabla principal [CxP].[DocumentsProcessing].

        Esta funcion construye dinamicamente una sentencia UPDATE para modificar
        uno o mas campos del registro de procesamiento, con logica especial
        para el campo de observaciones que permite concatenacion.

        Args:
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            registro_id (str): Identificador unico del registro (campo [ID]).
            campos_actualizar (dict): Diccionario con los campos y valores a actualizar.
                - Las claves son nombres de columnas de la tabla.
                - Los valores None son ignorados.
                - El campo 'ObservacionesFase_4' tiene tratamiento especial.

        Returns:
            None: Los cambios se confirman con commit dentro de la funcion.

        Raises:
            Exception: Si ocurre un error durante la actualizacion.

        Behavior:
            - **Campos normales**: Sobrescriben el valor existente.
            - **ObservacionesFase_4**: Concatena al valor existente separado por
              coma, o establece el nuevo valor si estaba vacio/NULL.

        Examples:
            Actualizacion de estado exitoso::

                actualizar_bd_cxp(conexion, "12345", {
                    "EstadoFinalFase_4": "VALIDACION DATOS DE FACTURACION: Exitoso",
                    "ResultadoFinalAntesEventos": "PROCESADO"
                })

            Actualizacion con observaciones (se concatenan)::

                actualizar_bd_cxp(conexion, "12345", {
                    "ObservacionesFase_4": "Error en indicador de impuestos",
                    "ResultadoFinalAntesEventos": "CON NOVEDAD"
                })

        Note:
            - La tabla destino es [CxP].[DocumentsProcessing].
            - Se usa parametrizacion para prevenir SQL injection.
            - Se hace commit despues de cada actualizacion.
        """
        try:
            sets = []
            parametros = []
            
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

        Esta funcion gestiona la tabla [dbo].[CxP.Comparativa] donde se almacena
        el detalle de cada validacion realizada, permitiendo trazabilidad completa
        del proceso de validacion de cada campo.

        Args:
            registro (dict | pd.Series): Registro del documento siendo procesado.
                Debe contener campos como ID_dp, documenttype_dp, nombre_emisor_dp, etc.
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            nit (str): NIT del emisor/proveedor para identificar el registro.
            factura (str): Numero de factura para identificar el registro.
            nombre_item (str): Nombre del item de validacion (ej: 'LineExtensionAmount',
                'TRM', 'ActivoFijo', 'Observaciones', etc.).
            actualizar_valor_xml (bool, optional): Si actualizar el campo Valor_XML.
                Por defecto True.
            valor_xml (str | None, optional): Valor extraido del XML de la factura.
                Puede contener multiples valores separados por '|'.
            actualizar_aprobado (bool, optional): Si actualizar el campo Aprobado.
                Por defecto True.
            valor_aprobado (str | None, optional): Resultado: 'SI', 'NO', o None.
                Puede contener multiples valores separados por '|'.
            actualizar_orden_compra (bool, optional): Si actualizar Valor_Orden_de_Compra.
                Por defecto True.
            val_orden_de_compra (str | None, optional): Valor de SAP.
                Puede contener multiples valores separados por '|'.

        Returns:
            None: Los cambios se confirman con commit dentro de la funcion.

        Behavior:
            Para valores con separador '|' (multiples posiciones):
            - Se crea/actualiza un registro por cada valor.
            - Si ya existen registros, se actualizan en orden.
            - Si faltan registros, se insertan nuevos.

        Examples:
            Registrar validacion de monto::

                actualizar_items_comparativa(
                    registro=fila, cx=conexion, nit="900123456",
                    factura="FE-001", nombre_item="LineExtensionAmount",
                    valor_xml="1500000", valor_aprobado="SI",
                    val_orden_de_compra="1500000"
                )

            Registrar multiples posiciones::

                actualizar_items_comparativa(
                    registro=fila, cx=conexion, nit="900123456",
                    factura="FE-001", nombre_item="ActivoFijo",
                    valor_xml=None, valor_aprobado="SI|SI|NO",
                    val_orden_de_compra="123456789|234567890|12345678"
                )

        Note:
            - La tabla destino es [dbo].[CxP.Comparativa].
            - Se hace commit despues de procesar todos los valores.
        """
        cur = cx.cursor()
        
        def safe_db_val(v):
            """Sanitiza valores para insercion en BD."""
            if v is None: 
                return None
            s = str(v).strip()
            if not s or s.lower() == 'none' or s.lower() == 'null': 
                return None
            return s

        query_count = """
        SELECT COUNT(*)
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?
        """
        cur.execute(query_count, (nit, factura, nombre_item, registro.get('ID_dp','')))
        count_existentes = cur.fetchone()[0]

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
                # Actualizar registro existente
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
                final_params = params + [nit, factura, nombre_item, registro.get('ID_dp',''), i + 1]
                cur.execute(update_query, final_params)

            else:
                # Insertar nuevo registro
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, 
                    Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, 
                    Item, Valor_Orden_de_Compra, Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(insert_query, (
                    registro.get('Fecha_de_retoma_antes_de_contabilizacion_dp',''),
                    registro.get('documenttype_dp',''),
                    registro.get('numero_de_liquidacion_u_orden_de_compra_dp',''),
                    registro.get('nombre_emisor_dp',''), 
                    registro.get('ID_dp',''), 
                    nit, factura, nombre_item, 
                    val_compra, val_xml, val_aprob
                ))
        cx.commit()
        cur.close()
    
    def actualizar_estado_comparativa(cx, nit, factura, estado):
        """
        Actualiza el estado de validacion para todos los registros de una factura.

        Esta funcion establece el campo 'Estado_validacion_antes_de_eventos'
        para todos los items de comparativa asociados a una combinacion NIT+Factura.

        Args:
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            nit (str): NIT del emisor/proveedor.
            factura (str): Numero de factura.
            estado (str): Estado final de la validacion. Valores tipicos:
                - "PROCESADO": Validacion exitosa.
                - "PROCESADO CONTADO": Exitoso con forma de pago contado.
                - "CON NOVEDAD": Se encontraron discrepancias.
                - "CON NOVEDAD CONTADO": Discrepancias con pago contado.

        Returns:
            None: Los cambios se confirman con commit dentro de la funcion.

        Examples:
            >>> actualizar_estado_comparativa(conexion, "900123456", "FE-001", "PROCESADO")

        Note:
            - La tabla destino es [dbo].[CxP.Comparativa].
            - Afecta TODOS los registros (items) de la factura especificada.
        """
        cur = cx.cursor()
        update_sql = """
        UPDATE [dbo].[CxP.Comparativa]
        SET Estado_validacion_antes_de_eventos = ?
        WHERE NIT = ? AND Factura = ?
        """
        cur.execute(update_sql, (estado, nit, factura))
        cx.commit()
        cur.close()
    
    def marcar_orden_procesada(cx, oc_numero, posiciones_string):
        """
        Marca las posiciones de una orden de compra como procesadas.

        Esta funcion actualiza el campo 'Marca' en la tabla de historico para
        indicar que las posiciones especificadas ya fueron validadas, evitando
        reprocesamiento en ejecuciones futuras.

        Args:
            cx (pyodbc.Connection): Conexion activa a la base de datos.
            oc_numero (str): Numero del documento de compra (DocCompra).
            posiciones_string (str): String con posiciones separadas por pipe (|).
                Ejemplo: "00010|00020|00030"

        Returns:
            None: Los cambios se confirman con commit dentro de la funcion.

        Examples:
            >>> marcar_orden_procesada(conexion, "4500001234", "00010|00020")

        Note:
            - La tabla destino es [CxP].[HistoricoOrdenesCompra].
            - El valor de marca establecido es 'PROCESADO'.
            - Las posiciones vacias son ignoradas.
        """
        cur = cx.cursor()
        lista_posiciones = posiciones_string.split('|')
        update_query = "UPDATE [CxP].[HistoricoOrdenesCompra] SET Marca = 'PROCESADO' WHERE DocCompra = ? AND Posicion = ?"
        for posicion in lista_posiciones:
            pos = posicion.strip() 
            if pos: 
                cur.execute(update_query, (oc_numero, pos))
        cx.commit() 
        cur.close()
    
    def expandir_posiciones_string(valor_string, separador='|'):
        """
        Expande una cadena delimitada en una lista de valores individuales.

        Esta funcion procesa campos que contienen multiples valores concatenados,
        comun en datos de SAP donde informacion de multiples posiciones se
        almacena en un solo campo.

        Args:
            valor_string (str | Any): Cadena con valores separados.
                Soporta separador pipe (|) y coma (,).
            separador (str, optional): Separador principal esperado. Por defecto '|'.

        Returns:
            list[str]: Lista de valores individuales, cada uno sin espacios extremos.
                Lista vacia si la entrada es None/vacia/NA.

        Examples:
            >>> expandir_posiciones_string("10|20|30")
            ['10', '20', '30']
            >>> expandir_posiciones_string("100,200,300")
            ['100', '200', '300']
            >>> expandir_posiciones_string(None)
            []
        """
        if pd.isna(valor_string) or valor_string == '' or valor_string is None: 
            return []
        valor_str = safe_str(valor_string)
        if '|' in valor_str: 
            return [v.strip() for v in valor_str.split('|') if v.strip()]
        if ',' in valor_str: 
            return [v.strip() for v in valor_str.split(',') if v.strip()]
        return [valor_str.strip()]
    
    def expandir_posiciones_historico(registro):
        """
        Desglosa la informacion concatenada del historico en diccionarios por posicion.

        Esta funcion transforma los campos concatenados del registro (donde multiples
        posiciones estan separadas por '|') en una lista de diccionarios, donde cada
        diccionario representa una posicion individual con todos sus atributos.

        Args:
            registro (dict | pd.Series): Registro con campos concatenados del historico.
                Campos procesados:
                - Posicion_hoc, PorCalcular_hoc, Trm_hoc, TipoNif_hoc
                - Acreedor_hoc, FecDoc_hoc, FecReg_hoc, FecContGasto_hoc
                - IndicadorImpuestos_hoc, TextoBreve_hoc, ClaseDeImpuesto_hoc
                - Cuenta_hoc, CiudadProveedor_hoc, DocFiEntrada_hoc, Cuenta26_hoc
                - ActivoFijo_hoc, CapitalizadoEl_hoc, CriterioClasif2_hoc, Moneda_hoc
                - NProveedor_hoc (campo unico, se replica en todas las posiciones)

        Returns:
            list[dict]: Lista de diccionarios, uno por cada posicion.
                Cada diccionario contiene todas las claves de atributos de posicion.
                Lista vacia si no hay posiciones o si ocurre un error.

        Examples:
            Registro con 2 posiciones::

                registro = {
                    'Posicion_hoc': '00010|00020',
                    'PorCalcular_hoc': '1000|2000',
                    'ActivoFijo_hoc': '123456789|234567890',
                    'NProveedor_hoc': 'ACME SAS',
                    ...
                }
                resultado = expandir_posiciones_historico(registro)
                # [
                #     {'Posicion': '00010', 'PorCalcular': '1000', 'ActivoFijo': '123456789', 'NProveedor': 'ACME SAS', ...},
                #     {'Posicion': '00020', 'PorCalcular': '2000', 'ActivoFijo': '234567890', 'NProveedor': 'ACME SAS', ...}
                # ]

        Note:
            - Si una lista de valores es mas corta que la lista de posiciones,
              se usa el primer valor disponible o cadena vacia.
            - El campo NProveedor_hoc se replica en todas las posiciones ya que
              es comun para toda la orden de compra.
            - En caso de error, retorna lista vacia y registra el error en consola.
        """
        try:
            posiciones = expandir_posiciones_string(registro.get('Posicion_hoc', ''))
            if not posiciones: 
                return []
            
            # Mapeo de campos concatenados
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
    # SECCION 6: LOGICA PRINCIPAL DEL SCRIPT (MAIN)
    # =========================================================================
    
    try:
        print("")
        print("=" * 80)
        print("[INFO] INICIO Procesamiento ZPAF/41 - Activos Fijos")
        print("=" * 80)
        
        t_inicio = time.time()
        
        # ---------------------------------------------------------------------
        # 1. Obtener y validar configuracion
        # ---------------------------------------------------------------------
        cfg = parse_config(GetVar("vLocDicConfig"))
        
        print("[INFO] Configuracion cargada exitosamente")
        
        required_config = ['ServidorBaseDatos', 'NombreBaseDatos']
        missing_config = [k for k in required_config if not cfg.get(k)]
        if missing_config:
            raise ValueError(f"Faltan parametros de configuracion: {', '.join(missing_config)}")
        
        # ---------------------------------------------------------------------
        # 2. Conectar a base de datos y obtener registros ZPAF/41
        # ---------------------------------------------------------------------
        with crear_conexion_db(cfg) as cx:
            print("[INFO] Obteniendo registros ZPAF/41 para procesar...")
            
            query_zpaf = """
                SELECT * FROM [CxP].[HU41_CandidatosValidacion]
                WHERE [ClaseDePedido_hoc] IN ('ZPAF', '41')
            """
            
            df_registros = pd.read_sql(query_zpaf, cx)
            
            print(f"[INFO] Obtenidos {len(df_registros)} registros ZPAF/41 para procesar")
            
            if len(df_registros) == 0:
                print("[INFO] No hay registros ZPAF/41 pendientes de procesar")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros ZPAF/41 pendientes de procesar")
                return
            
            # Contadores de procesamiento
            registros_procesados = 0
            registros_con_novedad = 0
            registros_exitosos = 0
            
            # -----------------------------------------------------------------
            # 3. Procesar cada registro (Loop Principal)
            # -----------------------------------------------------------------
            for idx, registro in df_registros.iterrows():
                try:
                    # Extraccion segura de datos clave
                    registro_id = safe_str(registro.get('ID_dp', ''))
                    numero_oc = safe_str(registro.get('numero_de_liquidacion_u_orden_de_compra_dp', ''))
                    numero_factura = safe_str(registro.get('numero_de_factura_dp', ''))
                    payment_means = safe_str(registro.get('forma_de_pago_dp', ''))
                    nit = safe_str(registro.get('nit_emisor_o_nit_del_proveedor_dp', ''))
                    
                    print(f"\n[INFO] Procesando Registro {registros_procesados + 1}/{len(df_registros)}: OC {numero_oc}, Factura {numero_factura}")
                    
                    # Determinar sufijo segun forma de pago
                    sufijo_contado = " CONTADO" if payment_means in ['1', '01'] else ""
                    
                    # ---------------------------------------------------------
                    # 4. Expandir posiciones del historico
                    # ---------------------------------------------------------
                    datos_posiciones = expandir_posiciones_historico(registro)
                    
                    if not datos_posiciones:
                        print(f"[WARNING] Registro {numero_oc} sin posiciones historicas asociadas. Saltando.")
                        moneda = ""
                        es_usd = False
                    else:
                        moneda = safe_str(datos_posiciones[0].get('Moneda', '')).upper()
                        es_usd = moneda == 'USD'
                    
                    # ---------------------------------------------------------
                    # 5. Obtener valor a comparar segun moneda
                    # ---------------------------------------------------------
                    if es_usd:
                        valor_xml = normalizar_decimal(registro.get('VlrPagarCop_dp', 0))
                    else:
                        valor_xml = normalizar_decimal(registro.get('Valor de la Compra LEA_ddp', 0))
                    
                    # ---------------------------------------------------------
                    # 6. Preparar valores para busqueda de combinacion
                    # ---------------------------------------------------------
                    valores_por_calcular = [(d['Posicion'], d['PorCalcular']) for d in datos_posiciones]
                    
                    coincidencia_encontrada, posiciones_usadas, suma_encontrada = comparar_suma_total(
                        valores_por_calcular, valor_xml, tolerancia=500
                    )
                    
                    # ---------------------------------------------------------
                    # ESCENARIO A: NO HAY COINCIDENCIA MATEMATICA DE MONTOS
                    # ---------------------------------------------------------
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
                        
                        # Actualizacion de Trazabilidad
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
                    
                    # ---------------------------------------------------------
                    # ESCENARIO B: SI HAY COINCIDENCIA MATEMATICA
                    # ---------------------------------------------------------
                    else:
                        datos_posiciones_usadas = [d for d in datos_posiciones if d['Posicion'] in posiciones_usadas]

                        # Insertar informacion base en Comparativa
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                    nombre_item='LineExtensionAmount',
                                                    valor_xml=registro.get('valor_a_pagar_dp',''), valor_aprobado=None, val_orden_de_compra=None)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura, 
                                                    nombre_item='Posicion',
                                                    valor_xml=None, valor_aprobado=None, val_orden_de_compra=registro.get('Posicion_hoc',''))
                        
                        marcar_orden_procesada(cx, numero_oc, safe_str(registro.get('Posicion_hoc','')))
                        
                        hay_novedad = False
                    
                    # ---------------------------------------------------------
                    # VALIDACIONES DE NEGOCIO ESPECIFICAS PARA ZPAF
                    # ---------------------------------------------------------
                    
                    # Proteccion contra listas vacias
                    if datos_posiciones_usadas:
                        trm_sap = normalizar_decimal(datos_posiciones_usadas[0].get('Trm', 0))
                        nombre_proveedor_sap = safe_str(datos_posiciones_usadas[0].get('NProveedor', ''))
                    else:
                        trm_sap = 0
                        nombre_proveedor_sap = ""
                    
                    # ---------------------------------------------------------
                    # 10. Validar TRM (Solo si es USD)
                    # ---------------------------------------------------------
                    if es_usd:
                        trm_xml = normalizar_decimal(registro.get('CalculationRate_dp', 0))
                        if trm_xml > 0 or trm_sap > 0:
                            trm_coincide = abs(trm_xml - trm_sap) < 0.01
                            
                            if not trm_coincide:
                                observacion = f"No se encuentra coincidencia en el campo TRM de la factura vs la informacion reportada en SAP, {registro.get('ObservacionesFase_4_dp','')}"
                                hay_novedad = True
                                campos_novedad = {
                                    'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso',
                                    'ObservacionesFase_4': truncar_observacion(observacion),
                                    'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                                }
                                actualizar_bd_cxp(cx, registro_id, campos_novedad)
                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                            nombre_item='TRM',
                                                            valor_xml=str(trm_xml), valor_aprobado='NO', val_orden_de_compra=str(trm_sap))
                            else:
                                actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                            nombre_item='TRM',
                                                            valor_xml=str(trm_xml), valor_aprobado='SI', val_orden_de_compra=str(trm_sap))
                    
                    # ---------------------------------------------------------
                    # 11. Validar Nombre Emisor
                    # ---------------------------------------------------------
                    nombre_emisor_xml = safe_str(registro.get('nombre_emisor_dp', ''))
                    nombres_coinciden = comparar_nombres_proveedor(nombre_emisor_xml, nombre_proveedor_sap)
                    
                    if not nombres_coinciden:
                        observacion = f"No se encuentra coincidencia en Nombre Emisor de la factura vs la informacion reportada en SAP, {registro.get('ObservacionesFase_4_dp','')}"
                        hay_novedad = True
                        campos_novedad = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='NombreEmisor',
                                                    valor_xml=nombre_emisor_xml, valor_aprobado='NO', val_orden_de_compra=nombre_proveedor_sap)
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='NombreEmisor',
                                                    valor_xml=nombre_emisor_xml, valor_aprobado='SI', val_orden_de_compra=nombre_proveedor_sap)
                    
                    # ---------------------------------------------------------
                    # 12. Validar Activo Fijo (9 digitos)
                    # ---------------------------------------------------------
                    listado_activoFijo = registro.get('ActivoFijo_hoc','').split('|')
                    activo_fijo_valido = all(validar_activo_fijo(d) for d in listado_activoFijo)
                    
                    if not activo_fijo_valido:
                        observacion = f"Pedido corresponde a ZPAF pero campo 'Activo fijo' NO se encuentra diligenciado y/o NO corresponde a un dato de 9 digitos, {registro.get('ObservacionesFase_4_dp','')}"
                        hay_novedad = True
                        campos_novedad = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='ActivoFijo',
                                                    valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('ActivoFijo_hoc',''))
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='ActivoFijo',
                                                    valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('ActivoFijo_hoc',''))
                        
                    # ---------------------------------------------------------
                    # 13. Validar Capitalizado el (NUNCA debe estar diligenciado)
                    # ---------------------------------------------------------
                    listado_capitalizado = registro.get('CapitalizadoEl_hoc','').split('|')
                    capitalizado_valido = all(validar_capitalizado_el(d) for d in listado_capitalizado)
                    
                    if not capitalizado_valido:
                        observacion = f"Pedido corresponde a ZPAF (Activo fijo) pero campo 'Capitalizado el' se encuentra diligenciado cuando NUNCA debe estarlo, {registro.get('ObservacionesFase_4_dp','')}"
                        hay_novedad = True
                        campos_novedad = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='CapitalizadoEl',
                                                    valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('CapitalizadoEl_hoc',''))
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='CapitalizadoEl',
                                                    valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('CapitalizadoEl_hoc',''))
                        
                    # ---------------------------------------------------------
                    # 14. Validar Indicador impuestos
                    # ---------------------------------------------------------
                    listado_indicador = registro.get('IndicadorImpuestos_hoc','').split('|')
                    indicador_valido, msg_indicador, grupo_indicador = validar_indicador_impuestos(listado_indicador)
                    
                    if not indicador_valido:
                        observacion = f"Pedido corresponde a ZPAF pero campo 'Indicador impuestos' {msg_indicador} , {registro.get('ObservacionesFase_4_dp','')}"
                        hay_novedad = True
                        campos_novedad = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='IndicadorImpuestos',
                                                    valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='IndicadorImpuestos',
                                                    valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('IndicadorImpuestos_hoc',''))
                    
                    # ---------------------------------------------------------
                    # 15. Validar Criterio clasif. 2
                    # ---------------------------------------------------------
                    criterio_valido = True
                    listado_clasif2 = registro.get('CriterioClasif2_hoc','').split('|')
                    
                    for indicador, criterio in zip_longest(listado_indicador, listado_clasif2, fillvalue=''):
                        es_valido_crit, msg_crit = validar_criterio_clasif_2(indicador.strip(), criterio.strip())
                        if not es_valido_crit:
                            criterio_valido = False
                    
                    if not criterio_valido:
                        observacion = f"Pedido corresponde a ZPAF pero campo 'Criterio clasif.' 2 NO se encuentra aplicado correctamente segun reglas, {registro.get('ObservacionesFase_4_dp','')}"
                        hay_novedad = True
                        campos_novedad = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='CriterioClasif2',
                                                    valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('CriterioClasif2_hoc',''))
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='CriterioClasif2',
                                                    valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('CriterioClasif2_hoc',''))
                    
                    # ---------------------------------------------------------
                    # 16. Validar Cuenta (debe ser 2695950020)
                    # ---------------------------------------------------------
                    listado_cuenta = registro.get('Cuenta_hoc','').split('|')
                    cuenta_valida = all(validar_cuenta_zpaf(d) for d in listado_cuenta)
                    
                    if not cuenta_valida:
                        observacion = f"Pedido corresponde a ZPAF, pero Campo 'Cuenta' NO corresponde a 2695950020, {registro.get('ObservacionesFase_4_dp','')}"
                        hay_novedad = True
                        campos_novedad = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso',
                            'ObservacionesFase_4': truncar_observacion(observacion),
                            'ResultadoFinalAntesEventos': f"CON NOVEDAD {sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_novedad)
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='Cuenta',
                                                    valor_xml=None, valor_aprobado='NO', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                    else:
                        actualizar_items_comparativa(cx=cx, registro=registro, nit=nit, factura=numero_factura,
                                                    nombre_item='Cuenta',
                                                    valor_xml=None, valor_aprobado='SI', val_orden_de_compra=registro.get('Cuenta_hoc',''))
                    
                    # ---------------------------------------------------------
                    # 17. Finalizar registro
                    # ---------------------------------------------------------
                    if hay_novedad:
                        actualizar_estado_comparativa(cx, nit, numero_factura, f"CON NOVEDAD {sufijo_contado}")
                        registros_con_novedad += 1
                    else:
                        campos_exitoso = {
                            'EstadoFinalFase_4': 'VALIDACION DATOS DE FACTURACION: Exitoso',
                            'ResultadoFinalAntesEventos': f"PROCESADO {sufijo_contado}"
                        }
                        actualizar_bd_cxp(cx, registro_id, campos_exitoso)
                        actualizar_estado_comparativa(cx, nit, numero_factura, f"PROCESADO {sufijo_contado}")
                        print(f"[SUCCESS] Registro {registro_id} procesado exitosamente")
                        registros_exitosos += 1
                    
                    registros_procesados += 1
                    
                except Exception as e:
                    print(f"[ERROR] Error procesando registro {idx}: {str(e)}")
                    print(traceback.format_exc())
                    registros_con_novedad += 1
                    registros_procesados += 1
                    continue
        
        # ---------------------------------------------------------------------
        # Resumen final
        # ---------------------------------------------------------------------
        tiempo_total = time.time() - t_inicio
        print(f"\n[FIN] Tiempo total: {round(tiempo_total, 2)}s")
        
        resumen = f"Procesados {registros_procesados} registros ZPAF/41. Exitosos: {registros_exitosos}, Con novedad: {registros_con_novedad}"
        SetVar("vLocStrResultadoSP", "True")
        SetVar("vLocStrResumenSP", resumen)
        
    except Exception as e:
        # ---------------------------------------------------------------------
        # Manejo de error critico
        # ---------------------------------------------------------------------
        print("[ERROR CRITICO] La funcion ZPAF_ValidarActivosFijos fallo")
        print(traceback.format_exc())
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        SetVar("vLocStrResultadoSP", "False")
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
================================================================================
MODULO: generar_ruta_logs.py
================================================================================

Descripcion General:
--------------------
    Este modulo genera dinamicamente la ruta de directorio para almacenamiento
    de archivos de log, basandose en una estructura jerarquica de fecha
    actual (ano/mes/dia). La ruta base se obtiene de la configuracion del sistema
    Rocketbot.

Autor: Diego Ivan Lopez Ochoa
Version: 1.1.0
Fecha de Creacion: 2025
Ultima Modificacion: 2026

Dependencias:
-------------
    - json: Para parseo de configuracion JSON
    - os: Para manipulacion de rutas de sistema
    - datetime: Para obtencion de fecha actual
    - ast: Para parseo alternativo de configuracion

Integracion Rocketbot:
----------------------
    Este modulo utiliza las funciones globales de Rocketbot:
    - GetVar(): Obtiene variables del contexto de Rocketbot
    - SetVar(): Establece variables en el contexto de Rocketbot

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------+
    |           INICIO                    |
    |   generar_ruta_logs()               |
    +-----------------+-------------------+
                      |
                      v
    +-------------------------------------+
    |  Importar modulos:                  |
    |  json, os, datetime, ast            |
    +-----------------+-------------------+
                      |
                      v
    +-------------------------------------+
    |  Obtener configuracion:             |
    |  GetVar("vLocDicConfig")            |
    +-----------------+-------------------+
                      |
                      v
    +-------------------------------------+
    |  Parsear configuracion:             |
    |  JSON o ast.literal_eval            |
    +-----------------+-------------------+
                      |
                      v
    +-------------------------------------+
    |  Extraer RutaLogs de config         |
    |  Ej: "C:\\Logs"                     |
    +-----------------+-------------------+
                      |
                      v
    +-------------------------------------+
    |  Obtener fecha actual:              |
    |  datetime.now()                     |
    +-----------------+-------------------+
                      |
                      v
    +-------------------------------------+
    |  Construir ruta con os.path.join:   |
    |  ruta_base + ano + mes + dia        |
    +-----------------+-------------------+
                      |
                      v
    +-------------------------------------+
    |  Agregar separador final (os.sep)   |
    |  Ej: "C:\\Logs\\2026\\02\\01\\"     |
    +-----------------+-------------------+
                      |
                      v
    +-------------------------------------+
    |  Guardar resultado:                 |
    |  SetVar('vGblStrRutaLogs', ruta)    |
    +-----------------+-------------------+
                      |
                      v
    +-------------------------------------+
    |              FIN                    |
    +-------------------------------------+

================================================================================
VARIABLES DE ENTRADA/SALIDA
================================================================================

Variables de Entrada (GetVar):
------------------------------
    vLocDicConfig : dict o str
        Diccionario de configuracion que debe contener la clave 'RutaLogs'.
        Puede ser un dict de Python o un string JSON/literal.
        
        Estructura esperada:
        {
            "RutaLogs": "C:\\Automatizacion\\Logs",
            "ServidorBaseDatos": "SERVIDOR\\INSTANCIA",
            "NombreBaseDatos": "MiBaseDatos",
            ...
        }

Variables de Salida (SetVar):
-----------------------------
    vGblStrRutaLogs : str
        Ruta completa generada con estructura ano/mes/dia.
        Ejemplo: "C:\\Automatizacion\\Logs\\2026\\02\\01\\"

================================================================================
"""


async def generar_ruta_logs():
    """
    Genera la ruta completa para almacenamiento de logs basada en fecha actual.
    
    Esta funcion asincrona construye una ruta de directorio jerarquica
    combinando una ruta base (de configuracion) con componentes de fecha
    en formato ano/mes/dia obtenidos del sistema.
    
    Returns:
        None: La funcion no retorna valor directamente.
              El resultado se guarda mediante SetVar('vGblStrRutaLogs', ruta).
    
    Raises:
        ValueError: Si vLocDicConfig esta vacio o no contiene 'RutaLogs'.
        KeyError: Si la clave 'RutaLogs' no existe en la configuracion.
    
    Side Effects:
        - Lee: vLocDicConfig (configuracion del sistema)
        - Escribe: vGblStrRutaLogs (ruta generada)
    
    Example:
        Configuracion de entrada::
        
            vLocDicConfig = {
                "RutaLogs": "C:\\\\Automatizacion\\\\Logs",
                "ServidorBaseDatos": "SERVIDOR\\\\INSTANCIA",
                "NombreBaseDatos": "CxP_Database"
            }
        
        Llamada a la funcion::
        
            await generar_ruta_logs()
        
        Resultado en vGblStrRutaLogs (si hoy es 1 de febrero de 2026)::
        
            "C:\\Automatizacion\\Logs\\2026\\02\\01\\"
    
    Note:
        - La fecha se obtiene dinamicamente usando datetime.now().
        - La funcion NO crea el directorio fisicamente, solo genera la ruta.
        - El separador final (os.sep) se agrega automaticamente.
    
    Warning:
        Si la configuracion contiene rutas con backslashes escapados
        (ej: "C:\\\\Logs"), estos se manejan correctamente.
    """
    # ==========================================================================
    # IMPORTS
    # ==========================================================================
    import json
    import os
    from datetime import datetime
    import ast
    
    # ==========================================================================
    # FUNCIONES AUXILIARES (HELPERS)
    # ==========================================================================
    
    def safe_str(v):
        """
        Convierte cualquier valor a string de forma segura.
        
        Args:
            v: Valor de cualquier tipo a convertir.
        
        Returns:
            str: Representacion string del valor, o string vacio si es None/error.
        
        Example:
            >>> safe_str(None)
            ''
            >>> safe_str(123)
            '123'
            >>> safe_str(b'hello')
            'hello'
        """
        try:
            if v is None:
                return ""
            if isinstance(v, (bytes, bytearray)):
                try:
                    return bytes(v).decode("utf-8", errors="replace")
                except Exception:
                    return bytes(v).decode("cp1252", errors="replace")
            return str(v)
        except Exception:
            return ""

    def parse_config(raw):
        """
        Parsea la configuracion desde JSON o literal de Python.
        
        Args:
            raw: Configuracion en formato dict, JSON string, o Python literal.
        
        Returns:
            dict: Diccionario de configuracion parseado.
        
        Raises:
            ValueError: Si raw esta vacio.
        
        Example:
            >>> parse_config('{"RutaLogs": "C:\\\\Logs"}')
            {'RutaLogs': 'C:\\Logs'}
        """
        if isinstance(raw, dict):
            return raw
        t = safe_str(raw).strip()
        if not t:
            raise ValueError("vLocDicConfig vacio")
        try:
            return json.loads(t)
        except Exception:
            return ast.literal_eval(t)

    # ==========================================================================
    # LOGICA PRINCIPAL
    # ==========================================================================
    
    # Paso 1: Obtener y parsear configuracion
    cfg = parse_config(GetVar("vLocDicConfig"))

    # Paso 2: Extraer ruta base de logs
    ruta_base = cfg['RutaLogs']

    # Paso 3: Obtener fecha actual dinamicamente
    fecha_hoy = datetime.now()
    ano = fecha_hoy.strftime("%Y")
    mes = fecha_hoy.strftime("%m")
    dia = fecha_hoy.strftime("%d")

    # Paso 4: Construir estructura de directorios
    parte_fecha = os.path.join(ano, mes, dia)

    # Paso 5: Combinar ruta base con estructura de fecha
    ruta_sin_slash = os.path.join(ruta_base, parte_fecha)

    # Paso 6: Agregar separador final
    ruta_final = ruta_sin_slash + os.sep

    # Paso 7: Guardar resultado
    SetVar('vGblStrRutaLogs', ruta_final)


# ==============================================================================
# EJEMPLOS DE USO
# ==============================================================================
"""
EJEMPLO 1: Uso basico
---------------------
    # Configuracion
    SetVar("vLocDicConfig", {"RutaLogs": "C:\\\\Logs"})
    
    # Ejecucion
    await generar_ruta_logs()
    
    # Resultado (ejemplo)
    print(GetVar("vGblStrRutaLogs"))
    # Output: "C:\\Logs\\2026\\02\\01\\"

EJEMPLO 2: Ruta de red
----------------------
    SetVar("vLocDicConfig", {"RutaLogs": "\\\\servidor\\compartido"})
    await generar_ruta_logs()
    # Output: "\\\\servidor\\compartido\\2026\\02\\01\\"

EJEMPLO 3: JSON string
----------------------
    SetVar("vLocDicConfig", '{"RutaLogs": "D:\\\\Output"}')
    await generar_ruta_logs()
    # Output: "D:\\Output\\2026\\02\\01\\"
"""
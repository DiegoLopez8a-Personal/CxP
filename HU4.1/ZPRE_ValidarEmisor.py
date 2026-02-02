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

def ZPRE_ValidarEmisor():
    import json, ast, traceback, pyodbc, pandas as pd, numpy as np
    from datetime import datetime
    from contextlib import contextmanager
    import time, warnings, unicodedata
    warnings.filterwarnings('ignore')
    
    def safe_str(v):
        if v is None: return ""
        if isinstance(v, str): return v.strip()
        if isinstance(v, (int, float)):
            if isinstance(v, float) and np.isnan(v): return ""
            return str(v)
        try: return str(v).strip()
        except: return ""
    
    # CORRECCIÓN: Función para truncar observaciones
    def truncar_observacion(obs):
        """Truncar observación a 3900 caracteres para prevenir overflow"""
        if not obs: return ""
        obs_str = safe_str(obs)
        return obs_str[:3900] if len(obs_str) > 3900 else obs_str
    
    def parse_config(raw):
        if isinstance(raw, dict): return raw
        try: return json.loads(safe_str(raw))
        except: return ast.literal_eval(safe_str(raw))
    
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
    
    
    def split_valores(v):
        if not v or pd.isna(v): return []
        return [x.strip() for x in str(v).split('|') if x.strip()]
    
    def contiene(campo, val):
        return val in split_valores(campo)
    
    def normalizar(texto):
        if not texto: return ""
        t = ''.join(c for c in unicodedata.normalize('NFD', texto.upper()) if unicodedata.category(c) != 'Mn')
        return ''.join(c if c.isalnum() or c.isspace() else '' for c in t).strip()
    
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))
        stats = {'total': 0, 'aprobados': 0, 'con_novedad': 0}
        
        with crear_conexion_db(cfg) as cx:
            df = pd.read_sql("""
            SELECT nit_emisor_o_nit_del_proveedor_dp, numero_de_factura_dp,
                   numero_de_liquidacion_u_orden_de_compra_dp, forma_de_pago_dp,
                   ClaseDePedido_hoc, nombre_emisor_dp, Acreedor_hoc
            FROM [CxP].[HU41_CandidatosValidacion] WITH (NOLOCK)
            """, cx)
            
            if df.empty:
                SetVar("vLocStrResultadoSP", "True")
                return True, "No hay registros", None, stats
            
            mask = df['ClaseDePedido_hoc'].apply(
                lambda x: contiene(x, 'ZPRE') or contiene(x, '45') if pd.notna(x) else False
            )
            df = df[mask].copy()
            stats['total'] = len(df)
            
            for idx, row in df.iterrows():
                try:
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    forma_pago = safe_str(row['forma_de_pago_dp'])
                    
                    nombre_dp = normalizar(safe_str(row['nombre_emisor_dp']))
                    nombres_hoc = [normalizar(x) for x in split_valores(row['Acreedor_hoc'])]
                    
                    match = any(nombre_dp == n for n in nombres_hoc)
                    
                    if match:
                        stats['aprobados'] += 1
                    else:
                        stats['con_novedad'] += 1
                        estado = 'CON NOVEDAD - CONTADO' if forma_pago in ('1', '01') else 'CON NOVEDAD'
                        
                        cur = cx.cursor()
                        cur.execute("""UPDATE [CxP].[DocumentsProcessing]
                        SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (nit, factura, oc))
                        
                        cur.execute("""SELECT ObservacionesFase_4 FROM [CxP].[DocumentsProcessing]
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (nit, factura, oc))
                        r = cur.fetchone()
                        obs_act = safe_str(r[0]) if r and r[0] else ""
                        nueva = "No coincide nombre del emisor"
                        obs_final = (nueva + ", " + obs_act) if obs_act else nueva
                        
                        # CORRECCIÓN: Truncar antes de UPDATE
                        obs_final = truncar_observacion(obs_final)
                        
                        cur.execute("""UPDATE [CxP].[DocumentsProcessing] SET ObservacionesFase_4 = ?
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (obs_final, nit, factura, oc))
                        
                        cur.execute("""UPDATE [CxP].[DocumentsProcessing] SET ResultadoFinalAntesEventos = ?
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (estado, nit, factura, oc))
                        
                        cur.execute("""SELECT Valor_XML FROM [dbo].[CxP.Comparativa]
                        WHERE NIT = ? AND Factura = ? AND Item = 'Observaciones'
                        """, (nit, factura))
                        rc = cur.fetchone()
                        obs_comp_act = safe_str(rc[0]) if rc and rc[0] else ""
                        obs_comp_final = (nueva + ", " + obs_comp_act) if obs_comp_act else nueva
                        
                        # CORRECCIÓN: Truncar antes de UPDATE
                        obs_comp_final = truncar_observacion(obs_comp_final)
                        
                        cur.execute("""UPDATE [dbo].[CxP.Comparativa] SET Valor_XML = ?
                        WHERE NIT = ? AND Factura = ? AND Item = 'Observaciones'
                        """, (obs_comp_final, nit, factura))
                        
                        cur.execute("""UPDATE [dbo].[CxP.Comparativa] SET Estado_validacion_antes_de_eventos = ?
                        WHERE NIT = ? AND Factura = ?
                        """, (estado, nit, factura))
                        cx.commit()
                        cur.close()
                except: pass
            
            msg = "OK. Total:" + str(stats['total'])
            SetVar("vLocStrResultadoSP", "True")
            SetVar("vLocStrResumenSP", msg)
            return True, msg, None, stats
    except Exception as e:
        print(traceback.format_exc())
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vLocStrResultadoSP", "False")
        return False, str(e), None, {}

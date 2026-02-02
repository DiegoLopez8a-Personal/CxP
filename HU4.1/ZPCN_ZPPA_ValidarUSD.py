"""
================================================================================
SCRIPT: ZPCN_ZPPA_ValidarUSD.py
================================================================================

Descripcion General:
--------------------
    Valida el valor a pagar en COP para pedidos ZPCN/ZPPA/42 con moneda USD.
    Compara la suma de valores PorCalcular del historico de ordenes de compra
    contra el valor a pagar COP de la factura.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Plataforma: RocketBot RPA

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |               ZPCN_ZPPA_ValidarUSD()                        |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Obtener configuracion y tolerancia desde vLocDicConfig     |
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
    |  - ClaseDePedido contiene ZPCN, ZPPA o 42                   |
    |  - Moneda contiene USD                                      |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Para cada registro:                                        |
    |  +-------------------------------------------------------+  |
    |  |  Calcular suma de PorCalcular_hoc                     |  |
    |  |  Obtener VlrPagarCop_dp                               |  |
    |  |  Calcular diferencia = |suma_hoc - vlr_cop|           |  |
    |  +-------------------------------------------------------+  |
    |  |  SI diferencia <= tolerancia:                         |  |
    |  |    -> Aprobado                                        |  |
    |  +-------------------------------------------------------+  |
    |  |  SI diferencia > tolerancia:                          |  |
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
        - Tolerancia: Tolerancia para comparacion (default: 500)

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
CRITERIOS DE FILTRADO
================================================================================

El script procesa solo registros que cumplan:

    1. ClaseDePedido contiene: ZPCN, ZPPA o 42
    2. Moneda contiene: USD

================================================================================
VALIDACION REALIZADA
================================================================================

    Valor a Pagar COP:
        - Suma todos los valores PorCalcular_hoc (separados por |)
        - Compara contra VlrPagarCop_dp
        - Tolerancia por defecto: $500 COP
        
    Si diferencia > tolerancia:
        - Estado: CON NOVEDAD o CON NOVEDAD - CONTADO (si forma_pago = 1/01)
        - Observacion: "No se encuentra coincidencia del Valor a pagar COP..."

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
        "NombreBaseDatos": "NotificationsPaddy",
        "Tolerancia": 500
    }))
    
    # Ejecutar funcion
    ZPCN_ZPPA_ValidarUSD()
    
    # Verificar resultado
    resultado = GetVar("vLocStrResultadoSP")  # "True"

================================================================================
NOTAS TECNICAS
================================================================================

    - Valores separados por | se suman individualmente
    - Observaciones se truncan a 3900 caracteres
    - Errores por registro no detienen el proceso
    - Commit se realiza por cada registro con novedad

================================================================================
"""

def ZPCN_ZPPA_ValidarUSD():
    import json, ast, traceback, pyodbc, pandas as pd, numpy as np
    from datetime import datetime
    from contextlib import contextmanager
    import time, warnings
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
    
    def sumar(valor_str):
        suma = 0.0
        for v in split_valores(valor_str):
            try:
                suma += float(v.replace(',', ''))
            except:
                pass
        return suma
    
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))
        tol = float(cfg.get('Tolerancia', 500))
        stats = {'total': 0, 'aprobados': 0, 'con_novedad': 0}
        
        with crear_conexion_db(cfg) as cx:
            df = pd.read_sql("""
            SELECT nit_emisor_o_nit_del_proveedor_dp, numero_de_factura_dp,
                   numero_de_liquidacion_u_orden_de_compra_dp, forma_de_pago_dp,
                   ClaseDePedido_hoc, PorCalcular_hoc, VlrPagarCop_dp, Moneda_hoc
            FROM [CxP].[HU41_CandidatosValidacion] WITH (NOLOCK)
            """, cx)
            
            if df.empty:
                SetVar("vLocStrResultadoSP", "True")
                return True, "No hay registros", None, stats
            
            mask_clase = df['ClaseDePedido_hoc'].apply(
                lambda x: contiene(x, 'ZPCN') or contiene(x, 'ZPPA') or contiene(x, '42') if pd.notna(x) else False
            )
            mask_usd = df['Moneda_hoc'].apply(
                lambda x: contiene(x, 'USD') if pd.notna(x) else False
            )
            df = df[mask_clase & mask_usd].copy()
            stats['total'] = len(df)
            
            for idx, row in df.iterrows():
                try:
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    forma_pago = safe_str(row['forma_de_pago_dp'])
                    
                    suma_hoc = sumar(row['PorCalcular_hoc'])
                    vlr_cop = sumar(row['VlrPagarCop_dp'])
                    diff = abs(suma_hoc - vlr_cop)
                    
                    if diff <= tol:
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
                        nueva = "No se encuentra coincidencia del Valor a pagar COP de la factura"
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

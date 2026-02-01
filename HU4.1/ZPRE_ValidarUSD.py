def ZPRE_ValidarUSD():
    """
    Valida la coincidencia de montos para pedidos ZPRE (Recepcion de Servicios) en moneda extranjera (USD).

    Esta funcion compara la suma total de los valores 'Por Calcular' extraidos de SAP (que pueden
    corresponder a multiples posiciones de la hoja de entrada de servicios) contra el valor total
    de la factura en pesos ('VlrPagarCop'). Esto asegura que la conversion y los montos base sean consistentes.

    Logica:
    1.  Filtra candidatos ZPRE/45 que sean en moneda USD.
    2.  Suma todos los valores de la columna 'PorCalcular_hoc' (SAP).
    3.  Suma todos los valores de la columna 'VlrPagarCop_dp' (XML - Valor en Pesos).
    4.  Compara la diferencia absoluta contra una tolerancia (500 COP).
    5.  Si la diferencia excede la tolerancia, marca el registro como "CON NOVEDAD".

    Variables de entrada (RocketBot):
        - vLocDicConfig (str | dict): Configuracion.

    Variables de salida (RocketBot):
        - vLocStrResultadoSP (str): "True" / "False".
        - vLocStrResumenSP (str): Resumen estadistico.

    Author:
        Diego Ivan Lopez Ochoa
    """

    # =========================================================================
    # SECCION 1: IMPORTACION DE LIBRERIAS
    # =========================================================================
    import json, ast, traceback, pyodbc, pandas as pd, numpy as np
    from datetime import datetime
    from contextlib import contextmanager
    import time, warnings
    warnings.filterwarnings('ignore')
    
    # =========================================================================
    # SECCION 2: FUNCIONES AUXILIARES (HELPERS)
    # =========================================================================
    
    def safe_str(v):
        """Convierte valor a string seguro."""
        if v is None: return ""
        if isinstance(v, str): return v.strip()
        if isinstance(v, (int, float)):
            if isinstance(v, float) and np.isnan(v): return ""
            return str(v)
        try: return str(v).strip()
        except: return ""
    
    def truncar_observacion(obs):
        """Truncar observación a 3900 caracteres para prevenir overflow"""
        if not obs: return ""
        obs_str = safe_str(obs)
        return obs_str[:3900] if len(obs_str) > 3900 else obs_str
    
    def parse_config(raw):
        """Parsea la configuracion."""
        if isinstance(raw, dict): return raw
        try: return json.loads(safe_str(raw))
        except: return ast.literal_eval(safe_str(raw))
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        """Context Manager para conexion a BD."""
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
        """Dividir string por | y retornar lista de valores"""
        if not v or pd.isna(v): return []
        return [x.strip() for x in str(v).split('|') if x.strip()]

    def contiene(campo, val):
        """Verificar si campo contiene valor buscado"""
        return val in split_valores(campo)
    
    def sumar(valor_str):
        """Suma los valores numericos contenidos en un string separado por pipes."""
        suma = 0.0
        for v in split_valores(valor_str):
            try:
                suma += float(v.replace(',', ''))
            except:
                pass
        return suma
    
    # =========================================================================
    # SECCION 3: LOGICA PRINCIPAL (MAIN)
    # =========================================================================
    
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
            
            # Filtro ZPRE / 45 y Moneda USD
            mask_clase = df['ClaseDePedido_hoc'].apply(
                lambda x: contiene(x, 'ZPRE') or contiene(x, '45') if pd.notna(x) else False
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
                    
                    # Sumar valores
                    suma_hoc = sumar(row['PorCalcular_hoc'])
                    vlr_cop = sumar(row['VlrPagarCop_dp'])
                    diff = abs(suma_hoc - vlr_cop)
                    
                    if diff <= tol:
                        stats['aprobados'] += 1
                    else:
                        stats['con_novedad'] += 1
                        estado = 'CON NOVEDAD - CONTADO' if forma_pago in ('1', '01') else 'CON NOVEDAD'
                        
                        cur = cx.cursor()

                        # Actualizar DP - Estado Fase 4
                        cur.execute("""UPDATE [CxP].[DocumentsProcessing]
                        SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (nit, factura, oc))
                        
                        # Actualizar DP - Observaciones
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
                        
                        # Actualizar DP - Resultado Final
                        cur.execute("""UPDATE [CxP].[DocumentsProcessing] SET ResultadoFinalAntesEventos = ?
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (estado, nit, factura, oc))
                        
                        # Actualizar Comparativa - Observaciones
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
                        
                        # Actualizar Comparativa - Estado General
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
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vLocStrResultadoSP", "False")
        return False, str(e), None, {}

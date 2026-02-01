# Author: Diego Ivan Lopez Ochoa
"""
Validación de TRM (ZPRE).

LOGICA:
Verificación de tasa de cambio para ZPRE.

VARIABLES ROCKETBOT:
- vLocDicConfig: Configuracion BD
- vLocStrResultadoSP: Resultado ejecucion
"""
def ZPRE_ValidarTRM():
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
    
    warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
    
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
            return json.loads(text)
        except:
            return ast.literal_eval(text)
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
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
                break
            except:
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                raise
        try:
            yield cx
            if cx:
                cx.commit()
        except:
            if cx:
                cx.rollback()
            raise
        finally:
            if cx:
                cx.close()
    
    def split_valores(valor_str):
        if not valor_str or pd.isna(valor_str):
            return []
        return [v.strip() for v in str(valor_str).split('|') if v.strip()]
    
    def contiene_valor(campo, valor_buscado):
        return valor_buscado in split_valores(campo)
    
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))
        tolerancia_trm = float(cfg.get('ToleranciaTRM', 10))
        stats = {'total': 0, 'aprobados': 0, 'con_novedad': 0}
        
        with crear_conexion_db(cfg) as cx:
            query = """
            SELECT nit_emisor_o_nit_del_proveedor_dp, numero_de_factura_dp,
                   numero_de_liquidacion_u_orden_de_compra_dp, forma_de_pago_dp,
                   ClaseDePedido_hoc, CalculationRate_dp, TRM_hoc, Moneda_hoc
            FROM [CxP].[HU41_CandidatosValidacion] WITH (NOLOCK)
            """
            df = pd.read_sql(query, cx)
            
            if df.empty:
                SetVar("vLocStrResultadoSP", "True")
                return True, "No hay registros", None, stats
            
            mask_clase = df['ClaseDePedido_hoc'].apply(
                lambda x: contiene_valor(x, 'ZPRE') or contiene_valor(x, '45') if pd.notna(x) else False
            )
            mask_usd = df['Moneda_hoc'].apply(lambda x: contiene_valor(x, 'USD') if pd.notna(x) else False)
            df_filtrado = df[mask_clase & mask_usd].copy()
            
            stats['total'] = len(df_filtrado)
            
            for idx, row in df_filtrado.iterrows():
                try:
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    forma_pago = safe_str(row['forma_de_pago_dp'])
                    
                    trm_dp = float(row['CalculationRate_dp']) if pd.notna(row['CalculationRate_dp']) else 0
                    trms_hoc = split_valores(row['TRM_hoc'])
                    
                    trm_match = False
                    for trm_str in trms_hoc:
                        try:
                            trm_hoc = float(trm_str.replace(',', ''))
                            if abs(trm_dp - trm_hoc) <= tolerancia_trm:
                                trm_match = True
                                break
                        except:
                            pass
                    
                    if trm_match:
                        stats['aprobados'] += 1
                    else:
                        stats['con_novedad'] += 1
                        estado_final = 'CON NOVEDAD - CONTADO' if forma_pago in ('1', '01') else 'CON NOVEDAD'
                        
                        cur = cx.cursor()
                        cur.execute("""
                        UPDATE [CxP].[DocumentsProcessing]
                        SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (nit, factura, oc))
                        
                        cur.execute("""
                        SELECT ObservacionesFase_4 FROM [CxP].[DocumentsProcessing]
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (nit, factura, oc))
                        result = cur.fetchone()
                        obs_actual = safe_str(result[0]) if result and result[0] else ""
                        nueva_obs = "No se encuentra coincidencia de TRM"
                        
                        if obs_actual:
                            obs_final = nueva_obs + ", " + obs_actual
                        else:
                            obs_final = nueva_obs
                        
                        # CORRECCIÓN: Truncar antes de UPDATE
                        obs_final = truncar_observacion(obs_final)
                        
                        cur.execute("""
                        UPDATE [CxP].[DocumentsProcessing] SET ObservacionesFase_4 = ?
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (obs_final, nit, factura, oc))
                        
                        cur.execute("""
                        UPDATE [CxP].[DocumentsProcessing] SET ResultadoFinalAntesEventos = ?
                        WHERE nit_emisor_o_nit_del_proveedor = ? AND numero_de_factura = ? AND numero_de_liquidacion_u_orden_de_compra = ?
                        """, (estado_final, nit, factura, oc))
                        
                        cur.execute("""
                        SELECT Valor_XML FROM [dbo].[CxP.Comparativa]
                        WHERE NIT = ? AND Factura = ? AND Item = 'Observaciones'
                        """, (nit, factura))
                        result_comp = cur.fetchone()
                        obs_comp_actual = safe_str(result_comp[0]) if result_comp and result_comp[0] else ""
                        
                        if obs_comp_actual:
                            obs_comp_final = nueva_obs + ", " + obs_comp_actual
                        else:
                            obs_comp_final = nueva_obs
                        
                        # CORRECCIÓN: Truncar antes de UPDATE
                        obs_comp_final = truncar_observacion(obs_comp_final)
                        
                        cur.execute("""
                        UPDATE [dbo].[CxP.Comparativa] SET Valor_XML = ?
                        WHERE NIT = ? AND Factura = ? AND Item = 'Observaciones'
                        """, (obs_comp_final, nit, factura))
                        
                        cur.execute("""
                        UPDATE [dbo].[CxP.Comparativa] SET Estado_validacion_antes_de_eventos = ?
                        WHERE NIT = ? AND Factura = ?
                        """, (estado_final, nit, factura))
                        cx.commit()
                        cur.close()
                
                except Exception as e:
                    print("[ERROR] " + str(e))
            
            msg = "OK. Total:" + str(stats['total']) + " Aprobados:" + str(stats['aprobados'])
            SetVar("vLocStrResultadoSP", "True")
            SetVar("vLocStrResumenSP", msg)
            return True, msg, None, stats
    
    except Exception as e:
        print(traceback.format_exc())
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vLocStrResultadoSP", "False")
        return False, str(e), None, {}
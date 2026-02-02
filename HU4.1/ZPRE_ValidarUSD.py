"""
================================================================================
SCRIPT: ZPRE_ValidarUSD.py
================================================================================

Descripcion General:
--------------------
    Valida el valor a pagar en COP para pedidos ZPRE/45 (Prepagos) con moneda USD.
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
    |                 ZPRE_ValidarUSD()                           |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Obtener configuracion y tolerancia desde vLocDicConfig     |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Consultar [CxP].[HU41_CandidatosValidacion]                |
    |  Filtrar: ClaseDePedido = ZPRE o 45                         |
    |  Filtrar: Moneda = USD                                      |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Para cada registro:                                        |
    |  +-------------------------------------------------------+  |
    |  |  Calcular suma de PorCalcular_hoc                     |  |
    |  |  Obtener VlrPagarCop_dp                               |  |
    |  |  Calcular diferencia absoluta                         |  |
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
        - ServidorBaseDatos: Servidor SQL Server
        - NombreBaseDatos: Base de datos
        - Tolerancia: Tolerancia para comparacion (default: 500)

================================================================================
VARIABLES DE SALIDA (RocketBot)
================================================================================

    vLocStrResultadoSP : str
        "True" si exitoso, "False" si error

    vLocStrResumenSP : str
        Resumen: "OK. Total:X"

================================================================================
CRITERIOS DE FILTRADO
================================================================================

El script procesa solo registros que cumplan:

    1. ClaseDePedido contiene: ZPRE o 45
    2. Moneda contiene: USD

================================================================================
VALIDACION REALIZADA
================================================================================

    Valor a Pagar COP:
        - Suma de PorCalcular_hoc (valores separados por |)
        - VlrPagarCop_dp (valor del XML)
        - Tolerancia por defecto: $500 COP
        
    Si diferencia > tolerancia:
        - Estado: CON NOVEDAD o CON NOVEDAD - CONTADO
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
    ZPRE_ValidarUSD()

================================================================================
NOTAS TECNICAS
================================================================================

    - Suma valores separados por | individualmente
    - Conversion de TRM ya aplicada en VlrPagarCop
    - Observaciones se truncan a 3900 caracteres
    - Errores por registro no detienen el proceso

================================================================================
"""

def ZPRE_ValidarUSD():
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
    
    print("=" * 80)
    print("[INICIO] Funcion ZPRE_ValidarUSD() iniciada")
    print("[INICIO] Timestamp: " + str(datetime.now()))
    print("=" * 80)
    
    def actualizar_items_comparativa(id_reg, cx, nit, factura, nombre_item, valores_lista,actualizar_valor_xml=False, valor_xml=None,actualizar_aprobado=False, valor_aprobado=None):
        cur = cx.cursor()
        
        query_count = """
        SELECT COUNT(*)
        FROM [dbo].[CxP.Comparativa]
        WHERE NIT = ?
          AND Factura = ?
          AND Item = ?
        """
        cur.execute(query_count, (nit, factura, nombre_item))
        count_actual = cur.fetchone()[0]
        
        count_necesario = len(valores_lista)
        
        if count_actual == 0:
            for i, valor in enumerate(valores_lista):
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                   ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra, Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                vxml = valor_xml if actualizar_valor_xml else None
                vaprob = valor_aprobado if actualizar_aprobado else None
                cur.execute(insert_query, (id_reg, nit, factura, nombre_item, valor, vxml, vaprob))
        
        elif count_actual < count_necesario:
            for i in range(count_actual):
                update_query = "UPDATE [dbo].[CxP.Comparativa] SET Valor_Orden_de_Compra = ?"
                params = [valores_lista[i]]
                
                if actualizar_valor_xml:
                    update_query += ", Valor_XML = ?"
                    params.append(valor_xml)
                
                if actualizar_aprobado:
                    update_query += ", Aprobado = ?"
                    params.append(valor_aprobado)
                
                update_query += """
                WHERE NIT = ? AND Factura = ? AND Item = ?
                  AND ID_registro IN (
                    SELECT ID_registro FROM [dbo].[CxP.Comparativa]
                    WHERE NIT = ? AND Factura = ? AND Item = ?
                    ORDER BY ID_registro
                    OFFSET ? ROWS FETCH NEXT 1 ROWS ONLY
                  )
                """
                params.extend([nit, factura, nombre_item, nit, factura, nombre_item, i])
                cur.execute(update_query, params)
            
            for i in range(count_actual, count_necesario):
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    ID_registro, NIT, Factura, Item, Valor_Orden_de_Compra, Valor_XML, Aprobado
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                vxml = valor_xml if actualizar_valor_xml else None
                vaprob = valor_aprobado if actualizar_aprobado else None
                cur.execute(insert_query, (id_reg, nit, factura, nombre_item, valores_lista[i], vxml, vaprob))
        
        else:
            for i, valor in enumerate(valores_lista):
                update_query = "UPDATE [dbo].[CxP.Comparativa] SET Valor_Orden_de_Compra = ?"
                params = [valor]
                
                if actualizar_valor_xml:
                    update_query += ", Valor_XML = ?"
                    params.append(valor_xml)
                
                if actualizar_aprobado:
                    update_query += ", Aprobado = ?"
                    params.append(valor_aprobado)
                
                update_query += """
                WHERE NIT = ? AND Factura = ? AND Item = ?
                  AND id IN (
                    SELECT ID_registro FROM [dbo].[CxP.Comparativa]
                    WHERE NIT = ? AND Factura = ? AND Item = ?
                    ORDER BY id
                    OFFSET ? ROWS FETCH NEXT 1 ROWS ONLY
                  )
                """
                params.extend([nit, factura, nombre_item, nit, factura, nombre_item, i])
                cur.execute(update_query, params)
        cx.commit()
        cur.close()
    
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
                print("[DEBUG] Commit final exitoso")
        except Exception as e:
            if cx:
                cx.rollback()
                print("[ERROR] Rollback: " + str(e))
            raise
        finally:
            if cx:
                try:
                    cx.close()
                    print("[DEBUG] Conexion cerrada")
                except:
                    pass
    
    def split_valores(valor_str):
        if not valor_str or valor_str == "" or pd.isna(valor_str):
            return []
        valores = str(valor_str).split('|')
        return [v.strip() for v in valores if v.strip()]
    
    def sumar_valores(valor_str, nombre_campo="campo"):
        valores = split_valores(valor_str)
        suma = 0.0
        for i, v in enumerate(valores):
            try:
                v_limpio = v.strip().replace(',', '')
                suma += float(v_limpio)
            except:
                pass
        print("[DEBUG] " + nombre_campo + " suma: " + str(suma))
        return suma
    
    def contiene_valor(campo, valor_buscado):
        valores = split_valores(campo)
        return valor_buscado in valores
    
    try:
        cfg = parse_config(GetVar("vLocDicConfig"))
        tolerancia = float(cfg.get('Tolerancia', 500))
        stats = {'total_registros': 0, 'aprobados': 0, 'con_novedad': 0, 'errores': 0}
        t_inicio = time.time()
        
        with crear_conexion_db(cfg) as cx:
            query_candidatos = """
            SELECT ID_dp, nit_emisor_o_nit_del_proveedor_dp, numero_de_factura_dp,
                   numero_de_liquidacion_u_orden_de_compra_dp, forma_de_pago_dp,
                   ClaseDePedido_hoc, PorCalcular_hoc, VlrPagarCop_dp,
                   Posicion_hoc, TipoNif_hoc, Acreedor_hoc, FecDoc_hoc, FecReg_hoc,
                   FecContGasto_hoc, IndicadorImpuestos_hoc, TextoBreve_hoc,
                   ClaseDeImpuesto_hoc, Cuenta_hoc, CiudadProveedor_hoc,
                   DocFiEntrada_hoc, Cuenta26_hoc, DocCompra_hoc, NitCedula_hoc, Moneda_hoc
            FROM [CxP].[HU41_CandidatosValidacion] WITH (NOLOCK)
            """
            df_candidatos = pd.read_sql(query_candidatos, cx)
            
            if df_candidatos.empty:
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros")
                return True, "No hay registros", None, stats
            
            mask_clase = df_candidatos['ClaseDePedido_hoc'].apply(
                lambda x: contiene_valor(x, 'ZPRE') or contiene_valor(x, '45') if pd.notna(x) else False
            )
            mask_usd = df_candidatos['Moneda_hoc'].apply(
                lambda x: contiene_valor(x, 'USD') if pd.notna(x) else False
            )
            df_filtrado = df_candidatos[mask_clase & mask_usd].copy()
            
            if df_filtrado.empty:
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros USD")
                return True, "No hay registros USD", None, stats
            
            stats['total_registros'] = len(df_filtrado)
            
            for idx, row in df_filtrado.iterrows():
                try:
                    id_reg = safe_str(row['ID_dp'])
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    forma_pago = safe_str(row['forma_de_pago_dp'])
                    
                    suma_por_calcular = sumar_valores(row['PorCalcular_hoc'], "PorCalcular")
                    vlr_pagar_cop = sumar_valores(row['VlrPagarCop_dp'], "VlrPagarCop")
                    diferencia = abs(suma_por_calcular - vlr_pagar_cop)
                    
                    if diferencia <= tolerancia:
                        stats['aprobados'] += 1
                        valores_porcalcular = split_valores(row['PorCalcular_hoc'])
                        actualizar_items_comparativa(id_reg, cx, nit, factura, 'LineExtensionAmount',
                                                   valores_porcalcular, True, str(vlr_pagar_cop), True, 'SI')
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
                        result_obs = cur.fetchone()
                        obs_actual = safe_str(result_obs[0]) if result_obs and result_obs[0] else ""
                        nueva_obs = "No se encuentra coincidencia del Valor a pagar COP de la factura"
                        
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
                        
                        valores_porcalcular = split_valores(row['PorCalcular_hoc'])
                        actualizar_items_comparativa(id_reg, cx, nit, factura, 'LineExtensionAmount',
                                                   valores_porcalcular, True, str(vlr_pagar_cop), True, 'NO')
                        
                        cur.execute("""
                        SELECT Valor_XML FROM [dbo].[CxP.Comparativa]
                        WHERE NIT = ? AND Factura = ? AND Item = 'Observaciones'
                        """, (nit, factura))
                        result_obs_comp = cur.fetchone()
                        obs_comp_actual = safe_str(result_obs_comp[0]) if result_obs_comp and result_obs_comp[0] else ""
                        nueva_obs_comp = "No se encuentra coincidencia del Valor a pagar COP de la factura"
                        
                        if obs_comp_actual:
                            obs_comp_final = nueva_obs_comp + ", " + obs_comp_actual
                        else:
                            obs_comp_final = nueva_obs_comp
                        
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
                
                except Exception as e_row:
                    print("[ERROR] Registro " + str(idx) + ": " + str(e_row))
                    stats['errores'] += 1
            
            stats['tiempo_total'] = time.time() - t_inicio
            msg = "OK. Total:" + str(stats['total_registros']) + " Aprobados:" + str(stats['aprobados'])
            SetVar("vLocStrResultadoSP", "True")
            SetVar("vLocStrResumenSP", msg)
            SetVar("vGblStrDetalleError", "")
            return True, msg, None, stats
    
    except Exception as e:
        print("[ERROR] " + str(e))
        SetVar("vGblStrDetalleError", traceback.format_exc())
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        SetVar("vLocStrResultadoSP", "False")
        return False, str(e), None, {}

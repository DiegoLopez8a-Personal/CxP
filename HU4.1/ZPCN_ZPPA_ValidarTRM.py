"""
================================================================================
SCRIPT: ZPCN_ZPPA_ValidarTRM.py
================================================================================

Descripcion General:
--------------------
    Valida la Tasa Representativa del Mercado (TRM) para pedidos ZPCN/ZPPA/42
    con moneda USD. Compara el CalculationRate del XML de factura contra el
    TRM registrado en el historico de ordenes de compra de SAP.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Plataforma: RocketBot RPA

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |              ZPCN_ZPPA_ValidarTRM()                         |
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
    |  - ClaseDePedido contiene ZPCN, ZPPA o 42                   |
    |  - Moneda contiene USD                                      |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Para cada registro:                                        |
    |  +-------------------------------------------------------+  |
    |  |  Obtener CalculationRate_dp (XML)                     |  |
    |  |  Obtener primer valor de Trm_hoc (SAP)                |  |
    |  +-------------------------------------------------------+  |
    |  |  SI |TRM_dp - TRM_hoc| > 0 (diferente):               |  |
    |  |    -> CON NOVEDAD                                     |  |
    |  |    -> Actualizar DocumentsProcessing                  |  |
    |  |    -> Actualizar Comparativa                          |  |
    |  |    -> Actualizar HistoricoOrdenesCompra               |  |
    |  +-------------------------------------------------------+  |
    |  |  SI TRM_dp == TRM_hoc:                                |  |
    |  |    -> Aprobado                                        |  |
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
        Resumen: "Proceso OK. Total:X Aprobados:Y ConNovedad:Z"

    vLocDicEstadisticas : str
        Diccionario de estadisticas

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

    TRM (Tasa Representativa del Mercado):
        - CalculationRate_dp (XML) - valor numerico
        - Trm_hoc (SAP) - se toma el PRIMER valor (si hay multiples)
        - Comparacion: valores deben ser IGUALES
        
    Si TRM_dp != TRM_hoc:
        - Estado: CON NOVEDAD o CON NOVEDAD - CONTADO
        - Observacion: "No se encuentra coincidencia en el campo TRM..."

================================================================================
TABLAS ACTUALIZADAS (Cuando hay novedad)
================================================================================

    [CxP].[DocumentsProcessing]
        - EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
        - ObservacionesFase_4 = Observacion concatenada
        - ResultadoFinalAntesEventos = Estado final

    [dbo].[CxP.Comparativa]
        - Valor_XML = Observacion (Item = 'Observaciones')
        - Estado_validacion_antes_de_eventos = Estado final

    [CxP].[HistoricoOrdenesCompra]
        - Marca = 'PROCESADO'

================================================================================
EJEMPLOS DE USO
================================================================================

    # Configurar variables en RocketBot
    SetVar("vLocDicConfig", json.dumps({
        "ServidorBaseDatos": "servidor.ejemplo.com",
        "NombreBaseDatos": "NotificationsPaddy"
    }))
    
    # Ejecutar funcion
    ZPCN_ZPPA_ValidarTRM()
    
    # Verificar resultado
    resultado = GetVar("vLocStrResultadoSP")  # "True"

================================================================================
NOTAS TECNICAS
================================================================================

    - Solo se compara contra el PRIMER valor de Trm_hoc
    - Comparacion es exacta (diferencia > 0 = novedad)
    - Actualiza HistoricoOrdenesCompra con Marca = 'PROCESADO'
    - Observaciones se truncan a 3900 caracteres

================================================================================
"""

def ZPCN_ZPPA_ValidarTRM():
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
    print("[INICIO] Funcion ZPCN_ZPPA_ValidarTRM() iniciada")
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
        """Verificar si campo (que puede tener valores separados por |) contiene valor buscado"""
        valores = split_valores(campo)
        return valor_buscado in valores
    
    def obtener_primer_valor(campo):
        """Obtener solo el primer valor de un campo que puede tener multiples valores separados por |"""
        valores = split_valores(campo)
        if valores:
            return valores[0]
        return ""
    
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
                ClaseDePedido_hoc,
                Moneda_hoc,
                Trm_hoc,
                CalculationRate_dp,
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
            
            print("[PASO 2] Aplicando filtros...")
            
            mask_clase = df_candidatos['ClaseDePedido_hoc'].apply(
                lambda x: contiene_valor(x, 'ZPPA') or contiene_valor(x, 'ZPCN') or contiene_valor(x, '42') if pd.notna(x) else False
            )
            
            print("[DEBUG] Registros con ClaseDePedido = ZPPA, ZPCN o 42: " + str(mask_clase.sum()))
            
            mask_moneda = df_candidatos['Moneda_hoc'].apply(
                lambda x: contiene_valor(x, 'USD') if pd.notna(x) else False
            )
            
            print("[DEBUG] Registros con Moneda = USD: " + str(mask_moneda.sum()))
            
            mask_final = mask_clase & mask_moneda
            df_filtrado = df_candidatos[mask_final].copy()
            
            print("[DEBUG] Registros despues de filtros: " + str(len(df_filtrado)))
            
            if df_filtrado.empty:
                print("[INFO] No hay registros que cumplan los filtros")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros con ClaseDePedido ZPPA/ZPCN/42 y Moneda USD")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay registros que cumplan filtros", None, stats
            
            stats['total_registros'] = len(df_filtrado)
            
            print("")
            print("[PASO 3] Procesando VALIDACION: TRM vs CalculationRate...")
            
            for idx, row in df_filtrado.iterrows():
                try:
                    print("")
                    print("[REGISTRO " + str(idx + 1) + "/" + str(len(df_filtrado)) + "] - VALIDACION TRM")
                    
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    forma_pago = safe_str(row['forma_de_pago_dp'])
                    
                    print("[DEBUG] NIT: " + nit)
                    print("[DEBUG] Factura: " + factura)
                    print("[DEBUG] OC: " + oc)
                    
                    trm_hoc_primer_valor = obtener_primer_valor(row['Trm_hoc'])
                    calculation_rate = safe_str(row['CalculationRate_dp'])
                    
                    print("[DEBUG] Trm_hoc (primer valor): '" + trm_hoc_primer_valor + "'")
                    print("[DEBUG] CalculationRate_dp: '" + calculation_rate + "'")
                    
                    if trm_hoc_primer_valor != calculation_rate:
                        print("[RESULTADO] CON NOVEDAD - TRM (valores diferentes)")
                        stats['con_novedad'] += 1
                        
                        if forma_pago == '1' or forma_pago == '01':
                            estado_final = 'CON NOVEDAD - CONTADO'
                        else:
                            estado_final = 'CON NOVEDAD'
                        
                        print("[DEBUG] Estado final: " + estado_final)
                        
                        cur = cx.cursor()
                        
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
                        nueva_obs = "No se encuentra coincidencia en el campo TRM de la factura vs la informacion reportada en SAP"
                        
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
                        
                        print("[UPDATE] Actualizando tabla CxP.Comparativa...")
                        
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
                        
                        update_estado_todos = """
                        UPDATE [dbo].[CxP.Comparativa]
                        SET Estado_validacion_antes_de_eventos = ?
                        WHERE NIT = ?
                          AND Factura = ?
                        """
                        cur.execute(update_estado_todos, (estado_final, nit, factura))
                        
                        print("[DEBUG] CxP.Comparativa actualizado OK")
                        
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
                        print("[UPDATE] Todas las tablas actualizadas OK (TRM)")
                        
                    else:
                        print("[RESULTADO] APROBADO - TRM (valores iguales)")
                        stats['aprobados'] += 1
                    
                except Exception as e_row:
                    print("[ERROR] Error procesando registro " + str(idx) + " (TRM): " + str(e_row))
                    stats['errores'] += 1
                    continue
            
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

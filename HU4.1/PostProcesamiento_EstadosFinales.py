"""
================================================================================
SCRIPT: PostProcesamiento_EstadosFinales.py
================================================================================

Descripcion General:
--------------------
    Realiza el post-procesamiento de estados finales para pedidos ZPRE/ZPPA/ZPCN/45/42.
    Asigna estados definitivos (APROBADO, APROBADO CONTADO, APROBADO SIN CONTABILIZACION)
    basandose en la clase de pedido, forma de pago y resultados de validaciones previas.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Plataforma: RocketBot RPA

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |        PostProcesamiento_EstadosFinales()                   |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------------------------------------------+
    |  Consultar [CxP].[HU41_CandidatosValidacion]                                                    |
    |  Filtrar: 'ZPRE', 'ZPPA', 'ZPCN', 'ZVEN', 'ZPSA', 'ZPSS', 'ZPAF', 'ZPSA'                        |
    +-----------------------------+-------------------------------------------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Para cada registro:                                        |
    |  +-------------------------------------------------------+  |
    |  |  Obtener: ClaseDePedido, forma_de_pago, estados       |  |
    |  +-------------------------------------------------------+  |
    |                            |                                |
    |     +----------------------+----------------------+         |
    |     |                      |                      |         |
    |     v                      v                      v         |
    |  Clase 31?            Contado?              Credito?        |
    |  (Servicios)          (01/1)                (otros)         |
    |     |                      |                      |         |
    |     v                      v                      v         |
    |  CON NOVEDAD        APROBADO             APROBADO          |
    |  + Obs especial     CONTADO              (normal)          |
    |  o APROBADO SIN                                            |
    |  CONTABILIZACION                                           |
    +-------------------------------------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Actualizar DocumentsProcessing, Comparativa, HOC           |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar estadisticas y configurar variables RocketBot     |
    +-------------------------------------------------------------+

================================================================================
REGLAS DE ESTADOS FINALES
================================================================================

    CASO 1 - Clase de Pedido 31 (Servicios):
    
        SI tiene CON NOVEDAD previo:
            -> Mantiene CON NOVEDAD + observacion especial
            
        SI NO tiene novedad:
            -> APROBADO SIN CONTABILIZACION
            -> Observacion: "Pedido corresponde a Servicios (Clase 31)"
    
    CASO 2 - Forma de pago CONTADO (01 o 1):
    
        -> APROBADO CONTADO
        -> Marca HOC como 'PROCESADO'
    
    CASO 3 - Forma de pago CREDITO (otros):
    
        -> APROBADO
        -> Marca HOC como 'PROCESADO'

================================================================================
VARIABLES DE ENTRADA (RocketBot)
================================================================================

    vLocDicConfig : str | dict
        - ServidorBaseDatos: Servidor SQL Server
        - NombreBaseDatos: Base de datos

================================================================================
VARIABLES DE SALIDA (RocketBot)
================================================================================

    vLocStrResultadoSP : str
        "True" si exitoso, "False" si error

    vLocStrResumenSP : str
        "Post-procesamiento OK. Total:X Clase31:Y Contado:Z Aprobado:W"

    vLocDicEstadisticas : str
        Diccionario con:
        - total_registros
        - con_novedad_clase31
        - aprobado_sin_contab
        - aprobado_contado
        - aprobado
        - errores

================================================================================
TABLAS ACTUALIZADAS
================================================================================

    [CxP].[DocumentsProcessing]
        - EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
        - ObservacionesFase_4 (si aplica)
        - ResultadoFinalAntesEventos

    [dbo].[CxP.Comparativa]
        - Estado_validacion_antes_de_eventos

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
    PostProcesamiento_EstadosFinales()
    
    # Verificar resultado
    resultado = GetVar("vLocStrResultadoSP")  # "True"

================================================================================
NOTAS TECNICAS
================================================================================

    - Se ejecuta DESPUES de todas las validaciones especificas
    - Solo procesa registros que pasaron validaciones previas
    - Clase 31 tiene tratamiento especial (sin contabilizacion)
    - Actualiza multiples posiciones de HOC por registro

================================================================================
"""

def PostProcesamiento_EstadosFinales():
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
    print("[INICIO] Funcion PostProcesamiento_EstadosFinales() iniciada")
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
    
    def contiene_valor(campo, valores_buscados):
        """Verificar si campo contiene alguno de los valores buscados"""
        valores = split_valores(campo)
        for v in valores_buscados:
            if v in valores:
                return True
        return False
    
    # ========================================================================
    # INICIO DE PROCESO
    # ========================================================================
    
    try:
        print("[DEBUG] Obteniendo configuracion...")
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[DEBUG] Configuracion obtenida OK")
        
        stats = {
            'total_registros': 0,
            'con_novedad_clase31': 0,
            'aprobado_sin_contab': 0,
            'aprobado_contado': 0,
            'aprobado': 0,
            'errores': 0,
            'tiempo_total': 0
        }
        
        t_inicio = time.time()
        fecha_ejecucion = datetime.now()
        
        with crear_conexion_db(cfg) as cx:
            
            # ================================================================
            # PASO 1: Consultar registros de HU41_CandidatosValidacion
            # ================================================================
            
            print("")
            print("[PASO 1] Consultando HU41_CandidatosValidacion...")
            
            # FILTRO MODIFICABLE: Filtrar por clases de pedido
            clases_pedido_filtro = ['ZPRE', 'ZPPA', 'ZPCN', 'ZVEN', 'ZPSA', 'ZPSS', 'ZPAF', 'ZPSA']
            
            query_candidatos = """
            SELECT 
                ID_dp,
                nit_emisor_o_nit_del_proveedor_dp,
                numero_de_factura_dp,
                numero_de_liquidacion_u_orden_de_compra_dp,
                ClaseDePedido_hoc,
                ClaseDeImpuesto_hoc,
                DocCompra_hoc,
                NitCedula_hoc,
                PorCalcular_hoc,
                TextoBreve_hoc
            FROM [CxP].[HU41_CandidatosValidacion] WITH (NOLOCK)
            WHERE 1=1
            """
            
            df_candidatos = pd.read_sql(query_candidatos, cx)
            print("[DEBUG] Total registros en HU41_CandidatosValidacion: " + str(len(df_candidatos)))
            
            if df_candidatos.empty:
                print("[INFO] No hay registros en HU41_CandidatosValidacion")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros para procesar")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay registros para procesar", None, stats
            
            # Aplicar filtro de ClaseDePedido
            print("[DEBUG] Aplicando filtro de ClaseDePedido: " + str(clases_pedido_filtro))
            mask_clase = df_candidatos['ClaseDePedido_hoc'].apply(
                lambda x: contiene_valor(x, clases_pedido_filtro) if pd.notna(x) else False
            )
            
            df_candidatos_filtrado = df_candidatos[mask_clase].copy()
            print("[DEBUG] Registros despues de filtro: " + str(len(df_candidatos_filtrado)))
            
            if df_candidatos_filtrado.empty:
                print("[INFO] No hay registros que cumplan el filtro de ClaseDePedido")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros con ClaseDePedido valido")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay registros con ClaseDePedido valido", None, stats
            
            # ================================================================
            # PASO 2: Consultar DocumentsProcessing
            # ================================================================
            
            print("")
            print("[PASO 2] Consultando DocumentsProcessing...")
            
            query_dp = """
            SELECT 
                nit_emisor_o_nit_del_proveedor,
                numero_de_factura,
                numero_de_liquidacion_u_orden_de_compra,
                forma_de_pago,
                ResultadoFinalAntesEventos,
                ObservacionesFase_4
            FROM [CxP].[DocumentsProcessing] WITH (NOLOCK)
            WHERE 1=1
            """
            
            df_dp = pd.read_sql(query_dp, cx)
            print("[DEBUG] Registros en DocumentsProcessing: " + str(len(df_dp)))
            
            # ================================================================
            # PASO 3: Hacer INNER JOIN
            # ================================================================
            
            print("[PASO 3] Haciendo INNER JOIN entre tablas...")
            
            df_merged = pd.merge(
                df_candidatos_filtrado,
                df_dp,
                left_on=['nit_emisor_o_nit_del_proveedor_dp', 'numero_de_factura_dp', 'numero_de_liquidacion_u_orden_de_compra_dp'],
                right_on=['nit_emisor_o_nit_del_proveedor', 'numero_de_factura', 'numero_de_liquidacion_u_orden_de_compra'],
                how='inner'
            )
            
            print("[DEBUG] Registros despues de INNER JOIN: " + str(len(df_merged)))
            
            if df_merged.empty:
                print("[INFO] No hay registros que coincidan entre HU41 y DocumentsProcessing")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay coincidencias entre tablas")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay coincidencias", None, stats
            
            stats['total_registros'] = len(df_merged)
            
            # ================================================================
            # PASO 4: Procesar cada registro
            # ================================================================
            
            print("")
            print("[PASO 4] Procesando post-validaciones...")
            
            for idx, row in df_merged.iterrows():
                try:
                    print("")
                    print("[REGISTRO " + str(idx + 1) + "/" + str(len(df_merged)) + "]")
                    
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    
                    resultado_actual = safe_str(row['ResultadoFinalAntesEventos'])
                    forma_pago = safe_str(row['forma_de_pago'])
                    clase_impuesto_completo = safe_str(row['ClaseDeImpuesto_hoc'])
                    obs_actual = safe_str(row['ObservacionesFase_4'])
                    
                    print("[DEBUG] NIT: " + nit)
                    print("[DEBUG] Factura: " + factura)
                    print("[DEBUG] OC: " + oc)
                    print("[DEBUG] ResultadoFinalAntesEventos: '" + resultado_actual + "'")
                    print("[DEBUG] forma_de_pago: '" + forma_pago + "'")
                    print("[DEBUG] ClaseDeImpuesto_hoc: '" + clase_impuesto_completo + "'")
                    
                    # CORRECCION: Verificar si ClaseDeImpuesto_hoc contiene '31'
                    tiene_clase31 = contiene_valor(clase_impuesto_completo, ['31'])
                    print("[DEBUG] Contiene Clase 31: " + str(tiene_clase31))
                    
                    tiene_con_novedad = "CON NOVEDAD" in resultado_actual.upper()
                    
                    # ====================================================
                    # CASO 1: TIENE CON NOVEDAD
                    # ====================================================
                    
                    if tiene_con_novedad:
                        print("[VALIDACION] Registro con CON NOVEDAD")
                        
                        if tiene_clase31:
                            print("[ACCION] ClaseDeImpuesto contiene 31 (ZOMAC-ZESE)")
                            stats['con_novedad_clase31'] += 1
                            
                            # Actualizar ResultadoFinalAntesEventos
                            # Agregar "EXCLUIDOS CONTABILIZACION" al final
                            nuevo_resultado = resultado_actual
                            if "EXCLUIDOS CONTABILIZACION" not in nuevo_resultado:
                                nuevo_resultado = resultado_actual + " EXCLUIDOS CONTABILIZACION"
                            
                            # Actualizar observaciones (PREPEND)
                            nueva_obs = "Factura corresponde a Clase de impuesto 31 (ZOMAC-ZESE)"
                            if obs_actual:
                                obs_final = nueva_obs + ", " + obs_actual
                            else:
                                obs_final = nueva_obs
                            
                            # Actualizar DocumentsProcessing
                            cur = cx.cursor()
                            
                            update_dp = """
                            UPDATE [CxP].[DocumentsProcessing]
                            SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso',
                                ObservacionesFase_4 = ?,
                                ResultadoFinalAntesEventos = ?
                            WHERE nit_emisor_o_nit_del_proveedor = ?
                              AND numero_de_factura = ?
                              AND numero_de_liquidacion_u_orden_de_compra = ?
                            """
                            cur.execute(update_dp, (obs_final, nuevo_resultado, nit, factura, oc))
                            print("[UPDATE] DocumentsProcessing actualizado")
                            
                            # Actualizar Comparativa - ESTADO en todas las filas
                            update_comparativa = """
                            UPDATE [dbo].[CxP.Comparativa]
                            SET Estado_validacion_antes_de_eventos = ?
                            WHERE NIT = ?
                              AND Factura = ?
                            """
                            cur.execute(update_comparativa, (nuevo_resultado, nit, factura))
                            print("[UPDATE] Comparativa actualizada (ESTADO)")
                            cx.commit()
                            cur.close()
                        
                        else:
                            print("[INFO] ClaseDeImpuesto NO contiene 31, no se procesa")
                    
                    # ====================================================
                    # CASO 2: NO TIENE CON NOVEDAD (APROBADO o vacio)
                    # ====================================================
                    
                    else:
                        print("[VALIDACION] Registro SIN CON NOVEDAD")
                        
                        # SUBCASO 2.1: ClaseDeImpuesto contiene 31
                        if tiene_clase31:
                            print("[ACCION] ClaseDeImpuesto contiene 31 (ZOMAC-ZESE)")
                            stats['aprobado_sin_contab'] += 1
                            
                            nuevo_resultado = "APROBADO SIN CONTABILIZACION"
                            
                            # Actualizar observaciones (PREPEND)
                            nueva_obs = "Factura corresponde a Clase de impuesto 31 (ZOMAC-ZESE)"
                            if obs_actual:
                                obs_final = nueva_obs + ", " + obs_actual
                            else:
                                obs_final = nueva_obs
                            
                            # Actualizar DocumentsProcessing
                            cur = cx.cursor()
                            
                            update_dp = """
                            UPDATE [CxP].[DocumentsProcessing]
                            SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso',
                                ObservacionesFase_4 = ?,
                                ResultadoFinalAntesEventos = ?
                            WHERE nit_emisor_o_nit_del_proveedor = ?
                              AND numero_de_factura = ?
                              AND numero_de_liquidacion_u_orden_de_compra = ?
                            """
                            cur.execute(update_dp, (obs_final, nuevo_resultado, nit, factura, oc))
                            print("[UPDATE] DocumentsProcessing actualizado")
                            
                            # Actualizar Comparativa
                            update_comparativa = """
                            UPDATE [dbo].[CxP.Comparativa]
                            SET Estado_validacion_antes_de_eventos = ?
                            WHERE NIT = ?
                              AND Factura = ?
                            """
                            cur.execute(update_comparativa, (nuevo_resultado, nit, factura))
                            print("[UPDATE] Comparativa actualizada (ESTADO)")
                            cx.commit()
                            cur.close()
                        
                        # SUBCASO 2.2: ClaseDeImpuesto NO contiene 31
                        else:
                            print("[VALIDACION] ClaseDeImpuesto NO contiene 31")
                            
                            # SUBCASO 2.2.1: forma_de_pago = "1" o "01"
                            if forma_pago in ['1', '01']:
                                print("[ACCION] forma_de_pago = " + forma_pago + " (CONTADO)")
                                stats['aprobado_contado'] += 1
                                
                                nuevo_resultado = "APROBADO CONTADO"
                                
                                # Actualizar observaciones (PREPEND)
                                nueva_obs = "Factura cuenta con forma de pago de contado"
                                if obs_actual:
                                    obs_final = nueva_obs + ", " + obs_actual
                                else:
                                    obs_final = nueva_obs
                                
                                # Actualizar DocumentsProcessing
                                cur = cx.cursor()
                                
                                update_dp = """
                                UPDATE [CxP].[DocumentsProcessing]
                                SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso',
                                    ObservacionesFase_4 = ?,
                                    ResultadoFinalAntesEventos = ?
                                WHERE nit_emisor_o_nit_del_proveedor = ?
                                  AND numero_de_factura = ?
                                  AND numero_de_liquidacion_u_orden_de_compra = ?
                                """
                                cur.execute(update_dp, (obs_final, nuevo_resultado, nit, factura, oc))
                                print("[UPDATE] DocumentsProcessing actualizado")
                                
                                # Actualizar Comparativa
                                update_comparativa = """
                                UPDATE [dbo].[CxP.Comparativa]
                                SET Estado_validacion_antes_de_eventos = ?
                                WHERE NIT = ?
                                  AND Factura = ?
                                """
                                cur.execute(update_comparativa, (nuevo_resultado, nit, factura))
                                print("[UPDATE] Comparativa actualizada (ESTADO)")
                                
                                # Actualizar HistoricoOrdenesCompra (solo columna Marca)
                                doccompra_list = split_valores(row['DocCompra_hoc'])
                                nitcedula_list = split_valores(row['NitCedula_hoc'])
                                porcalcular_list = split_valores(row['PorCalcular_hoc'])
                                textobreve_list = split_valores(row['TextoBreve_hoc'])
                                
                                num_actualizados = 0
                                max_len = max(len(doccompra_list), len(nitcedula_list), len(porcalcular_list), len(textobreve_list))
                                
                                for i in range(max_len):
                                    doccompra = doccompra_list[i] if i < len(doccompra_list) else ""
                                    nitcedula = nitcedula_list[i] if i < len(nitcedula_list) else ""
                                    porcalcular = porcalcular_list[i] if i < len(porcalcular_list) else ""
                                    textobreve = textobreve_list[i] if i < len(textobreve_list) else ""
                                    
                                    if doccompra and nitcedula:
                                        update_hoc = """
                                        UPDATE [CxP].[HistoricoOrdenesCompra]
                                        SET Marca = 'PROCESADO'
                                        WHERE DocCompra = ?
                                          AND NitCedula = ?
                                          AND PorCalcular = ?
                                          AND TextoBreve = ?
                                        """
                                        cur.execute(update_hoc, (doccompra, nitcedula, porcalcular, textobreve))
                                        num_actualizados += 1
                                
                                print("[UPDATE] HistoricoOrdenesCompra: " + str(num_actualizados) + " registros actualizados")
                                cx.commit()
                                cur.close()
                            
                            # SUBCASO 2.2.2: forma_de_pago != "1" ni "01"
                            else:
                                print("[ACCION] forma_de_pago != 1/01 (CREDITO)")
                                stats['aprobado'] += 1
                                
                                nuevo_resultado = "APROBADO"
                                
                                # Actualizar DocumentsProcessing
                                cur = cx.cursor()
                                
                                update_dp = """
                                UPDATE [CxP].[DocumentsProcessing]
                                SET EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso',
                                    ResultadoFinalAntesEventos = ?
                                WHERE nit_emisor_o_nit_del_proveedor = ?
                                  AND numero_de_factura = ?
                                  AND numero_de_liquidacion_u_orden_de_compra = ?
                                """
                                cur.execute(update_dp, (nuevo_resultado, nit, factura, oc))
                                print("[UPDATE] DocumentsProcessing actualizado")
                                
                                # Actualizar Comparativa
                                update_comparativa = """
                                UPDATE [dbo].[CxP.Comparativa]
                                SET Estado_validacion_antes_de_eventos = ?
                                WHERE NIT = ?
                                  AND Factura = ?
                                """
                                cur.execute(update_comparativa, (nuevo_resultado, nit, factura))
                                print("[UPDATE] Comparativa actualizada (ESTADO)")
                                
                                # Actualizar HistoricoOrdenesCompra (solo columna Marca)
                                doccompra_list = split_valores(row['DocCompra_hoc'])
                                nitcedula_list = split_valores(row['NitCedula_hoc'])
                                porcalcular_list = split_valores(row['PorCalcular_hoc'])
                                textobreve_list = split_valores(row['TextoBreve_hoc'])
                                
                                num_actualizados = 0
                                max_len = max(len(doccompra_list), len(nitcedula_list), len(porcalcular_list), len(textobreve_list))
                                
                                for i in range(max_len):
                                    doccompra = doccompra_list[i] if i < len(doccompra_list) else ""
                                    nitcedula = nitcedula_list[i] if i < len(nitcedula_list) else ""
                                    porcalcular = porcalcular_list[i] if i < len(porcalcular_list) else ""
                                    textobreve = textobreve_list[i] if i < len(textobreve_list) else ""
                                    
                                    if doccompra and nitcedula:
                                        update_hoc = """
                                        UPDATE [CxP].[HistoricoOrdenesCompra]
                                        SET Marca = 'PROCESADO'
                                        WHERE DocCompra = ?
                                          AND NitCedula = ?
                                          AND PorCalcular = ?
                                          AND TextoBreve = ?
                                        """
                                        cur.execute(update_hoc, (doccompra, nitcedula, porcalcular, textobreve))
                                        num_actualizados += 1
                                
                                print("[UPDATE] HistoricoOrdenesCompra: " + str(num_actualizados) + " registros actualizados")
                                cx.commit()
                                cur.close()
                
                except Exception as e_row:
                    print("[ERROR] Error procesando registro " + str(idx) + ": " + str(e_row))
                    stats['errores'] += 1
                    continue
            
            # ================================================================
            # FIN DE PROCESO
            # ================================================================
            
            stats['tiempo_total'] = time.time() - t_inicio
            
            print("")
            print("=" * 80)
            print("[FIN] Post-procesamiento completado")
            print("=" * 80)
            print("[ESTADISTICAS]")
            print("  Total registros: " + str(stats['total_registros']))
            print("  CON NOVEDAD + Clase 31: " + str(stats['con_novedad_clase31']))
            print("  APROBADO SIN CONTABILIZACION: " + str(stats['aprobado_sin_contab']))
            print("  APROBADO CONTADO: " + str(stats['aprobado_contado']))
            print("  APROBADO: " + str(stats['aprobado']))
            print("  Errores: " + str(stats['errores']))
            print("  Tiempo total: " + str(round(stats['tiempo_total'], 2)) + "s")
            print("=" * 80)
            
            msg = ("Post-procesamiento OK. Total:" + str(stats['total_registros']) + 
                   " Clase31:" + str(stats['con_novedad_clase31'] + stats['aprobado_sin_contab']) +
                   " Contado:" + str(stats['aprobado_contado']) + 
                   " Aprobado:" + str(stats['aprobado']))
            
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

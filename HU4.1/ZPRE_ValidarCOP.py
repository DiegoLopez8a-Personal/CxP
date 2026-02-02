"""
================================================================================
SCRIPT: ZPCN_ZPPA_ValidarCOP.py
================================================================================

Descripcion General:
--------------------
    Valida el valor total de factura para pedidos ZPCN/ZPPA/42 con moneda COP.
    Compara la suma de valores PorCalcular del historico de ordenes de compra
    contra el valor de compra LEA del XML de factura.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Plataforma: RocketBot RPA

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |              ZPCN_ZPPA_ValidarCOP()                         |
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
    |  - Moneda contiene COP o esta vacia                         |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Para cada registro:                                        |
    |  +-------------------------------------------------------+  |
    |  |  Sumar PorCalcular_hoc (SAP)                          |  |
    |  |  Sumar Valor de la Compra LEA_ddp (XML)               |  |
    |  |  Calcular diferencia absoluta                         |  |
    |  +-------------------------------------------------------+  |
    |  |  SI diferencia > tolerancia ($500):                   |  |
    |  |    -> CON NOVEDAD                                     |  |
    |  |    -> Actualizar DocumentsProcessing                  |  |
    |  |    -> Actualizar Comparativa (Valor = NO)             |  |
    |  |    -> Actualizar HistoricoOrdenesCompra               |  |
    |  +-------------------------------------------------------+  |
    |  |  SI diferencia <= tolerancia:                         |  |
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
    2. Moneda contiene: COP o esta vacia (asume COP por defecto)

================================================================================
VALIDACION REALIZADA
================================================================================

    Valor Total de Factura:
        - Suma de PorCalcular_hoc (SAP) - valores separados por |
        - Suma de "Valor de la Compra LEA_ddp" (XML)
        - Tolerancia: $500 COP
        
    Si diferencia > tolerancia:
        - Estado: CON NOVEDAD o CON NOVEDAD - CONTADO
        - Observacion: "No se encuentra coincidencia en el valor total..."
        - Item 'Valor' en Comparativa = Aprobado: 'NO'

================================================================================
TABLAS ACTUALIZADAS (Cuando hay novedad)
================================================================================

    [CxP].[DocumentsProcessing]
        - EstadoFinalFase_4 = 'VALIDACION DATOS DE FACTURACION: Exitoso'
        - ObservacionesFase_4 = Observacion concatenada
        - ResultadoFinalAntesEventos = Estado final

    [dbo].[CxP.Comparativa]
        - Aprobado = 'NO' (Item = 'Valor')
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
    ZPCN_ZPPA_ValidarCOP()
    
    # Verificar resultado
    resultado = GetVar("vLocStrResultadoSP")  # "True"

================================================================================
NOTAS TECNICAS
================================================================================

    - Tolerancia fija de $500 COP
    - Crea items 'Valor' y 'Observaciones' si no existen (verificar_y_crear_item)
    - Valores se suman individualmente (puede haber multiples posiciones)
    - Observaciones se truncan a 3900 caracteres

================================================================================
"""

def ZPRE_ValidarCOP():
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
    print("[INICIO] Funcion ZPRE_ValidarCOP() iniciada")
    print("[INICIO] Timestamp: " + str(datetime.now()))
    print("=" * 80)
    
    def actualizar_items_comparativa(id_reg, cx, nit, factura, nombre_item, valores_lista, actualizar_valor_xml=False, valor_xml=None,actualizar_aprobado=False, valor_aprobado=None):
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
                
                # CORRECCIÓN: Eliminar TOP 1 (incompatible con OFFSET/FETCH)
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
                
                # CORRECCIÓN: Eliminar TOP 1 (incompatible con OFFSET/FETCH)
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
        if not valor_str or valor_str == "" or pd.isna(valor_str):
            return []
        valores = str(valor_str).split('|')
        return [v.strip() for v in valores if v.strip()]
    
    def convertir_a_numero(valor_str):
        if not valor_str or valor_str == "" or pd.isna(valor_str):
            return 0.0
        try:
            v = str(valor_str).strip()
            v = v.replace(',', '')
            return float(v)
        except:
            print("[WARNING] No se pudo convertir '" + str(valor_str) + "' a numero, usando 0.0")
            return 0.0
    
    def sumar_valores(valor_str, nombre_campo="campo"):
        valores = split_valores(valor_str)
        suma = 0.0
        errores = []
        
        for i, v in enumerate(valores):
            try:
                v_limpio = v.strip()
                v_limpio = v_limpio.replace(',', '')
                valor_num = float(v_limpio)
                suma += valor_num
                
                if i < 3:
                    print("[DEBUG] " + nombre_campo + "[" + str(i) + "]: '" + v + "' -> " + str(valor_num))
                    
            except ValueError as e:
                errores.append("Valor[" + str(i) + "]='" + v + "' no convertible: " + str(e))
            except Exception as e:
                errores.append("Error[" + str(i) + "]='" + v + "': " + str(e))
        
        if errores:
            print("[WARNING] Errores en conversion de " + nombre_campo + ":")
            for err in errores[:5]:
                print("  " + err)
        
        print("[DEBUG] " + nombre_campo + " - Total valores: " + str(len(valores)) + " | Suma: " + str(suma))
        
        return suma
    
    def contiene_valor(campo, valor_buscado):
        valores = split_valores(campo)
        return valor_buscado in valores
    
    try:
        print("[DEBUG] Obteniendo configuracion...")
        cfg = parse_config(GetVar("vLocDicConfig"))
        print("[DEBUG] Configuracion obtenida OK")
        print("[DEBUG] Servidor: " + cfg.get('ServidorBaseDatos', 'N/A'))
        print("[DEBUG] Base de datos: " + cfg.get('NombreBaseDatos', 'N/A'))
        
        tolerancia = float(cfg.get('Tolerancia', 500))
        print("[DEBUG] Tolerancia configurada: " + str(tolerancia))
        
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
                PorCalcular_hoc,
                [Valor de la Compra LEA_ddp],
                Posicion_hoc,
                TipoNif_hoc,
                Acreedor_hoc,
                FecDoc_hoc,
                FecReg_hoc,
                FecContGasto_hoc,
                IndicadorImpuestos_hoc,
                TextoBreve_hoc,
                ClaseDeImpuesto_hoc,
                Cuenta_hoc,
                CiudadProveedor_hoc,
                DocFiEntrada_hoc,
                Cuenta26_hoc,
                DocCompra_hoc,
                NitCedula_hoc,
                Moneda_hoc
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
                lambda x: contiene_valor(x, 'ZPRE') or contiene_valor(x, '45') if pd.notna(x) else False
            )
            
            print("[DEBUG] Registros con ClaseDePedido = ZPRE o 45: " + str(mask_clase.sum()))
            
            def es_cop_o_vacio(valor):
                if pd.isna(valor) or valor == "":
                    return True
                valores = split_valores(valor)
                return 'COP' in valores or len(valores) == 0
            
            mask_cop = df_candidatos['Moneda_hoc'].apply(es_cop_o_vacio)
            
            print("[DEBUG] Registros con Moneda COP/vacio: " + str(mask_cop.sum()))
            
            df_filtrado = df_candidatos[mask_clase & mask_cop].copy()
            
            print("[DEBUG] Registros despues de filtros: " + str(len(df_filtrado)))
            
            if df_filtrado.empty:
                print("[INFO] No hay registros que cumplan los filtros")
                SetVar("vLocStrResultadoSP", "True")
                SetVar("vLocStrResumenSP", "No hay registros con ClaseDePedido ZPRE/45 y COP")
                SetVar("vLocDicEstadisticas", str(stats))
                return True, "No hay registros que cumplan filtros", None, stats
            
            stats['total_registros'] = len(df_filtrado)
            
            print("")
            print("[PASO 3] Procesando registros...")
            
            for idx, row in df_filtrado.iterrows():
                try:
                    print("")
                    print("[REGISTRO " + str(idx + 1) + "/" + str(len(df_filtrado)) + "]")
                    
                    id_reg = safe_str(row['ID_dp'])
                    nit = safe_str(row['nit_emisor_o_nit_del_proveedor_dp'])
                    factura = safe_str(row['numero_de_factura_dp'])
                    oc = safe_str(row['numero_de_liquidacion_u_orden_de_compra_dp'])
                    forma_pago = safe_str(row['forma_de_pago_dp'])
                    
                    print("[DEBUG] NIT: " + nit)
                    print("[DEBUG] Factura: " + factura)
                    print("[DEBUG] OC: " + oc)
                    print("")
                    
                    print("[CALCULO] Sumando valores de PorCalcular_hoc...")
                    suma_por_calcular = sumar_valores(row['PorCalcular_hoc'], "PorCalcular_hoc")
                    
                    print("[CALCULO] Sumando valores de Valor de la Compra LEA_ddp...")
                    suma_valor_compra = sumar_valores(row['Valor de la Compra LEA_ddp'], "Valor_Compra_LEA")
                    
                    diferencia = abs(suma_por_calcular - suma_valor_compra)
                    print("")
                    
                    print("[DEBUG] Suma PorCalcular: " + str(suma_por_calcular))
                    print("[DEBUG] Suma Valor Compra: " + str(suma_valor_compra))
                    print("[DEBUG] Diferencia: " + str(diferencia))
                    print("[DEBUG] Tolerancia: " + str(tolerancia))
                    
                    if diferencia <= tolerancia:
                        print("[RESULTADO] APROBADO (diferencia <= " + str(tolerancia) + ")")
                        stats['aprobados'] += 1
                        
                        print("[UPDATE] Actualizando tabla CxP.Comparativa...")
                        
                        valores_porcalcular = split_valores(row['PorCalcular_hoc'])
                        valores_posicion = split_valores(row['Posicion_hoc'])
                        valores_tiponif = split_valores(row['TipoNif_hoc'])
                        valores_acreedor = split_valores(row['Acreedor_hoc'])
                        valores_fecdoc = split_valores(row['FecDoc_hoc'])
                        valores_fecreg = split_valores(row['FecReg_hoc'])
                        valores_feccontgasto = split_valores(row['FecContGasto_hoc'])
                        valores_indicadorimpuestos = split_valores(row['IndicadorImpuestos_hoc'])
                        valores_textobreve = split_valores(row['TextoBreve_hoc'])
                        valores_clasedeimpuesto = split_valores(row['ClaseDeImpuesto_hoc'])
                        valores_cuenta = split_valores(row['Cuenta_hoc'])
                        valores_ciudadproveedor = split_valores(row['CiudadProveedor_hoc'])
                        valores_docfientrada = split_valores(row['DocFiEntrada_hoc'])
                        valores_cuenta26 = split_valores(row['Cuenta26_hoc'])
                        
                        actualizar_items_comparativa(
                            id_reg, cx, nit, factura,
                            'LineExtensionAmount',
                            valores_porcalcular,
                            actualizar_valor_xml=True,
                            valor_xml=str(suma_valor_compra),
                            actualizar_aprobado=True,
                            valor_aprobado='SI'
                        )
                        
                        if valores_posicion:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'Posicion', valores_posicion)
                        
                        if valores_tiponif:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'TipoNIF', valores_tiponif)
                        
                        if valores_acreedor:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'Acreedor', valores_acreedor)
                        
                        if valores_fecdoc:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'FecDoc', valores_fecdoc)
                        
                        if valores_fecreg:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'FecReg', valores_fecreg)
                        
                        if valores_feccontgasto:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'FecContGasto', valores_feccontgasto)
                        
                        if valores_indicadorimpuestos:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'IndicadorImpuestos', valores_indicadorimpuestos)
                        
                        if valores_textobreve:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'TextoBreve', valores_textobreve)
                        
                        if valores_clasedeimpuesto:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'ClaseDeImpuesto', valores_clasedeimpuesto)
                        
                        if valores_cuenta:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'Cuenta', valores_cuenta)
                        
                        if valores_ciudadproveedor:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'CiudadProveedor', valores_ciudadproveedor)
                        
                        if valores_docfientrada:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'DocFIEntrada', valores_docfientrada)
                        
                        if valores_cuenta26:
                            actualizar_items_comparativa(id_reg, cx, nit, factura, 'Cuenta26', valores_cuenta26)
                        
                        print("[UPDATE] Tabla CxP.Comparativa actualizada OK")
                        
                    else:
                        print("[RESULTADO] CON NOVEDAD (diferencia > " + str(tolerancia) + ")")
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
                        nueva_obs = "No se encuentra coincidencia del Valor a pagar de la factura"
                        
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
                        
                        valores_porcalcular = split_valores(row['PorCalcular_hoc'])
                        
                        actualizar_items_comparativa(
                            id_reg, cx, nit, factura,
                            'LineExtensionAmount',
                            valores_porcalcular,
                            actualizar_valor_xml=True,
                            valor_xml=str(suma_valor_compra),
                            actualizar_aprobado=True,
                            valor_aprobado='NO'
                        )
                        
                        cur = cx.cursor()
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
                        nueva_obs_comp = "No se encuentra coincidencia del Valor a pagar de la factura"
                        
                        if obs_comp_actual:
                            obs_comp_final = nueva_obs_comp + ", " + obs_comp_actual
                        else:
                            obs_comp_final = nueva_obs_comp
                        
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
                        cx.commit()
                        cur.close()
                        
                        print("[DEBUG] CxP.Comparativa actualizado OK")
                        
                        print("[UPDATE] Actualizando tabla HistoricoOrdenesCompra...")
                        
                        valores_doccompra = split_valores(row['DocCompra_hoc'])
                        valores_nitcedula = split_valores(row['NitCedula_hoc'])
                        valores_porcalcular_hoc = split_valores(row['PorCalcular_hoc'])
                        valores_textobreve = split_valores(row['TextoBreve_hoc'])
                        
                        cur = cx.cursor()
                        num_actualizados = 0
                        for i in range(max(len(valores_doccompra), len(valores_nitcedula), 
                                          len(valores_porcalcular_hoc), len(valores_textobreve))):
                            
                            doccompra_val = valores_doccompra[i] if i < len(valores_doccompra) else ""
                            nitcedula_val = valores_nitcedula[i] if i < len(valores_nitcedula) else ""
                            porcalcular_val = valores_porcalcular_hoc[i] if i < len(valores_porcalcular_hoc) else ""
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
                        print("[UPDATE] Todas las tablas actualizadas OK")
                        
                except Exception as e_row:
                    print("[ERROR] Error procesando registro " + str(idx) + ": " + str(e_row))
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
            
            msg = "Proceso OK. Total:" + str(stats['total_registros']) + " Aprobados:" + str(stats['aprobados']) + " ConNovedad:" + str(stats['con_novedad'])
            
            SetVar("vLocStrResultadoSP", "True")
            SetVar("vLocStrResumenSP", msg)
            SetVar("vLocDicEstadisticas", str(stats))
            SetVar("vGblStrDetalleError", "")
            SetVar("vGblStrSystemError", "")
            
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

def HU8_GenerarReportesCxP():
    """
    Función para generar reportes y organizar archivos del proceso CxP.
    
    Esta función es el componente final del proceso de automatización (Historia de Usuario 8).
    Su objetivo es doble:
    1.  **Gestión de Archivos:** Organizar los archivos físicos (PDFs y XMLs) de las facturas procesadas,
        moviéndolos desde una carpeta de entrada a una estructura de carpetas histórica basada en fecha y estado.
    2.  **Generación de Reportes:** Crear múltiples reportes en Excel que consolidan la información del proceso
        para diferentes audiencias (Auditoría, Contabilidad, Tesorería).

    Funcionalidades:
        1.  Crear árbol de carpetas en File Server (Año/Mes/Día/Estado).
        2.  Identificar y verificar archivos XML/PDF asociados a cada registro.
        3.  Mover archivos a su carpeta destino según el estado final (Aprobado, Rechazado, Con Novedad).
        4.  Generar los siguientes reportes:
            - **Reporte_de_ejecución_CXP**: Detalle diario de facturas procesadas.
            - **Reporte_de_ejecución_GRANOS**: Filtro específico para granos.
            - **Reporte_de_ejecución_MAÍZ**: Filtro específico para maíz.
            - **Reporte_de_ejecución_COMERCIALIZADOS**: Filtro para comercializados.
            - **Reporte_KPIs_CXP**: Estadísticas mensuales.
            - **Consolidado_FV_CXP_ConNovedad**: Acumulado de novedades.
            - **Consolidado_CXP_NoExitososRechazados**: Acumulado de rechazos.
            - **Consolidado_CXP_Pendientes**: Documentos pendientes de gestión.
            - **Consolidado_Global_CXP**: Trazabilidad anual completa.
            - **Consolidado_NC_ND_CXP**: Reporte de Notas Crédito y Débito.

    Variables de entrada (RocketBot):
        - vLocDicConfig (str | dict): Configuración general.
            - RutaFileServer: Ruta raíz para carpetas y reportes.
            - DiaReporteMensualAnual: Día del mes para ejecutar reportes mensuales.
            - MesReporteAnual: Mes para ejecutar reporte anual.

    Variables de salida (RocketBot):
        - vLocStrResultadoSP: "True" / "False".
        - vLocStrResumenSP: Resumen de la ejecución.
    
    Author:
        Diego Ivan Lopez Ochoa
    """
    
    # =========================================================================
    # IMPORTS
    # =========================================================================
    import json
    import ast
    import traceback
    import pyodbc
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    from contextlib import contextmanager
    import time
    import warnings
    import re
    import os
    import shutil
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils.dataframe import dataframe_to_rows
    from openpyxl.utils import get_column_letter
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    warnings.filterwarnings('ignore')
    
    # =========================================================================
    # CONSTANTES
    # =========================================================================
    
    MESES_ESPANOL = {
        1: '01. Enero', 2: '02. Febrero', 3: '03. Marzo', 4: '04. Abril',
        5: '05. Mayo', 6: '06. Junio', 7: '07. Julio', 8: '08. Agosto',
        9: '09. Septiembre', 10: '10. Octubre', 11: '11. Noviembre', 12: '12. Diciembre'
    }
    
    CARPETAS_INSUMOS = [
        'EN ESPERA',
        'CON NOVEDAD NO CONTADO',
        'CON NOVEDAD NO CONTADO/EXCLUIDOS CONTABILIZACION',
        'CON NOVEDAD CONTADO',
        'CON NOVEDAD CONTADO/EXCLUIDOS CONTABILIZACION',
        'APROBADOS NO CONTADO',
        'APROBADOS CONTADO',
        'APROBADOS SIN CONTABILIZACION',
        'APROBADO CONTADO Y O EVENTO MANUAL',
        'NO EXITOSOS',
        'PENDIENTES',
        'RECLASIFICADOS',
        'RECHAZADOS',
        'ND EXITOSOS',
        'NC ENCONTRADOS'
    ]
    
    # =========================================================================
    # FUNCIONES AUXILIARES BÁSICAS
    # =========================================================================
    
    def safe_str(v):
        """Convierte un valor a string de manera segura."""
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
            if isinstance(v, float) and (np.isnan(v) or pd.isna(v)):
                return ""
            return str(v)
        try:
            return str(v).strip()
        except:
            return ""
    
    def parse_config(raw):
        """Parsea la configuración desde RocketBot."""
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
    
    def campo_vacio(valor):
        """Verifica si un campo está vacío."""
        valor_str = safe_str(valor)
        return valor_str == "" or valor_str.lower() in ('null', 'none', 'nan')
    
    def campo_con_valor(valor):
        """Verifica si un campo tiene valor."""
        return not campo_vacio(valor)
    
    # =========================================================================
    # CONEXIÓN A BASE DE DATOS
    # =========================================================================
    
    @contextmanager
    def crear_conexion_db(cfg, max_retries=3):
        """
        Context Manager para establecer una conexion segura y resiliente a SQL Server.
        """
        required = ["ServidorBaseDatos", "NombreBaseDatos"]
        missing = [k for k in required if not cfg.get(k)]
        if missing:
            raise ValueError(f"Parametros faltantes: {', '.join(missing)}")

        usuario = cfg['UsuarioBaseDatos']
        contrasena = cfg['ClaveBaseDatos']
        
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
                
    def actualizar_insumos_comparativa(registro, cx, nit, factura, nombre_item, valor_insumo):
        """
        Actualiza específicamente los ítems de insumos (InsumoPDF, InsumoXML) 
        en la tabla [dbo].[CxP.Comparativa].
        """
        cur = cx.cursor()
        id_reg = registro.get('ID_dp', '')

        def safe_db_val(v):
            if v is None: return None
            s = str(v).strip()
            return None if s.lower() in ['none', 'null', ''] else s

        val_final = safe_db_val(valor_insumo)

        try:
            # Verificar si ya existe el registro para este ítem específico
            query_check = """
            SELECT COUNT(*) FROM [dbo].[CxP.Comparativa]
            WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?
            """
            cur.execute(query_check, (nit, factura, nombre_item, id_reg))
            existe = cur.fetchone()[0] > 0

            if existe:
                # Actualizar solo el campo Valor_XML
                update_query = """
                UPDATE [dbo].[CxP.Comparativa]
                SET Valor_XML = ?
                WHERE NIT = ? AND Factura = ? AND Item = ? AND ID_registro = ?
                """
                cur.execute(update_query, (val_final, nit, factura, nombre_item, id_reg))
            else:
                # Insertar registro nuevo con la información base del documento
                insert_query = """
                INSERT INTO [dbo].[CxP.Comparativa] (
                    Fecha_de_retoma_antes_de_contabilizacion, Tipo_de_Documento, 
                    Orden_de_Compra, Nombre_Proveedor, ID_registro, NIT, Factura, 
                    Item, Valor_XML
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(insert_query, (
                    registro.get('Fecha_de_retoma_antes_de_contabilizacion_dp',''),
                    registro.get('documenttype_dp',''),
                    registro.get('numero_de_liquidacion_u_orden_de_compra_dp',''),
                    registro.get('nombre_emisor_dp',''), 
                    id_reg, nit, factura, nombre_item, val_final
                ))
            
            cx.commit()
        except Exception as e:
            print(f"[ERROR] Error actualizando insumo {nombre_item}: {str(e)}")
            cx.rollback()
        finally:
            cur.close()
    
    # =========================================================================
    # FUNCIONES DE GESTIÓN DE CARPETAS
    # =========================================================================
    
    def verificar_acceso_ruta(ruta_base):
        """Verifica si se tiene acceso a la ruta del File Server."""
        try:
            if os.path.exists(ruta_base):
                # Intentar crear un archivo temporal para verificar permisos de escritura
                test_file = os.path.join(ruta_base, '.test_access')
                try:
                    with open(test_file, 'w') as f:
                        f.write('test')
                    os.remove(test_file)
                    return True
                except:
                    print(f"[WARNING] Acceso de solo lectura a {ruta_base}")
                    return True  # Al menos tiene acceso de lectura
            return False
        except Exception as e:
            print(f"[ERROR] Error verificando acceso: {str(e)}")
            return False
    
    def crear_arbol_carpetas(ruta_base, fecha_ejecucion, ult_numero):
        """
        Crea el árbol completo de carpetas según la estructura requerida.
        """
        try:
            anio = fecha_ejecucion.year
            mes = fecha_ejecucion.month
            dia = fecha_ejecucion.day
            
            try:
                mes_nombre = MESES_ESPANOL.get(mes)
                dia_str = f'{dia:02d}'
                if not mes_nombre or not dia_str:
                    raise Exception('No fue posible extraer el nombre del mes o del dia para la creacion de las carpetas')
            except Exception as e:
                raise e
            
            ult_numero = f"EJECUCION {ult_numero} CXP"
            
            # Estructura principal
            rutas_crear = [
                # Consolidados y retorno
                os.path.join(ruta_base, str(anio), mes_nombre, 'CONSOLIDADOS'),
                os.path.join(ruta_base, str(anio), mes_nombre, 'INSUMO DE RETORNO'),
                
                # Resultados del día
                os.path.join(ruta_base, str(anio), mes_nombre, dia_str, 'RESULTADOS BOT CXP'),
                
                # Carpetas de insumos por estado
            ]
            
            # Agregar carpetas de insumos
            ruta_insumos = os.path.join(ruta_base, str(anio), mes_nombre, dia_str, ult_numero, 'CXP', 'INSUMOS')
            for carpeta in CARPETAS_INSUMOS:
                rutas_crear.append(os.path.join(ruta_insumos, carpeta))
            
            # Materia Prima Granos
            rutas_crear.extend([
                os.path.join(ruta_base, str(anio), 'MATERIA PRIMA GRANOS', str(anio), mes_nombre, 'INSUMO'),
                os.path.join(ruta_base, str(anio), 'MATERIA PRIMA GRANOS', str(anio), mes_nombre, 'RESULTADO'),
            ])
            
            # Materia Prima Maíz
            rutas_crear.extend([
                os.path.join(ruta_base, str(anio), 'MATERIA PRIMA MAIZ', str(anio), mes_nombre, 'INSUMO'),
                os.path.join(ruta_base, str(anio), 'MATERIA PRIMA MAIZ', str(anio), mes_nombre, 'RESULTADO'),
            ])
            
            # Comercializados
            rutas_crear.extend([
                os.path.join(ruta_base, str(anio), 'COMERCIALIZADOS', str(anio), mes_nombre, dia_str, 'INSUMO', 'CON NOVEDAD'),
                os.path.join(ruta_base, str(anio), 'COMERCIALIZADOS', str(anio), mes_nombre, dia_str, 'INSUMO', 'EN ESPERA'),
                os.path.join(ruta_base, str(anio), 'COMERCIALIZADOS', str(anio), mes_nombre, dia_str, 'RESULTADO'),
            ])
            
            # Crear todas las carpetas
            for ruta in rutas_crear:
                os.makedirs(ruta, exist_ok=True)
            
            print(f"[INFO] Arbol de carpetas creado/verificado exitosamente")
            
            # Retornar rutas importantes
            return {
                'consolidados': os.path.join(ruta_base, str(anio), mes_nombre, 'CONSOLIDADOS'),
                'insumo_retorno': os.path.join(ruta_base, str(anio), mes_nombre, 'INSUMO DE RETORNO'),
                'resultados_dia': os.path.join(ruta_base, str(anio), mes_nombre, dia_str, 'RESULTADOS BOT CXP'),
                'insumos_cxp': ruta_insumos,
                'granos_resultado': os.path.join(ruta_base, str(anio), 'MATERIA PRIMA GRANOS', str(anio), mes_nombre, 'RESULTADO'),
                'maiz_resultado': os.path.join(ruta_base, str(anio), 'MATERIA PRIMA MAIZ', str(anio), mes_nombre, 'RESULTADO'),
                'comercializados_resultado': os.path.join(ruta_base, str(anio), 'COMERCIALIZADOS', str(anio), mes_nombre, dia_str, 'RESULTADO'),
                'comercializados_insumo': os.path.join(ruta_base, str(anio), 'COMERCIALIZADOS', str(anio), mes_nombre, dia_str, 'INSUMO'),
                'global_anual': os.path.join(ruta_base, str(anio)),
            }
            
        except Exception as e:
            print(f"[ERROR] Error creando arbol de carpetas: {str(e)}")
            raise
    
    def determinar_carpeta_destino(resultado_final, tipo_documento):
        """
        Determina la carpeta de destino según el resultado final y tipo de documento.
        """
        resultado = safe_str(resultado_final).upper()
        tipo = safe_str(tipo_documento).upper()
        
        # ND siempre va a ND EXITOSOS si es exitoso
        if tipo == 'ND':
            if 'NO EXITOSO' in resultado:
                return 'NO EXITOSOS'
            elif 'EXITOSO' in resultado:
                return 'ND EXITOSOS'
            elif 'PENDIENTE' in resultado:
                return 'PENDIENTES'
            else:
                return ''
        
        # NC
        if tipo == 'NC':
            if 'ENCONTRADO' in resultado:
                return 'NC ENCONTRADOS'
        
        # FV y NC común
        if 'NO EXITOSO' in resultado:
            return 'NO EXITOSOS'
        
        if 'PENDIENTE' in resultado:
            return 'PENDIENTES'
        
        if 'EN ESPERA' in resultado:
            return 'EN ESPERA'
        
        if 'RECHAZADO' in resultado:
            return 'RECHAZADOS'
        
        if 'RECLASIFICADO' in resultado:
            return 'RECLASIFICADOS'
        
        # CON NOVEDAD
        if 'CON NOVEDAD' in resultado:
            if 'EXCLUIDOS CONTABILIZACION' in resultado or 'EXCLUIDO CONTABILIZACION' in resultado:
                if 'CONTADO' in resultado:
                    return 'CON NOVEDAD CONTADO/EXCLUIDOS CONTABILIZACION'
                else:
                    return 'CON NOVEDAD NO CONTADO/EXCLUIDOS CONTABILIZACION'
            elif 'CONTADO' in resultado:
                return 'CON NOVEDAD CONTADO'
            else:
                return 'CON NOVEDAD NO CONTADO'
        
        # APROBADO
        if 'APROBADO' in resultado:
            if 'CONTADO Y/O EVENTO MANUAL' in resultado or 'CONTADO Y O EVENTO MANUAL' in resultado:
                return 'APROBADO CONTADO Y O EVENTO MANUAL'
            elif 'SIN CONTABILIZACION' in resultado:
                return 'APROBADOS SIN CONTABILIZACION'
            elif 'CONTADO' in resultado:
                return 'APROBADOS CONTADO'
            else:
                return 'APROBADOS NO CONTADO'
        
        # Por defecto
        return 'PENDIENTES'
    
    # =========================================================================
    # FUNCIONES DE MANEJO DE ARCHIVOS
    # =========================================================================
    
    def verificar_archivos_insumo(ruta_respaldo, nombre_archivos):
        """
        Verifica la existencia de archivos XML y PDF a partir de un nombre que ya incluye extensión.
        """
        xml_encontrado = False
        pdf_encontrado = False
        ruta_xml = None
        ruta_pdf = None
        
        try:
            if campo_vacio(ruta_respaldo) or campo_vacio(nombre_archivos):
                return xml_encontrado, pdf_encontrado, ruta_xml, ruta_pdf
            
            # 1. Limpiamos el nombre y separamos la extensión
            nombre_completo = safe_str(nombre_archivos)
            # os.path.splitext separa 'archivo.xml' en ('archivo', '.xml')
            nombre_base, ext = os.path.splitext(nombre_completo)
            
            # 2. Definimos las rutas finales
            # Forzamos las extensiones correctas partiendo del nombre base
            ruta_xml = os.path.join(ruta_respaldo, f"{nombre_base}.xml")
            ruta_pdf = os.path.join(ruta_respaldo, f"{nombre_base}.pdf")
            
            # 3. Verificamos XML
            if os.path.exists(ruta_xml):
                xml_encontrado = True
            else:
                ruta_xml = None # Limpiamos si no existe
                
            # 4. Verificamos PDF
            if os.path.exists(ruta_pdf):
                pdf_encontrado = True
            else:
                ruta_pdf = None # Limpiamos si no existe
                
        except Exception as e:
            print(f"[ERROR] Error verificando archivos: {str(e)}")
        
        return xml_encontrado, pdf_encontrado, ruta_xml, ruta_pdf
    
    def mover_archivos_a_destino(ruta_xml, carpeta_destino, numero_oc=None, ruta_comercializados=None):
        """
        Mueve los archivos XML y PDF a la carpeta de destino.
        También copia a comercializados si OC inicia con 50.
        """
        nueva_ruta = None
        
        try:
            os.makedirs(carpeta_destino, exist_ok=True)
            
            archivos_movidos = []
            
            if ruta_xml and os.path.exists(ruta_xml):
                nombre_xml = os.path.basename(ruta_xml)
                destino_xml = os.path.join(carpeta_destino, nombre_xml)
                shutil.copy2(ruta_xml, destino_xml)
                archivos_movidos.append(destino_xml)
                
                # Copiar a comercializados si aplica
                if numero_oc and safe_str(numero_oc).startswith('50') and ruta_comercializados:
                    os.makedirs(ruta_comercializados, exist_ok=True)
                    shutil.copy2(ruta_xml, os.path.join(ruta_comercializados, nombre_xml))
            
            if archivos_movidos:
                nueva_ruta = carpeta_destino
                
            return nueva_ruta
            
        except Exception as e:
            print(f"[ERROR] Error moviendo archivos: {str(e)}")
            return None
        
    
    # =========================================================================
    # FUNCIONES DE FORMATO EXCEL
    # =========================================================================
    
    def aplicar_formato_encabezado(ws, num_columnas):
        """Aplica formato de encabezado a la primera fila."""
        header_font = Font(bold=True, color='FFFFFF')
        header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
        header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for col in range(1, num_columnas + 1):
            cell = ws.cell(row=1, column=col)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = thin_border
    
    def ajustar_ancho_columnas(ws):
        """Ajusta automáticamente el ancho de las columnas al contenido."""
        for column_cells in ws.columns:
            max_length = 0
            column = column_cells[0].column_letter
            
            for cell in column_cells:
                try:
                    if cell.value:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                except:
                    pass
            
            adjusted_width = min(max_length + 2, 50)  # Máximo 50 caracteres
            ws.column_dimensions[column].width = adjusted_width
    
    def crear_excel_desde_df(df, ruta_archivo, nombre_hoja='Datos'):
        """Crea un archivo Excel formateado desde un DataFrame."""
        try:
            wb = Workbook()
            ws = wb.active
            ws.title = nombre_hoja
            
            # Escribir encabezados
            for col_idx, column in enumerate(df.columns, 1):
                ws.cell(row=1, column=col_idx, value=column)
            
            # Escribir datos
            for row_idx, row in enumerate(df.itertuples(index=False), 2):
                for col_idx, value in enumerate(row, 1):
                    if pd.isna(value):
                        ws.cell(row=row_idx, column=col_idx, value='')
                    else:
                        ws.cell(row=row_idx, column=col_idx, value=value)
            
            # Aplicar formato
            aplicar_formato_encabezado(ws, len(df.columns))
            ajustar_ancho_columnas(ws)
            
            wb.save(ruta_archivo)
            print(f"[INFO] Archivo creado: {ruta_archivo}")
            return True
            
        except Exception as e:
            print(f"[ERROR] Error creando Excel: {str(e)}")
            return False
    
    def crear_excel_multihoja(hojas_data, ruta_archivo):
        """
        Crea un archivo Excel con múltiples hojas.
        """
        try:
            wb = Workbook()
            
            # Eliminar hoja por defecto
            if 'Sheet' in wb.sheetnames:
                del wb['Sheet']
            
            for nombre_hoja, df in hojas_data.items():
                ws = wb.create_sheet(title=nombre_hoja[:31])  # Excel limita a 31 caracteres
                
                # Escribir encabezados
                for col_idx, column in enumerate(df.columns, 1):
                    ws.cell(row=1, column=col_idx, value=column)
                
                # Escribir datos
                for row_idx, row in enumerate(df.itertuples(index=False), 2):
                    for col_idx, value in enumerate(row, 1):
                        if pd.isna(value):
                            ws.cell(row=row_idx, column=col_idx, value='')
                        else:
                            ws.cell(row=row_idx, column=col_idx, value=value)
                
                # Aplicar formato
                aplicar_formato_encabezado(ws, len(df.columns))
                ajustar_ancho_columnas(ws)
            
            wb.save(ruta_archivo)
            print(f"[INFO] Archivo multihoja creado: {ruta_archivo}")
            return True
            
        except Exception as e:
            print(f"[ERROR] Error creando Excel multihoja: {str(e)}")
            return False
    
    # =========================================================================
    # FUNCIONES DE GENERACIÓN DE REPORTES
    # =========================================================================
    
    def generar_reporte_cxp(df_main, df_detalles, df_historico, rutabase):
        """
        Genera el Reporte de Ejecución CXP (Diario).
        Cruza la información principal, detalles y el histórico de órdenes.
        """
        
        # ---------------------------------------------------------
        # 1. PREPARACIÓN DE DATOS (LÓGICA SQL EN PANDAS)
        # ---------------------------------------------------------
        
        # Paso A: Limpieza de claves para asegurar que los cruces funcionen
        df_main['nit_join'] = df_main['nit_emisor_o_nit_del_proveedor'].astype(str).str.strip()
        df_main['factura_join'] = df_main['numero_de_factura'].astype(str).str.strip()
        df_main['doc_compra_join'] = df_main['numero_de_liquidacion_u_orden_de_compra'].astype(str).str.strip()

        df_detalles['nit_join'] = df_detalles['NIT'].astype(str).str.strip()
        df_detalles['factura_join'] = df_detalles['Factura'].astype(str).str.strip()

        df_historico['nit_join'] = df_historico['NitCedula'].astype(str).str.strip()
        df_historico['doc_compra_join'] = df_historico['DocCompra'].astype(str).str.strip()

        # Paso B: Lógica de la Tabla 3 (HistoricoOrdenesCompra)
        df_historico_unique = df_historico.drop_duplicates(subset=['nit_join', 'doc_compra_join'], keep='first')

        # Paso C: Cruce Principal (Main + Detalles)
        df_merged = pd.merge(
            df_main,
            df_detalles,
            how='left',
            left_on=['nit_join', 'factura_join'],
            right_on=['nit_join', 'factura_join']
        )

        # Paso D: Cruce con Histórico (Main + Historico)
        df_final = pd.merge(
            df_merged,
            df_historico_unique[['nit_join', 'doc_compra_join', 'ClaseDePedido']],
            how='left',
            left_on=['nit_join', 'doc_compra_join'],
            right_on=['nit_join', 'doc_compra_join']
        )

        # ---------------------------------------------------------
        # 2. RENOMBRADO Y SELECCIÓN DE COLUMNAS (MAPPING)
        # ---------------------------------------------------------
        column_mapping = {
            'executionDate': 'Fecha de ejecución',
            'Fecha_de_retoma_antes_de_contabilizacion': 'Fecha 1ra Revisión',
            'executionNum': 'ID Ejecución',
            'ID': 'ID Registro',
            'documenttype': 'Tipo Documento',
            'numero_de_liquidacion_u_orden_de_compra': 'Orden de Compra',
            'ClaseDePedido': 'Clase de Pedido', # Viene de la tabla 3
            'nit_emisor_o_nit_del_proveedor': 'NIT',
            'nombre_emisor': 'Nombre Proveedor',
            'numero_de_factura': 'Factura',
            'Item': 'Item', # Viene de la tabla 2
            'Valor_XML': 'Valor XML',
            'Valor_Orden_de_Compra': 'Valor OC',
            'Valor_Orden_de_Compra_Comercializados': 'Valor OC Comercializados',
            'Aprobado': 'Aprobado',
            'ResultadoFinalAntesEventos': 'Estado Validación',
            'Fecha_retoma_contabilizacion': 'Fecha Retoma Contab.',
            'Estado_contabilizacion': 'Estado Contabilización',
            'Fecha_de_retoma_compensacion': 'Fecha Retoma Comp.',
            'Estado_compensacion': 'Estado Compensación'
        }

        # Seleccionamos solo las columnas que existen en el mapping y las renombramos
        cols_to_keep = [c for c in column_mapping.keys() if c in df_final.columns]
        df_final = df_final[cols_to_keep].rename(columns=column_mapping)

        desired_order = list(column_mapping.values())
        final_cols = [c for c in desired_order if c in df_final.columns]
        df_final = df_final[final_cols]

        # ---------------------------------------------------------
        # 3. GENERACIÓN DEL EXCEL "HERMOSO"
        # ---------------------------------------------------------
        ahora = datetime.now()
        str_fecha_hora = ahora.strftime('%d%m%Y_%H%M')
        nombre_archivo = f"Reporte_de_ejecución_CXP_{str_fecha_hora}.xlsx"
        ruta_completa = os.path.join(rutabase, nombre_archivo)
        
        sheets_config = {
            'FACTURAS': 'FV',
            'NC': 'NC',
            'ND': 'ND'
        }

        with pd.ExcelWriter(ruta_completa, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'fg_color': '#1F4E78',
                'font_color': '#FFFFFF',
                'border': 1
            })
            
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
            money_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
            text_format = workbook.add_format({'border': 1})

            for sheet_name, doc_type in sheets_config.items():
                df_sheet = df_final[df_final['Tipo Documento'] == doc_type].copy()
                
                if df_sheet.empty:
                    continue

                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                
                worksheet = writer.sheets[sheet_name]
                (max_row, max_col) = df_sheet.shape
                
                column_settings = [{'header': column} for column in df_sheet.columns]
                worksheet.add_table(0, 0, max_row, max_col - 1, {
                    'columns': column_settings,
                    'style': 'TableStyleMedium2',
                    'name': f'Tabla_{sheet_name}'
                })

                for col_num, value in enumerate(df_sheet.columns):
                    worksheet.write(0, col_num, value, header_format)

                for i, col in enumerate(df_sheet.columns):
                    column_len = max(df_sheet[col].astype(str).map(len).max(), len(col)) + 2
                    column_len = min(column_len, 50) 
                    
                    if 'Fecha' in col:
                        worksheet.set_column(i, i, 12, date_format)
                    elif 'Valor' in col:
                        worksheet.set_column(i, i, 15, money_format)
                    else:
                        worksheet.set_column(i, i, column_len, text_format)

        print(f"✅ Reporte generado exitosamente: {ruta_completa}")

    def generar_reporte_granos(df_main, df_detalles, rutabase):
        """
        Genera el reporte de GRANOS en un archivo Excel con UNA SOLA HOJA.
        """
        
        df_main['nit_join'] = df_main['nit_emisor_o_nit_del_proveedor'].apply(lambda x: str(x).strip().replace('.0', '') if pd.notnull(x) else '')
        df_main['factura_join'] = df_main['numero_de_factura'].astype(str).str.strip()
        
        df_detalles['nit_join'] = df_detalles['NIT'].apply(lambda x: str(x).strip().replace('.0', '') if pd.notnull(x) else '')
        df_detalles['factura_join'] = df_detalles['Factura'].astype(str).str.strip()

        df_final = pd.merge(
            df_main,
            df_detalles, 
            how='left',
            left_on=['nit_join', 'factura_join'],
            right_on=['nit_join', 'factura_join']
        )

        column_mapping = {
            'executionDate': 'Fecha de ejecución',
            'Fecha_de_retoma_antes_de_contabilizacion': 'Fecha de retoma',
            'executionNum': 'ID ejecución',
            'ID': 'ID Registro',
            'documenttype': 'Tipo de documento',
            'numero_de_liquidacion_u_orden_de_compra': 'Orden de Compra',
            'nit_emisor_o_nit_del_proveedor': 'NIT',
            'nombre_emisor': 'Nombre Proveedor',
            'numero_de_factura': 'Factura',
            'ResultadoFinalAntesEventos': 'Estado validación antes de eventos',
            'Item': 'Item',
            'Valor_XML': 'Valor XML',
            'Valor_Orden_de_Compra': 'Valor Orden de Compra',
            'Valor_Orden_de_Compra_Comercializados': 'Valor OC Comercializados',
            'Aprobado': 'Aprobado'
        }

        cols_to_keep = [c for c in column_mapping.keys() if c in df_final.columns]
        df_final = df_final[cols_to_keep].rename(columns=column_mapping)

        desired_order = [
            'Fecha de ejecución', 'Fecha de retoma', 'ID ejecución', 'ID Registro', 
            'Tipo de documento', 'Orden de Compra', 'NIT', 'Nombre Proveedor', 'Factura', 
            'Item', 'Valor XML', 'Valor Orden de Compra', 'Valor OC Comercializados', 
            'Aprobado', 'Estado validación antes de eventos'
        ]
        final_cols = [c for c in desired_order if c in df_final.columns]
        df_final = df_final[final_cols]

        ahora = datetime.now()
        str_fecha_hora = ahora.strftime('%d%m%Y_%H%M')
        nombre_archivo = f"Reporte_de_ejecución_GRANOS_{str_fecha_hora}.xlsx"
        
        ruta_completa = os.path.join(rutabase, nombre_archivo)
        sheet_name = "Facturas"

        with pd.ExcelWriter(ruta_completa, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'fg_color': '#1F4E78',
                'font_color': '#FFFFFF',
                'border': 1
            })
            
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
            money_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
            text_format = workbook.add_format({'border': 1})

            df_final.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
            
            worksheet = writer.sheets[sheet_name]
            (max_row, max_col) = df_final.shape
            
            column_settings = [{'header': col} for col in df_final.columns]
            worksheet.add_table(0, 0, max_row, max_col - 1, {
                'columns': column_settings,
                'style': 'TableStyleMedium2',
                'name': 'TablaGranos'
            })
            
            for col_num, value in enumerate(df_final.columns):
                worksheet.write(0, col_num, value, header_format)

            for i, col in enumerate(df_final.columns):
                col_len = max(df_final[col].astype(str).map(len).max(), len(col)) + 3
                col_len = min(col_len, 60)
                
                if 'Fecha' in col:
                    worksheet.set_column(i, i, 14, date_format)
                elif 'Valor' in col:
                    worksheet.set_column(i, i, 18, money_format)
                else:
                    worksheet.set_column(i, i, col_len, text_format)

        print(f"✅ Reporte generado exitosamente: {ruta_completa}")
    
    def generar_reporte_maiz(df_main, df_detalles, rutabase):
        """
        Genera el reporte de MAIZ en un archivo Excel con UNA SOLA HOJA.
        """
        
        df_main['nit_join'] = df_main['nit_emisor_o_nit_del_proveedor'].apply(lambda x: str(x).strip().replace('.0', '') if pd.notnull(x) else '')
        df_main['factura_join'] = df_main['numero_de_factura'].astype(str).str.strip()
        
        df_detalles['nit_join'] = df_detalles['NIT'].apply(lambda x: str(x).strip().replace('.0', '') if pd.notnull(x) else '')
        df_detalles['factura_join'] = df_detalles['Factura'].astype(str).str.strip()

        df_final = pd.merge(
            df_main,
            df_detalles, 
            how='left',
            left_on=['nit_join', 'factura_join'],
            right_on=['nit_join', 'factura_join']
        )

        column_mapping = {
            'executionDate': 'Fecha de ejecución',
            'Fecha_de_retoma_antes_de_contabilizacion': 'Fecha de retoma',
            'executionNum': 'ID ejecución',
            'ID': 'ID Registro',
            'documenttype': 'Tipo de documento',
            'numero_de_liquidacion_u_orden_de_compra': 'Orden de Compra',
            'nit_emisor_o_nit_del_proveedor': 'NIT',
            'nombre_emisor': 'Nombre Proveedor',
            'numero_de_factura': 'Factura',
            'ResultadoFinalAntesEventos': 'Estado validación antes de eventos',
            'Item': 'Item',
            'Valor_XML': 'Valor XML',
            'Valor_Orden_de_Compra': 'Valor Orden de Compra',
            'Valor_Orden_de_Compra_Comercializados': 'Valor OC Comercializados',
            'Aprobado': 'Aprobado'
        }

        cols_to_keep = [c for c in column_mapping.keys() if c in df_final.columns]
        df_final = df_final[cols_to_keep].rename(columns=column_mapping)

        desired_order = [
            'Fecha de ejecución', 'Fecha de retoma', 'ID ejecución', 'ID Registro', 
            'Tipo de documento', 'Orden de Compra', 'NIT', 'Nombre Proveedor', 'Factura', 
            'Item', 'Valor XML', 'Valor Orden de Compra', 'Valor OC Comercializados', 
            'Aprobado', 'Estado validación antes de eventos'
        ]
        final_cols = [c for c in desired_order if c in df_final.columns]
        df_final = df_final[final_cols]

        ahora = datetime.now()
        str_fecha_hora = ahora.strftime('%d%m%Y_%H%M')
        nombre_archivo = f"Reporte_de_ejecución_MAIZ_{str_fecha_hora}.xlsx"
        
        ruta_completa = os.path.join(rutabase, nombre_archivo)
        
        sheet_name = "Facturas"

        with pd.ExcelWriter(ruta_completa, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'fg_color': '#1F4E78',
                'font_color': '#FFFFFF',
                'border': 1
            })
            
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
            money_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
            text_format = workbook.add_format({'border': 1})

            df_final.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
            
            worksheet = writer.sheets[sheet_name]
            (max_row, max_col) = df_final.shape
            
            column_settings = [{'header': col} for col in df_final.columns]
            worksheet.add_table(0, 0, max_row, max_col - 1, {
                'columns': column_settings,
                'style': 'TableStyleMedium2',
                'name': 'TablaGranos'
            })
            
            for col_num, value in enumerate(df_final.columns):
                worksheet.write(0, col_num, value, header_format)

            for i, col in enumerate(df_final.columns):
                col_len = max(df_final[col].astype(str).map(len).max(), len(col)) + 3
                col_len = min(col_len, 60)
                
                if 'Fecha' in col:
                    worksheet.set_column(i, i, 14, date_format)
                elif 'Valor' in col:
                    worksheet.set_column(i, i, 18, money_format)
                else:
                    worksheet.set_column(i, i, col_len, text_format)

        print(f"✅ Reporte generado exitosamente: {ruta_completa}")
    
    def generar_reporte_comercializados(df_main, df_detalles, rutabase):
        """
        Genera el reporte de COMERCIALIZADOS.
        """
        
        df_main['nit_join'] = df_main['nit_emisor_o_nit_del_proveedor'].astype(str).str.strip()
        df_main['factura_join'] = df_main['numero_de_factura'].astype(str).str.strip()
        df_main['doc_compra_join'] = df_main['numero_de_liquidacion_u_orden_de_compra'].astype(str).str.strip()

        df_detalles['nit_join'] = df_detalles['NIT'].astype(str).str.strip()
        df_detalles['factura_join'] = df_detalles['Factura'].astype(str).str.strip()

        df_merged = pd.merge(
            df_main,
            df_detalles,
            how='left',
            left_on=['nit_join', 'factura_join'],
            right_on=['nit_join', 'factura_join']
        )

        df_final = df_merged

        column_mapping = {
            'executionDate': 'Fecha de ejecución',
            'Fecha_de_retoma_antes_de_contabilizacion': 'Fecha 1ra Revisión',
            'executionNum': 'ID Ejecución',
            'ID': 'ID Registro',
            'documenttype': 'Tipo Documento',
            'numero_de_liquidacion_u_orden_de_compra': 'Orden de Compra',
            'nit_emisor_o_nit_del_proveedor': 'NIT',
            'nombre_emisor': 'Nombre Proveedor',
            'numero_de_factura': 'Factura',
            'Item': 'Item',
            'Valor_XML': 'Valor XML',
            'Valor_Orden_de_Compra': 'Valor OC',
            'Valor_Orden_de_Compra_Comercializados': 'Valor OC Comercializados',
            'Aprobado': 'Aprobado',
            'ResultadoFinalAntesEventos': 'Estado Validación',
        }

        cols_to_keep = [c for c in column_mapping.keys() if c in df_final.columns]
        df_final = df_final[cols_to_keep].rename(columns=column_mapping)

        desired_order = list(column_mapping.values())
        final_cols = [c for c in desired_order if c in df_final.columns]
        df_final = df_final[final_cols]

        ahora = datetime.now()
        str_fecha_hora = ahora.strftime('%d%m%Y_%H%M')
        nombre_archivo = f"Reporte_de_ejecución_COMERCIALIZADOS_{str_fecha_hora}.xlsx"
        
        ruta_completa = os.path.join(rutabase, nombre_archivo)
        
        sheets_config = {
            'FACTURAS': 'FV'
        }

        with pd.ExcelWriter(ruta_completa, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'fg_color': '#1F4E78',
                'font_color': '#FFFFFF',
                'border': 1
            })
            
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
            money_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
            text_format = workbook.add_format({'border': 1})

            for sheet_name, doc_type in sheets_config.items():
                df_sheet = df_final[df_final['Tipo Documento'] == doc_type].copy()
                
                if df_sheet.empty:
                    continue

                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                
                worksheet = writer.sheets[sheet_name]
                (max_row, max_col) = df_sheet.shape
                
                column_settings = [{'header': column} for column in df_sheet.columns]
                worksheet.add_table(0, 0, max_row, max_col - 1, {
                    'columns': column_settings,
                    'style': 'TableStyleMedium2',
                    'name': f'Tabla_{sheet_name}'
                })

                for col_num, value in enumerate(df_sheet.columns):
                    worksheet.write(0, col_num, value, header_format)

                for i, col in enumerate(df_sheet.columns):
                    column_len = max(df_sheet[col].astype(str).map(len).max(), len(col)) + 2
                    column_len = min(column_len, 50) 
                    
                    if 'Fecha' in col:
                        worksheet.set_column(i, i, 12, date_format)
                    elif 'Valor' in col:
                        worksheet.set_column(i, i, 15, money_format)
                    else:
                        worksheet.set_column(i, i, column_len, text_format)

        print(f"✅ Reporte generado exitosamente: {ruta_completa}")

    def generar_consolidado_novedades(df_historico_novedades, df_docs_processing, df_historico_ordenes, rutabase):
        """
        Genera el reporte Consolidado CXP.
        """
        
        hoy = datetime.now()
        primer_dia_este_mes = hoy.replace(day=1)
        ultimo_dia_mes_anterior = primer_dia_este_mes - timedelta(days=1)
        
        target_month = ultimo_dia_mes_anterior.month
        target_year = ultimo_dia_mes_anterior.year
        
        print(f"📅 Generando reporte para el periodo: {target_year}-{target_month:02d}")

        df_historico_novedades['Fecha_ejecucion'] = pd.to_datetime(df_historico_novedades['Fecha_ejecucion'])
        
        mask_mes_anterior = (
            (df_historico_novedades['Fecha_ejecucion'].dt.month == target_month) & 
            (df_historico_novedades['Fecha_ejecucion'].dt.year == target_year)
        )
        df_total_mensual = df_historico_novedades[mask_mes_anterior].copy()

        cols_map_1 = {
            'Fecha_ejecucion': 'Fecha de ejecución',
            'Fecha_de_retoma': 'Fecha de primera revisión antes de contab.',
            'ID_ejecucion': 'ID ejecución',
            'ID_registro': 'ID Registro',
            'Nit': 'NIT',
            'Nombre_Proveedor': 'Nombre Proveedor',
            'Orden_de_compra': 'Orden de Compra',
            'Factura': 'Factura',
            'Fec_Doc': 'Fec.Doc',
            'Fec_Reg': 'Fec.Reg',
            'Observaciones': 'Observaciones'
        }
        
        cols_existentes_1 = [c for c in cols_map_1.keys() if c in df_total_mensual.columns]
        df_total_mensual = df_total_mensual[cols_existentes_1].rename(columns=cols_map_1)

        def limpiar_clave(val):
            return str(val).strip().replace('.0', '') if pd.notnull(val) else ''

        df_docs_processing['nit_join'] = df_docs_processing['nit_emisor_o_nit_del_proveedor'].apply(limpiar_clave)
        df_docs_processing['doc_compra_join'] = df_docs_processing['numero_de_liquidacion_u_orden_de_compra'].apply(limpiar_clave)
        
        df_historico_ordenes['nit_join'] = df_historico_ordenes['NitCedula'].apply(limpiar_clave)
        df_historico_ordenes['doc_compra_join'] = df_historico_ordenes['DocCompra'].apply(limpiar_clave)

        df_vigentes_full = pd.merge(
            df_docs_processing,
            df_historico_ordenes[['nit_join', 'doc_compra_join', 'FecDoc', 'FecReg']],
            how='left',
            left_on=['nit_join', 'doc_compra_join'],
            right_on=['nit_join', 'doc_compra_join']
        )

        cols_map_2 = {
            'executionDate': 'Fecha de ejecución',
            'executionNum': 'ID ejecución',
            'ID': 'ID Registro',
            'documenttype': 'Tipo de documento',
            'nit_emisor_o_nit_del_proveedor': 'NIT',
            'nombre_emisor': 'Nombre Proveedor',
            'numero_de_liquidacion_u_orden_de_compra': 'Orden de Compra',
            'numero_de_factura': 'Factura',
            'FecDoc': 'Fec.Doc',
            'FecReg': 'Fec.Reg',
            'ObservacionesFase_4': 'Observaciones'
        }

        cols_existentes_2 = [c for c in cols_map_2.keys() if c in df_vigentes_full.columns]
        df_vigentes = df_vigentes_full[cols_existentes_2].rename(columns=cols_map_2)

        ahora = datetime.now()
        str_fecha_hora = ahora.strftime('%Y%m')
        nombre_archivo = f"Consolidado_FV_CXP_ConNovedad_{str_fecha_hora}.xlsx"
        
        ruta_completa = os.path.join(rutabase, nombre_archivo)
        
        with pd.ExcelWriter(ruta_completa, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True, 'text_wrap': True, 'valign': 'vcenter', 'align': 'center',
                'fg_color': '#1F4E78', 'font_color': '#FFFFFF', 'border': 1
            })
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
            text_format = workbook.add_format({'border': 1})

            hojas = {
                'Total Mensual': df_total_mensual,
                'Vigentes': df_vigentes
            }

            for sheet_name, df_sheet in hojas.items():
                if df_sheet.empty:
                    pd.DataFrame(columns=df_sheet.columns).to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                else:
                    df_sheet.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                
                worksheet = writer.sheets[sheet_name]
                (max_row, max_col) = df_sheet.shape
                
                if max_row > 0:
                    column_settings = [{'header': col} for col in df_sheet.columns]
                    worksheet.add_table(0, 0, max_row, max_col - 1, {
                        'columns': column_settings,
                        'style': 'TableStyleMedium2',
                        'name': f'Tabla_{sheet_name.replace(" ", "")}'
                    })
                else:
                    for col_num, value in enumerate(df_sheet.columns):
                        worksheet.write(0, col_num, value, header_format)

                for col_num, value in enumerate(df_sheet.columns):
                    worksheet.write(0, col_num, value, header_format)

                for i, col in enumerate(df_sheet.columns):
                    col_len = max(df_sheet[col].astype(str).map(len).max(), len(col)) + 2 if not df_sheet.empty else len(col) + 2
                    col_len = min(col_len, 50)
                    
                    if 'Fec' in col or 'Fecha' in col:
                        worksheet.set_column(i, i, 14, date_format)
                    else:
                        worksheet.set_column(i, i, col_len, text_format)

        print(f"✅ Reporte generado: {ruta_completa}")
    
    def generar_consolidado_no_exitosos_rechazados(df_no_exitosos_sql, df_rechazados_sql, rutabase):
        """
        Genera el reporte Consolidado CXP No Exitosos y Rechazados.
        """
        hoy = datetime.now()
        primer_dia_este_mes = hoy.replace(day=1)
        ultimo_dia_mes_anterior = primer_dia_este_mes - timedelta(days=1)
        
        target_month = ultimo_dia_mes_anterior.month
        target_year = ultimo_dia_mes_anterior.year
        
        print(f"📅 Generando reporte para el periodo: {target_year}-{target_month:02d}")
        
        col_map = {
            'executionDate': 'Fecha de ejecución',
            'executionNum': 'ID ejecución',
            'ID': 'ID Registro',
            'documenttype': 'Tipo de documento',
            'numero_de_liquidacion_u_orden_de_compra': 'Orden de Compra',
            'nit_emisor_o_nit_del_proveedor': 'NIT',
            'nombre_emisor': 'Nombre Proveedor',
            'numero_de_factura': 'Factura',
            'ObservacionesFase_4': 'Observaciones'
        }

        def procesar_df(df_input):
            df_input['executionDate'] = pd.to_datetime(df_input['executionDate'])
            
            mask = (df_input['executionDate'].dt.month == target_month) & \
                (df_input['executionDate'].dt.year == target_year)
            df_filtered = df_input[mask].copy()
            
            cols_existentes = [c for c in col_map.keys() if c in df_filtered.columns]
            df_final = df_filtered[cols_existentes].rename(columns=col_map)
            
            orden_deseado = [
                'Fecha de ejecución', 'ID ejecución', 'ID Registro', 'Tipo de documento',
                'Orden de Compra', 'NIT', 'Nombre Proveedor', 'Factura', 'Observaciones'
            ]
            cols_finales = [c for c in orden_deseado if c in df_final.columns]
            return df_final[cols_finales]

        df_sheet_no_exitosos = procesar_df(df_no_exitosos_sql)
        df_sheet_rechazados = procesar_df(df_rechazados_sql)

        ahora = datetime.now()
        str_fecha_hora = ahora.strftime('%Y%m')
        nombre_archivo = f"Consolidado_CXP_NoExitososRechazados_{str_fecha_hora}.xlsx"
        
        ruta_completa = os.path.join(rutabase, nombre_archivo)
        
        sheets_config = {
            'No Exitosos Vigentes': df_sheet_no_exitosos,
            'Rechazados Total MES Con Evento': df_sheet_rechazados
        }

        with pd.ExcelWriter(ruta_completa, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True, 'text_wrap': True, 'valign': 'vcenter', 'align': 'center',
                'fg_color': '#1F4E78', 'font_color': '#FFFFFF', 'border': 1
            })
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
            text_format = workbook.add_format({'border': 1})

            for sheet_name, df_data in sheets_config.items():
                if df_data.empty:
                    pd.DataFrame(columns=df_data.columns).to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                else:
                    df_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                
                worksheet = writer.sheets[sheet_name]
                (max_row, max_col) = df_data.shape
                
                if max_row > 0:
                    column_settings = [{'header': col} for col in df_data.columns]
                    worksheet.add_table(0, 0, max_row, max_col - 1, {
                        'columns': column_settings,
                        'style': 'TableStyleMedium2',
                        'name': f'T_{sheet_name.split()[0]}'
                    })
                else:
                    for col_num, value in enumerate(df_data.columns):
                        worksheet.write(0, col_num, value, header_format)

                for col_num, value in enumerate(df_data.columns):
                    worksheet.write(0, col_num, value, header_format)

                for i, col in enumerate(df_data.columns):
                    col_len = max(df_data[col].astype(str).map(len).max(), len(col)) + 2 if not df_data.empty else len(col) + 2
                    col_len = min(col_len, 50)
                    
                    if 'Fecha' in col:
                        worksheet.set_column(i, i, 14, date_format)
                    else:
                        worksheet.set_column(i, i, col_len, text_format)

        print(f"✅ Reporte generado: {ruta_completa}")
    
    def generar_consolidado_pendientes(df_eventos_sql, df_compensacion_sql, df_contabilizacion_sql, rutabase):
        """
        Genera el reporte Consolidado CXP Pendientes.
        """
        
        base_mapping = {
            'executionDate': 'Fecha de ejecución',
            'executionNum': 'ID ejecución',
            'ID': 'ID Registro',
            'documenttype': 'Tipo de documento',
            'nit_emisor_o_nit_del_proveedor': 'NIT',
            'nombre_emisor': 'Nombre Proveedor',
            'numero_de_liquidacion_u_orden_de_compra': 'Orden de Compra',
            'numero_de_factura': 'Factura',
            'ObservacionesFase_4': 'Observaciones'
        }

        def preparar_hoja(df, nombre_columna_retoma):
            df_out = df.copy()
            
            if 'executionDate' in df_out.columns:
                df_out['executionDate'] = pd.to_datetime(df_out['executionDate'])
            if 'Fecha_de_retoma_antes_de_contabilizacion' in df_out.columns:
                df_out['Fecha_de_retoma_antes_de_contabilizacion'] = pd.to_datetime(df_out['Fecha_de_retoma_antes_de_contabilizacion'])

            mapping_especifico = base_mapping.copy()
            if 'Fecha_de_retoma_antes_de_contabilizacion' in df_out.columns:
                mapping_especifico['Fecha_de_retoma_antes_de_contabilizacion'] = nombre_columna_retoma
                
            cols_existentes = [c for c in mapping_especifico.keys() if c in df_out.columns]
            df_out = df_out[cols_existentes].rename(columns=mapping_especifico)
            
            orden_ideal = [
                'Fecha de ejecución', nombre_columna_retoma, 'ID ejecución', 'ID Registro', 
                'Tipo de documento', 'Orden de Compra', 'NIT', 'Nombre Proveedor', 
                'Factura', 'Observaciones'
            ]
            cols_finales = [c for c in orden_ideal if c in df_out.columns]
            
            return df_out[cols_finales]

        df_sheet1 = preparar_hoja(df_eventos_sql, 'Fecha retoma eventos')
        df_sheet2 = preparar_hoja(df_compensacion_sql, 'Fecha retoma compensación')
        df_sheet3 = preparar_hoja(df_contabilizacion_sql, 'Fecha retoma contabilización')

        ahora = datetime.now()
        str_fecha_hora = ahora.strftime('%Y%m')
        nombre_archivo = f"Consolidado_CXP_Pendientes_{str_fecha_hora}.xlsx"
        
        ruta_completa = os.path.join(rutabase, nombre_archivo)
        
        sheets_config = {
            'Pendiente Eventos Vigentes': df_sheet1,
            'Pendiente Compen. Vigentes': df_sheet2,
            'Pendiente Contab. Vigentes': df_sheet3
        }

        with pd.ExcelWriter(ruta_completa, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True, 'text_wrap': True, 'valign': 'vcenter', 'align': 'center',
                'fg_color': '#1F4E78', 'font_color': '#FFFFFF', 'border': 1
            })
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
            text_format = workbook.add_format({'border': 1})

            for sheet_name, df_data in sheets_config.items():
                if df_data.empty:
                    pd.DataFrame(columns=df_data.columns).to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                else:
                    df_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
                
                worksheet = writer.sheets[sheet_name]
                (max_row, max_col) = df_data.shape
                
                if max_row > 0:
                    column_settings = [{'header': col} for col in df_data.columns]
                    worksheet.add_table(0, 0, max_row, max_col - 1, {
                        'columns': column_settings,
                        'style': 'TableStyleMedium2',
                        'name': f'T_{sheet_name.replace(" ", "").replace(".", "")}'
                    })
                else:
                    for col_num, value in enumerate(df_data.columns):
                        worksheet.write(0, col_num, value, header_format)

                for col_num, value in enumerate(df_data.columns):
                    worksheet.write(0, col_num, value, header_format)

                for i, col in enumerate(df_data.columns):
                    len_contenido = df_data[col].astype(str).map(len).max() if not df_data.empty else 0
                    col_len = max(len_contenido, len(col)) + 2
                    col_len = min(col_len, 50)
                    
                    if 'Fecha' in col:
                        worksheet.set_column(i, i, 14, date_format)
                    else:
                        worksheet.set_column(i, i, col_len, text_format)

        print(f"✅ Reporte generado: {ruta_completa}")
    
    def generar_consolidado_nc_nd_actualizado(df_nc_encontrados_sql, df_nc_novedad_sql, df_nd_sql, rutabase):
        """
        Genera el reporte Consolidado NC ND CXP.
        """
        
        hoy = datetime.now()
        primer_dia_este_mes = hoy.replace(day=1)
        ultimo_dia_mes_anterior = primer_dia_este_mes - timedelta(days=1)
        
        target_month = ultimo_dia_mes_anterior.month
        target_year = ultimo_dia_mes_anterior.year
        
        str_periodo = f"{target_month:02d}{target_year}"
        print(f"📅 Periodo objetivo (Mes Anterior): {target_month:02d}-{target_year}")

        cols_nc_encontrados = [
            'Fecha de ejecución', 'ID ejecución', 'ID Registro', 'NIT', 'Nombre Proveedor', 
            'Nota Credito', 'Tipo de nota crédito', 'Referencia', 'LineExtensionAmount', 
            'Estado', 'Observaciones'
        ]
        
        cols_nc_novedad = [
            'Fecha de ejecución', 'ID ejecución', 'ID Registro', 'NIT', 'Nombre Proveedor', 
            'Nota Credito', 'Tipo de nota crédito', 'Referencia', 'LineExtensionAmount', 
            'Observaciones'
        ]
        
        cols_nd_total = [
            'Fecha de ejecución', 'ID ejecución', 'ID Registro', 'NIT', 'Nombre Proveedor', 
            'Nota Debito', 'Tipo de nota débito', 'Referencia', 'LineExtensionAmount',
            'Observaciones'
        ]

        def procesar_hoja(df_input, columnas_destino, filtrar_mes=False, tipo='NC'):
            df = df_input.copy()
            
            if 'executionDate' in df.columns:
                df['executionDate'] = pd.to_datetime(df['executionDate'])
                
            if filtrar_mes:
                mask = (df['executionDate'].dt.month == target_month) & \
                    (df['executionDate'].dt.year == target_year)
                df = df[mask].copy()
                
            col_doc_num = 'Nota Credito' if tipo == 'NC' else 'Nota Debito'
            col_doc_type = 'Tipo de nota crédito' if tipo == 'NC' else 'Tipo de nota débito'
            col_valor = 'valor_a_pagar' if tipo == 'NC' else 'valor_a_pagar'
            
            mapping = {
                'executionDate': 'Fecha de ejecución',
                'executionNum': 'ID ejecución',
                'ID': 'ID Registro',
                'nit_emisor_o_nit_del_proveedor': 'NIT',
                'nombre_emisor': 'Nombre Proveedor',
                'Numero_de_nota_credito': col_doc_num,
                'Tipo_de_nota_cred_deb': col_doc_type,
                'NotaCreditoReferenciada': 'Referencia',
                col_valor: 'LineExtensionAmount',
                'ResultadoFinalAntesEventos': 'Estado',
                'ObservacionesFase_4': 'Observaciones'
            }
            
            df = df.rename(columns=mapping)
            
            for col in columnas_destino:
                if col not in df.columns:
                    df[col] = None
                    
            return df[columnas_destino]

        df_s1 = procesar_hoja(df_nc_encontrados_sql, cols_nc_encontrados, filtrar_mes=True, tipo='NC')
        df_s2 = procesar_hoja(df_nc_novedad_sql, cols_nc_novedad, filtrar_mes=False, tipo='NC')
        df_s3 = procesar_hoja(df_nd_sql, cols_nd_total, filtrar_mes=True, tipo='ND')

        ahora = datetime.now()
        str_fecha_hora = ahora.strftime('%Y%m')
        nombre_archivo = f"Consolidado_NC_ND_CXP_{str_fecha_hora}.xlsx"
        
        ruta_completa = os.path.join(rutabase, nombre_archivo)
        
        sheets_config = {
            'NC Encontrados-NoExitosos MES': df_s1,
            'NC Con Novedad Vigentes': df_s2,
            'ND Total Mes': df_s3
        }

        with pd.ExcelWriter(ruta_completa, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True, 'text_wrap': True, 'valign': 'vcenter', 'align': 'center',
                'fg_color': '#1F4E78', 'font_color': '#FFFFFF', 'border': 1
            })
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
            money_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
            text_format = workbook.add_format({'border': 1})

            for sheet_name, df_data in sheets_config.items():
                start_row = 1
                df_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row)
                
                worksheet = writer.sheets[sheet_name]
                (max_row, max_col) = df_data.shape
                
                if max_row > 0:
                    column_settings = [{'header': col} for col in df_data.columns]
                    worksheet.add_table(0, 0, max_row, max_col - 1, {
                        'columns': column_settings,
                        'style': 'TableStyleMedium2',
                        'name': f'T_{sheet_name.split()[0]}_{sheet_name.split()[-1]}'
                    })
                else:
                    for col_num, value in enumerate(df_data.columns):
                        worksheet.write(0, col_num, value, header_format)

                for col_num, value in enumerate(df_data.columns):
                    worksheet.write(0, col_num, value, header_format)

                for i, col in enumerate(df_data.columns):
                    len_contenido = df_data[col].astype(str).map(len).max() if not df_data.empty else 0
                    col_len = max(len_contenido, len(col)) + 2
                    col_len = min(col_len, 50)
                    
                    if 'Fecha' in col:
                        worksheet.set_column(i, i, 14, date_format)
                    elif 'Amount' in col or 'Valor' in col:
                        worksheet.set_column(i, i, 18, money_format)
                    else:
                        worksheet.set_column(i, i, col_len, text_format)

        print(f"✅ Reporte generado: {ruta_completa}")
    
    def generar_reporte_anual_global(df_facturas_sql, df_nc_sql, df_nd_sql, rutabase):
        """
        Genera el reporte Consolidado Global CXP.
        """
        
        hoy = datetime.now()
        anio_anterior = hoy.year - 1
        
        print(f"📅 Generando reporte anual para el año: {anio_anterior}")

        cols_facturas = [
            'Fecha de ejecución', 'ID ejecución', 'ID Registro', 'Tipo de documento', 
            'NIT', 'Nombre Proveedor', 'Orden de Compra', 'Factura', 
            'Nota Credito', 'Tipo de nota crédito', 'Nota Debito', 'Tipo de nota débito', 
            'Estado validación antes de eventos', 
            'Fecha - hora Evento Acuse de Recibo', 'Estado Evento Acuse de Recibo',
            'Fecha - hora Evento Recibo del bien y/o prestación del servicio', 'Estado Evento Recibo del bien y/o prestación del servicio',
            'Fecha - hora Evento Aceptación Expresa', 'Estado Evento Aceptación Expresa',
            'Fecha - hora Evento Reclamo de la Factura Electrónica de Venta', 'Estado Evento Reclamo',
            'Estado contabilización', 'Estado compensación', 'Observaciones'
        ]

        cols_nc = [
            'Fecha de ejecución', 'ID ejecución', 'ID Registro', 'Tipo de documento', 
            'NIT', 'Nombre Proveedor', 'Nota Credito', 'Tipo de nota crédito', 
            'Factura', 'Estado validación antes de eventos', 'Observaciones'
        ]

        cols_nd = [
            'Fecha de ejecución', 'ID ejecución', 'ID Registro', 'Tipo de documento', 
            'NIT', 'Nombre Proveedor', 'Factura', 'Nota Debito', 'Tipo de nota débito', 
            'Estado validación antes de eventos', 'Observaciones'
        ]

        def procesar_hoja(df_input, target_columns, mapping_especifico):
            df = df_input.copy()
            
            if 'executionDate' in df.columns:
                df['executionDate'] = pd.to_datetime(df['executionDate'])
                mask = (df['executionDate'].dt.year == anio_anterior)
                df = df[mask].copy()
            
            base_mapping = {
                'executionDate': 'Fecha de ejecución',
                'executionNum': 'ID ejecución',
                'ID': 'ID Registro',
                'documenttype': 'Tipo de documento',
                'nit_emisor_o_nit_del_proveedor': 'NIT',
                'nombre_emisor': 'Nombre Proveedor',
                'numero_de_liquidacion_u_orden_de_compra': 'Orden de Compra',
                'numero_de_factura': 'Factura',
                'ResultadoFinalAntesEventos': 'Estado validación antes de eventos',
                'ObservacionesFase_4': 'Observaciones',
                'Estado_contabilizacion': 'Estado contabilización',
                'EstadoCompensacionFase_7': 'Estado compensación'
            }
            base_mapping.update(mapping_especifico)
            
            df = df.rename(columns=base_mapping)
            
            for col in target_columns:
                if col not in df.columns:
                    df[col] = None
            
            return df[target_columns]

        map_facturas = {
            'Numero_de_nota_credito': 'Nota Credito', 
            'Tipo_de_nota_cred_deb': 'Tipo de nota crédito'
        }
        df_s1 = procesar_hoja(df_facturas_sql, cols_facturas, map_facturas)
        
        map_nc = {
            'Numero_de_nota_credito': 'Nota Credito',
            'Tipo_de_nota_cred_deb': 'Tipo de nota crédito'
        }
        df_s2 = procesar_hoja(df_nc_sql, cols_nc, map_nc)
        
        map_nd = {
            'Numero_de_nota_credito': 'Nota Debito',
            'Tipo_de_nota_cred_deb': 'Tipo de nota débito'
        }
        df_s3 = procesar_hoja(df_nd_sql, cols_nd, map_nd)

        ahora = datetime.now()
        str_fecha_hora = ahora.strftime('%Y')
        nombre_archivo = f"Consolidado_Global_CXP_{str_fecha_hora}.xlsx"
        
        ruta_completa = os.path.join(rutabase, nombre_archivo)
        
        sheets_config = {
            'Total Anual Facturas': df_s1,
            'Total Anual Notas Crédito': df_s2,
            'Total Anual Notas Débito': df_s3
        }

        with pd.ExcelWriter(ruta_completa, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True, 'text_wrap': True, 'valign': 'vcenter', 'align': 'center',
                'fg_color': '#1F4E78', 'font_color': '#FFFFFF', 'border': 1
            })
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1})
            text_format = workbook.add_format({'border': 1})

            for sheet_name, df_data in sheets_config.items():
                start_row = 1
                df_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row)
                
                worksheet = writer.sheets[sheet_name]
                (max_row, max_col) = df_data.shape
                
                if max_row > 0:
                    column_settings = [{'header': col} for col in df_data.columns]
                    worksheet.add_table(0, 0, max_row, max_col - 1, {
                        'columns': column_settings,
                        'style': 'TableStyleMedium2',
                        'name': f'T_{sheet_name.replace(" ", "")}'
                    })
                else:
                    for col_num, value in enumerate(df_data.columns):
                        worksheet.write(0, col_num, value, header_format)

                for col_num, value in enumerate(df_data.columns):
                    worksheet.write(0, col_num, value, header_format)

                for i, col in enumerate(df_data.columns):
                    len_contenido = df_data[col].astype(str).map(len).max() if not df_data.empty else 0
                    col_len = max(len_contenido, len(col)) + 2
                    col_len = min(col_len, 50)
                    
                    if 'Fecha' in col:
                        worksheet.set_column(i, i, 14, date_format)
                    else:
                        worksheet.set_column(i, i, col_len, text_format)

        print(f"✅ Reporte generado: {ruta_completa}")

    # =========================================================================
    # PROCESAMIENTO PRINCIPAL
    # =========================================================================
    
    try:
        print("=" * 80)
        print("[INICIO] HU8 - Generación de Reportes CxP")
        print("=" * 80)
        
        t_inicio = time.time()
        
        cfg = parse_config(GetVar("vLocDicConfig"))
        
        ruta_base = cfg.get('RutaFileServer', r'\\172.16.250.222\BOT_Validacion_FV_NC_ND_CXP')
        fecha_ejecucion = datetime.now()
        
        with crear_conexion_db(cfg) as cx:
        
            print(f"[INFO] Ruta base: {ruta_base}")
            print(f"[INFO] Fecha ejecucion: {fecha_ejecucion}")
            
            query_num_ejecucion = "SELECT MAX(CAST([executionNum] AS INT)) as max_val FROM [CxP].[DocumentsProcessing]"
            
            df_resultado = pd.read_sql(query_num_ejecucion, cx)
            ultimo_num = df_resultado['max_val'].iloc[0]
            
            if not verificar_acceso_ruta(ruta_base):
                raise Exception(f"No se tiene acceso a la ruta: {ruta_base}")
            
            rutas = crear_arbol_carpetas(ruta_base, fecha_ejecucion, ultimo_num)
        
            print("\n[PASO 3] Procesando archivos de registros...")
            
            query_registros_insumos = "SELECT * FROM [CxP].[DocumentsProcessing]"
            
            df_registros_insumos = pd.read_sql(query_registros_insumos, cx)
            print(f"[INFO] {len(df_registros_insumos)} registros para procesar archivos")
            
            archivos_procesados = 0
            
            for idx, reg in df_registros_insumos.iterrows():
                try:
                    registro_id = reg['ID']
                    tipo_doc = safe_str(reg['documenttype'])
                    nombre_archivos = safe_str(reg['actualizacionNombreArchivos'])
                    ruta_respaldo = safe_str(reg['RutaArchivo'])
                    resultado_final = safe_str(reg['ResultadoFinalAntesEventos'])
                    numero_oc = safe_str(reg['numero_de_liquidacion_u_orden_de_compra'])
                    nit = safe_str(reg['nit_emisor_o_nit_del_proveedor'])
                    factura = safe_str(reg['numero_de_factura'])
                    estado_xml = str(reg.get('Insumo_XML', '')).strip().lower()
                    estado_pdf = str(reg.get('Insumo_PDF', '')).strip().lower()
                    
                    if estado_xml in ['', 'none', 'nan'] or estado_pdf in ['', 'none', 'nan']:
                        xml_enc, pdf_enc, ruta_xml, ruta_pdf = verificar_archivos_insumo(ruta_respaldo, nombre_archivos)
                        
                        cur = cx.cursor()
                        cur.execute("""
                            UPDATE [CxP].[DocumentsProcessing]
                            SET [Insumo_XML] = ?,
                                [Insumo_PDF] = ?
                            WHERE [ID] = ?
                        """, (
                            'ENCONTRADO' if xml_enc else 'NO ENCONTRADO',
                            'ENCONTRADO' if pdf_enc else 'NO ENCONTRADO',
                            registro_id
                        ))
                        cx.commit()
                        cur.close()
                        
                        actualizar_insumos_comparativa(reg, cx, nit, factura, 'InsumoPDF', 'ENCONTRADO' if pdf_enc else 'NO ENCONTRADO')
                        actualizar_insumos_comparativa(reg, cx, nit, factura, 'InsumoPDF', 'ENCONTRADO' if xml_enc else 'NO ENCONTRADO')
                        
                        if xml_enc or pdf_enc:
                            carpeta_destino = determinar_carpeta_destino(resultado_final, tipo_doc)
                            ruta_destino_completa = os.path.join(rutas['insumos_cxp'], carpeta_destino)
                            
                            ruta_comercializados = None
                            if numero_oc.startswith('50'):
                                ruta_comercializados = cfg.get('HU4RutaInsumos')
                            
                            if pdf_enc and not xml_enc:
                                nueva_ruta = mover_archivos_a_destino(
                                    ruta_pdf, ruta_destino_completa,
                                    numero_oc, ruta_comercializados
                                )
                            else:
                                nueva_ruta = mover_archivos_a_destino(
                                    ruta_pdf, ruta_destino_completa,
                                    numero_oc, ruta_comercializados
                                )
                            
                            if nueva_ruta:
                                cur = cx.cursor()
                                cur.execute("""
                                    UPDATE [CxP].[DocumentsProcessing]
                                    SET [Ruta_respaldo] = ?
                                    WHERE [ID] = ?
                                """, (nueva_ruta, registro_id))
                                cx.commit()
                                cur.close()
                            
                            archivos_procesados += 1
                    
                except Exception as e:
                    print(f"[ERROR] Procesando archivo registro {reg['ID']}: {e}")
                    # No detenemos el loop, intentamos con el siguiente
            
            #REPORTES DIARIOS
            try:
                print(f"[OK] {archivos_procesados} archivos procesados")
                
                with crear_conexion_db(cfg) as cx:
                    query1 = """
                    SELECT [executionDate], [Fecha_de_retoma_antes_de_contabilizacion], [executionNum], [ID], 
                        [documenttype], [numero_de_liquidacion_u_orden_de_compra], [nit_emisor_o_nit_del_proveedor],
                        [nombre_emisor], [numero_de_factura], [ResultadoFinalAntesEventos], 
                        [Fecha_retoma_contabilizacion], [Estado_contabilizacion]
                    FROM [CxP].[DocumentsProcessing]
                    """
                    df_main = pd.read_sql(query1, cx)

                    query2 = "SELECT * FROM [dbo].[CxP.Comparativa]"
                    df_detalles = pd.read_sql(query2, cx)

                    query3 = "SELECT * FROM [CxP].[HistoricoOrdenesCompra]"
                    df_historico = pd.read_sql(query3, cx)

                    generar_reporte_cxp(df_main, df_detalles, df_historico, rutas['resultados_dia'])
                    
                    # REPORTE GRANOS
                    query1 = """SELECT 
                            [executionDate]
                            ,[Fecha_de_retoma_antes_de_contabilizacion]
                            ,[executionNum]
                            ,[ID]
                            ,[documenttype]
                            ,[numero_de_liquidacion_u_orden_de_compra]
                            ,[nit_emisor_o_nit_del_proveedor]
                            ,[nombre_emisor]
                            ,[numero_de_factura]
                            ,[ResultadoFinalAntesEventos]
                            ,[Fecha_retoma_contabilizacion]
                            ,[Estado_contabilizacion]
                        FROM [CxP].[DocumentsProcessing]
                        WHERE [agrupacion] LIKE '%MAPG%'"""
                    
                    df_main_granos = pd.read_sql(query1, cx)
                    generar_reporte_granos(df_main_granos, df_detalles, rutas['granos_resultado'])
                    
                    # REPORTE MAIZ
                    query1 = """SELECT 
                            [executionDate]
                            ,[Fecha_de_retoma_antes_de_contabilizacion]
                            ,[executionNum]
                            ,[ID]
                            ,[documenttype]
                            ,[numero_de_liquidacion_u_orden_de_compra]
                            ,[nit_emisor_o_nit_del_proveedor]
                            ,[nombre_emisor]
                            ,[numero_de_factura]
                            ,[ResultadoFinalAntesEventos]
                            ,[Fecha_retoma_contabilizacion]
                            ,[Estado_contabilizacion]
                        FROM [CxP].[DocumentsProcessing]
                        WHERE [agrupacion] LIKE '%MAPM%'"""
                    
                    df_main_maiz = pd.read_sql(query1, cx)
                    generar_reporte_maiz(df_main_maiz, df_detalles, rutas['maiz_resultado'])
            
                    # REPORTE COMERCIALIZADOS
                    query1 = """SELECT 
                            [executionDate]
                            ,[Fecha_de_retoma_antes_de_contabilizacion]
                            ,[executionNum]
                            ,[ID]
                            ,[documenttype]
                            ,[numero_de_liquidacion_u_orden_de_compra]
                            ,[nit_emisor_o_nit_del_proveedor]
                            ,[nombre_emisor]
                            ,[numero_de_factura]
                            ,[ResultadoFinalAntesEventos]
                        FROM [CxP].[DocumentsProcessing]
                        WHERE [numero_de_liquidacion_u_orden_de_compra] LIKE '50%'"""
                        
                    df_main_comer = pd.read_sql(query1, cx)
                    generar_reporte_comercializados(df_main_comer, df_detalles, rutas['comercializados_resultado'])
                    
                    hoy = datetime.now()

                    if hoy.day == int(safe_str(cfg['DiaReporteMensualAnual'])):
                        
                        print("✅ Hoy es el dia seleccionado para ejecutar los reportes mensuales")
                        
                        query1 = """SELECT [Fecha_ejecucion],[Fecha_de_retoma],[ID_ejecucion],[ID_registro],[Nit],[Nombre_Proveedor],[Orden_de_compra],[Factura],[Fec_Doc],[Fec_Reg],[Observaciones] FROM [CxP].[HistoricoNovedades] ORDER BY Factura"""
                        df_historico_novedades = pd.read_sql(query1, cx)
                        
                        query2 = """SELECT [executionDate],[Fecha_de_retoma_antes_de_contabilizacion],[executionNum],[ID],[documenttype],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[numero_de_liquidacion_u_orden_de_compra],[numero_de_factura],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE ResultadoFinalAntesEventos LIKE '%CON NOVEDAD%'"""
                        df_docs_processing = pd.read_sql(query2, cx)
                        
                        generar_consolidado_novedades(df_historico_novedades, df_docs_processing, df_historico, rutas['consolidados'])
                        
                        query1 = """SELECT [executionDate],[Fecha_de_retoma_antes_de_contabilizacion],[executionNum],[ID],[documenttype],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[numero_de_liquidacion_u_orden_de_compra],[numero_de_factura],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE ResultadoFinalAntesEventos LIKE '%RECHAZADO%'"""
                        df_rechazados_sql = pd.read_sql(query1, cx)
                        
                        query2 = """SELECT [executionDate],[Fecha_de_retoma_antes_de_contabilizacion],[executionNum],[ID],[documenttype],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[numero_de_liquidacion_u_orden_de_compra],[numero_de_factura],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE ResultadoFinalAntesEventos LIKE '%NO EXITOSO%'"""
                        df_no_exitosos_sql = pd.read_sql(query2, cx)
                        
                        generar_consolidado_no_exitosos_rechazados(df_no_exitosos_sql, df_rechazados_sql, rutas['consolidados'])
                        
                        query1 = """SELECT [executionDate],[Fecha_de_retoma_antes_de_contabilizacion],[executionNum],[ID],[documenttype],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[numero_de_liquidacion_u_orden_de_compra],[numero_de_factura],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE ResultadoFinalAntesEventos LIKE '%PENDIENTE%'"""
                        df_eventos_sql = pd.read_sql(query1, cx)
                        
                        query2 = """SELECT [executionDate],[Fecha_de_retoma_antes_de_contabilizacion],[executionNum],[ID],[documenttype],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[numero_de_liquidacion_u_orden_de_compra],[numero_de_factura],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE EstadoCompensacionFase_7 LIKE '%CONTABILIZACION PENDIENTE%'"""
                        df_contabilizacion_sql = pd.read_sql(query2, cx)
                        
                        query3 = """SELECT [executionDate],[Fecha_de_retoma_antes_de_contabilizacion],[executionNum],[ID],[documenttype],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[numero_de_liquidacion_u_orden_de_compra],[numero_de_factura],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE EstadoCompensacionFase_7 LIKE '%COMPENSACION PENDIENTE%'"""
                        df_compensacion_sql = pd.read_sql(query3, cx)
                                
                        generar_consolidado_pendientes(df_eventos_sql, df_compensacion_sql, df_contabilizacion_sql, rutas['consolidados'])
                        
                        query1 = """SELECT [executionDate],[executionNum],[ID],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[Numero_de_nota_credito],[Tipo_de_nota_cred_deb],[NotaCreditoReferenciada],[valor_a_pagar],[ResultadoFinalAntesEventos],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing]  WHERE documenttype = 'NC' AND (ResultadoFinalAntesEventos LIKE '%ENCONTRADOS%' OR ResultadoFinalAntesEventos LIKE '%NO EXITOSOS%') """
                        df_nc_encontrados_sql = pd.read_sql(query1, cx)
                        
                        query2 = """SELECT [executionDate],[executionNum],[ID],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[Numero_de_nota_credito],[Tipo_de_nota_cred_deb],[NotaCreditoReferenciada],[valor_a_pagar],[ResultadoFinalAntesEventos],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE documenttype = 'NC' AND ResultadoFinalAntesEventos LIKE '%CON NOVEDAD%'"""
                        df_nc_novedad_sql = pd.read_sql(query2, cx)
                        
                        query3 = """SELECT [executionDate],[executionNum],[ID],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[Numero_de_nota_credito],[Tipo_de_nota_cred_deb],[NotaCreditoReferenciada],[valor_a_pagar],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE documenttype = 'ND'"""
                        df_nd_sql = pd.read_sql(query3, cx)
                        
                        generar_consolidado_nc_nd_actualizado(df_nc_encontrados_sql, df_nc_novedad_sql, df_nd_sql, rutas['consolidados'])
                        
                        hoy = datetime.now()

                        if hoy.month == int(safe_str(cfg['MesReporteAnual'])):
                            query1 = """SELECT [executionDate],[executionNum],[ID],[documenttype],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[numero_de_liquidacion_u_orden_de_compra],[numero_de_factura],[Numero_de_nota_credito],[Tipo_de_nota_cred_deb],[Numero_de_nota_credito],[Tipo_de_nota_cred_deb],[ResultadoFinalAntesEventos],[ObservacionesFase_4],[Estado_contabilizacion],[EstadoCompensacionFase_7] FROM [CxP].[DocumentsProcessing]"""
                            df_facturas_sql = pd.read_sql(query1, cx)
                            
                            query2 = """SELECT [executionDate],[executionNum],[ID],[documenttype],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[numero_de_factura],[Numero_de_nota_credito],[Tipo_de_nota_cred_deb],[ResultadoFinalAntesEventos],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE documenttype = 'NC'"""
                            df_nc_sql = pd.read_sql(query2, cx)
                            
                            query3 = """SELECT [executionDate],[executionNum],[ID],[documenttype],[nit_emisor_o_nit_del_proveedor],[nombre_emisor],[numero_de_factura],[Numero_de_nota_credito],[Tipo_de_nota_cred_deb],[ResultadoFinalAntesEventos],[ObservacionesFase_4] FROM [CxP].[DocumentsProcessing] WHERE documenttype = 'ND'"""
                            df_nd_sql = pd.read_sql(query3, cx)
                            
                            generar_reporte_anual_global(df_facturas_sql, df_nc_sql, df_nd_sql, rutas['global_anual'])
                    else:
                        print(f"❌ Hoy es día {hoy.day}, no es el dia para generar los reportes mensuales")
                        
            except Exception as e:
                print(f"[ERROR] Error generando reportes: {e}")
                traceback.print_exc()
                SetVar("vGblStrDetalleError", str(traceback.format_exc()))
                SetVar("vGblStrSystemError", "ErrorHU4_4.1")
                SetVar("vLocStrResultadoSP", "False")
                raise e
        
        tiempo_total = time.time() - t_inicio
        print(f"[FIN] HU8 - Generación de Reportes CxP completado. Tiempo: {round(tiempo_total, 2)}s")
        resumen = f"HU8 completada. reportes generados"
        SetVar("vLocStrResultadoSP", "True")
        SetVar("vLocStrResumenSP", resumen)
        
    except Exception as e:
        print("[ERROR CRITICO] La funcion HU8_GenerarReportesCxP fallo")
        print(f"[ERROR] Mensaje: {str(e)}")
        traceback.print_exc()
        SetVar("vGblStrDetalleError", str(traceback.format_exc()))
        SetVar("vGblStrSystemError", "ErrorHU4_4.1")
        SetVar("vLocStrResultadoSP", "False")
        raise e

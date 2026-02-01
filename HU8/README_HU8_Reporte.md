# HU8_Reporte.py - Generación de Reportes y Gestión de Archivos

## Descripción General

Este script implementa la **Historia de Usuario 8 (HU8)** del proceso de automatización de Cuentas por Pagar (CxP). Es el componente final del flujo y tiene dos responsabilidades principales:

1.  **Gestión Documental (File Ops):** Organizar los archivos físicos (XML y PDF) de las facturas procesadas. Mueve los archivos desde una carpeta de entrada a una estructura de carpetas histórica en el servidor de archivos, clasificada por Año, Mes, Día y Estado del procesamiento (Aprobado, Rechazado, Con Novedad, etc.).
2.  **Generación de Reportes (Reporting):** Generar una serie de reportes en Excel (diarios, mensuales y anuales) que consolidan la información del procesamiento para auditoría y control.

El script interactúa directamente con la base de datos SQL Server (`[CxP].[DocumentsProcessing]`, `[dbo].[CxP.Comparativa]`, `[CxP].[HistoricoOrdenesCompra]`) y el sistema de archivos de Windows.

## Prerrequisitos y Dependencias

*   **Python 3.x**
*   **Librerías:**
    *   `pyodbc`: Conexión a base de datos SQL Server.
    *   `pandas`: Manipulación de datos y generación de reportes.
    *   `xlsxwriter`: Motor de escritura para archivos Excel con formato avanzado.
    *   `openpyxl`: Manipulación adicional de Excel.
    *   `os`, `shutil`: Operaciones de sistema de archivos.
*   **RocketBot:** El script espera ser ejecutado dentro de un entorno RocketBot, utilizando `GetVar` y `SetVar`.

## Configuración (Inputs de RocketBot)

El script lee la variable global `vLocDicConfig` que debe contener un diccionario (o JSON string) con:

*   **`ServidorBaseDatos`**: Dirección del servidor SQL.
*   **`NombreBaseDatos`**: Nombre de la BD.
*   **`UsuarioBaseDatos`**: Usuario SQL.
*   **`ClaveBaseDatos`**: Contraseña SQL.
*   **`RutaFileServer`**: Ruta raíz donde se crearán las carpetas y reportes (ej. `\\172.16.250.222\BOT_Validacion_FV_NC_ND_CXP`).
*   **`DiaReporteMensualAnual`**: Día del mes (int) en que se deben ejecutar los reportes acumulados mensuales.
*   **`MesReporteAnual`**: Mes (int) en que se debe ejecutar el reporte anual.
*   **`HU4RutaInsumos`**: Ruta específica para copiar archivos de comercializados (OC 50*).

## Salidas (Outputs a RocketBot)

*   **`vLocStrResultadoSP`**: "True" si el proceso finaliza correctamente, "False" si falla.
*   **`vLocStrResumenSP`**: Mensaje resumen de la ejecución.
*   **`vGblStrDetalleError`**: Detalle del error (traceback) en caso de fallo.
*   **`vGblStrSystemError`**: Código de error para el sistema.

## Estructura de Carpetas Generada

El script crea automáticamente una estructura jerárquica:

```
RutaFileServer/
├── AÑO/
│   ├── MES/
│   │   ├── DIA/
│   │   │   ├── RESULTADOS BOT CXP/ (Reportes diarios)
│   │   │   ├── EJECUCION X CXP/
│   │   │   │   └── CXP/
│   │   │   │       └── INSUMOS/
│   │   │   │           ├── APROBADOS CONTADO/
│   │   │   │           ├── RECHAZADOS/
│   │   │   │           ├── CON NOVEDAD.../
│   │   │           ...
│   │   ├── CONSOLIDADOS/ (Reportes mensuales)
│   │   ├── INSUMO DE RETORNO/
```

## Reportes Generados

### 1. Diarios
*   **Reporte_de_ejecución_CXP**: Reporte general de todas las facturas del día.
*   **Reporte_de_ejecución_GRANOS**: Filtro específico para materia prima granos.
*   **Reporte_de_ejecución_MAÍZ**: Filtro específico para maíz.
*   **Reporte_de_ejecución_COMERCIALIZADOS**: Filtro para OC que inician con '50'.

### 2. Mensuales (Condicionales)
*   **Consolidado_FV_CXP_ConNovedad**: Facturas con novedades acumuladas.
*   **Consolidado_CXP_NoExitososRechazados**: Facturas rechazadas o fallidas.
*   **Consolidado_CXP_Pendientes**: Documentos pendientes de gestión manual.
*   **Consolidado_NC_ND_CXP**: Notas Crédito y Débito.

### 3. Anuales
*   **Consolidado_Global_CXP**: Trazabilidad completa del año.

## Funciones Principales

### `HU8_GenerarReportesCxP()`
Función principal que orquesta todo el flujo.
1.  Conecta a la BD.
2.  Verifica/Crea carpetas.
3.  Itera sobre `[CxP].[DocumentsProcessing]` para mover archivos.
4.  Ejecuta las sub-funciones de generación de Excel.

### `crear_arbol_carpetas(ruta_base, fecha, ult_numero)`
Crea la estructura de directorios necesaria para el día actual y devuelve un diccionario con las rutas clave.

### `mover_archivos_a_destino(...)`
Mueve físicamente los archivos `.xml` y `.pdf` desde su ubicación temporal a la carpeta final determinada por el estado de validación.

### `generar_reporte_cxp(...)`
Consulta SQL + Pandas para generar el Excel diario con formato visual (colores, bordes, filtros).

## Manejo de Errores

El script cuenta con bloques `try-except` robustos.
*   **Conexión BD**: Implementa reintento automático (SQL Auth -> Windows Auth).
*   **Archivos**: Si un archivo no se puede mover, se loguea el error pero el proceso continúa con el siguiente registro.
*   **Crítico**: Si falla la generación de reportes, se captura el traceback y se envía a RocketBot.

## Autor
**Diego Ivan Lopez Ochoa**

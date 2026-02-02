/*
================================================================================
STORED PROCEDURE: [CxP].[HU4_I_NumLiquidacion_50]
================================================================================

Descripcion General:
--------------------
    Procesa documentos con numero de liquidacion que comienza con '50'
    (Comercializados). Copia los archivos de insumo a una carpeta especifica
    y marca los documentos como CON NOVEDAD - COMERCIALIZADOS.
    
    Implementa el patron QUEUE/FINALIZE para operaciones de archivos:
    - QUEUE: Obtiene lista de archivos a copiar y genera BatchId
    - FINALIZE: Recibe resultados de la copia y actualiza estados
    
    A diferencia del Punto H (que mueve archivos), este SP COPIA archivos.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Base de Datos: NotificationsPaddy
Schema: CxP

================================================================================
PATRON QUEUE/FINALIZE
================================================================================

Este SP implementa un patron de dos fases para operaciones de archivos:

FASE 1 - QUEUE:
    1. Identifica documentos con numero liquidacion '50%' y estado CON NOVEDAD
    2. Crea registros en tabla de cola [HU4_Punto_I_FileOpsQueue]
    3. Genera BatchId unico para el lote
    4. Retorna lista de archivos a copiar con rutas origen y destino

FASE 2 - FINALIZE:
    1. Recibe BatchId y JSON con resultados de la copia
    2. Actualiza estado de la cola (OK/FAIL)
    3. Actualiza DocumentsProcessing con observaciones
    4. Actualiza Comparativa
    5. Inserta en ReporteNovedades

El proceso externo (Python) ejecuta la copia fisica de archivos
entre las dos fases.

================================================================================
DIAGRAMA DE FLUJO - MODO QUEUE
================================================================================

    +-------------------------------------------------------------+
    |                   MODO = 'QUEUE'                            |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Validar parametros y tablas requeridas                     |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Crear tabla [HU4_Punto_I_FileOpsQueue] si no existe        |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Limpiar cola: DELETE FROM [HU4_Punto_I_FileOpsQueue]       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Buscar candidatos en DocumentsProcessing:                  |
    |  - numero_liquidacion LIKE '50%'                            |
    |  - ResultadoFinalAntesEventos = 'CON NOVEDAD'               |
    |  - Dentro de @DiasMaximos                                   |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Insertar en cola con:                                      |
    |  - BatchId = NEWID()                                        |
    |  - Operacion = 'COPY'                                       |
    |  - Accion = 'CON_NOVEDAD_COMERCIALIZADOS'                   |
    |  - CarpetaDestino = carpeta COMERCIALIZADOS                 |
    |  - Estado = 'PENDIENTE'                                     |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar lista de archivos a copiar:                       |
    |  - BatchId, ID_registro, executionNum                       |
    |  - NombreArchivo, RutaOrigenFull, CarpetaDestino            |
    |  (Solo registros con TieneInsumos = 1)                      |
    +-------------------------------------------------------------+

================================================================================
DIAGRAMA DE FLUJO - MODO FINALIZE
================================================================================

    +-------------------------------------------------------------+
    |                  MODO = 'FINALIZE'                          |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Validar @BatchId (requerido)                               |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Parsear @ResultadosJson con OPENJSON                       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Actualizar cola: Estado = 'OK' o 'FAIL'                    |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Construir mensajes segun resultado:                        |
    |  - Exitoso: "Factura corresponde a COMERCIALIZADOS"         |
    |  - Fallido: "...No se logran mover insumos..."              |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Actualizar DocumentsProcessing:                            |
    |  - ResultadoFinalAntesEventos = 'CON NOVEDAD - COMERCIALIZADOS'
    |  - EstadoFinalFase_4 = 'Exitoso'                            |
    |  - ObservacionesFase_4 con mensaje                          |
    |  - RutaArchivo (si copia exitosa)                           |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Actualizar Comparativa y ReporteNovedades                  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar resumen:                                          |
    |  BatchId, IDsFinalizados, OK, FAIL, RegistrosReporte        |
    +-------------------------------------------------------------+

================================================================================
PARAMETROS
================================================================================

    @Modo VARCHAR(10) = 'QUEUE'
        Modo de operacion: 'QUEUE' o 'FINALIZE'
        
    @executionNum INT = NULL
        Numero de ejecucion para filtrar (opcional)
        
    @BatchId UNIQUEIDENTIFIER = NULL
        ID del lote (requerido en FINALIZE)
        
    @DiasMaximos INT = 120
        Dias maximos desde la fecha de retoma
        
    @UseBogotaTime BIT = 0
        Usar hora de Bogota en lugar de hora del servidor
        
    @BatchSize INT = 500
        Cantidad maxima de documentos a procesar
        
    @ResultadosJson NVARCHAR(MAX) = NULL
        JSON con resultados de la copia (usado en FINALIZE)

================================================================================
CRITERIOS DE SELECCION
================================================================================

Un documento es candidato para Punto I si cumple:

    1. numero_de_liquidacion_u_orden_de_compra LIKE '50%'
       - El numero de liquidacion comienza con 50
       
    2. ResultadoFinalAntesEventos = 'CON NOVEDAD'
       - Ya fue procesado y tiene novedad
       
    3. Fecha de retoma dentro de @DiasMaximos
       - O fecha de retoma es NULL
       
    4. TieneInsumos = 1 (para retornar archivos)
       - RutaArchivo no vacia
       - actualizacionNombreArchivos no vacio

================================================================================
DIFERENCIAS CON PUNTO H
================================================================================

    Punto H (Agrupacion):
        - Criterio: agrupacion MAPG o MAPM
        - Operacion: MOVE (mover archivos)
        - Estado final: EXCLUIDO GRANOS / EXCLUIDO MAIZ
        
    Punto I (NumLiquidacion_50):
        - Criterio: numero_liquidacion LIKE '50%'
        - Operacion: COPY (copiar archivos)
        - Estado final: CON NOVEDAD - COMERCIALIZADOS

================================================================================
CARPETA DESTINO
================================================================================

Ruta fija:
    \\172.16.250.222\BOT_Validacion_FV_NC_ND_CXP\COMERCIALIZADOS\INSUMO

================================================================================
TABLA DE COLA: [CxP].[HU4_Punto_I_FileOpsQueue]
================================================================================

Estructura:
    QueueId             BIGINT IDENTITY     - ID unico de cola
    BatchId             UNIQUEIDENTIFIER    - ID del lote
    ID_registro         BIGINT              - ID del documento
    executionNum        INT                 - Numero de ejecucion
    Operacion           VARCHAR(10)         - 'COPY'
    RutaOrigen          NVARCHAR(4000)      - Carpeta origen
    NombresArchivos     NVARCHAR(4000)      - Archivos separados por ;
    Accion              NVARCHAR(50)        - CON_NOVEDAD_COMERCIALIZADOS
    CarpetaDestino      NVARCHAR(4000)      - Carpeta destino
    Estado              VARCHAR(20)         - PENDIENTE/OK/FAIL
    NuevaRutaArchivo    NVARCHAR(4000)      - Ruta despues de copiar
    ErrorMsg            NVARCHAR(4000)      - Mensaje de error
    FechaCreacion       DATETIME2(3)        - Fecha de creacion
    FechaActualizacion  DATETIME2(3)        - Fecha de actualizacion

================================================================================
RESULTSETS DE SALIDA
================================================================================

MODO QUEUE - Retorna lista de archivos:
---------------------------------------
    BatchId             UNIQUEIDENTIFIER
    ID_registro         BIGINT
    executionNum        INT
    Operacion           VARCHAR
    Accion              NVARCHAR
    CarpetaDestino      NVARCHAR
    NombreArchivo       NVARCHAR
    RutaOrigenFull      NVARCHAR

    NOTA: Solo retorna registros donde TieneInsumos = 1

MODO FINALIZE - Retorna resumen:
--------------------------------
    BatchId                     UNIQUEIDENTIFIER
    IDsFinalizados              INT
    OK                          INT
    FAIL                        INT
    RegistrosReporteNovedades   INT

================================================================================
EJEMPLOS DE USO
================================================================================

-- Ejemplo 1: Ejecutar QUEUE
EXEC [CxP].[HU4_I_NumLiquidacion_50]
    @Modo = 'QUEUE',
    @DiasMaximos = 120,
    @BatchSize = 500;

-- Ejemplo 2: Ejecutar FINALIZE con resultados
DECLARE @Resultados NVARCHAR(MAX) = '[
    {"ID_registro":"123","MovimientoExitoso":"true","NuevaRutaArchivo":"\\\\server\\path","ErrorMsg":""},
    {"ID_registro":"124","MovimientoExitoso":"false","NuevaRutaArchivo":"","ErrorMsg":"Error"}
]';

EXEC [CxP].[HU4_I_NumLiquidacion_50]
    @Modo = 'FINALIZE',
    @BatchId = 'B2C3D4E5-F6A7-8901-BCDE-F23456789012',
    @ResultadosJson = @Resultados;

-- Ejemplo 3: Ver candidatos antes de ejecutar
SELECT 
    ID,
    numero_de_liquidacion_u_orden_de_compra,
    ResultadoFinalAntesEventos,
    RutaArchivo,
    actualizacionNombreArchivos
FROM [CxP].[DocumentsProcessing]
WHERE numero_de_liquidacion_u_orden_de_compra LIKE '50%'
  AND ResultadoFinalAntesEventos = 'CON NOVEDAD';

================================================================================
INTEGRACION CON SCRIPTS PYTHON
================================================================================

Scripts relacionados:
    - ejecutar_HU4_I_NumLiquidacion_50_QUEUE.py
    - ejecutar_HU4_I_NumLiquidacion_50_FINALIZE.py
    - ejecutar_FileOps_PuntoI_COPIAR.py

Flujo tipico:
    1. Python ejecuta SP en modo QUEUE
    2. Python recibe lista de archivos
    3. Python copia archivos fisicamente (shutil.copy2)
    4. Python ejecuta SP en modo FINALIZE con resultados JSON

================================================================================
MANEJO DE ERRORES
================================================================================

    - Errores de parametros: RAISERROR con mensaje descriptivo
    - Errores en QUEUE: ROLLBACK y RAISERROR
    - Errores en FINALIZE: ROLLBACK y RAISERROR
    - Modo invalido: RAISERROR "Modo invalido. Use QUEUE o FINALIZE."

================================================================================
NOTAS TECNICAS
================================================================================

    - La cola se limpia completamente en cada QUEUE (DELETE con TABLOCK)
    - STRING_SPLIT se usa para separar archivos por punto y coma
    - OPENJSON se usa para parsear resultados en FINALIZE
    - TieneInsumos filtra solo registros con archivos validos
    - Las observaciones se concatenan (no se sobrescriben)
    - El indice UX_HU4_Punto_I_ID_Pendiente evita duplicados pendientes

================================================================================
*/

USE [NotificationsPaddy]
GO
/****** Object:  StoredProcedure [CxP].[HU4_I_NumLiquidacion_50]    Script Date: 01/02/2026 4:41:31 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [CxP].[HU4_I_NumLiquidacion_50]
(
    @Modo           VARCHAR(10) = 'QUEUE',
    @executionNum   INT = NULL,
    @BatchId        UNIQUEIDENTIFIER = NULL,
    @DiasMaximos    INT = 120,
    @UseBogotaTime  BIT = 0,
    @BatchSize      INT = 500,
    @ResultadosJson NVARCHAR(MAX) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @DiasMaximos IS NULL OR @DiasMaximos <= 0
    BEGIN
        RAISERROR('Parametro invalido: @DiasMaximos debe ser > 0.', 16, 1);
        RETURN;
    END;

    IF @BatchSize IS NULL OR @BatchSize <= 0
        SET @BatchSize = 500;

    IF OBJECT_ID(N'[CxP].[DocumentsProcessing]', N'U') IS NULL
    BEGIN
        RAISERROR('No existe la tabla requerida: [CxP].[DocumentsProcessing].', 16, 1);
        RETURN;
    END;

    IF OBJECT_ID(N'[dbo].[CxP.Comparativa]', N'U') IS NULL
    BEGIN
        RAISERROR('No existe la tabla requerida: [dbo].[CxP.Comparativa].', 16, 1);
        RETURN;
    END;

    IF OBJECT_ID(N'[CxP].[ReporteNovedades]', N'U') IS NULL
    BEGIN
        BEGIN TRY
            CREATE TABLE [CxP].[ReporteNovedades]
            (
                RowID                BIGINT IDENTITY(1,1) NOT NULL,
                ID                   VARCHAR(50)     NOT NULL,
                Fecha_Carga          VARCHAR(20)     NOT NULL,
                Nit                  VARCHAR(50)     NULL,
                Nombre_Proveedor     VARCHAR(500)    NULL,
                Orden_de_compra      VARCHAR(100)    NULL,
                Numero_factura       VARCHAR(100)    NULL,
                Estado_CXP_Bot       VARCHAR(200)    NULL,
                Observaciones        NVARCHAR(MAX)   NULL,
                SP_Origen            VARCHAR(100)    NULL,
                Fecha_Insercion      DATETIME2(3)    NOT NULL DEFAULT SYSDATETIME(),
                
                CONSTRAINT PK_ReporteNovedades PRIMARY KEY CLUSTERED (RowID ASC)
            );
            
            CREATE NONCLUSTERED INDEX IX_ReporteNovedades_ID
            ON [CxP].[ReporteNovedades] (ID);
            
            CREATE NONCLUSTERED INDEX IX_ReporteNovedades_Estado
            ON [CxP].[ReporteNovedades] (Estado_CXP_Bot);
            
            CREATE NONCLUSTERED INDEX IX_ReporteNovedades_Fecha
            ON [CxP].[ReporteNovedades] (Fecha_Carga);
            
            CREATE NONCLUSTERED INDEX IX_ReporteNovedades_SPOrigen
            ON [CxP].[ReporteNovedades] (SP_Origen);
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() NOT IN (2714, 1913, 1911)
                THROW;
        END CATCH
    END;

    IF OBJECT_ID(N'[CxP].[HU4_Punto_I_FileOpsQueue]', N'U') IS NULL
    BEGIN
        BEGIN TRY
            EXEC(N'
                CREATE TABLE [CxP].[HU4_Punto_I_FileOpsQueue]
                (
                    QueueId            BIGINT IDENTITY(1,1) NOT NULL
                        CONSTRAINT PK_HU4_Punto_I_FileOpsQueue PRIMARY KEY,
                    BatchId            UNIQUEIDENTIFIER NOT NULL,
                    ID_registro        BIGINT NOT NULL,
                    executionNum       INT NULL,
                    Operacion          VARCHAR(10) NOT NULL,
                    RutaOrigen         NVARCHAR(4000) NULL,
                    NombresArchivos    NVARCHAR(4000) NULL,
                    Accion             NVARCHAR(50) NOT NULL,
                    CarpetaDestino     NVARCHAR(4000) NOT NULL,
                    Estado             VARCHAR(20) NOT NULL
                        CONSTRAINT DF_HU4_Punto_I_Estado DEFAULT (''PENDIENTE''),
                    NuevaRutaArchivo   NVARCHAR(4000) NULL,
                    ErrorMsg           NVARCHAR(4000) NULL,
                    FechaCreacion      DATETIME2(3) NOT NULL
                        CONSTRAINT DF_HU4_Punto_I_FechaCreacion DEFAULT (SYSUTCDATETIME()),
                    FechaActualizacion DATETIME2(3) NULL
                );
            ');

            EXEC(N'
                CREATE INDEX IX_HU4_Punto_I_Batch_Estado
                ON [CxP].[HU4_Punto_I_FileOpsQueue](BatchId, Estado)
                INCLUDE (ID_registro, executionNum, Operacion, Accion, CarpetaDestino);
            ');

            EXEC(N'
                CREATE UNIQUE INDEX UX_HU4_Punto_I_ID_Pendiente
                ON [CxP].[HU4_Punto_I_FileOpsQueue](ID_registro)
                WHERE Estado = ''PENDIENTE'';
            ');
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() NOT IN (2714, 1913, 1911)
                THROW;
        END CATCH
    END;

    DECLARE @Now DATETIME2(3) =
        CASE
            WHEN @UseBogotaTime = 1
                THEN CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'SA Pacific Standard Time' AS DATETIME2(3))
            ELSE CAST(SYSDATETIME() AS DATETIME2(3))
        END;

    DECLARE @Cutoff DATETIME2(3) = DATEADD(DAY, -@DiasMaximos, @Now);
    DECLARE @Take INT = @BatchSize;
    DECLARE @CarpetaDestino NVARCHAR(4000) = N'\\172.16.250.222\BOT_Validacion_FV_NC_ND_CXP\COMERCIALIZADOS\INSUMO';
    DECLARE @EstadoFase4Exitoso NVARCHAR(200) = N'Exitoso';

    IF UPPER(@Modo) = 'QUEUE'
    BEGIN
        DECLARE @NewBatch UNIQUEIDENTIFIER = NEWID();

        BEGIN TRY
            BEGIN TRAN;

            DELETE FROM [CxP].[HU4_Punto_I_FileOpsQueue] WITH (TABLOCK);

            IF OBJECT_ID('tempdb..#candidatos') IS NOT NULL DROP TABLE #candidatos;
            CREATE TABLE #candidatos
            (
                ID              BIGINT NOT NULL PRIMARY KEY,
                executionNum    INT NULL,
                RutaOrigen      NVARCHAR(4000) NOT NULL,
                NombresArchivos NVARCHAR(4000) NOT NULL,
                TieneInsumos    BIT NOT NULL
            );

            INSERT INTO #candidatos (ID, executionNum, RutaOrigen, NombresArchivos, TieneInsumos)
            SELECT TOP (@Take)
                dp.ID,
                dp.executionNum,
                LTRIM(RTRIM(COALESCE(dp.RutaArchivo, N''))) AS RutaOrigen,
                LTRIM(RTRIM(COALESCE(dp.actualizacionNombreArchivos, N''))) AS NombresArchivos,
                CASE
                    WHEN COALESCE(LTRIM(RTRIM(dp.RutaArchivo)), N'') <> N''
                     AND COALESCE(LTRIM(RTRIM(dp.actualizacionNombreArchivos)), N'') <> N''
                    THEN 1 ELSE 0
                END AS TieneInsumos
            FROM [CxP].[DocumentsProcessing] dp
            WHERE
                (@executionNum IS NULL OR dp.executionNum = @executionNum)
                AND (dp.Fecha_de_retoma_antes_de_contabilizacion IS NULL
                     OR CAST(dp.Fecha_de_retoma_antes_de_contabilizacion AS DATETIME2(3)) >= @Cutoff)
                AND LTRIM(RTRIM(COALESCE(dp.numero_de_liquidacion_u_orden_de_compra, N''))) LIKE N'50%'
                AND UPPER(LTRIM(RTRIM(COALESCE(dp.ResultadoFinalAntesEventos, N'')))) = N'CON NOVEDAD'
            ORDER BY dp.ID;

            INSERT INTO [CxP].[HU4_Punto_I_FileOpsQueue]
            (BatchId, ID_registro, executionNum, Operacion, RutaOrigen, NombresArchivos, Accion, CarpetaDestino, Estado)
            SELECT 
                @NewBatch, 
                c.ID, 
                c.executionNum, 
                'COPY', 
                c.RutaOrigen, 
                c.NombresArchivos, 
                'CON_NOVEDAD_COMERCIALIZADOS', 
                @CarpetaDestino, 
                'PENDIENTE'
            FROM #candidatos c;

            COMMIT;

            SELECT
                @NewBatch AS BatchId,
                q.ID_registro,
                q.executionNum,
                q.Accion,
                q.Operacion,
                q.CarpetaDestino,
                q.RutaOrigen,
                q.NombresArchivos,
                LTRIM(RTRIM(s.value)) AS NombreArchivo,
                CASE
                    WHEN q.RutaOrigen LIKE '%' + LTRIM(RTRIM(s.value)) THEN q.RutaOrigen
                    WHEN RIGHT(RTRIM(q.RutaOrigen), 1) IN ('\', '/')
                        THEN q.RutaOrigen + LTRIM(RTRIM(s.value))
                    ELSE q.RutaOrigen + '\' + LTRIM(RTRIM(s.value))
                END AS RutaOrigenFull
            FROM [CxP].[HU4_Punto_I_FileOpsQueue] q
            INNER JOIN #candidatos c ON c.ID = q.ID_registro
            OUTER APPLY STRING_SPLIT(COALESCE(q.NombresArchivos,''), ';') s
            WHERE q.BatchId = @NewBatch
              AND UPPER(LTRIM(RTRIM(q.Estado))) = 'PENDIENTE'
              AND c.TieneInsumos = 1
              AND LTRIM(RTRIM(s.value)) <> ''
            ORDER BY q.QueueId;

            RETURN;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK;
            DECLARE @ErrMsgQ NVARCHAR(2000) = CONCAT(
                'Fallo en SP [CxP].[HU4_I_NumLiquidacion_50] MODO=QUEUE. ',
                'Error ', ERROR_NUMBER(), ', Severity ', ERROR_SEVERITY(),
                ', State ', ERROR_STATE(), ', Linea ', ERROR_LINE(),
                ': ', ERROR_MESSAGE()
            );
            RAISERROR(@ErrMsgQ, 16, 1);
            RETURN;
        END CATCH
    END;

    IF UPPER(@Modo) = 'FINALIZE'
    BEGIN
        IF @BatchId IS NULL
        BEGIN
            RAISERROR('BatchId es requerido en FINALIZE.', 16, 1);
            RETURN;
        END;

        BEGIN TRY
            BEGIN TRAN;

            DECLARE @Resultados TABLE
            (
                ID_registro BIGINT NOT NULL PRIMARY KEY,
                MovimientoExitoso BIT NOT NULL,
                NuevaRutaArchivo NVARCHAR(4000) NULL,
                ErrorMsg NVARCHAR(4000) NULL
            );

            IF NULLIF(LTRIM(RTRIM(@ResultadosJson)), N'') IS NOT NULL
            BEGIN
                INSERT INTO @Resultados(ID_registro, MovimientoExitoso, NuevaRutaArchivo, ErrorMsg)
                SELECT
                    TRY_CAST(ID_registro AS BIGINT),
                    CASE WHEN LOWER(ISNULL(MovimientoExitoso,'false')) IN ('1','true','si','yes','y') THEN 1 ELSE 0 END,
                    NuevaRutaArchivo,
                    ErrorMsg
                FROM OPENJSON(@ResultadosJson)
                WITH (
                    ID_registro NVARCHAR(50) '$.ID_registro',
                    MovimientoExitoso NVARCHAR(10) '$.MovimientoExitoso',
                    NuevaRutaArchivo NVARCHAR(4000) '$.NuevaRutaArchivo',
                    ErrorMsg NVARCHAR(4000) '$.ErrorMsg'
                )
                WHERE TRY_CAST(ID_registro AS BIGINT) IS NOT NULL;
            END;

            UPDATE q
               SET q.Estado = CASE WHEN r.MovimientoExitoso = 1 THEN 'OK' ELSE 'FAIL' END,
                   q.NuevaRutaArchivo = r.NuevaRutaArchivo,
                   q.ErrorMsg = r.ErrorMsg,
                   q.FechaActualizacion = SYSUTCDATETIME()
            FROM [CxP].[HU4_Punto_I_FileOpsQueue] q
            INNER JOIN @Resultados r ON r.ID_registro = q.ID_registro
            WHERE q.BatchId = @BatchId AND UPPER(LTRIM(RTRIM(q.Estado))) = 'PENDIENTE';

            IF OBJECT_ID('tempdb..#BatchAll') IS NOT NULL DROP TABLE #BatchAll;
            SELECT DISTINCT q.ID_registro AS ID
            INTO #BatchAll
            FROM [CxP].[HU4_Punto_I_FileOpsQueue] q
            WHERE q.BatchId = @BatchId;

            IF OBJECT_ID('tempdb..#BatchConResultados') IS NOT NULL DROP TABLE #BatchConResultados;
            SELECT 
                r.ID_registro AS ID,
                r.MovimientoExitoso,
                r.NuevaRutaArchivo
            INTO #BatchConResultados
            FROM @Resultados r;

            IF OBJECT_ID('tempdb..#Msg') IS NOT NULL DROP TABLE #Msg;
            SELECT
                b.ID,
                CASE
                    WHEN r.ID IS NOT NULL AND r.MovimientoExitoso = 1
                        THEN N'Factura corresponde a COMERCIALIZADOS'
                    WHEN r.ID IS NOT NULL AND r.MovimientoExitoso = 0
                        THEN N'Factura corresponde a COMERCIALIZADOS - No se logran mover insumos a carpeta COMERCIALIZADOS'
                    ELSE N'Factura corresponde a COMERCIALIZADOS - No se logran mover insumos a carpeta COMERCIALIZADOS'
                END AS Mensaje,
                N'CON NOVEDAD - COMERCIALIZADOS' AS ResultadoFinal,
                CASE WHEN r.ID IS NOT NULL THEN r.NuevaRutaArchivo ELSE NULL END AS NuevaRutaArchivo
            INTO #Msg
            FROM #BatchAll b
            LEFT JOIN #BatchConResultados r ON r.ID = b.ID;

            UPDATE dp
               SET dp.EstadoFinalFase_4 = @EstadoFase4Exitoso,
                   dp.ObservacionesFase_4 = LEFT(
                       LTRIM(RTRIM(CONCAT(
                           m.Mensaje,
                           CASE
                               WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N''
                               ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4))
                           END
                       ))),
                       3900
                   ),
                   dp.ResultadoFinalAntesEventos = m.ResultadoFinal
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Msg m ON m.ID = dp.ID;

            UPDATE dp
               SET dp.RutaArchivo = CASE
                    WHEN m.NuevaRutaArchivo IS NOT NULL AND LTRIM(RTRIM(m.NuevaRutaArchivo)) <> N''
                        THEN m.NuevaRutaArchivo
                    ELSE dp.RutaArchivo
               END
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Msg m ON m.ID = dp.ID
            WHERE m.NuevaRutaArchivo IS NOT NULL;

            UPDATE c
               SET c.Valor_XML = LEFT(dp.ObservacionesFase_4, 3900)
            FROM [dbo].[CxP.Comparativa] c
            INNER JOIN #Msg m ON m.ID = c.ID_registro
            INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = m.ID
            WHERE c.Item = N'Observaciones';

            UPDATE c
               SET c.Estado_validacion_antes_de_eventos = m.ResultadoFinal
            FROM [dbo].[CxP.Comparativa] c
            INNER JOIN #Msg m ON m.ID = c.ID_registro;

            DECLARE @RegistrosReporteNovedades INT = 0;
            
            INSERT INTO [CxP].[ReporteNovedades]
            (ID, Fecha_Carga, Nit, Nombre_Proveedor, Orden_de_compra, Numero_factura, Estado_CXP_Bot, Observaciones, SP_Origen)
            SELECT
                CAST(dp.ID AS VARCHAR(50)),
                CONVERT(VARCHAR(20), CAST(@Now AS DATE), 120),
                ISNULL(CAST(dp.nit_emisor_o_nit_del_proveedor AS VARCHAR(50)), ''),
                ISNULL(SUBSTRING(dp.nombre_emisor, 1, 500), ''),
                ISNULL(SUBSTRING(dp.numero_de_liquidacion_u_orden_de_compra, 1, 100), ''),
                ISNULL(SUBSTRING(dp.numero_de_factura, 1, 100), ''),
                ISNULL(SUBSTRING(dp.ResultadoFinalAntesEventos, 1, 200), ''),
                ISNULL(dp.ObservacionesFase_4, ''),
                'HU4_I_NumLiquidacion_50'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Msg m ON m.ID = dp.ID
            WHERE dp.ResultadoFinalAntesEventos = N'CON NOVEDAD - COMERCIALIZADOS';
            
            SET @RegistrosReporteNovedades = @@ROWCOUNT;

            COMMIT;

            SELECT
                @BatchId AS BatchId,
                COUNT(*) AS IDsFinalizados,
                SUM(CASE WHEN q.Estado = 'OK' THEN 1 ELSE 0 END) AS OK,
                SUM(CASE WHEN q.Estado = 'FAIL' THEN 1 ELSE 0 END) AS FAIL,
                @RegistrosReporteNovedades AS RegistrosReporteNovedades
            FROM [CxP].[HU4_Punto_I_FileOpsQueue] q
            WHERE q.BatchId = @BatchId;

            RETURN;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK;
            DECLARE @ErrMsgF NVARCHAR(2000) = CONCAT(
                'Fallo en SP [CxP].[HU4_I_NumLiquidacion_50] MODO=FINALIZE. ',
                'Error ', ERROR_NUMBER(), ', Severity ', ERROR_SEVERITY(),
                ', State ', ERROR_STATE(), ', Linea ', ERROR_LINE(),
                ': ', ERROR_MESSAGE()
            );
            RAISERROR(@ErrMsgF, 16, 1);
            RETURN;
        END CATCH
    END;

    RAISERROR('Modo invalido. Use QUEUE o FINALIZE.', 16, 1);
    RETURN;
END

/*
================================================================================
STORED PROCEDURE: [CxP].[HU4_H_Agrupacion]
================================================================================

Descripcion General:
--------------------
    Procesa documentos con agrupacion MAPG (Materia Prima Granos) o 
    MAPM (Materia Prima Maiz). Mueve los archivos de insumo a carpetas
    especificas y marca los documentos como EXCLUIDO GRANOS o EXCLUIDO MAIZ.
    
    Implementa el patron QUEUE/FINALIZE para operaciones de archivos:
    - QUEUE: Obtiene lista de archivos a mover y genera BatchId
    - FINALIZE: Recibe resultados del movimiento y actualiza estados

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Base de Datos: NotificationsPaddy
Schema: CxP

================================================================================
PATRON QUEUE/FINALIZE
================================================================================

Este SP implementa un patron de dos fases para operaciones de archivos:

FASE 1 - QUEUE:
    1. Identifica documentos con agrupacion MAPG o MAPM
    2. Crea registros en tabla de cola [HU4_Punto_H_FileOpsQueue]
    3. Genera BatchId unico para el lote
    4. Retorna lista de archivos a mover con rutas origen y destino

FASE 2 - FINALIZE:
    1. Recibe BatchId y JSON con resultados del movimiento
    2. Actualiza estado de la cola (OK/FAIL)
    3. Actualiza DocumentsProcessing con observaciones
    4. Actualiza Comparativa
    5. Inserta en ReporteNovedades

El proceso externo (Python) ejecuta el movimiento fisico de archivos
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
    |  Crear tabla [HU4_Punto_H_FileOpsQueue] si no existe        |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Limpiar cola: DELETE FROM [HU4_Punto_H_FileOpsQueue]       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Buscar candidatos en DocumentsProcessing:                  |
    |  - agrupacion contiene MAPG o MAPM                          |
    |  - RutaArchivo no vacia                                     |
    |  - actualizacionNombreArchivos no vacio                     |
    |  - Dentro de @DiasMaximos                                   |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Insertar en cola con:                                      |
    |  - BatchId = NEWID()                                        |
    |  - Operacion = 'MOVE'                                       |
    |  - Accion = 'EXCLUIDO GRANOS' o 'EXCLUIDO MAIZ'             |
    |  - CarpetaDestino segun agrupacion                          |
    |  - Estado = 'PENDIENTE'                                     |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar lista de archivos a mover:                        |
    |  - BatchId, ID_registro, executionNum                       |
    |  - NombreArchivo, RutaOrigenFull, CarpetaDestino            |
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
    |  Parsear @ResultadosJson con OPENJSON:                      |
    |  - ID_registro, MovimientoExitoso                           |
    |  - NuevaRutaArchivo, ErrorMsg                               |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Actualizar cola:                                           |
    |  - Estado = 'OK' o 'FAIL'                                   |
    |  - NuevaRutaArchivo, ErrorMsg                               |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Construir mensajes segun resultado:                        |
    |  - MAPG exitoso: "Factura excluida corresponde a MAPG"      |
    |  - MAPG fallido: "...No se logran mover insumos..."         |
    |  - MAPM exitoso: "Factura excluida corresponde a MAPM"      |
    |  - MAPM fallido: "...No se logran mover insumos..."         |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Actualizar DocumentsProcessing:                            |
    |  - EstadoFinalFase_4, ObservacionesFase_4                   |
    |  - ResultadoFinalAntesEventos                               |
    |  - RutaArchivo (si movimiento exitoso)                      |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Actualizar Comparativa y ReporteNovedades                  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar resumen: BatchId, OK, FAIL, RegistrosReporte      |
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
        JSON con resultados del movimiento (usado en FINALIZE)

================================================================================
ESTRUCTURA DEL JSON DE RESULTADOS
================================================================================

El parametro @ResultadosJson debe tener esta estructura:

    [
        {
            "ID_registro": "12345",
            "MovimientoExitoso": "true",
            "NuevaRutaArchivo": "\\\\servidor\\carpeta\\",
            "ErrorMsg": ""
        },
        {
            "ID_registro": "12346",
            "MovimientoExitoso": "false",
            "NuevaRutaArchivo": "",
            "ErrorMsg": "Archivo no encontrado"
        }
    ]

Valores aceptados para MovimientoExitoso:
    - true, 1, si, yes, y  -> Exito
    - false, 0, no         -> Fallo

================================================================================
CARPETAS DESTINO
================================================================================

Ruta base: \\172.16.250.222\BOT_Validacion_FV_NC_ND_CXP\

    MAPG (Granos):
        \\172.16.250.222\BOT_Validacion_FV_NC_ND_CXP\MATERIA PRIMA GRANOS\INSUMO
        
    MAPM (Maiz):
        \\172.16.250.222\BOT_Validacion_FV_NC_ND_CXP\MATERIA PRIMA MAIZ\INSUMO

================================================================================
TABLA DE COLA: [CxP].[HU4_Punto_H_FileOpsQueue]
================================================================================

Estructura:
    QueueId             BIGINT IDENTITY     - ID unico de cola
    BatchId             UNIQUEIDENTIFIER    - ID del lote
    ID_registro         BIGINT              - ID del documento
    executionNum        INT                 - Numero de ejecucion
    Operacion           VARCHAR(10)         - 'MOVE'
    RutaOrigen          NVARCHAR(4000)      - Carpeta origen
    NombresArchivos     NVARCHAR(4000)      - Archivos separados por ;
    Accion              NVARCHAR(30)        - EXCLUIDO GRANOS/MAIZ
    CarpetaDestino      NVARCHAR(4000)      - Carpeta destino
    Estado              VARCHAR(20)         - PENDIENTE/OK/FAIL
    NuevaRutaArchivo    NVARCHAR(4000)      - Ruta despues de mover
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

MODO FINALIZE - Retorna resumen:
--------------------------------
    BatchId                     UNIQUEIDENTIFIER
    OK                          INT
    FAIL                        INT
    RegistrosReporteNovedades   INT

================================================================================
EJEMPLOS DE USO
================================================================================

-- Ejemplo 1: Ejecutar QUEUE
EXEC [CxP].[HU4_H_Agrupacion]
    @Modo = 'QUEUE',
    @DiasMaximos = 120,
    @BatchSize = 500;

-- Ejemplo 2: Ejecutar FINALIZE con resultados
DECLARE @Resultados NVARCHAR(MAX) = '[
    {"ID_registro":"123","MovimientoExitoso":"true","NuevaRutaArchivo":"\\\\server\\path","ErrorMsg":""},
    {"ID_registro":"124","MovimientoExitoso":"false","NuevaRutaArchivo":"","ErrorMsg":"Error"}
]';

EXEC [CxP].[HU4_H_Agrupacion]
    @Modo = 'FINALIZE',
    @BatchId = 'A1B2C3D4-E5F6-7890-ABCD-EF1234567890',
    @ResultadosJson = @Resultados;

-- Ejemplo 3: QUEUE con executionNum especifico
EXEC [CxP].[HU4_H_Agrupacion]
    @Modo = 'QUEUE',
    @executionNum = 5,
    @BatchSize = 100;

================================================================================
INTEGRACION CON SCRIPTS PYTHON
================================================================================

Scripts relacionados:
    - ejecutar_HU4_H_Agrupacion_QUEUE.py
    - ejecutar_HU4_H_Agrupacion_FINALIZE.py
    - ejecutar_FileOps_PuntoH_MOVER.py

Flujo tipico:
    1. Python ejecuta SP en modo QUEUE
    2. Python recibe lista de archivos
    3. Python mueve archivos fisicamente (shutil.move)
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
    - Las observaciones se concatenan (no se sobrescriben)
    - El indice UX_HU4_Punto_H_ID_Pendiente evita duplicados pendientes

================================================================================
*/

USE [NotificationsPaddy]
GO
/****** Object:  StoredProcedure [CxP].[HU4_H_Agrupacion]    Script Date: 01/02/2026 4:41:05 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [CxP].[HU4_H_Agrupacion]
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

    IF OBJECT_ID(N'[CxP].[HU4_Punto_H_FileOpsQueue]', N'U') IS NULL
    BEGIN
        BEGIN TRY
            EXEC(N'
                CREATE TABLE [CxP].[HU4_Punto_H_FileOpsQueue]
                (
                    QueueId            BIGINT IDENTITY(1,1) NOT NULL
                        CONSTRAINT PK_HU4_Punto_H_FileOpsQueue PRIMARY KEY,
                    BatchId            UNIQUEIDENTIFIER NOT NULL,
                    ID_registro        BIGINT NOT NULL,
                    executionNum       INT NULL,
                    Operacion          VARCHAR(10) NOT NULL,
                    RutaOrigen         NVARCHAR(4000) NULL,
                    NombresArchivos    NVARCHAR(4000) NULL,
                    Accion             NVARCHAR(30) NOT NULL,
                    CarpetaDestino     NVARCHAR(4000) NOT NULL,
                    Estado             VARCHAR(20) NOT NULL
                        CONSTRAINT DF_HU4_Punto_H_Estado DEFAULT (''PENDIENTE''),
                    NuevaRutaArchivo   NVARCHAR(4000) NULL,
                    ErrorMsg           NVARCHAR(4000) NULL,
                    FechaCreacion      DATETIME2(3) NOT NULL
                        CONSTRAINT DF_HU4_Punto_H_FechaCreacion DEFAULT (SYSUTCDATETIME()),
                    FechaActualizacion DATETIME2(3) NULL
                );
            ');

            EXEC(N'
                CREATE INDEX IX_HU4_Punto_H_Batch_Estado
                ON [CxP].[HU4_Punto_H_FileOpsQueue](BatchId, Estado)
                INCLUDE (ID_registro, executionNum, Operacion, Accion, CarpetaDestino);
            ');

            EXEC(N'
                CREATE UNIQUE INDEX UX_HU4_Punto_H_ID_Pendiente
                ON [CxP].[HU4_Punto_H_FileOpsQueue](ID_registro)
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
    DECLARE @BaseDestino NVARCHAR(4000) = N'\\172.16.250.222\BOT_Validacion_FV_NC_ND_CXP\';
    DECLARE @EstadoFase4Exitoso NVARCHAR(200) = N'VALIDACIÓN DATOS DE FACTURACIÓN: Exitoso';

    IF UPPER(@Modo) = 'QUEUE'
    BEGIN
        DECLARE @NewBatch UNIQUEIDENTIFIER = NEWID();

        BEGIN TRY
            BEGIN TRAN;

            DELETE FROM [CxP].[HU4_Punto_H_FileOpsQueue] WITH (TABLOCK);

            IF OBJECT_ID('tempdb..#candidatos') IS NOT NULL DROP TABLE #candidatos;
            CREATE TABLE #candidatos
            (
                ID              BIGINT NOT NULL PRIMARY KEY,
                executionNum    INT NULL,
                Accion          NVARCHAR(30) NOT NULL,
                RutaOrigen      NVARCHAR(4000) NOT NULL,
                NombresArchivos NVARCHAR(4000) NOT NULL,
                CarpetaDestino  NVARCHAR(4000) NOT NULL
            );

            INSERT INTO #candidatos (ID, executionNum, Accion, RutaOrigen, NombresArchivos, CarpetaDestino)
            SELECT TOP (@Take)
                dp.ID,
                dp.executionNum,
                CASE WHEN ISNULL(g.HasMAPG,0) = 1 THEN N'EXCLUIDO GRANOS' ELSE N'EXCLUIDO MAIZ' END AS Accion,
                LTRIM(RTRIM(TRY_CONVERT(NVARCHAR(4000), dp.RutaArchivo))) AS RutaOrigen,
                LTRIM(RTRIM(TRY_CONVERT(NVARCHAR(4000), dp.actualizacionNombreArchivos))) AS NombresArchivos,
                CASE
                    WHEN ISNULL(g.HasMAPG,0) = 1 THEN @BaseDestino + N'MATERIA PRIMA GRANOS\INSUMO'
                    ELSE @BaseDestino + N'MATERIA PRIMA MAIZ\INSUMO'
                END AS CarpetaDestino
            FROM [CxP].[DocumentsProcessing] dp
            OUTER APPLY (
                SELECT
                    MAX(CASE WHEN UPPER(LTRIM(RTRIM(s.value))) = 'MAPG' THEN 1 ELSE 0 END) AS HasMAPG,
                    MAX(CASE WHEN UPPER(LTRIM(RTRIM(s.value))) = 'MAPM' THEN 1 ELSE 0 END) AS HasMAPM
                FROM STRING_SPLIT(COALESCE(dp.agrupacion, ''), ';') s
            ) g
            WHERE
                (@executionNum IS NULL OR dp.executionNum = @executionNum)
                AND (dp.Fecha_de_retoma_antes_de_contabilizacion IS NULL
                     OR CAST(dp.Fecha_de_retoma_antes_de_contabilizacion AS DATETIME2(3)) >= @Cutoff)
                AND (ISNULL(g.HasMAPG,0) = 1 OR ISNULL(g.HasMAPM,0) = 1)
                AND NULLIF(LTRIM(RTRIM(TRY_CONVERT(NVARCHAR(4000), dp.RutaArchivo))), N'') IS NOT NULL
                AND NULLIF(LTRIM(RTRIM(TRY_CONVERT(NVARCHAR(4000), dp.actualizacionNombreArchivos))), N'') IS NOT NULL
            ORDER BY dp.ID;

            INSERT INTO [CxP].[HU4_Punto_H_FileOpsQueue]
            (BatchId, ID_registro, executionNum, Operacion, RutaOrigen, NombresArchivos, Accion, CarpetaDestino, Estado)
            SELECT @NewBatch, c.ID, c.executionNum, 'MOVE', c.RutaOrigen, c.NombresArchivos, c.Accion, c.CarpetaDestino, 'PENDIENTE'
            FROM #candidatos c;

            COMMIT;

            SELECT
                q.BatchId,
                q.ID_registro,
                q.executionNum,
                q.Accion,
                q.Operacion,
                q.CarpetaDestino,
                LTRIM(RTRIM(s.value)) AS NombreArchivo,
                CASE
                    WHEN q.RutaOrigen LIKE '%' + LTRIM(RTRIM(s.value)) THEN q.RutaOrigen
                    WHEN RIGHT(RTRIM(q.RutaOrigen), 1) IN ('\', '/')
                        THEN q.RutaOrigen + LTRIM(RTRIM(s.value))
                    ELSE q.RutaOrigen + '\' + LTRIM(RTRIM(s.value))
                END AS RutaOrigenFull
            FROM [CxP].[HU4_Punto_H_FileOpsQueue] q
            OUTER APPLY STRING_SPLIT(COALESCE(q.NombresArchivos,''), ';') s
            WHERE q.BatchId = @NewBatch
              AND UPPER(LTRIM(RTRIM(q.Estado))) = 'PENDIENTE'
              AND LTRIM(RTRIM(s.value)) <> ''
            ORDER BY q.QueueId;

            RETURN;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK;
            DECLARE @ErrMsgQ NVARCHAR(2000) = CONCAT(
                'Fallo en SP [CxP].[HU4_H_Agrupacion] MODO=QUEUE. ',
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
            FROM [CxP].[HU4_Punto_H_FileOpsQueue] q
            INNER JOIN @Resultados r ON r.ID_registro = q.ID_registro
            WHERE q.BatchId = @BatchId AND UPPER(LTRIM(RTRIM(q.Estado))) = 'PENDIENTE';

            IF OBJECT_ID('tempdb..#BatchFinalize') IS NOT NULL DROP TABLE #BatchFinalize;
            SELECT q.ID_registro AS ID, q.Accion, r.MovimientoExitoso, r.NuevaRutaArchivo
            INTO #BatchFinalize
            FROM [CxP].[HU4_Punto_H_FileOpsQueue] q
            INNER JOIN @Resultados r ON r.ID_registro = q.ID_registro
            WHERE q.BatchId = @BatchId;

            IF OBJECT_ID('tempdb..#Msg') IS NOT NULL DROP TABLE #Msg;
            SELECT
                b.ID,
                CASE
                    WHEN b.Accion = N'EXCLUIDO GRANOS' AND b.MovimientoExitoso = 1
                        THEN N'Factura excluida corresponde a MAPG'
                    WHEN b.Accion = N'EXCLUIDO GRANOS' AND b.MovimientoExitoso = 0
                        THEN N'Factura excluida corresponde a MAPG - No se logran mover insumos a carpeta MATERIA PRIMA GRANOS'
                    WHEN b.Accion = N'EXCLUIDO MAIZ' AND b.MovimientoExitoso = 1
                        THEN N'Factura excluida corresponde a MAPM'
                    WHEN b.Accion = N'EXCLUIDO MAIZ' AND b.MovimientoExitoso = 0
                        THEN N'Factura excluida corresponde a MAPM - No se logran mover insumos a carpeta MATERIA PRIMA MAIZ'
                    ELSE N'Factura excluida por agrupacion'
                END AS Mensaje,
                CASE WHEN b.Accion = N'EXCLUIDO GRANOS' THEN N'EXCLUIDO GRANOS' ELSE N'EXCLUIDO MAIZ' END AS ResultadoFinal,
                b.NuevaRutaArchivo
            INTO #Msg
            FROM #BatchFinalize b;

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
            INNER JOIN #Msg m ON m.ID = dp.ID;

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
                'HU4_H_Agrupacion'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Msg m ON m.ID = dp.ID
            WHERE dp.ResultadoFinalAntesEventos IN (N'EXCLUIDO GRANOS', N'EXCLUIDO MAIZ');
            
            SET @RegistrosReporteNovedades = @@ROWCOUNT;

            COMMIT;

            SELECT
                @BatchId AS BatchId,
                SUM(CASE WHEN q.Estado = 'OK' THEN 1 ELSE 0 END) AS OK,
                SUM(CASE WHEN q.Estado = 'FAIL' THEN 1 ELSE 0 END) AS FAIL,
                @RegistrosReporteNovedades AS RegistrosReporteNovedades
            FROM [CxP].[HU4_Punto_H_FileOpsQueue] q
            WHERE q.BatchId = @BatchId;

            RETURN;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK;
            DECLARE @ErrMsgF NVARCHAR(2000) = CONCAT(
                'Fallo en SP [CxP].[HU4_H_Agrupacion] MODO=FINALIZE. ',
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

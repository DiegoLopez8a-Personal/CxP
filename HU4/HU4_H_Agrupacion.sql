-- Author: Diego Ivan Lopez Ochoa
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

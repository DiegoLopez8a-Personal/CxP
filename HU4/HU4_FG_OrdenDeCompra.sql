-- Author: Diego Ivan Lopez Ochoa
USE [NotificationsPaddy]
GO
/****** Object:  StoredProcedure [CxP].[HU4_FG_OrdenDeCompra]    Script Date: 01/02/2026 4:40:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [CxP].[HU4_FG_OrdenDeCompra]
    @DiasMaximos INT = 120,
    @BatchSize   INT = 500
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'[CxP].[ReporteNovedades]', N'U') IS NULL
    BEGIN
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
    END

    DECLARE @ExecutionDateTimeOffset DATETIMEOFFSET(3) = SYSDATETIMEOFFSET();
    DECLARE @ExecutionDateTime       DATETIME2(3)      = CAST(@ExecutionDateTimeOffset AS DATETIME2(3));
    DECLARE @CutoffRetoma DATETIME2(3) = DATEADD(DAY, -@DiasMaximos, @ExecutionDateTime);

    IF @DiasMaximos IS NULL OR @DiasMaximos <= 0
        THROW 52000, 'Parametro invalido: @DiasMaximos debe ser > 0.', 1;

    IF @BatchSize IS NULL OR @BatchSize <= 0
        THROW 52001, 'Parametro invalido: @BatchSize debe ser > 0.', 1;

    IF OBJECT_ID(N'[CxP].[DocumentsProcessing]', N'U') IS NULL
        THROW 52002, 'No existe la tabla requerida: [CxP].[DocumentsProcessing].', 1;

    IF OBJECT_ID(N'[dbo].[CxP.Comparativa]', N'U') IS NULL
        THROW 52003, 'No existe la tabla requerida: [dbo].[CxP.Comparativa].', 1;

    IF OBJECT_ID('tempdb..#EstadosOmitir') IS NOT NULL DROP TABLE #EstadosOmitir;
    CREATE TABLE #EstadosOmitir
    (
        Estado NVARCHAR(200) NOT NULL PRIMARY KEY
    );

    INSERT INTO #EstadosOmitir(Estado) VALUES
        (N'APROBADO'),
        (N'APROBADO CONTADO Y/O EVENTO MANUAL'),
        (N'APROBADO SIN CONTABILIZACION'),
        (N'RECHAZADO'),
        (N'RECLASIFICAR'),
        (N'RECHAZADO - RETORNADO'),
        (N'CON NOVEDAD - RETORNADO'),
        (N'EN ESPERA DE POSICIONES'),
        (N'EXCLUIDO IMPORTACIONES'),
        (N'NO EXITOSO'),
        (N'EXCLUIDO COSTO INDIRECTO FLETES');

    IF OBJECT_ID('tempdb..#ProcessedIDs') IS NOT NULL DROP TABLE #ProcessedIDs;
    CREATE TABLE #ProcessedIDs (ID BIGINT NOT NULL PRIMARY KEY);

    IF OBJECT_ID('tempdb..#Batch') IS NOT NULL DROP TABLE #Batch;
    CREATE TABLE #Batch (ID BIGINT NOT NULL PRIMARY KEY);

    IF OBJECT_ID('tempdb..#ImpAll') IS NOT NULL DROP TABLE #ImpAll;
    CREATE TABLE #ImpAll (ID BIGINT NOT NULL PRIMARY KEY);

    IF OBJECT_ID('tempdb..#FleteAll') IS NOT NULL DROP TABLE #FleteAll;
    CREATE TABLE #FleteAll (ID BIGINT NOT NULL PRIMARY KEY);

    DECLARE
        @TotalProcesados          INT = 0,
        @TotalRetomaSetNull       INT = 0,
        @TotalImpExcluidos        INT = 0,
        @TotalFleteExcluidos      INT = 0,
        @TotalComparativaObs      INT = 0,
        @TotalComparativaEstado   INT = 0,
        @TotalRegistrosReporte    INT = 0;

    DECLARE @MsgImp   NVARCHAR(4000) = N'Factura excluida corresponde a Importaciones';
    DECLARE @MsgFlete NVARCHAR(4000) = N'Factura excluida corresponde a costo indirecto fletes';

    WHILE 1 = 1
    BEGIN
        TRUNCATE TABLE #Batch;

        INSERT INTO #Batch(ID)
        SELECT TOP (@BatchSize)
            dp.ID
        FROM [CxP].[DocumentsProcessing] dp WITH (READPAST)
        WHERE dp.documenttype = N'FV'
          AND (
                dp.Fecha_de_retoma_antes_de_contabilizacion IS NULL
                OR CAST(dp.Fecha_de_retoma_antes_de_contabilizacion AS DATETIME2(3)) >= @CutoffRetoma
              )
          AND (
                dp.ResultadoFinalAntesEventos IS NULL
                OR NOT EXISTS (SELECT 1 FROM #EstadosOmitir e WHERE e.Estado = dp.ResultadoFinalAntesEventos)
              )
          AND (
                LEFT(TRY_CONVERT(NVARCHAR(100), dp.numero_de_liquidacion_u_orden_de_compra), 2) IN (N'40', N'46')
              )
          AND NOT EXISTS (SELECT 1 FROM #ProcessedIDs p WHERE p.ID = dp.ID)
        ORDER BY dp.ID;

        IF @@ROWCOUNT = 0 BREAK;

        INSERT INTO #ProcessedIDs(ID)
        SELECT b.ID
        FROM #Batch b
        WHERE NOT EXISTS (SELECT 1 FROM #ProcessedIDs p WHERE p.ID = b.ID);

        SET @TotalProcesados += (SELECT COUNT(1) FROM #Batch);

        BEGIN TRY
            BEGIN TRAN;

            UPDATE dp
               SET dp.Fecha_de_retoma_antes_de_contabilizacion = @ExecutionDateTime
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Batch b ON b.ID = dp.ID
            WHERE dp.Fecha_de_retoma_antes_de_contabilizacion IS NULL;

            SET @TotalRetomaSetNull += @@ROWCOUNT;

            IF OBJECT_ID('tempdb..#ImpBatch') IS NOT NULL DROP TABLE #ImpBatch;
            CREATE TABLE #ImpBatch (ID BIGINT NOT NULL PRIMARY KEY);

            IF OBJECT_ID('tempdb..#FleteBatch') IS NOT NULL DROP TABLE #FleteBatch;
            CREATE TABLE #FleteBatch (ID BIGINT NOT NULL PRIMARY KEY);

            INSERT INTO #ImpBatch(ID)
            SELECT dp.ID
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Batch b ON b.ID = dp.ID
            WHERE LEFT(TRY_CONVERT(NVARCHAR(100), dp.numero_de_liquidacion_u_orden_de_compra), 2) = N'40';

            INSERT INTO #FleteBatch(ID)
            SELECT dp.ID
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Batch b ON b.ID = dp.ID
            WHERE LEFT(TRY_CONVERT(NVARCHAR(100), dp.numero_de_liquidacion_u_orden_de_compra), 2) = N'46';

            IF EXISTS (SELECT 1 FROM #ImpBatch)
            BEGIN
                UPDATE dp
                   SET dp.EstadoFinalFase_4 = N'VALIDACIÓN DATOS DE FACTURACIÓN: Exitoso',
                       dp.ResultadoFinalAntesEventos = N'EXCLUIDO IMPORTACIONES',
                       dp.ObservacionesFase_4 = LEFT(
                            CASE
                                WHEN LEFT(LTRIM(RTRIM(ISNULL(dp.ObservacionesFase_4, N''))), LEN(@MsgImp)) = @MsgImp
                                    THEN LTRIM(RTRIM(ISNULL(dp.ObservacionesFase_4, N'')))
                                ELSE LTRIM(RTRIM(CONCAT(
                                        @MsgImp,
                                        CASE
                                            WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N''
                                            ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4))
                                        END
                                    )))
                            END,
                            3900
                       )
                FROM [CxP].[DocumentsProcessing] dp
                INNER JOIN #ImpBatch i ON i.ID = dp.ID;

                SET @TotalImpExcluidos += @@ROWCOUNT;

                UPDATE c
                   SET c.Valor_XML = LEFT(dp.ObservacionesFase_4, 3900)
                FROM [dbo].[CxP.Comparativa] c
                INNER JOIN #ImpBatch i ON i.ID = c.ID_registro
                INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = i.ID
                WHERE c.Item = N'Observaciones';

                SET @TotalComparativaObs += @@ROWCOUNT;

                INSERT INTO [dbo].[CxP.Comparativa]
                (
                    Fecha_de_ejecucion,
                    Fecha_de_retoma_antes_de_contabilizacion,
                    ID_ejecucion,
                    ID_registro,
                    Tipo_de_documento,
                    Orden_de_Compra,
                    Clase_de_pedido,
                    NIT,
                    Nombre_Proveedor,
                    Factura,
                    Item,
                    Valor_XML,
                    Valor_Orden_de_Compra,
                    Valor_Orden_de_Compra_Comercializados,
                    Aprobado,
                    Estado_validacion_antes_de_eventos,
                    Fecha_de_retoma_contabilizacion,
                    Estado_contabilizacion,
                    Fecha_de_retoma_compensacion,
                    Estado_compensacion
                )
                SELECT
                    @ExecutionDateTime,
                    CAST(dp.Fecha_de_retoma_antes_de_contabilizacion AS DATETIME2(3)),
                    NULL,
                    dp.ID,
                    dp.documenttype,
                    TRY_CONVERT(NVARCHAR(100), dp.numero_de_liquidacion_u_orden_de_compra),
                    NULL,
                    TRY_CONVERT(BIGINT, dp.nit_emisor_o_nit_del_proveedor),
                    TRY_CONVERT(NVARCHAR(300), dp.nombre_emisor),
                    TRY_CONVERT(NVARCHAR(100), dp.numero_de_factura),
                    N'Observaciones',
                    LEFT(TRY_CONVERT(NVARCHAR(4000), dp.ObservacionesFase_4), 3900),
                    NULL, NULL, NULL,
                    N'EXCLUIDO IMPORTACIONES',
                    NULL, NULL, NULL, NULL
                FROM [CxP].[DocumentsProcessing] dp
                INNER JOIN #ImpBatch i ON i.ID = dp.ID
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [dbo].[CxP.Comparativa] c
                    WHERE c.ID_registro = dp.ID AND c.Item = N'Observaciones'
                );

                UPDATE c
                   SET c.Estado_validacion_antes_de_eventos = N'EXCLUIDO IMPORTACIONES'
                FROM [dbo].[CxP.Comparativa] c
                INNER JOIN #ImpBatch i ON i.ID = c.ID_registro;

                SET @TotalComparativaEstado += @@ROWCOUNT;

                INSERT INTO #ImpAll(ID)
                SELECT i.ID
                FROM #ImpBatch i
                WHERE NOT EXISTS (SELECT 1 FROM #ImpAll a WHERE a.ID = i.ID);
            END

            IF EXISTS (SELECT 1 FROM #FleteBatch)
            BEGIN
                UPDATE dp
                   SET dp.EstadoFinalFase_4 = N'VALIDACIÓN DATOS DE FACTURACIÓN: Exitoso',
                       dp.ResultadoFinalAntesEventos = N'EXCLUIDO COSTO INDIRECTO FLETES',
                       dp.ObservacionesFase_4 = LEFT(
                            CASE
                                WHEN LEFT(LTRIM(RTRIM(ISNULL(dp.ObservacionesFase_4, N''))), LEN(@MsgFlete)) = @MsgFlete
                                    THEN LTRIM(RTRIM(ISNULL(dp.ObservacionesFase_4, N'')))
                                ELSE LTRIM(RTRIM(CONCAT(
                                        @MsgFlete,
                                        CASE
                                            WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N''
                                            ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4))
                                        END
                                    )))
                            END,
                            3900
                       )
                FROM [CxP].[DocumentsProcessing] dp
                INNER JOIN #FleteBatch f ON f.ID = dp.ID;

                SET @TotalFleteExcluidos += @@ROWCOUNT;

                UPDATE c
                   SET c.Valor_XML = LEFT(dp.ObservacionesFase_4, 3900)
                FROM [dbo].[CxP.Comparativa] c
                INNER JOIN #FleteBatch f ON f.ID = c.ID_registro
                INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = f.ID
                WHERE c.Item = N'Observaciones';

                SET @TotalComparativaObs += @@ROWCOUNT;

                INSERT INTO [dbo].[CxP.Comparativa]
                (
                    Fecha_de_ejecucion,
                    Fecha_de_retoma_antes_de_contabilizacion,
                    ID_ejecucion,
                    ID_registro,
                    Tipo_de_documento,
                    Orden_de_Compra,
                    Clase_de_pedido,
                    NIT,
                    Nombre_Proveedor,
                    Factura,
                    Item,
                    Valor_XML,
                    Valor_Orden_de_Compra,
                    Valor_Orden_de_Compra_Comercializados,
                    Aprobado,
                    Estado_validacion_antes_de_eventos,
                    Fecha_de_retoma_contabilizacion,
                    Estado_contabilizacion,
                    Fecha_de_retoma_compensacion,
                    Estado_compensacion
                )
                SELECT
                    @ExecutionDateTime,
                    CAST(dp.Fecha_de_retoma_antes_de_contabilizacion AS DATETIME2(3)),
                    NULL,
                    dp.ID,
                    dp.documenttype,
                    TRY_CONVERT(NVARCHAR(100), dp.numero_de_liquidacion_u_orden_de_compra),
                    NULL,
                    TRY_CONVERT(BIGINT, dp.nit_emisor_o_nit_del_proveedor),
                    TRY_CONVERT(NVARCHAR(300), dp.nombre_emisor),
                    TRY_CONVERT(NVARCHAR(100), dp.numero_de_factura),
                    N'Observaciones',
                    LEFT(TRY_CONVERT(NVARCHAR(4000), dp.ObservacionesFase_4), 3900),
                    NULL, NULL, NULL,
                    N'EXCLUIDO COSTO INDIRECTO FLETES',
                    NULL, NULL, NULL, NULL
                FROM [CxP].[DocumentsProcessing] dp
                INNER JOIN #FleteBatch f ON f.ID = dp.ID
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [dbo].[CxP.Comparativa] c
                    WHERE c.ID_registro = dp.ID AND c.Item = N'Observaciones'
                );

                UPDATE c
                   SET c.Estado_validacion_antes_de_eventos = N'EXCLUIDO COSTO INDIRECTO FLETES'
                FROM [dbo].[CxP.Comparativa] c
                INNER JOIN #FleteBatch f ON f.ID = c.ID_registro;

                SET @TotalComparativaEstado += @@ROWCOUNT;

                INSERT INTO #FleteAll(ID)
                SELECT f.ID
                FROM #FleteBatch f
                WHERE NOT EXISTS (SELECT 1 FROM #FleteAll a WHERE a.ID = f.ID);
            END

            COMMIT;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK;

            DECLARE @ErrMsg NVARCHAR(4000) = CONCAT(
                'Fallo en SP [CxP].[HU4_FG_OrdenDeCompra]. ',
                'Error ', ERROR_NUMBER(), ', Severity ', ERROR_SEVERITY(),
                ', State ', ERROR_STATE(), ', Linea ', ERROR_LINE(),
                ': ', ERROR_MESSAGE()
            );

            SELECT
                CAST(@ExecutionDateTimeOffset AS DATETIME2(3)) AS FechaEjecucion,
                @DiasMaximos AS DiasMaximos,
                @BatchSize   AS BatchSize,
                0            AS RegistrosReporteNovedades,
                @ErrMsg      AS Error;

            THROW 52099, @ErrMsg, 1;
        END CATCH
    END;

    INSERT INTO [CxP].[ReporteNovedades]
    (
        ID,
        Fecha_Carga,
        Nit,
        Nombre_Proveedor,
        Orden_de_compra,
        Numero_factura,
        Estado_CXP_Bot,
        Observaciones,
        SP_Origen
    )
    SELECT
        CAST(dp.ID AS VARCHAR(50)),
        CONVERT(VARCHAR(20), CAST(@ExecutionDateTime AS DATE), 120),
        ISNULL(CAST(dp.nit_emisor_o_nit_del_proveedor AS VARCHAR(50)), ''),
        ISNULL(SUBSTRING(dp.nombre_emisor, 1, 500), ''),
        ISNULL(SUBSTRING(dp.numero_de_liquidacion_u_orden_de_compra, 1, 100), ''),
        ISNULL(SUBSTRING(dp.numero_de_factura, 1, 100), ''),
        ISNULL(SUBSTRING(dp.ResultadoFinalAntesEventos, 1, 200), ''),
        ISNULL(dp.ObservacionesFase_4, ''),
        'HU4_FG_OrdenDeCompra'
    FROM [CxP].[DocumentsProcessing] dp
    INNER JOIN #ProcessedIDs p ON p.ID = dp.ID
    WHERE dp.ResultadoFinalAntesEventos IN (
        N'EXCLUIDO IMPORTACIONES',
        N'EXCLUIDO COSTO INDIRECTO FLETES'
    );

    SET @TotalRegistrosReporte = @@ROWCOUNT;

    SELECT
        CAST(@ExecutionDateTimeOffset AS DATETIME2(3)) AS FechaEjecucion,
        @DiasMaximos AS DiasMaximos,
        @BatchSize   AS BatchSize,
        (SELECT COUNT(1) FROM #ProcessedIDs) AS RegistrosProcesados,
        @TotalRetomaSetNull                 AS RetomaSetDesdeNull,
        @TotalImpExcluidos                  AS ExcluidosImportaciones,
        @TotalFleteExcluidos                AS ExcluidosCostoIndirectoFletes,
        @TotalComparativaObs                AS ComparativaObservacionesActualizadas,
        @TotalComparativaEstado             AS ComparativaEstadosActualizados,
        @TotalRegistrosReporte              AS RegistrosReporteNovedades;

    SELECT
        dp.ID,
        dp.numero_de_factura,
        dp.nit_emisor_o_nit_del_proveedor,
        dp.documenttype,
        dp.numero_de_liquidacion_u_orden_de_compra,
        dp.Fecha_de_retoma_antes_de_contabilizacion,
        DATEDIFF(DAY, CAST(dp.Fecha_de_retoma_antes_de_contabilizacion AS DATETIME2(3)), @ExecutionDateTime) AS DiasTranscurridosDesdeRetoma,
        dp.ResultadoFinalAntesEventos,
        dp.EstadoFinalFase_4,
        dp.ObservacionesFase_4
    FROM [CxP].[DocumentsProcessing] dp
    INNER JOIN #ProcessedIDs p ON p.ID = dp.ID
    ORDER BY dp.ID;
END

USE [NotificationsPaddy]
GO
/****** Object:  StoredProcedure [CxP].[HU4_D_Validacion_NITs]    Script Date: 01/02/2026 4:39:27 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [CxP].[HU4_D_Validacion_NITs]
(
    @ListaNits NVARCHAR(MAX)
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Exito   BIT = 0;
    DECLARE @Resumen NVARCHAR(4000) = N'';
    DECLARE @FechaEjecucion DATETIME2(3) = SYSDATETIME();

    BEGIN TRY

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

        IF @ListaNits IS NULL OR LTRIM(RTRIM(@ListaNits)) = N''
        BEGIN
            SET @Exito = 1;
            SET @Resumen = N'No se recibieron NITs para validar.';
            SELECT @Exito AS exito, @Resumen AS resumen, 0 AS RegistrosReporteNovedades;
            RETURN;
        END;

        DECLARE @Nits TABLE (
            NIT_NORM NVARCHAR(100) NOT NULL PRIMARY KEY
        );

        INSERT INTO @Nits(NIT_NORM)
        SELECT DISTINCT
            REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(value)), N' ', N''), N'.', N''), N'-', N'') AS NIT_NORM
        FROM STRING_SPLIT(@ListaNits, ',')
        WHERE LTRIM(RTRIM(value)) <> N'';

        DECLARE @NitsUnicos INT = (SELECT COUNT(*) FROM @Nits);
        IF @NitsUnicos = 0
        BEGIN
            SET @Exito = 1;
            SET @Resumen = N'No se pudieron extraer NITs válidos desde la lista.';
            SELECT @Exito AS exito, @Resumen AS resumen, 0 AS RegistrosReporteNovedades;
            RETURN;
        END;

        UPDATE c
        SET c.Estado_validacion_antes_de_eventos = dp.ResultadoFinalAntesEventos
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN [CxP].[DocumentsProcessing] dp
            ON dp.ID = c.ID_registro
        INNER JOIN @Nits n
            ON n.NIT_NORM = REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(dp.nit_emisor_o_nit_del_proveedor)), N' ', N''), N'.', N''), N'-', N'')
        WHERE dp.ResultadoFinalAntesEventos IN (N'RECHAZADO', N'RECHAZADO - PENDIENTE')
          AND ISNULL(LTRIM(RTRIM(c.Estado_validacion_antes_de_eventos)), N'') = N'';

        DECLARE @ComparativaMarcada INT = @@ROWCOUNT;

        DECLARE @Candidatos INT;

        SELECT @Candidatos = COUNT_BIG(*)
        FROM [CxP].[DocumentsProcessing] dp
        INNER JOIN @Nits n
            ON n.NIT_NORM = REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(dp.nit_emisor_o_nit_del_proveedor)), N' ', N''), N'.', N''), N'-', N'')
        WHERE dp.ResultadoFinalAntesEventos IN (N'RECHAZADO', N'RECHAZADO - PENDIENTE');

        DECLARE @Updated TABLE (ID_registro INT NOT NULL PRIMARY KEY);

        UPDATE dp
        SET
            dp.ObservacionesFase_4 = LEFT(
                LTRIM(RTRIM(CONCAT(
                    N'Nit no aplica RECHAZO por campos mandatorios',
                    CASE
                        WHEN ISNULL(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') = N'' THEN N''
                        ELSE N', ' + dp.ObservacionesFase_4
                    END
                ))),
                3900
            ),
            dp.ResultadoFinalAntesEventos = N'CON NOVEDAD'
        OUTPUT inserted.ID INTO @Updated(ID_registro)
        FROM [CxP].[DocumentsProcessing] dp
        INNER JOIN @Nits n
            ON n.NIT_NORM = REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(dp.nit_emisor_o_nit_del_proveedor)), N' ', N''), N'.', N''), N'-', N'')
        WHERE dp.ResultadoFinalAntesEventos IN (N'RECHAZADO', N'RECHAZADO - PENDIENTE');

        DECLARE @RegistrosActualizados INT = (SELECT COUNT(*) FROM @Updated);

        UPDATE c
        SET
            c.Valor_XML = LEFT(dp.ObservacionesFase_4, 3900),
            c.Estado_validacion_antes_de_eventos = dp.ResultadoFinalAntesEventos
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN [CxP].[DocumentsProcessing] dp
            ON dp.ID = c.ID_registro
        INNER JOIN @Updated u
            ON u.ID_registro = dp.ID
        WHERE c.Item = N'Observaciones';

        DECLARE @ComparativaObservaciones INT = @@ROWCOUNT;

        DECLARE @RegistrosReporteNovedades INT = 0;

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
            CONVERT(VARCHAR(20), CAST(@FechaEjecucion AS DATE), 120),
            ISNULL(CAST(dp.nit_emisor_o_nit_del_proveedor AS VARCHAR(50)), ''),
            ISNULL(SUBSTRING(dp.nombre_emisor, 1, 500), ''),
            ISNULL(SUBSTRING(dp.numero_de_liquidacion_u_orden_de_compra, 1, 100), ''),
            ISNULL(SUBSTRING(dp.numero_de_factura, 1, 100), ''),
            ISNULL(SUBSTRING(dp.ResultadoFinalAntesEventos, 1, 200), ''),
            ISNULL(dp.ObservacionesFase_4, ''),
            'HU4_D_Validacion_NITs'
        FROM [CxP].[DocumentsProcessing] dp
        INNER JOIN @Updated u ON u.ID_registro = dp.ID;

        SET @RegistrosReporteNovedades = @@ROWCOUNT;

        SET @Exito = 1;
        SET @Resumen =
            N'Validacion de NITs finalizada. '
            + N'NITs unicos: ' + CAST(@NitsUnicos AS NVARCHAR(10))
            + N'. Candidatos: ' + CAST(ISNULL(@Candidatos, 0) AS NVARCHAR(20))
            + N'. Registros corregidos: ' + CAST(@RegistrosActualizados AS NVARCHAR(10))
            + N'. Comparativa marcada (rechazo): ' + CAST(@ComparativaMarcada AS NVARCHAR(10))
            + N'. Comparativa Observaciones actualizada: ' + CAST(@ComparativaObservaciones AS NVARCHAR(10))
            + N'. Registros en Reporte Novedades: ' + CAST(@RegistrosReporteNovedades AS NVARCHAR(10));

    END TRY
    BEGIN CATCH
        SET @Exito = 0;
        SET @Resumen = N'Ejecucion con error: ' + ERROR_MESSAGE();
        SET @RegistrosReporteNovedades = 0;
    END CATCH;

    SELECT 
        @Exito AS exito, 
        @Resumen AS resumen,
        ISNULL(@RegistrosReporteNovedades, 0) AS RegistrosReporteNovedades;
END

USE [NotificationsPaddy]
GO
/****** Object:  StoredProcedure [CxP].[HU4_ABCD_CamposObligatorios]    Script Date: 01/02/2026 4:33:03 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [CxP].[HU4_ABCD_CamposObligatorios]
    @DiasMaximos INT = 120,
    @BatchSize   INT = 500
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ExecutionDateTime DATETIMEOFFSET(3) = SYSDATETIMEOFFSET();
    DECLARE @DiasMaximosPlus30 INT = @DiasMaximos + 30;

    IF @DiasMaximos IS NULL OR @DiasMaximos <= 0
        THROW 50000, 'Parametro invalido: @DiasMaximos debe ser > 0.', 1;

    IF @BatchSize IS NULL OR @BatchSize <= 0
        THROW 50001, 'Parametro invalido: @BatchSize debe ser > 0.', 1;

    IF OBJECT_ID(N'[CxP].[DocumentsProcessing]', N'U') IS NULL
        THROW 50002, 'No existe la tabla requerida: [CxP].[DocumentsProcessing].', 1;

    IF OBJECT_ID(N'[dbo].[CxP.Comparativa]', N'U') IS NULL
        THROW 50003, 'No existe la tabla requerida: [dbo].[CxP.Comparativa].', 1;

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
    
    BEGIN TRY
        TRUNCATE TABLE [CxP].[ReporteNovedades];
    END TRY
    BEGIN CATCH
        DELETE FROM [CxP].[ReporteNovedades];
    END CATCH

    DECLARE @HasCalculationRate BIT =
        CASE WHEN COL_LENGTH(N'CxP.DocumentsProcessing', N'CalculationRate') IS NOT NULL THEN 1 ELSE 0 END;

    DECLARE @ExecDate DATE = CAST(@ExecutionDateTime AS DATE);
    DECLARE @CutoffPlus30 DATETIME2(3) = DATEADD(DAY, -@DiasMaximosPlus30, CAST(@ExecDate AS DATETIME2(3)));
    DECLARE @Cutoff       DATETIME2(3) = DATEADD(DAY, -@DiasMaximos,      CAST(@ExecDate AS DATETIME2(3)));

    DECLARE @EstadosOmitir TABLE (Estado NVARCHAR(200) PRIMARY KEY);
    INSERT INTO @EstadosOmitir (Estado) VALUES
        (N'APROBADO'),
        (N'APROBADO CONTADO Y/O EVENTO MANUAL'),
        (N'APROBADO SIN CONTABILIZACION'),
        (N'RECHAZADO'),
        (N'RECLASIFICAR'),
        (N'RECHAZADO - RETORNADO'),
        (N'CON NOVEDAD - RETORNADO'),
        (N'EN ESPERA DE POSICIONES'),
        (N'NO EXITOSO');

    BEGIN TRY
        TRUNCATE TABLE [dbo].[CxP.Comparativa];
    END TRY
    BEGIN CATCH
        BEGIN TRY
            DELETE FROM [dbo].[CxP.Comparativa];
        END TRY
        BEGIN CATCH
            DECLARE @m1 NVARCHAR(4000) = CONCAT(
                'Error limpiando [dbo].[CxP.Comparativa]. Error ', ERROR_NUMBER(),
                ', Linea ', ERROR_LINE(), ': ', ERROR_MESSAGE()
            );
            THROW 50004, @m1, 1;
        END CATCH
    END CATCH;

    DECLARE @Items TABLE (
        SortOrder INT NOT NULL PRIMARY KEY,
        Item NVARCHAR(200) NOT NULL UNIQUE
    );

    INSERT INTO @Items (SortOrder, Item) VALUES
    (  1,N'AttachedDocument'),
    (  2,N'UBLVersion'),
    (  3,N'ProfileExecutionID'),
    (  4,N'ParentDocumentID'),
    (  5,N'NombreEmisor'),
    (  6,N'NITEmisor'),
    (  7,N'TipoPersonaEmisor'),
    (  8,N'DigitoVerificacionEmisor'),
    (  9,N'TaxLevelCodeEmisor'),
    ( 10,N'NombreReceptor'),
    ( 11,N'NitReceptor'),
    ( 12,N'TipoPersonaReceptor'),
    ( 13,N'DigitoVerificacionReceptor'),
    ( 14,N'TaxLevelCodeReceptor'),
    ( 15,N'FechaEmisionDocumento'),
    ( 16,N'ValidationResultCode'),
    ( 17,N'InvoiceTypecode'),
    ( 18,N'ResponseCode'),
    ( 19,N'DescripcionCodigo'),
    ( 20,N'LineExtensionAmount'),
    ( 21,N'CufeUUID'),
    ( 22,N'DocumentType'),
    ( 23,N'NumeroLineas'),
    ( 24,N'MetodoPago'),
    ( 25,N'ValidationDate'),
    ( 26,N'PaymentDueDate'),
    ( 27,N'CondicionPago'),
    ( 28,N'DocumentCurrencyCode'),
    ( 29,N'CalculationRate'),
    ( 30,N'VlrPagarCop'),
    ( 31,N'PaymentMeans'),
    ( 32,N'NotaCreditoReferenciada'),
    ( 33,N'Posicion'),
    ( 34,N'ValorPorCalcularPosicion'),
    ( 35,N'ValorPorCalcularMEPosicion'),
    ( 36,N'TRM'),
    ( 37,N'PrecioUnitarioProducto'),
    ( 38,N'CantidadProducto'),
    ( 39,N'ValorPorCalcularSAP'),
    ( 40,N'TipoNIF'),
    ( 41,N'Acreedor'),
    ( 42,N'FecDoc'),
    ( 43,N'FecReg'),
    ( 44,N'FechaContGasto'),
    ( 45,N'IndicadorImpuestos'),
    ( 46,N'TextoBreve'),
    ( 47,N'ClaseImpuesto'),
    ( 48,N'Cuenta'),
    ( 49,N'CiudadProveedor'),
    ( 50,N'PoblacionServicio'),
    ( 51,N'DocFIEntrada'),
    ( 52,N'CTA26'),
    ( 53,N'ActivoFijo'),
    ( 54,N'CapitalizadoEl'),
    ( 55,N'CriterioClasif2'),
    ( 56,N'Orden'),
    ( 57,N'CentroCoste'),
    ( 58,N'ClaseOrden'),
    ( 59,N'ElementoPEP'),
    ( 60,N'Emplazamiento'),
    ( 61,N'FechaContabilizacionFE'),
    ( 62,N'DocumentoContable'),
    ( 63,N'FechaHoraEventoAcuseRecibo'),
    ( 64,N'EstadoEventoAcuseRecibo'),
    ( 65,N'FechaHoraEventoReciboBienServicio'),
    ( 66,N'EstadoEventoReciboBienServicio'),
    ( 67,N'FechaHoraEventoAceptacionExpresa'),
    ( 68,N'EstadoEventoAceptacionExpresa'),
    ( 69,N'FechaHoraEventoReclamoFactura'),
    ( 70,N'EstadoEventoReclamoFactura'),
    ( 71,N'ActualizacionNombreArchivos'),
    ( 72,N'RutaRespaldo'),
    ( 73,N'InsumoXML'),
    ( 74,N'InsumoPDF'),
    ( 75,N'Observaciones');

    DECLARE
        @TotalRegistrosProcesados            INT = 0,
        @TotalRetomaSetDesdeNull             INT = 0,
        @TotalMarcadosNoExitoso              INT = 0,
        @TotalMarcadosRechazado              INT = 0,
        @TotalOKDentroDiasMaximos            INT = 0,
        @TotalFilasInsertadasComparativa     INT = 0,
        @TotalRegistrosReporteNovedades      INT = 0;

    IF OBJECT_ID('tempdb..#ProcessedIDs') IS NOT NULL DROP TABLE #ProcessedIDs;
    CREATE TABLE #ProcessedIDs (ID BIGINT NOT NULL PRIMARY KEY);

    WHILE 1 = 1
    BEGIN
        IF OBJECT_ID('tempdb..#Batch') IS NOT NULL DROP TABLE #Batch;
        CREATE TABLE #Batch (ID BIGINT NOT NULL PRIMARY KEY);

        INSERT INTO #Batch (ID)
        SELECT TOP (@BatchSize) dp.ID
        FROM [CxP].[DocumentsProcessing] dp WITH (READPAST)
        LEFT JOIN #ProcessedIDs p ON p.ID = dp.ID
        WHERE p.ID IS NULL
          AND dp.documenttype = N'FV'
          AND (
                dp.ResultadoFinalAntesEventos IS NULL
                OR NOT EXISTS (SELECT 1 FROM @EstadosOmitir e WHERE e.Estado = dp.ResultadoFinalAntesEventos)
              )
          AND (
                dp.Fecha_de_retoma_antes_de_contabilizacion IS NULL
                OR dp.Fecha_de_retoma_antes_de_contabilizacion >= @CutoffPlus30
              )
        ORDER BY dp.ID
        OPTION (RECOMPILE);

        IF @@ROWCOUNT = 0 BREAK;

        INSERT INTO #ProcessedIDs (ID)
        SELECT ID FROM #Batch;

        SET @TotalRegistrosProcesados += (SELECT COUNT(1) FROM #Batch);

        BEGIN TRY
            BEGIN TRAN;

            UPDATE dp
               SET dp.Fecha_de_retoma_antes_de_contabilizacion = @ExecutionDateTime
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Batch b ON b.ID = dp.ID
            WHERE dp.Fecha_de_retoma_antes_de_contabilizacion IS NULL;

            SET @TotalRetomaSetDesdeNull += @@ROWCOUNT;

            INSERT INTO [dbo].[CxP.Comparativa] WITH (TABLOCK)
            (
                [Fecha_de_ejecucion],
                [Fecha_de_retoma_antes_de_contabilizacion],
                [ID_ejecucion],
                [ID_registro],
                [Tipo_de_documento],
                [Orden_de_Compra],
                [Clase_de_pedido],
                [NIT],
                [Nombre_Proveedor],
                [Factura],
                [Item],
                [Valor_XML],
                [Valor_Orden_de_Compra],
                [Valor_Orden_de_Compra_Comercializados],
                [Aprobado],
                [Estado_validacion_antes_de_eventos],
                [Fecha_de_retoma_contabilizacion],
                [Estado_contabilizacion],
                [Fecha_de_retoma_compensacion],
                [Estado_compensacion]
            )
            SELECT
                @ExecutionDateTime,
                dp.Fecha_de_retoma_antes_de_contabilizacion,
                NULL,
                dp.ID,
                dp.documenttype,
                dp.numero_de_liquidacion_u_orden_de_compra,
                NULL,
                dp.nit_emisor_o_nit_del_proveedor,
                dp.nombre_emisor,
                dp.numero_de_factura,
                it.Item,
                CASE it.Item
                    WHEN N'AttachedDocument'               THEN TRY_CONVERT(NVARCHAR(4000), dp.attached_document)
                    WHEN N'UBLVersion'                     THEN TRY_CONVERT(NVARCHAR(4000), dp.ubl_version)
                    WHEN N'ProfileExecutionID'             THEN TRY_CONVERT(NVARCHAR(4000), dp.ambiente_de_ejecucion_id)
                    WHEN N'ParentDocumentID'               THEN NULL
                    WHEN N'NombreEmisor'                   THEN TRY_CONVERT(NVARCHAR(4000), dp.nombre_emisor)
                    WHEN N'NITEmisor'                      THEN TRY_CONVERT(NVARCHAR(4000), dp.nit_emisor_o_nit_del_proveedor)
                    WHEN N'TipoPersonaEmisor'              THEN TRY_CONVERT(NVARCHAR(4000), dp.Tipo_Persona_Emisor)
                    WHEN N'DigitoVerificacionEmisor'       THEN TRY_CONVERT(NVARCHAR(4000), dp.Digito_de_verificacion_Emisor)
                    WHEN N'TaxLevelCodeEmisor'             THEN TRY_CONVERT(NVARCHAR(4000), dp.responsabilidad_tributaria_emisor)
                    WHEN N'NombreReceptor'                 THEN TRY_CONVERT(NVARCHAR(4000), dp.nombre_del_adquiriente)
                    WHEN N'NitReceptor'                    THEN TRY_CONVERT(NVARCHAR(4000), dp.nit_del_adquiriente)
                    WHEN N'TipoPersonaReceptor'            THEN TRY_CONVERT(NVARCHAR(4000), dp.tipo_persona)
                    WHEN N'DigitoVerificacionReceptor'     THEN TRY_CONVERT(NVARCHAR(4000), dp.digito_de_verificacion)
                    WHEN N'TaxLevelCodeReceptor'           THEN TRY_CONVERT(NVARCHAR(4000), dp.responsabilidad_tributaria_adquiriente)
                    WHEN N'FechaEmisionDocumento'          THEN CASE WHEN dp.fecha_de_emision_documento IS NULL THEN NULL
                                                                     ELSE CONVERT(NVARCHAR(33), dp.fecha_de_emision_documento, 126) END
                    WHEN N'ValidationResultCode'           THEN TRY_CONVERT(NVARCHAR(4000), dp.validationresultcode)
                    WHEN N'InvoiceTypecode'                THEN TRY_CONVERT(NVARCHAR(4000), dp.codigo_tipo_de_documento)
                    WHEN N'ResponseCode'                   THEN TRY_CONVERT(NVARCHAR(4000), dp.codigo_de_uso_autorizado_por_la_dian)
                    WHEN N'DescripcionCodigo'              THEN TRY_CONVERT(NVARCHAR(4000), dp.descripcion_del_codigo)
                    WHEN N'LineExtensionAmount'            THEN TRY_CONVERT(NVARCHAR(4000), dp.valor_a_pagar)
                    WHEN N'CufeUUID'                       THEN TRY_CONVERT(NVARCHAR(4000), dp.cufeuuid)
                    WHEN N'DocumentType'                   THEN TRY_CONVERT(NVARCHAR(4000), dp.documenttype)
                    WHEN N'NumeroLineas'                   THEN NULL
                    WHEN N'MetodoPago'                     THEN TRY_CONVERT(NVARCHAR(4000), dp.medio_de_pago)
                    WHEN N'ValidationDate'                 THEN CASE WHEN dp.fechaValidacion IS NULL THEN NULL
                                                                     ELSE CONVERT(NVARCHAR(33), dp.fechaValidacion, 126) END
                    WHEN N'PaymentDueDate'                 THEN CASE WHEN dp.fecha_de_validacion_forma_de_pago IS NULL THEN NULL
                                                                     ELSE CONVERT(NVARCHAR(33), dp.fecha_de_validacion_forma_de_pago, 126) END
                    WHEN N'CondicionPago'                  THEN TRY_CONVERT(NVARCHAR(4000), dp.forma_de_pago)
                    WHEN N'CalculationRate'                THEN NULL
                    WHEN N'ActualizacionNombreArchivos'    THEN TRY_CONVERT(NVARCHAR(4000), dp.actualizacionNombreArchivos)
                    WHEN N'RutaRespaldo'                   THEN TRY_CONVERT(NVARCHAR(4000), dp.RutaArchivo)
                    WHEN N'InsumoXML'                      THEN NULL
                    WHEN N'InsumoPDF'                      THEN NULL
                    WHEN N'Observaciones'                  THEN NULL
                    ELSE NULL
                END,
                CASE it.Item
                    WHEN N'AttachedDocument'           THEN N'AttachedDocument'
                    WHEN N'UBLVersion'                 THEN N'UBL 2.1'
                    WHEN N'ProfileExecutionID'         THEN N'1'
                    WHEN N'NitReceptor'                THEN N'860031606'
                    WHEN N'TipoPersonaReceptor'        THEN N'31'
                    WHEN N'DigitoVerificacionReceptor' THEN N'6'
                    ELSE NULL
                END,
                NULL,
                CASE
                    WHEN dp.Fecha_de_retoma_antes_de_contabilizacion < @Cutoff THEN NULL
                    ELSE
                        CASE it.Item
                            WHEN N'UBLVersion'                 THEN CASE WHEN NULLIF(LTRIM(RTRIM(dp.ubl_version)), N'') IN (N'UBL 2.1', N'DIAN 2.1') THEN N'SI' ELSE N'NO' END
                            WHEN N'ProfileExecutionID'         THEN CASE WHEN dp.ambiente_de_ejecucion_id = 1 THEN N'SI' ELSE N'NO' END
                            WHEN N'ParentDocumentID'           THEN CASE WHEN NULLIF(LTRIM(RTRIM(dp.numero_de_factura)), N'') IS NOT NULL THEN N'SI' ELSE N'NO' END
                            WHEN N'NombreEmisor'               THEN CASE WHEN NULLIF(LTRIM(RTRIM(dp.nombre_emisor)), N'') IS NOT NULL THEN N'SI' ELSE N'NO' END
                            WHEN N'NITEmisor'                  THEN CASE WHEN NULLIF(LTRIM(RTRIM(dp.nit_emisor_o_nit_del_proveedor)), N'') IS NOT NULL THEN N'SI' ELSE N'NO' END
                            WHEN N'TipoPersonaEmisor'          THEN CASE WHEN dp.Tipo_Persona_Emisor IN (13, 31) THEN N'SI' ELSE N'NO' END
                            WHEN N'DigitoVerificacionEmisor'   THEN CASE WHEN dp.Digito_de_verificacion_Emisor IS NOT NULL THEN N'SI' ELSE N'NO' END
                            WHEN N'TipoPersonaReceptor'        THEN CASE WHEN dp.tipo_persona IN (13,31) THEN N'SI' ELSE N'NO' END
                            WHEN N'DigitoVerificacionReceptor' THEN CASE WHEN dp.digito_de_verificacion = 6 THEN N'SI' ELSE N'NO' END
                            WHEN N'NitReceptor'                THEN CASE WHEN dp.nit_del_adquiriente = 860031606 THEN N'SI' ELSE N'NO' END
                            WHEN N'FechaEmisionDocumento'      THEN CASE WHEN dp.fecha_de_emision_documento IS NOT NULL THEN N'SI' ELSE N'NO' END
                            ELSE NULL
                        END
                END,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Batch b ON b.ID = dp.ID
            CROSS JOIN @Items it;

            SET @TotalFilasInsertadasComparativa += @@ROWCOUNT;

            IF @HasCalculationRate = 1
            BEGIN
                DECLARE @sqlCalc NVARCHAR(MAX) = N'
                    UPDATE c
                       SET c.Valor_XML = TRY_CONVERT(NVARCHAR(4000), dp.CalculationRate)
                    FROM [dbo].[CxP.Comparativa] c
                    INNER JOIN #Batch b ON b.ID = c.ID_registro
                    INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = b.ID
                    WHERE c.Item = N''CalculationRate'';
                ';
                EXEC sys.sp_executesql @sqlCalc;
            END

            IF OBJECT_ID('tempdb..#Expired') IS NOT NULL DROP TABLE #Expired;
            CREATE TABLE #Expired (ID BIGINT NOT NULL PRIMARY KEY);

            INSERT INTO #Expired (ID)
            SELECT dp.ID
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Batch b ON b.ID = dp.ID
            WHERE dp.Fecha_de_retoma_antes_de_contabilizacion < @Cutoff;

            IF EXISTS (SELECT 1 FROM #Expired)
            BEGIN
                UPDATE dp
                   SET dp.ObservacionesFase_4 = LEFT(
                        LTRIM(RTRIM(CONCAT(
                            N'Registro excede el plazo maximo de retoma',
                            CASE
                                WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N''
                                ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4))
                            END
                        ))),
                        3900
                       ),
                       dp.ResultadoFinalAntesEventos = N'NO EXITOSO',
                       dp.EstadoFinalFase_4 = N'VALIDACIÓN DATOS DE FACTURACIÓN: No exitoso.'
                FROM [CxP].[DocumentsProcessing] dp
                INNER JOIN #Expired e ON e.ID = dp.ID;

                SET @TotalMarcadosNoExitoso += @@ROWCOUNT;
            END

            IF OBJECT_ID('tempdb..#Eligible') IS NOT NULL DROP TABLE #Eligible;
            CREATE TABLE #Eligible (ID BIGINT NOT NULL PRIMARY KEY);

            INSERT INTO #Eligible (ID)
            SELECT dp.ID
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Batch b ON b.ID = dp.ID
            WHERE dp.documenttype = N'FV'
              AND dp.Fecha_de_retoma_antes_de_contabilizacion >= @Cutoff
              AND (
                    dp.ResultadoFinalAntesEventos IS NULL
                    OR NOT EXISTS (SELECT 1 FROM @EstadosOmitir e WHERE e.Estado = dp.ResultadoFinalAntesEventos)
                  );

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en UBL version.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE NULLIF(LTRIM(RTRIM(dp.ubl_version)), N'') NOT IN (N'UBL 2.1', N'DIAN 2.1');

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en ID Ambiente de ejecución.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE dp.ambiente_de_ejecucion_id <> 1 OR dp.ambiente_de_ejecucion_id IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Número de factura.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE NULLIF(LTRIM(RTRIM(dp.numero_de_factura)), N'') IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Nombre Emisor.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE NULLIF(LTRIM(RTRIM(dp.nombre_emisor)), N'') IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Nit emisor.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE NULLIF(LTRIM(RTRIM(dp.nit_emisor_o_nit_del_proveedor)), N'') IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Nit emisor.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE dp.Tipo_Persona_Emisor IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Tipo Persona Emisor.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE dp.Tipo_Persona_Emisor NOT IN (13, 31) OR dp.Tipo_Persona_Emisor IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Dígito de verificación Emisor.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE dp.Digito_de_verificacion_Emisor IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Tipo Persona Adquiriente.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE dp.tipo_persona NOT IN (13,31) OR dp.tipo_persona IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Dígito de verificación Adquiriente.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE dp.digito_de_verificacion <> 6 OR dp.digito_de_verificacion IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Nit del Adquiriente.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE dp.nit_del_adquiriente <> 860031606 OR dp.nit_del_adquiriente IS NULL;

            UPDATE dp
               SET dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'Se rechaza factura por inconsistencia y/o ausencia de dato en Fecha de emisión documento.',
                        CASE WHEN NULLIF(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') IS NULL THEN N'' ELSE N', ' + LTRIM(RTRIM(dp.ObservacionesFase_4)) END
                    ))),
                    3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'RECHAZADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE dp.fecha_de_emision_documento IS NULL;

            UPDATE c
               SET c.Estado_validacion_antes_de_eventos = dp.ResultadoFinalAntesEventos
            FROM [dbo].[CxP.Comparativa] c
            INNER JOIN #Batch b ON b.ID = c.ID_registro
            INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = b.ID
            WHERE dp.ResultadoFinalAntesEventos IS NOT NULL;

            UPDATE c
               SET c.Valor_XML = LEFT(dp.ObservacionesFase_4, 3900)
            FROM [dbo].[CxP.Comparativa] c
            INNER JOIN #Batch b ON b.ID = c.ID_registro
            INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = b.ID
            WHERE c.Item = N'Observaciones'
              AND dp.ObservacionesFase_4 IS NOT NULL;

            IF OBJECT_ID(N'[CxP].[DetailsProcessing]', N'U') IS NOT NULL
            BEGIN
                ;WITH Cnt AS
                (
                    SELECT
                        dp.ID,
                        COUNT(1) AS LineCount
                    FROM [CxP].[DocumentsProcessing] dp
                    INNER JOIN #Batch b ON b.ID = dp.ID
                    LEFT JOIN [CxP].[DetailsProcessing] d
                        ON d.numero_de_factura = dp.numero_de_factura
                       AND d.nit_emisor_o_nit_del_proveedor = dp.nit_emisor_o_nit_del_proveedor
                    GROUP BY dp.ID
                )
                UPDATE c
                   SET c.Valor_XML = TRY_CONVERT(NVARCHAR(50), cnt.LineCount)
                FROM [dbo].[CxP.Comparativa] c
                INNER JOIN Cnt cnt ON cnt.ID = c.ID_registro
                WHERE c.Item = N'NumeroLineas';
            END

            DECLARE @BatchRechazados INT = 0, @BatchElegibles INT = 0;

            SELECT @BatchElegibles = COUNT(1) FROM #Eligible;

            SELECT @BatchRechazados = COUNT(1)
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Eligible e ON e.ID = dp.ID
            WHERE dp.ResultadoFinalAntesEventos = N'RECHAZADO';

            SET @TotalMarcadosRechazado += @BatchRechazados;
            SET @TotalOKDentroDiasMaximos += (@BatchElegibles - @BatchRechazados);

            COMMIT;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK;

            DECLARE @ErrMsg NVARCHAR(4000) = CONCAT(
                'Fallo en SP [CxP].[HU4_ABCD_CamposObligatorios]. ',
                'Error ', ERROR_NUMBER(), ', Severity ', ERROR_SEVERITY(),
                ', State ', ERROR_STATE(), ', Linea ', ERROR_LINE(),
                ': ', ERROR_MESSAGE()
            );

            SELECT
                @ExecutionDateTime AS FechaEjecucion,
                @DiasMaximos       AS DiasMaximos,
                @BatchSize         AS BatchSize,
                @ErrMsg            AS Error;

            THROW 50010, @ErrMsg, 1;
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
        'HU4_ABCD_CamposObligatorios'
    FROM [CxP].[DocumentsProcessing] dp
    INNER JOIN #ProcessedIDs p ON p.ID = dp.ID
    WHERE dp.documenttype = N'FV'
      AND dp.ResultadoFinalAntesEventos IS NOT NULL
      AND LTRIM(RTRIM(dp.ResultadoFinalAntesEventos)) <> N'';
    
    SET @TotalRegistrosReporteNovedades = @@ROWCOUNT;

    SELECT
        @ExecutionDateTime                  AS FechaEjecucion,
        @DiasMaximos                        AS DiasMaximos,
        @BatchSize                          AS BatchSize,
        (SELECT COUNT(1) FROM #ProcessedIDs) AS RegistrosProcesados,
        @TotalRetomaSetDesdeNull            AS RetomaSetDesdeNull,
        @TotalMarcadosNoExitoso             AS MarcadosNoExitoso,
        @TotalMarcadosRechazado             AS MarcadosRechazado,
        @TotalOKDentroDiasMaximos           AS OKDentroDiasMaximos,
        @TotalFilasInsertadasComparativa    AS FilasInsertadasComparativa,
        @TotalRegistrosReporteNovedades     AS RegistrosReporteNovedades;

    SELECT
        dp.ID,
        dp.numero_de_factura,
        dp.nit_emisor_o_nit_del_proveedor,
        dp.documenttype,
        dp.Fecha_de_retoma_antes_de_contabilizacion,
        DATEDIFF(DAY, dp.Fecha_de_retoma_antes_de_contabilizacion, @ExecutionDateTime) AS DiasTranscurridosDesdeRetoma,
        dp.ResultadoFinalAntesEventos,
        dp.EstadoFinalFase_4,
        dp.ObservacionesFase_4
    FROM [CxP].[DocumentsProcessing] dp
    INNER JOIN #ProcessedIDs p ON p.ID = dp.ID
    ORDER BY dp.ID;
END

/*
================================================================================
STORED PROCEDURE: [CxP].[HU4_E_CamposReglamentarios]
================================================================================

Descripcion General:
--------------------
    Valida campos reglamentarios de documentos de facturacion electronica.
    Verifica responsabilidades tributarias, codigos de tipo de documento,
    descripcion del codigo DIAN, medios de pago, y ano de emision.
    
    Los documentos que no cumplen se marcan como CON NOVEDAD o 
    CON NOVEDAD ANO CERRADO.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Base de Datos: NotificationsPaddy
Schema: CxP

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |          [CxP].[HU4_E_CamposReglamentarios]                  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Crear tabla [CxP].[ReporteNovedades] si no existe          |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Parsear parametros de listas:                              |
    |  - @TaxLevelCode -> #TaxLevelCodeTable                      |
    |  - @InvoiceTypecode -> #InvoiceTypecodeTable                |
    |  - @EstadosOmitir -> #EstadosOmitirTable                    |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Cargar documentos elegibles en #Docs:                      |
    |  - Estado no en lista de omitir                             |
    |  - Fecha retoma dentro de @DiasMaximos                      |
    |  - documenttype = 'FV'                                      |
    +-----------------------------+-------------------------------+
                                  |
                  +---------------+---------------+
                  |    Hay documentos?            |
                  +---------------+---------------+
                         |                |
                         | NO             | SI
                         v                v
    +------------------------+   +--------------------------------+
    |  Retornar sin procesar |   |  Validar TaxLevelCode Emisor   |
    |  "SIN DOCUMENTOS"      |   +----------------+---------------+
    +------------------------+                    |
                                                  v
    +-------------------------------------------------------------+
    |  Validar TaxLevelCode Adquiriente                           |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Procesar en lotes (@BatchSize):                            |
    |  WHILE @MinRow <= @TotalDocs                                |
    |  +-------------------------------------------------------+  |
    |  |  Validar TaxLevelCode (Emisor y Adquiriente)          |  |
    |  +-------------------------------------------------------+  |
    |  |  Validar ValidationResultCode (02)                    |  |
    |  +-------------------------------------------------------+  |
    |  |  Validar InvoiceTypecode (01,02,03,04,91,92,96)       |  |
    |  +-------------------------------------------------------+  |
    |  |  Validar DescripcionCodigo (DIAN)                     |  |
    |  +-------------------------------------------------------+  |
    |  |  Validar PaymentMeans (01, 02)                        |  |
    |  +-------------------------------------------------------+  |
    |  |  Validar Ano de Emision (si mes <> enero)             |  |
    |  +-------------------------------------------------------+  |
    |  |  Actualizar Comparativa con Observaciones             |  |
    |  +-------------------------------------------------------+  |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Insertar en [CxP].[ReporteNovedades]                       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar ResultSet con resumen                             |
    +-------------------------------------------------------------+

================================================================================
PARAMETROS
================================================================================

    @DiasMaximos INT = 120
        Dias maximos desde la fecha de retoma.
        
    @BatchSize INT = 500
        Cantidad de documentos por lote.
        
    @RangoMaxValor INT = 500
        Rango maximo de valor (no utilizado actualmente).
        
    @TaxLevelCode NVARCHAR(MAX) = 'O-13,O-15,O-23,O-47,R-99-PN'
        Lista de codigos de responsabilidad tributaria validos.
        Separados por comas.
        
    @InvoiceTypecode NVARCHAR(MAX) = '01,02,03,04,91,92,96'
        Lista de tipos de factura validos.
        Separados por comas.
        
    @EstadosOmitir NVARCHAR(MAX) = 'APROBADO,APROBADO CONTADO Y/O...'
        Lista de estados a omitir del procesamiento.
        Separados por comas.

================================================================================
VALIDACIONES REALIZADAS
================================================================================

1. TaxLevelCode Emisor
   - El campo responsabilidad_tributaria_emisor debe contener
     al menos un codigo de la lista @TaxLevelCode
   - Se separa por punto y coma (;) para validar
   
2. TaxLevelCode Adquiriente
   - El campo responsabilidad_tributaria_adquiriente debe contener
     al menos un codigo de la lista @TaxLevelCode
   - Se separa por punto y coma (;) para validar
   
3. ValidationResultCode
   - Debe ser '02'
   - Mensaje: "ValidationResultCode diferente a 02"
   
4. InvoiceTypecode
   - Debe estar en la lista: 01, 02, 03, 04, 91, 92, 96
   - Mensaje: "InvoiceTypecode diferente a los valores esperados"
   
5. DescripcionCodigo
   - Debe ser exactamente "Documento validado por la DIAN"
   - Mensaje: "DescripcionCodigo diferente a..."
   
6. PaymentMeans
   - Debe ser 01 o 02
   - Mensaje: "PaymentMeans diferente a 01 o 02"
   
7. Ano de Emision (solo si mes actual <> enero)
   - El ano de fecha_de_emision_documento debe ser el ano actual
   - Mensaje: "Ano de Fecha de emision documento diferente..."
   - Estado: CON NOVEDAD ANO CERRADO

================================================================================
ESTADOS DE SALIDA
================================================================================

    CON NOVEDAD
        - Documento con problemas en campos reglamentarios
        
    CON NOVEDAD ANO CERRADO
        - Documento con ano de emision diferente al actual
        - Solo se aplica si el mes actual NO es enero

================================================================================
TABLAS UTILIZADAS
================================================================================

Tablas de Entrada:
------------------
    [CxP].[DocumentsProcessing]
        - responsabilidad_tributaria_emisor
        - responsabilidad_tributaria_adquiriente
        - validationresultcode
        - codigo_tipo_de_documento
        - descripcion_del_codigo
        - forma_de_pago
        - fecha_de_emision_documento

Tablas de Salida:
-----------------
    [dbo].[CxP.Comparativa]
        - Se actualizan Items con Valor_Orden_de_Compra y Aprobado
        
    [CxP].[ReporteNovedades]
        - Se insertan registros con novedades

================================================================================
RESULTSET DE SALIDA
================================================================================

ResultSet Unico:
----------------
    FechaEjecucion              DATETIME2   - Fecha y hora
    RegistrosProcesados         INT         - Total procesados
    RegistrosConNovedad         INT         - Con estado CON NOVEDAD
    RegistrosAnoCerrado         INT         - Con estado ANO CERRADO
    LotesProcesados             INT         - Cantidad de lotes
    TiempoTotalSegundos         INT         - Duracion
    RegistrosInsertadosReporte  INT         - En ReporteNovedades
    Estado                      VARCHAR     - COMPLETADO o SIN DOCUMENTOS

================================================================================
EJEMPLOS DE USO
================================================================================

-- Ejemplo 1: Ejecucion con valores por defecto
EXEC [CxP].[HU4_E_CamposReglamentarios];

-- Ejemplo 2: Parametros personalizados
EXEC [CxP].[HU4_E_CamposReglamentarios]
    @DiasMaximos = 90,
    @BatchSize = 1000,
    @TaxLevelCode = 'O-13,O-15,O-23';

-- Ejemplo 3: Modificar estados a omitir
EXEC [CxP].[HU4_E_CamposReglamentarios]
    @EstadosOmitir = 'APROBADO,RECHAZADO';

================================================================================
NOTAS TECNICAS
================================================================================

    - Usa STRING_SPLIT para parsear listas de parametros
    - Los TaxLevelCode se separan por punto y coma (;) en los datos
    - El chequeo de ano cerrado solo aplica si mes <> 1 (enero)
    - Las observaciones se concatenan con las existentes
    - Limite de 3900 caracteres en ObservacionesFase_4
    - Procesa en lotes para evitar bloqueos

================================================================================
*/

USE [NotificationsPaddy]
GO
/****** Object:  StoredProcedure [CxP].[HU4_E_CamposReglamentarios]    Script Date: 01/02/2026 4:39:58 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [CxP].[HU4_E_CamposReglamentarios]
(
    @DiasMaximos     INT = 120,
    @BatchSize       INT = 500,
    @RangoMaxValor   INT = 500,
    @TaxLevelCode    NVARCHAR(MAX) = N'O-13,O-15,O-23,O-47,R-99-PN',
    @InvoiceTypecode NVARCHAR(MAX) = N'01,02,03,04,91,92,96',
    @EstadosOmitir   NVARCHAR(MAX) = N'APROBADO,APROBADO CONTADO Y/O EVENTO MANUAL,APROBADO SIN CONTABILIZACION,RECHAZADO,RECLASIFICAR,RECHAZADO - RETORNADO,CON NOVEDAD - RETORNADO,EN ESPERA DE POSICIONES,NO EXITOSO'
)
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

    DECLARE @Now        DATETIME2(3) = SYSDATETIME();
    DECLARE @AnioActual INT = YEAR(@Now);
    DECLARE @MesActual  INT = MONTH(@Now);

    DECLARE @TotalDocs    INT = 0;
    DECLARE @CurrentBatch INT = 0;
    DECLARE @StartTime    DATETIME2(3) = SYSDATETIME();

    DECLARE @MinRow INT = 1;
    DECLARE @MaxRow INT;

    DECLARE @RegistrosProcesados INT = 0;
    DECLARE @RegistrosConNovedad INT = 0;
    DECLARE @RegistrosAnoCerrado INT = 0;

    IF OBJECT_ID('tempdb..#TaxLevelCodeTable') IS NOT NULL DROP TABLE #TaxLevelCodeTable;
    IF OBJECT_ID('tempdb..#InvoiceTypecodeTable') IS NOT NULL DROP TABLE #InvoiceTypecodeTable;
    IF OBJECT_ID('tempdb..#EstadosOmitirTable') IS NOT NULL DROP TABLE #EstadosOmitirTable;
    IF OBJECT_ID('tempdb..#Docs') IS NOT NULL DROP TABLE #Docs;
    IF OBJECT_ID('tempdb..#BatchIDs') IS NOT NULL DROP TABLE #BatchIDs;

    CREATE TABLE #TaxLevelCodeTable (Codigo NVARCHAR(50) PRIMARY KEY);
    CREATE TABLE #InvoiceTypecodeTable (Codigo NVARCHAR(50) PRIMARY KEY);
    CREATE TABLE #EstadosOmitirTable (Estado NVARCHAR(200) PRIMARY KEY);

    INSERT INTO #TaxLevelCodeTable (Codigo)
    SELECT DISTINCT LTRIM(RTRIM(value))
    FROM STRING_SPLIT(@TaxLevelCode, ',')
    WHERE LEN(LTRIM(RTRIM(value))) > 0;

    INSERT INTO #InvoiceTypecodeTable (Codigo)
    SELECT DISTINCT LTRIM(RTRIM(value))
    FROM STRING_SPLIT(@InvoiceTypecode, ',')
    WHERE LEN(LTRIM(RTRIM(value))) > 0;

    INSERT INTO #EstadosOmitirTable (Estado)
    SELECT DISTINCT LTRIM(RTRIM(value))
    FROM STRING_SPLIT(@EstadosOmitir, ',')
    WHERE LEN(LTRIM(RTRIM(value))) > 0;

    CREATE TABLE #Docs
    (
        RowNum INT IDENTITY(1,1) PRIMARY KEY,
        ID INT NOT NULL,

        OldResultadoFinalAntesEventos NVARCHAR(100) NULL,

        responsabilidad_tributaria_emisor      NVARCHAR(MAX) NULL,
        responsabilidad_tributaria_adquiriente NVARCHAR(MAX) NULL,
        validationresultcode                   NVARCHAR(50) NULL,
        codigo_de_uso_autorizado_por_la_dian   NVARCHAR(50) NULL,
        codigo_tipo_de_documento               NVARCHAR(50) NULL,
        descripcion_del_codigo                 NVARCHAR(MAX) NULL,
        fecha_de_emision_documento             DATETIME NULL,
        documenttype                           NVARCHAR(50) NULL,

        TieneCoincidenciaEmisor      BIT NOT NULL DEFAULT 0,
        TieneCoincidenciaAdquiriente BIT NOT NULL DEFAULT 0,

        INDEX IX_Docs_ID NONCLUSTERED (ID)
    );

    INSERT INTO #Docs
    (
        ID,
        OldResultadoFinalAntesEventos,
        responsabilidad_tributaria_emisor,
        responsabilidad_tributaria_adquiriente,
        validationresultcode,
        codigo_de_uso_autorizado_por_la_dian,
        codigo_tipo_de_documento,
        descripcion_del_codigo,
        fecha_de_emision_documento,
        documenttype
    )
    SELECT
        dp.ID,
        dp.ResultadoFinalAntesEventos,
        dp.responsabilidad_tributaria_emisor,
        dp.responsabilidad_tributaria_adquiriente,
        dp.validationresultcode,
        dp.codigo_de_uso_autorizado_por_la_dian,
        dp.codigo_tipo_de_documento,
        dp.descripcion_del_codigo,
        dp.fecha_de_emision_documento,
        dp.documenttype
    FROM [CxP].[DocumentsProcessing] dp
    WHERE ISNULL(dp.ResultadoFinalAntesEventos, N'') NOT IN (SELECT Estado FROM #EstadosOmitirTable)
      AND dp.Fecha_de_retoma_antes_de_contabilizacion IS NOT NULL
      AND DATEDIFF(DAY, dp.Fecha_de_retoma_antes_de_contabilizacion, @Now) <= @DiasMaximos
      AND UPPER(ISNULL(dp.documenttype, N'')) = N'FV';

    SET @TotalDocs = @@ROWCOUNT;

    IF @TotalDocs = 0
    BEGIN
        SELECT
            @Now AS FechaEjecucion,
            0 AS RegistrosProcesados,
            0 AS RegistrosConNovedad,
            0 AS RegistrosAnoCerrado,
            0 AS LotesProcesados,
            0 AS TiempoTotalSegundos,
            'SIN DOCUMENTOS PARA PROCESAR' AS Estado;
        RETURN;
    END;

    ;WITH EmisorOK AS
    (
        SELECT DISTINCT d.ID
        FROM #Docs d
        CROSS APPLY STRING_SPLIT(ISNULL(d.responsabilidad_tributaria_emisor, N''), ';') s
        WHERE LEN(LTRIM(RTRIM(s.value))) > 0
          AND LTRIM(RTRIM(s.value)) IN (SELECT Codigo FROM #TaxLevelCodeTable)
    )
    UPDATE d
       SET d.TieneCoincidenciaEmisor = 1
    FROM #Docs d
    INNER JOIN EmisorOK e ON e.ID = d.ID;

    ;WITH AdqOK AS
    (
        SELECT DISTINCT d.ID
        FROM #Docs d
        CROSS APPLY STRING_SPLIT(ISNULL(d.responsabilidad_tributaria_adquiriente, N''), ';') s
        WHERE LEN(LTRIM(RTRIM(s.value))) > 0
          AND LTRIM(RTRIM(s.value)) IN (SELECT Codigo FROM #TaxLevelCodeTable)
    )
    UPDATE d
       SET d.TieneCoincidenciaAdquiriente = 1
    FROM #Docs d
    INNER JOIN AdqOK a ON a.ID = d.ID;

    CREATE TABLE #BatchIDs (ID INT NOT NULL PRIMARY KEY);

    WHILE @MinRow <= @TotalDocs
    BEGIN
        SET @CurrentBatch = @CurrentBatch + 1;
        SET @MaxRow = @MinRow + @BatchSize - 1;

        TRUNCATE TABLE #BatchIDs;

        INSERT INTO #BatchIDs (ID)
        SELECT d.ID
        FROM #Docs d
        WHERE d.RowNum BETWEEN @MinRow AND @MaxRow;

        UPDATE c
           SET c.Valor_Orden_de_Compra = d.responsabilidad_tributaria_emisor,
               c.Aprobado = N'SI'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'TaxLevelCodeEmisor'
          AND d.TieneCoincidenciaEmisor = 1
          AND d.TieneCoincidenciaAdquiriente = 1;

        UPDATE c
           SET c.Valor_Orden_de_Compra = d.responsabilidad_tributaria_adquiriente,
               c.Aprobado = N'SI'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'TaxLevelCodeReceptor'
          AND d.TieneCoincidenciaEmisor = 1
          AND d.TieneCoincidenciaAdquiriente = 1;

        UPDATE dp
           SET dp.EstadoFinalFase_4 = N'VALIDACION DATOS DE FACTURACION: Exitoso.',
               dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'No se encuentra informacion en Taxlevelcode ' + 
                        CASE 
                            WHEN d.TieneCoincidenciaEmisor = 0 AND d.TieneCoincidenciaAdquiriente = 0 
                                THEN N'(Emisor y Receptor)'
                            WHEN d.TieneCoincidenciaEmisor = 0 
                                THEN N'(Emisor)'
                            ELSE N'(Receptor)'
                        END,
                        CASE WHEN ISNULL(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') <> N'' 
                             THEN N', ' + dp.ObservacionesFase_4 ELSE N'' END
                    ))),
                    3900
               ),
               dp.ResultadoFinalAntesEventos = N'CON NOVEDAD'
        FROM [CxP].[DocumentsProcessing] dp
        INNER JOIN #Docs d ON d.ID = dp.ID
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE (d.TieneCoincidenciaEmisor = 0 OR d.TieneCoincidenciaAdquiriente = 0);

        UPDATE c
           SET c.Valor_Orden_de_Compra = @TaxLevelCode,
               c.Aprobado = N'NO'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item IN (N'TaxLevelCodeEmisor', N'TaxLevelCodeReceptor')
          AND (d.TieneCoincidenciaEmisor = 0 OR d.TieneCoincidenciaAdquiriente = 0);

        UPDATE c
           SET c.Valor_Orden_de_Compra = N'02 - 002',
               c.Aprobado = N'SI'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'ValidationResultCode'
          AND LTRIM(RTRIM(ISNULL(d.validationresultcode, N''))) IN (N'02', N'002');

        UPDATE dp
           SET dp.EstadoFinalFase_4 = N'VALIDACION DATOS DE FACTURACION: Exitoso.',
               dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'ValidationResultCode diferente a 02 o 002',
                        CASE WHEN ISNULL(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') <> N'' 
                             THEN N', ' + dp.ObservacionesFase_4 ELSE N'' END
                    ))),
                    3900
               ),
               dp.ResultadoFinalAntesEventos = N'CON NOVEDAD'
        FROM [CxP].[DocumentsProcessing] dp
        INNER JOIN #Docs d ON d.ID = dp.ID
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE LTRIM(RTRIM(ISNULL(d.validationresultcode, N''))) NOT IN (N'02', N'002');

        UPDATE c
           SET c.Valor_Orden_de_Compra = N'02 - 002',
               c.Aprobado = N'NO'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'ValidationResultCode'
          AND LTRIM(RTRIM(ISNULL(d.validationresultcode, N''))) NOT IN (N'02', N'002');

        UPDATE c
           SET c.Valor_Orden_de_Compra = N'02 - 002',
               c.Aprobado = N'SI'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'ResponseCode'
          AND LTRIM(RTRIM(ISNULL(d.codigo_de_uso_autorizado_por_la_dian, N''))) IN (N'02', N'002');

        UPDATE dp
           SET dp.EstadoFinalFase_4 = N'VALIDACION DATOS DE FACTURACION: Exitoso.',
               dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'ResponseCode diferente a 02 o 002',
                        CASE WHEN ISNULL(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') <> N'' 
                             THEN N', ' + dp.ObservacionesFase_4 ELSE N'' END
                    ))),
                    3900
               ),
               dp.ResultadoFinalAntesEventos = N'CON NOVEDAD'
        FROM [CxP].[DocumentsProcessing] dp
        INNER JOIN #Docs d ON d.ID = dp.ID
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE LTRIM(RTRIM(ISNULL(d.codigo_de_uso_autorizado_por_la_dian, N''))) NOT IN (N'02', N'002');

        UPDATE c
           SET c.Valor_Orden_de_Compra = N'02 - 002',
               c.Aprobado = N'NO'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'ResponseCode'
          AND LTRIM(RTRIM(ISNULL(d.codigo_de_uso_autorizado_por_la_dian, N''))) NOT IN (N'02', N'002');

        UPDATE c
           SET c.Valor_Orden_de_Compra = @InvoiceTypecode,
               c.Aprobado = N'SI'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'InvoiceTypecode'
          AND LTRIM(RTRIM(ISNULL(d.codigo_tipo_de_documento, N''))) IN (SELECT Codigo FROM #InvoiceTypecodeTable);

        UPDATE dp
           SET dp.EstadoFinalFase_4 = N'VALIDACION DATOS DE FACTURACION: Exitoso.',
               dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'InvoiceTypecode diferente a la lista permitida',
                        CASE WHEN ISNULL(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') <> N'' 
                             THEN N', ' + dp.ObservacionesFase_4 ELSE N'' END
                    ))),
                    3900
               ),
               dp.ResultadoFinalAntesEventos = N'CON NOVEDAD'
        FROM [CxP].[DocumentsProcessing] dp
        INNER JOIN #Docs d ON d.ID = dp.ID
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE LTRIM(RTRIM(ISNULL(d.codigo_tipo_de_documento, N''))) NOT IN (SELECT Codigo FROM #InvoiceTypecodeTable);

        UPDATE c
           SET c.Valor_Orden_de_Compra = @InvoiceTypecode,
               c.Aprobado = N'NO'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'InvoiceTypecode'
          AND LTRIM(RTRIM(ISNULL(d.codigo_tipo_de_documento, N''))) NOT IN (SELECT Codigo FROM #InvoiceTypecodeTable);

        UPDATE c
           SET c.Valor_Orden_de_Compra = N'Documento validado por la DIAN',
               c.Aprobado = N'SI'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'DescripcionCodigo'
          AND LTRIM(RTRIM(ISNULL(d.descripcion_del_codigo, N''))) = N'Documento validado por la DIAN';

        UPDATE dp
           SET dp.EstadoFinalFase_4 = N'VALIDACION DATOS DE FACTURACION: Exitoso.',
               dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'DescripcionCodigo diferente a "Documento validado por la DIAN"',
                        CASE WHEN ISNULL(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') <> N'' 
                             THEN N', ' + dp.ObservacionesFase_4 ELSE N'' END
                    ))),
                    3900
               ),
               dp.ResultadoFinalAntesEventos = N'CON NOVEDAD'
        FROM [CxP].[DocumentsProcessing] dp
        INNER JOIN #Docs d ON d.ID = dp.ID
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE LTRIM(RTRIM(ISNULL(d.descripcion_del_codigo, N''))) <> N'Documento validado por la DIAN';

        UPDATE c
           SET c.Valor_Orden_de_Compra = N'Documento validado por la DIAN',
               c.Aprobado = N'NO'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE c.Item = N'DescripcionCodigo'
          AND LTRIM(RTRIM(ISNULL(d.descripcion_del_codigo, N''))) <> N'Documento validado por la DIAN';

        UPDATE c
           SET c.Valor_Orden_de_Compra = N'01 - 02',
               c.Aprobado = N'SI'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = d.ID
        WHERE c.Item = N'PaymentMeans'
          AND UPPER(LTRIM(RTRIM(ISNULL(dp.forma_de_pago, N'')))) IN (N'01', N'02', N'1', N'2');

        UPDATE dp
           SET dp.EstadoFinalFase_4 = N'VALIDACION DATOS DE FACTURACION: Exitoso.',
               dp.ObservacionesFase_4 = LEFT(
                    LTRIM(RTRIM(CONCAT(
                        N'PaymentMeans diferente a 01 o 02',
                        CASE WHEN ISNULL(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') <> N'' 
                             THEN N', ' + dp.ObservacionesFase_4 ELSE N'' END
                    ))),
                    3900
               ),
               dp.ResultadoFinalAntesEventos = N'CON NOVEDAD'
        FROM [CxP].[DocumentsProcessing] dp
        INNER JOIN #Docs d ON d.ID = dp.ID
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        WHERE UPPER(LTRIM(RTRIM(ISNULL(dp.forma_de_pago, N'')))) NOT IN (N'01', N'02', N'1', N'2');

        UPDATE c
           SET c.Valor_Orden_de_Compra = N'01 - 02',
               c.Aprobado = N'NO'
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = d.ID
        WHERE c.Item = N'PaymentMeans'
          AND UPPER(LTRIM(RTRIM(ISNULL(dp.forma_de_pago, N'')))) NOT IN (N'01', N'02', N'1', N'2');

        IF @MesActual <> 1
        BEGIN
            UPDATE dp
               SET dp.EstadoFinalFase_4 = N'VALIDACION DATOS DE FACTURACION: Exitoso.',
                   dp.ObservacionesFase_4 = LEFT(
                        LTRIM(RTRIM(CONCAT(
                            N'Ano de Fecha de emision documento diferente a ano en curso',
                            CASE WHEN ISNULL(LTRIM(RTRIM(dp.ObservacionesFase_4)), N'') <> N'' 
                                 THEN N', ' + dp.ObservacionesFase_4 ELSE N'' END
                        ))),
                        3900
                   ),
                   dp.ResultadoFinalAntesEventos = N'CON NOVEDAD ANO CERRADO'
            FROM [CxP].[DocumentsProcessing] dp
            INNER JOIN #Docs d ON d.ID = dp.ID
            INNER JOIN #BatchIDs b ON b.ID = d.ID
            WHERE d.fecha_de_emision_documento IS NOT NULL
              AND YEAR(d.fecha_de_emision_documento) <> @AnioActual;

            UPDATE c
               SET c.Valor_Orden_de_Compra = N'ANO CERRADO',
                   c.Aprobado = N'NO'
            FROM [dbo].[CxP.Comparativa] c
            INNER JOIN #Docs d ON d.ID = c.ID_registro
            INNER JOIN #BatchIDs b ON b.ID = d.ID
            WHERE c.Item = N'FechaEmisionDocumento'
              AND d.fecha_de_emision_documento IS NOT NULL
              AND YEAR(d.fecha_de_emision_documento) <> @AnioActual;
        END;

        UPDATE c
           SET c.Valor_XML = LEFT(dp.ObservacionesFase_4, 3900)
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = d.ID
        WHERE c.Item = N'Observaciones'
          AND ISNULL(dp.ResultadoFinalAntesEventos, N'') <> ISNULL(d.OldResultadoFinalAntesEventos, N'');

        UPDATE c
           SET c.Estado_validacion_antes_de_eventos = dp.ResultadoFinalAntesEventos
        FROM [dbo].[CxP.Comparativa] c
        INNER JOIN #Docs d ON d.ID = c.ID_registro
        INNER JOIN #BatchIDs b ON b.ID = d.ID
        INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = d.ID
        WHERE ISNULL(dp.ResultadoFinalAntesEventos, N'') <> ISNULL(d.OldResultadoFinalAntesEventos, N'');

        SET @MinRow = @MaxRow + 1;
    END;

    SET @RegistrosProcesados = @TotalDocs;

    SELECT @RegistrosAnoCerrado = COUNT(*)
    FROM #Docs d
    INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = d.ID
    WHERE dp.ResultadoFinalAntesEventos = N'CON NOVEDAD ANO CERRADO';

    SELECT @RegistrosConNovedad = COUNT(*)
    FROM #Docs d
    INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = d.ID
    WHERE dp.ResultadoFinalAntesEventos = N'CON NOVEDAD';

    DECLARE @RegistrosInsertadosReporte INT = 0;
    
    IF OBJECT_ID(N'[CxP].[ReporteNovedades]', N'U') IS NOT NULL
    BEGIN
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
            CONVERT(VARCHAR(20), CAST(@Now AS DATE), 120),
            ISNULL(CAST(dp.nit_emisor_o_nit_del_proveedor AS VARCHAR(50)), ''),
            ISNULL(SUBSTRING(dp.nombre_emisor, 1, 500), ''),
            ISNULL(SUBSTRING(dp.numero_de_liquidacion_u_orden_de_compra, 1, 100), ''),
            ISNULL(SUBSTRING(dp.numero_de_factura, 1, 100), ''),
            ISNULL(SUBSTRING(dp.ResultadoFinalAntesEventos, 1, 200), ''),
            ISNULL(dp.ObservacionesFase_4, ''),
            'HU4_E_CamposReglamentarios'
        FROM #Docs d
        INNER JOIN [CxP].[DocumentsProcessing] dp ON dp.ID = d.ID
        WHERE dp.ResultadoFinalAntesEventos IS NOT NULL
          AND LTRIM(RTRIM(dp.ResultadoFinalAntesEventos)) <> N''
          AND (
              dp.ResultadoFinalAntesEventos LIKE N'%CON NOVEDAD%'
              OR dp.ResultadoFinalAntesEventos LIKE N'%AÑO CERRADO%'
              OR dp.ResultadoFinalAntesEventos LIKE N'%ANO CERRADO%'
          );
        
        SET @RegistrosInsertadosReporte = @@ROWCOUNT;
    END

    SELECT
        @Now AS FechaEjecucion,
        @RegistrosProcesados AS RegistrosProcesados,
        @RegistrosConNovedad AS RegistrosConNovedad,
        @RegistrosAnoCerrado AS RegistrosAnoCerrado,
        @CurrentBatch AS LotesProcesados,
        DATEDIFF(SECOND, @StartTime, SYSDATETIME()) AS TiempoTotalSegundos,
        @RegistrosInsertadosReporte AS RegistrosInsertadosReporte,
        'COMPLETADO' AS Estado;

    IF OBJECT_ID('tempdb..#TaxLevelCodeTable') IS NOT NULL DROP TABLE #TaxLevelCodeTable;
    IF OBJECT_ID('tempdb..#InvoiceTypecodeTable') IS NOT NULL DROP TABLE #InvoiceTypecodeTable;
    IF OBJECT_ID('tempdb..#EstadosOmitirTable') IS NOT NULL DROP TABLE #EstadosOmitirTable;
    IF OBJECT_ID('tempdb..#BatchIDs') IS NOT NULL DROP TABLE #BatchIDs;
    IF OBJECT_ID('tempdb..#Docs') IS NOT NULL DROP TABLE #Docs;
END

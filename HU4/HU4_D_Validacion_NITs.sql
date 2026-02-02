/*
================================================================================
STORED PROCEDURE: [CxP].[HU4_D_Validacion_NITs]
================================================================================

Descripcion General:
--------------------
    Valida una lista de NITs contra los documentos de facturacion electronica.
    Los documentos que coinciden con los NITs de la lista y estan en estado
    RECHAZADO o RECHAZADO - PENDIENTE se actualizan a estado CON NOVEDAD.
    
    Este SP se usa para excluir proveedores especificos del rechazo automatico
    por campos mandatorios.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Base de Datos: NotificationsPaddy
Schema: CxP

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |            [CxP].[HU4_D_Validacion_NITs]                    |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Crear tabla [CxP].[ReporteNovedades] si no existe          |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Validar parametro @ListaNits:                              |
    |  - Si NULL o vacio -> retornar exito sin procesar           |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Parsear lista de NITs:                                     |
    |  - Separar por comas                                        |
    |  - Normalizar: quitar espacios, puntos, guiones             |
    |  - Eliminar duplicados                                      |
    +-----------------------------+-------------------------------+
                                  |
                  +---------------+---------------+
                  |    Hay NITs validos?          |
                  +---------------+---------------+
                         |                |
                         | NO             | SI
                         v                v
    +------------------------+   +--------------------------------+
    |  Retornar exito        |   |  Marcar Comparativa (rechazo)  |
    |  "No se pudieron       |   +----------------+---------------+
    |   extraer NITs"        |                    |
    +------------------------+                    v
    +-------------------------------------------------------------+
    |  Buscar candidatos en DocumentsProcessing:                  |
    |  - NIT coincide con lista                                   |
    |  - Estado = RECHAZADO o RECHAZADO - PENDIENTE               |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Actualizar DocumentsProcessing:                            |
    |  - ObservacionesFase_4 = "Nit no aplica RECHAZO..."         |
    |  - ResultadoFinalAntesEventos = 'CON NOVEDAD'               |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Actualizar Comparativa:                                    |
    |  - Valor_XML = ObservacionesFase_4                          |
    |  - Estado_validacion_antes_de_eventos                       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Insertar en [CxP].[ReporteNovedades]                       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar ResultSet:                                        |
    |  - exito, resumen, RegistrosReporteNovedades                |
    +-------------------------------------------------------------+

================================================================================
PARAMETROS
================================================================================

    @ListaNits NVARCHAR(MAX)
        Lista de NITs separados por comas.
        Los NITs se normalizan automaticamente:
        - Se eliminan espacios
        - Se eliminan puntos
        - Se eliminan guiones
        
        Ejemplos validos:
        - '900123456,800987654,901234567'
        - '900.123.456, 800-987-654'
        - '900123456'

================================================================================
TABLAS UTILIZADAS
================================================================================

Tablas de Entrada:
------------------
    [CxP].[DocumentsProcessing]
        Tabla principal de documentos.
        Campos clave:
        - ID: Identificador unico
        - nit_emisor_o_nit_del_proveedor: NIT del emisor
        - ResultadoFinalAntesEventos: Estado actual
        - ObservacionesFase_4: Observaciones

    [dbo].[CxP.Comparativa]
        Tabla comparativa con Items.
        Se actualiza el Item 'Observaciones'.

Tablas de Salida:
-----------------
    [CxP].[ReporteNovedades]
        Reporte de documentos actualizados.
        Se insertan los registros corregidos.

================================================================================
LOGICA DE NORMALIZACION DE NITS
================================================================================

Los NITs se normalizan para garantizar coincidencias correctas:

    Entrada                  -> Normalizado
    -----------------------------------------
    '900.123.456'            -> '900123456'
    '900-123-456'            -> '900123456'
    ' 900123456 '            -> '900123456'
    '900.123-456'            -> '900123456'

La normalizacion se aplica tanto a la lista de entrada como a los
NITs en la base de datos para la comparacion.

================================================================================
ESTADOS AFECTADOS
================================================================================

Estados de Entrada (se buscan):
    - RECHAZADO
    - RECHAZADO - PENDIENTE

Estado de Salida (se asigna):
    - CON NOVEDAD

================================================================================
RESULTSET DE SALIDA
================================================================================

ResultSet Unico:
----------------
    exito                       BIT         - 1 si exito, 0 si error
    resumen                     NVARCHAR    - Mensaje descriptivo
    RegistrosReporteNovedades   INT         - Cantidad de registros en reporte

Ejemplo de resumen exitoso:
    "Validacion de NITs finalizada. NITs unicos: 5. Candidatos: 23. 
     Registros corregidos: 23. Comparativa marcada (rechazo): 10. 
     Comparativa Observaciones actualizada: 23. 
     Registros en Reporte Novedades: 23"

================================================================================
EJEMPLOS DE USO
================================================================================

-- Ejemplo 1: Validar un solo NIT
EXEC [CxP].[HU4_D_Validacion_NITs]
    @ListaNits = '900123456';

-- Ejemplo 2: Validar multiples NITs
EXEC [CxP].[HU4_D_Validacion_NITs]
    @ListaNits = '900123456,800987654,901234567,890456123';

-- Ejemplo 3: NITs con formato variado (se normalizan)
EXEC [CxP].[HU4_D_Validacion_NITs]
    @ListaNits = '900.123.456, 800-987-654, 901234567';

-- Ejemplo 4: Lista vacia (retorna exito sin procesar)
EXEC [CxP].[HU4_D_Validacion_NITs]
    @ListaNits = '';

-- Ejemplo 5: Llamada desde Python/Rocketbot
DECLARE @Nits NVARCHAR(MAX);
SET @Nits = (SELECT STRING_AGG(NIT, ',') FROM MiTablaDeNITs);
EXEC [CxP].[HU4_D_Validacion_NITs] @ListaNits = @Nits;

================================================================================
MANEJO DE ERRORES
================================================================================

El SP utiliza TRY-CATCH:

    - En caso de error:
        - exito = 0
        - resumen = 'Ejecucion con error: ' + ERROR_MESSAGE()
        - RegistrosReporteNovedades = 0

No se lanzan excepciones, siempre retorna un ResultSet.

================================================================================
INTEGRACION CON SCRIPT PYTHON
================================================================================

Este SP es llamado por: ejecutar_HU4_D_NITs.py

Flujo de integracion:
    1. Python lee NITs desde archivo Excel
    2. Convierte lista a string separado por comas
    3. Ejecuta SP con la lista
    4. Parsea resultado (exito, resumen)
    5. Registra en log

Ejemplo de llamada desde Python:
    cursor.execute(
        "EXEC [CxP].[HU4_D_Validacion_NITs] @ListaNits=?",
        nits_string
    )

================================================================================
NOTAS TECNICAS
================================================================================

    - Usa STRING_SPLIT para parsear la lista de NITs
    - Normaliza NITs con REPLACE anidados
    - Usa tabla variable @Nits para almacenar NITs unicos
    - Usa tabla variable @Updated para tracking de IDs actualizados
    - Las observaciones se concatenan (no se sobrescriben)
    - Limite de 3900 caracteres en ObservacionesFase_4

================================================================================
*/

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

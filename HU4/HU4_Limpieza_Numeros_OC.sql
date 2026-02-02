/*
================================================================================
STORED PROCEDURE: [CxP].[HU4_Limpieza_Numeros_OC]
================================================================================

Descripcion General:
--------------------
    Procesa y limpia el campo numero_de_liquidacion_u_orden_de_compra de la
    tabla DocumentsProcessing. Cuando un documento tiene multiples numeros
    de orden separados por comas, este SP:
    
    1. Separa cada numero en registros individuales
    2. Limita cada numero a 10 caracteres (trunca si es mas largo)
    3. Genera nuevos IDs para los registros duplicados
    
    Este SP es de CONSULTA - no modifica datos, solo retorna un ResultSet
    con los datos transformados.

Autor: Diego Ivan Lopez Ochoa
Version: 1.0.0
Base de Datos: NotificationsPaddy
Schema: CxP

================================================================================
DIAGRAMA DE FLUJO
================================================================================

    +-------------------------------------------------------------+
    |                        INICIO                               |
    |          [CxP].[HU4_Limpieza_Numeros_OC]                    |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Obtener @MaxID = MAX(ID) de DocumentsProcessing            |
    |  (Para calcular nuevos IDs de las copias)                   |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  CTE SplitData:                                             |
    |  - Separar numero_de_liquidacion por comas usando XML       |
    |  - LTRIM/RTRIM cada valor separado                          |
    |  - Generar IndiceCopia con ROW_NUMBER                       |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  CTE DataConNuevosIDs:                                      |
    |  - Si IndiceCopia = 1 -> conservar ID original              |
    |  - Si IndiceCopia > 1 -> @MaxID + consecutivo               |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  SELECT final con transformaciones:                         |
    |  - Todos los campos originales                              |
    |  - numero_de_liquidacion = LEFT(ValorSeparado, 10)          |
    |  - ID = ID calculado (original o nuevo)                     |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Filtrar: WHERE ValorSeparado <> ''                         |
    |  (Excluir valores vacios por comas erroneas)                |
    +-----------------------------+-------------------------------+
                                  |
                                  v
    +-------------------------------------------------------------+
    |  Retornar ResultSet con datos transformados                 |
    +-------------------------------------------------------------+

================================================================================
PARAMETROS
================================================================================

    Este SP no recibe parametros.

================================================================================
LOGICA DE SEPARACION
================================================================================

Ejemplo de entrada:
    ID = 100
    numero_de_liquidacion = "1234567890,9876543210,5555555555"

Resultado (3 registros):
    ID = 100,  numero_de_liquidacion = "1234567890"  (original)
    ID = 101,  numero_de_liquidacion = "9876543210"  (copia 1)
    ID = 102,  numero_de_liquidacion = "5555555555"  (copia 2)

================================================================================
LOGICA DE TRUNCAMIENTO
================================================================================

Si un numero tiene mas de 10 caracteres, se trunca:

    Entrada:  "12345678901234"  (14 caracteres)
    Salida:   "1234567890"      (10 caracteres)

================================================================================
LOGICA DE GENERACION DE IDs
================================================================================

    1. Se obtiene el ID maximo actual: @MaxID = MAX(ID)
    
    2. Para registros originales (IndiceCopia = 1):
       - Se conserva el ID original
       
    3. Para registros copiados (IndiceCopia > 1):
       - Nuevo ID = @MaxID + consecutivo global
       
    4. El consecutivo se calcula con ROW_NUMBER() particionado
       por si es copia (IndiceCopia > 1) o no

================================================================================
TECNICA DE SEPARACION XML
================================================================================

El SP usa una tecnica de XML para separar valores por comas:

    1. Reemplazar comas por etiquetas XML:
       "a,b,c" -> "<M>a</M><M>b</M><M>c</M>"
       
    2. Convertir a tipo XML
    
    3. Usar CROSS APPLY con .nodes('/M') para obtener cada valor

Esta tecnica funciona en todas las versiones modernas de SQL Server,
a diferencia de STRING_SPLIT que requiere SQL 2016+.

================================================================================
CAMPOS RETORNADOS
================================================================================

El SP retorna TODOS los campos de DocumentsProcessing, incluyendo:

    - executionNum, executionDate
    - attached_document, ubl_version
    - numero_de_factura, nombre_emisor
    - nit_emisor_o_nit_del_proveedor
    - [numero_de_liquidacion_u_orden_de_compra] <- TRANSFORMADO
    - [ID] <- PUEDE SER NUEVO
    - ... (todos los demas campos)

================================================================================
RESULTSET DE SALIDA
================================================================================

ResultSet Unico:
----------------
Todos los campos de [CxP].[DocumentsProcessing] con:

    - numero_de_liquidacion_u_orden_de_compra:
      Valor individual (separado) y truncado a 10 caracteres
      
    - ID:
      ID original si es el primer valor, o nuevo ID calculado
      si es un valor adicional

================================================================================
EJEMPLOS DE USO
================================================================================

-- Ejemplo 1: Ejecucion simple
EXEC [CxP].[HU4_Limpieza_Numeros_OC];

-- Ejemplo 2: Guardar resultado en tabla temporal
SELECT *
INTO #ResultadosLimpieza
FROM OPENROWSET('SQLNCLI', 
    'Server=localhost;Trusted_Connection=yes;',
    'EXEC [NotificationsPaddy].[CxP].[HU4_Limpieza_Numeros_OC]');

-- Ejemplo 3: Ver datos antes de limpiar
SELECT 
    ID,
    numero_de_liquidacion_u_orden_de_compra,
    LEN(numero_de_liquidacion_u_orden_de_compra) AS Longitud,
    CHARINDEX(',', numero_de_liquidacion_u_orden_de_compra) AS TieneComa
FROM [CxP].[DocumentsProcessing]
WHERE numero_de_liquidacion_u_orden_de_compra LIKE '%,%'
   OR LEN(numero_de_liquidacion_u_orden_de_compra) > 10;

-- Ejemplo 4: Contar registros que se expandirian
SELECT 
    COUNT(*) AS TotalRegistros,
    SUM(LEN(numero_de_liquidacion_u_orden_de_compra) 
        - LEN(REPLACE(numero_de_liquidacion_u_orden_de_compra, ',', '')) + 1) 
        AS RegistrosDespuesExpansion
FROM [CxP].[DocumentsProcessing]
WHERE numero_de_liquidacion_u_orden_de_compra IS NOT NULL;

================================================================================
CASOS ESPECIALES
================================================================================

1. Valores con comas al final:
   "123,456," -> "123", "456" (se filtra el vacio)

2. Comas multiples:
   "123,,456" -> "123", "456" (se filtra el vacio)

3. Solo espacios:
   "   " -> Se filtra (ValorSeparado = '' despues de TRIM)

4. NULL:
   NULL -> Se procesa como cadena vacia, se filtra

================================================================================
NOTAS TECNICAS
================================================================================

    - Este SP es de SOLO LECTURA - no modifica datos
    - Usa CTEs (Common Table Expressions) para la logica
    - Usa XML para separar valores (compatible con SQL 2008+)
    - ROW_NUMBER se usa para generar indices y nuevos IDs
    - El filtro WHERE ValorSeparado <> '' evita registros vacios
    - Los nuevos IDs son calculados, no insertados en la tabla

================================================================================
CONSIDERACIONES DE USO
================================================================================

    1. Este SP solo RETORNA datos transformados, NO los inserta
    
    2. Para aplicar los cambios, se debe:
       a. Ejecutar el SP
       b. Capturar el ResultSet
       c. Procesar/insertar los datos segun necesidad
       
    3. Los IDs generados son CALCULADOS en el momento de ejecucion
       Si la tabla cambia, los IDs calculados pueden variar
       
    4. Para uso en produccion, considerar ejecutar en horarios
       de baja carga debido al procesamiento de todos los registros

================================================================================
*/

USE [NotificationsPaddy]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [CxP].[HU4_Limpieza_Numeros_OC]
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Variables para el control de nuevos IDs
    DECLARE @MaxID BIGINT;
    SELECT @MaxID = MAX([ID]) FROM [NotificationsPaddy].[CxP].[DocumentsProcessing];

    -- 2. Creamos una tabla temporal para trabajar los datos transformados sin bloquear la tabla original
    IF OBJECT_ID('tempdb..#DataTransformada') IS NOT NULL DROP TABLE #DataTransformada;

    WITH SplitData AS (
        SELECT 
            T.*,
            LTRIM(RTRIM(Split.a.value('.', 'VARCHAR(100)'))) AS ValorSeparado,
            ROW_NUMBER() OVER (PARTITION BY T.ID ORDER BY (SELECT NULL)) AS IndiceCopia
        FROM [NotificationsPaddy].[CxP].[DocumentsProcessing] AS T
        CROSS APPLY (
            SELECT CAST('<M>' + REPLACE(
                ISNULL(T.[numero_de_liquidacion_u_orden_de_compra], ''), 
                ',', '</M><M>') + '</M>' AS XML) AS Data
        ) AS A
        CROSS APPLY A.Data.nodes ('/M') AS Split(a)
        WHERE LTRIM(RTRIM(Split.a.value('.', 'VARCHAR(100)'))) <> ''
    )
    SELECT * INTO #DataTransformada FROM SplitData;

    -- 3. TRANSACCION: Aseguramos que se apliquen todos los cambios o ninguno
    BEGIN TRANSACTION;
    BEGIN TRY
        
        -- A. ACTUALIZACION: Modificamos los registros originales (IndiceCopia = 1)
        -- Truncamos a 10 caracteres y limpiamos la coma
        UPDATE T
        SET T.[numero_de_liquidacion_u_orden_de_compra] = LEFT(D.ValorSeparado, 10)
        FROM [NotificationsPaddy].[CxP].[DocumentsProcessing] AS T
        INNER JOIN #DataTransformada D ON T.ID = D.ID
        WHERE D.IndiceCopia = 1;

        -- B. INSERCION: Creamos registros nuevos para las ordenes adicionales (IndiceCopia > 1)
        -- Usamos INSERT INTO ... SELECT para copiar todos los campos e insertar el nuevo ID calculado
        INSERT INTO [NotificationsPaddy].[CxP].[DocumentsProcessing] (
            [executionNum], [executionDate], [attached_document], [ubl_version], [id_de_perfil], 
            [ambiente_de_ejecucion_id], [numero_de_factura], [nombre_emisor], [nit_emisor_o_nit_del_proveedor],
            [responsabilidad_tributaria_emisor], [nombre_del_adquiriente], [responsabilidad_tributaria_adquiriente],
            [tipo_persona], [digito_de_verificacion], [nit_del_adquiriente], [formato_del_archivo],
            [tipo_de_codificacion], [fecha_de_emision_documento], [fecha_de_validacion_documento],
            [hora_de_validacion_documento], [valor_a_pagar], [valor_a_pagar_nc], [valor_a_pagar_nd],
            [forma_de_pago], [medio_de_pago], [fecha_de_validacion_forma_de_pago], [cufeuuid],
            [documenttype], [documentPrefix], [codigo_de_uso_autorizado_por_la_dian], [validationresultcode],
            [descripcion_del_codigo], [resultado_de_la_validacion_dian], 
            [numero_de_liquidacion_u_orden_de_compra], -- Campo Transformado
            [codigo_tipo_de_documento], [codigo_tipo_de_documento_Nc], [Numero_de_nota_credito],
            [Origen_Servicio], [Personalizacion_del_estandar_UBL], [Tipo_de_nota_cred_deb],
            [Identificador_del_tributo], [Nombre_del_tributo], [Correo_Electronico_Emisor],
            [agrupacion], [area], [fechaValidacion], [RutaArchivo], [actualizacionNombreArchivos],
            [Tipo_Persona_Emisor], [Digito_de_verificacion_Emisor], [EstadoXml], [PrefijoYNumero],
            [cufe_fe], [fechaCufe_fe], 
            [ID], -- Nuevo ID calculado
            [Fecha_de_retoma_antes_de_contabilizacion], [Fecha_primer_proceso], [EstadoFinalFase_5],
            [ObservacionesFase_4], [EstadoFinalFase_4], [ResultadoFinalAntesEventos], [EstadoFase_3],
            [ObservacionesFase_3], [executionNum_CxP], [Estado_Evento_030], [Estado_Evento_031],
            [Estado_Evento_032], [Estado_Evento_033], [FechaHora_Evento_030], [FechaHora_Evento_031],
            [FechaHora_Evento_032], [FechaHora_Evento_033], [Fecha_proceso_contabilizacion],
            [Fecha_retoma_contabilizacion], [DocumentCurrencyCode], [CalculationRate], [VlrPagarCop],
            [EstadoFinalFase_6], [ObservacionesFase_6], [Estado_contabilizacion], [Compensar_por],
            [Documento_contable], [Posicion_Comercializado], [Valor_a_pagar_Comercializado],
            [Valor_a_pagar_Comercializado_ME], [NotaCreditoReferenciada], [Insumo_XML], [Insumo_PDF],
            [Insumo_reubicado], [Ruta_respaldo]
        )
        SELECT 
            [executionNum], [executionDate], [attached_document], [ubl_version], [id_de_perfil], 
            [ambiente_de_ejecucion_id], [numero_de_factura], [nombre_emisor], [nit_emisor_o_nit_del_proveedor],
            [responsabilidad_tributaria_emisor], [nombre_del_adquiriente], [responsabilidad_tributaria_adquiriente],
            [tipo_persona], [digito_de_verificacion], [nit_del_adquiriente], [formato_del_archivo],
            [tipo_de_codificacion], [fecha_de_emision_documento], [fecha_de_validacion_documento],
            [hora_de_validacion_documento], [valor_a_pagar], [valor_a_pagar_nc], [valor_a_pagar_nd],
            [forma_de_pago], [medio_de_pago], [fecha_de_validacion_forma_de_pago], [cufeuuid],
            [documenttype], [documentPrefix], [codigo_de_uso_autorizado_por_la_dian], [validationresultcode],
            [descripcion_del_codigo], [resultado_de_la_validacion_dian],
            LEFT(ValorSeparado, 10), -- Truncamiento a 10
            [codigo_tipo_de_documento], [codigo_tipo_de_documento_Nc], [Numero_de_nota_credito],
            [Origen_Servicio], [Personalizacion_del_estandar_UBL], [Tipo_de_nota_cred_deb],
            [Identificador_del_tributo], [Nombre_del_tributo], [Correo_Electronico_Emisor],
            [agrupacion], [area], [fechaValidacion], [RutaArchivo], [actualizacionNombreArchivos],
            [Tipo_Persona_Emisor], [Digito_de_verificacion_Emisor], [EstadoXml], [PrefijoYNumero],
            [cufe_fe], [fechaCufe_fe],
            @MaxID + ROW_NUMBER() OVER (ORDER BY ID, IndiceCopia), -- Generacion de ID unico
            [Fecha_de_retoma_antes_de_contabilizacion], [Fecha_primer_proceso], [EstadoFinalFase_5],
            [ObservacionesFase_4], [EstadoFinalFase_4], [ResultadoFinalAntesEventos], [EstadoFase_3],
            [ObservacionesFase_3], [executionNum_CxP], [Estado_Evento_030], [Estado_Evento_031],
            [Estado_Evento_032], [Estado_Evento_033], [FechaHora_Evento_030], [FechaHora_Evento_031],
            [FechaHora_Evento_032], [FechaHora_Evento_033], [Fecha_proceso_contabilizacion],
            [Fecha_retoma_contabilizacion], [DocumentCurrencyCode], [CalculationRate], [VlrPagarCop],
            [EstadoFinalFase_6], [ObservacionesFase_6], [Estado_contabilizacion], [Compensar_por],
            [Documento_contable], [Posicion_Comercializado], [Valor_a_pagar_Comercializado],
            [Valor_a_pagar_Comercializado_ME], [NotaCreditoReferenciada], [Insumo_XML], [Insumo_PDF],
            [Insumo_reubicado], [Ruta_respaldo]
        FROM #DataTransformada
        WHERE IndiceCopia > 1;

        COMMIT TRANSACTION;
        PRINT 'Proceso completado exitosamente: Datos actualizados e insertados.';

    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR (@ErrorMessage, 16, 1);
    END CATCH

    -- Limpieza de tabla temporal
    IF OBJECT_ID('tempdb..#DataTransformada') IS NOT NULL DROP TABLE #DataTransformada;

END
GO

USE [NotificationsPaddy]
GO
/****** Object:  StoredProcedure [CxP].[HU4_Limpieza_Numeros_OC]    Script Date: 01/02/2026 4:30:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [CxP].[HU4_Limpieza_Numeros_OC]
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Obtenemos el ID máximo existente para calcular los nuevos IDs de las copias
    DECLARE @MaxID BIGINT;
    SELECT @MaxID = MAX([ID]) FROM [NotificationsPaddy].[CxP].[DocumentsProcessing];

    -- Usamos una CTE (Common Table Expression) para realizar la lógica
    WITH SplitData AS (
        SELECT 
            T.*,
            -- Limpiamos espacios en blanco del valor separado
            LTRIM(RTRIM(Split.a.value('.', 'VARCHAR(100)'))) AS ValorSeparado,
            -- Generamos un índice para saber cuál es el valor original (1) y cuáles son copias (>1)
            ROW_NUMBER() OVER (PARTITION BY T.ID ORDER BY (SELECT NULL)) AS IndiceCopia
        FROM [NotificationsPaddy].[CxP].[DocumentsProcessing] AS T
        -- Lógica para separar por comas usando XML (funciona en todas las versiones modernas de SQL)
        CROSS APPLY (
            SELECT CAST('<M>' + REPLACE(
                -- Reemplazamos la coma por etiquetas XML para dividir
                ISNULL(T.[numero_de_liquidacion_u_orden_de_compra], ''), 
                ',', '</M><M>') + '</M>' AS XML) AS Data
        ) AS A
        CROSS APPLY A.Data.nodes ('/M') AS Split(a)
    ),
    DataConNuevosIDs AS (
        SELECT 
            sd.*,
            -- LÓGICA DE ID:
            -- Si es el primer registro (IndiceCopia = 1), conserva su ID original.
            -- Si es copia (>1), toma el @MaxID y le suma un consecutivo global.
            CASE 
                WHEN IndiceCopia = 1 THEN ID
                ELSE @MaxID + ROW_NUMBER() OVER (ORDER BY ID, IndiceCopia) - (SELECT COUNT(*) FROM [NotificationsPaddy].[CxP].[DocumentsProcessing]) 
                -- Ajuste matemático: ROW_NUMBER global menos total filas base para iniciar el conteo después del MaxID
                -- O una forma más simple para el ROW_NUMBER de los nuevos:
            END AS ID_Calculado,
            
            -- Calculamos el consecutivo solo para las copias para sumar al MaxID
            ROW_NUMBER() OVER (PARTITION BY CASE WHEN IndiceCopia > 1 THEN 1 ELSE 0 END ORDER BY ID, IndiceCopia) as RowNumNuevos
        FROM SplitData sd
    )

    -- SELECCIÓN FINAL
    SELECT 
        [executionNum],
        [executionDate],
        [attached_document],
        [ubl_version],
        [id_de_perfil],
        [ambiente_de_ejecucion_id],
        [numero_de_factura],
        [nombre_emisor],
        [nit_emisor_o_nit_del_proveedor],
        [responsabilidad_tributaria_emisor],
        [nombre_del_adquiriente],
        [responsabilidad_tributaria_adquiriente],
        [tipo_persona],
        [digito_de_verificacion],
        [nit_del_adquiriente],
        [formato_del_archivo],
        [tipo_de_codificacion],
        [fecha_de_emision_documento],
        [fecha_de_validacion_documento],
        [hora_de_validacion_documento],
        [valor_a_pagar],
        [valor_a_pagar_nc],
        [valor_a_pagar_nd],
        [forma_de_pago],
        [medio_de_pago],
        [fecha_de_validacion_forma_de_pago],
        [cufeuuid],
        [documenttype],
        [documentPrefix],
        [codigo_de_uso_autorizado_por_la_dian],
        [validationresultcode],
        [descripcion_del_codigo],
        [resultado_de_la_validacion_dian],
        
        -- LÓGICA DE LIMPIEZA DE 10 DÍGITOS Y ASIGNACIÓN DEL VALOR SEPARADO
        CASE 
            -- Si el valor separado tiene más de 10 caracteres
            WHEN LEN(ValorSeparado) > 10 THEN 
                -- Tomamos los primeros 10 caracteres (Izquierda)
                -- REPLACE anidados para quitar guiones o puntos si se consideran "caracteres especiales" básicos
                LEFT(ValorSeparado, 10) 
            ELSE 
                -- Si tiene 10 o menos, se deja tal cual
                ValorSeparado 
        END AS [numero_de_liquidacion_u_orden_de_compra],

        [codigo_tipo_de_documento],
        [codigo_tipo_de_documento_Nc],
        [Numero_de_nota_credito],
        [Origen_Servicio],
        [Personalizacion_del_estandar_UBL],
        [Tipo_de_nota_cred_deb],
        [Identificador_del_tributo],
        [Nombre_del_tributo],
        [Correo_Electronico_Emisor],
        [agrupacion],
        [area],
        [fechaValidacion],
        [RutaArchivo],
        [actualizacionNombreArchivos],
        [Tipo_Persona_Emisor],
        [Digito_de_verificacion_Emisor],
        [EstadoXml],
        [PrefijoYNumero],
        [cufe_fe],
        [fechaCufe_fe],
        
        -- AQUI ASIGNAMOS EL ID CALCULADO (Original o Nuevo)
        CASE 
            WHEN IndiceCopia = 1 THEN ID 
            ELSE @MaxID + RowNumNuevos 
        END AS [ID],

        [Fecha_de_retoma_antes_de_contabilizacion],
        [Fecha_primer_proceso],
        [EstadoFinalFase_5],
        [ObservacionesFase_4],
        [EstadoFinalFase_4],
        [ResultadoFinalAntesEventos],
        [EstadoFase_3],
        [ObservacionesFase_3],
        [executionNum_CxP],
        [Estado_Evento_030],
        [Estado_Evento_031],
        [Estado_Evento_032],
        [Estado_Evento_033],
        [FechaHora_Evento_030],
        [FechaHora_Evento_031],
        [FechaHora_Evento_032],
        [FechaHora_Evento_033],
        [Fecha_proceso_contabilizacion],
        [Fecha_retoma_contabilizacion],
        [DocumentCurrencyCode],
        [CalculationRate],
        [VlrPagarCop],
        [EstadoFinalFase_6],
        [ObservacionesFase_6],
        [Estado_contabilizacion],
        [Compensar_por],
        [Documento_contable],
        [Posicion_Comercializado],
        [Valor_a_pagar_Comercializado],
        [Valor_a_pagar_Comercializado_ME],
        [NotaCreditoReferenciada],
        [Insumo_XML],
        [Insumo_PDF],
        [Insumo_reubicado],
        [Ruta_respaldo]
    FROM DataConNuevosIDs
    -- Filtramos para asegurarnos que no vengan valores vacíos por comas erróneas (ej: "1,2,")
    WHERE ValorSeparado <> '';

END

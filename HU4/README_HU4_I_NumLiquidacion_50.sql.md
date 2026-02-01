# HU4_I_NumLiquidacion_50.sql

## Descripción General
Este archivo SQL contiene consultas y lógica de base de datos para la **Historia de Usuario 4 (HU4)**.
Propósito: **Lógica SQL para Comercializados (OC 50*).**

## Estructura y Lógica
El script realiza las siguientes operaciones sobre la base de datos (generalmente tablas como `[CxP].[DocumentsProcessing]`):
*   **Selección de Datos:** Identifica registros candidatos para validación.
*   **Actualización de Estados:** Marca registros como 'Aprobado', 'Rechazado' o 'Con Novedad' según las reglas de negocio.
*   **Validaciones Específicas:**
    *   Filtra y procesa reglas específicas para documentos cuya orden de compra inicia por 50.

## Tablas Afectadas
*   `[CxP].[DocumentsProcessing]`
*   `[dbo].[CxP.Comparativa]` (en algunos casos)
*   Tablas maestras de proveedores/SAP.

## Autor
**Diego Ivan Lopez Ochoa**

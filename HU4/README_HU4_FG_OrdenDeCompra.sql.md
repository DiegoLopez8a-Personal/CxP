# HU4_FG_OrdenDeCompra.sql

## Descripción General
Este archivo SQL contiene consultas y lógica de base de datos para la **Historia de Usuario 4 (HU4)**.
Propósito: **Validación SQL de Órdenes de Compra.**

## Estructura y Lógica
El script realiza las siguientes operaciones sobre la base de datos (generalmente tablas como `[CxP].[DocumentsProcessing]`):
*   **Selección de Datos:** Identifica registros candidatos para validación.
*   **Actualización de Estados:** Marca registros como 'Aprobado', 'Rechazado' o 'Con Novedad' según las reglas de negocio.
*   **Validaciones Específicas:**
    *   Compara el valor y datos de la factura contra la Orden de Compra registrada en SAP/Historico.

## Tablas Afectadas
*   `[CxP].[DocumentsProcessing]`
*   `[dbo].[CxP.Comparativa]` (en algunos casos)
*   Tablas maestras de proveedores/SAP.

## Autor
**Diego Ivan Lopez Ochoa**

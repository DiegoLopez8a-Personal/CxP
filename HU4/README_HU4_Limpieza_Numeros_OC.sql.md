# HU4_Limpieza_Numeros_OC.sql

## Descripción General
Este archivo SQL contiene consultas y lógica de base de datos para la **Historia de Usuario 4 (HU4)**.
Propósito: **Script de limpieza de datos.**

## Estructura y Lógica
El script realiza las siguientes operaciones sobre la base de datos (generalmente tablas como `[CxP].[DocumentsProcessing]`):
*   **Selección de Datos:** Identifica registros candidatos para validación.
*   **Actualización de Estados:** Marca registros como 'Aprobado', 'Rechazado' o 'Con Novedad' según las reglas de negocio.
*   **Validaciones Específicas:**
    *   Normaliza y limpia el campo de número de orden de compra (elimina caracteres especiales, espacios) para permitir cruces correctos.

## Tablas Afectadas
*   `[CxP].[DocumentsProcessing]`
*   `[dbo].[CxP.Comparativa]` (en algunos casos)
*   Tablas maestras de proveedores/SAP.

## Autor
**Diego Ivan Lopez Ochoa**

# GenerarReporte_Retorno.py

## Descripción General
Este script es parte de la **Historia de Usuario 4.1 (HU4.1)**, enfocada en la validación avanzada de documentos SAP (ZPAF, ZPSA, ZPSS, ZVEN, etc.).
Función específica: **Generación de insumo de retorno.**

## Lógica de Validación
Implementa reglas de negocio estrictas:
*   **Conexión BD:** Usa `pyodbc` con reintento (SQL/Trusted).
*   **Consulta de Candidatos:** Lee de `[CxP].[HU41_CandidatosValidacion]`.
*   **Validación:**
    *   Genera un archivo Excel/CSV con el resultado del procesamiento para ser devuelto al usuario o sistema origen.
*   **Trazabilidad:** Escribe los resultados detallados en `[dbo].[CxP.Comparativa]`.

## Variables de RocketBot
*   **Entrada:** `vLocDicConfig` (Configuración BD).
*   **Salida:** `vLocStrResultadoSP`, `vGblStrDetalleError`.

## Autor
**Diego Ivan Lopez Ochoa**

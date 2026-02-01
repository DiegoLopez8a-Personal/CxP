# HU4.2_ValidarNC_ND.py

## 📄 Descripción General

Este script implementa la lógica de validación para **Notas Crédito (NC)** y **Notas Débito (ND)**, correspondiente a la Historia de Usuario 4.2.

Su objetivo principal es verificar que estos documentos electrónicos cumplan con las reglas de negocio, tributarias y financieras antes de ser procesados o contabilizados en el sistema. El script cruza información entre la base de datos de recepción de facturas (`[CxP].[DocumentsProcessing]`) y las tablas de trazabilidad (`[CxP].[Comparativa_NC]`, `[CxP].[Comparativa_ND]`).

**Autor:** Diego Ivan Lopez Ochoa

---

## 🚀 Flujo de Ejecución

1.  **Inicialización:**
    *   Conecta a la base de datos SQL Server.
    *   Limpia las tablas de comparativa (`TRUNCATE`).
    *   Puebla las tablas comparativas con los registros pendientes (Snapshot inicial).

2.  **Procesamiento de Notas Crédito (NC):**
    *   **Regla de Retoma:** Verifica que la fecha de retoma no exceda el plazo máximo configurado (ej: 120 días).
    *   **Validaciones Tributarias:**
        *   Emisor/Receptor: NIT, Nombre, Tipo de Persona, Dígito de Verificación.
        *   Receptor esperado: DIANA CORPORACION SAS o DICORP SAS.
        *   Códigos fiscales: `O-13`, `O-15`, `R-99-PN`, etc.
    *   **Referencia a Factura:**
        *   Busca la factura original (`FV`) referenciada por la NC.
        *   Compara el valor de la NC contra el valor de la factura (Tolerancia 0.01).
    *   **Tipos de NC:** Manejo especial para Tipo 20 (sin referencia) vs otros tipos.

3.  **Procesamiento de Notas Débito (ND):**
    *   Aplica validaciones tributarias similares a las NC.
    *   Actualiza el estado a `EXITOSO` si cumple las reglas básicas.

4.  **Reportería:**
    *   Genera un archivo Excel de "Retorno" con las novedades encontradas para gestión manual.

---

## 🛠️ Detalles Técnicos

### Tablas Involucradas

*   `[CxP].[DocumentsProcessing]`: Tabla transaccional principal.
*   `[CxP].[Comparativa_NC]`: Trazabilidad detallada para Notas Crédito.
*   `[CxP].[Comparativa_ND]`: Trazabilidad detallada para Notas Débito.

### Variables de Entrada (RocketBot)

*   `vLocDicConfig`:
    *   `PlazoMaximoRetoma`: Días máximos permitidos para procesar una NC antigua.
    *   `RutaBaseReporteNC`: Ruta para guardar el reporte Excel.
    *   `NombreReporteNC`: Nombre base del reporte.

### Variables de Salida (RocketBot)

*   `vLocStrResultadoSP`: `True` / `False`.
*   `vLocStrResumenSP`: Resumen (ej: "Procesamiento Finalizado. NC: 10, ND: 5").

### Manejo de Fechas

El script incluye una función robusta `calcular_dias_diferencia` que soporta múltiples formatos de fecha (`%Y-%m-%d`, `%d/%m/%Y`) para manejar la variabilidad en los datos de entrada.

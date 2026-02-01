# ejecutar_HU4_I_NumLiquidacion_50_FINALIZE.py

## Descripción General
Este script es parte del componente **HU4** (Validaciones de Campos Obligatorios y Reglas de Negocio) del sistema de automatización de Cuentas por Pagar.
Se encarga específicamente de: **Finalizar validación de números de liquidación (Serie 50).**

El script se ejecuta dentro del entorno de **RocketBot** y utiliza `pyodbc` para interactuar con la base de datos SQL Server.

## Funcionalidades Principales
1.  **Conexión a Base de Datos:** Establece conexión segura a la BD configurada en `vLocDicConfig`.
2.  **Ejecución de Lógica de Negocio:**
    *   Ejecuta procedimientos almacenados o consultas SQL complejas definidas en archivos `.sql` asociados o embebidos.
    *   Cierra el procesamiento para documentos de comercializados (que inician con 50).
3.  **Manejo de Errores:** Captura excepciones, actualiza variables de error de RocketBot (`vGblStrDetalleError`) y asegura el cierre de recursos.

## Variables de RocketBot (Inputs/Outputs)

### Entradas
*   **`vLocDicConfig`**: Diccionario de configuración (JSON) con credenciales de BD (`ServidorBaseDatos`, `NombreBaseDatos`, etc.).
*   **`vLocStrQuery...`** (Opcional): En algunos casos recibe consultas SQL dinámicas.

### Salidas
*   **`vLocStrResultadoSP`**: "True" si la ejecución fue exitosa, "False" si falló.
*   **`vLocStrResumenSP`**: Mensaje descriptivo del resultado.
*   **`vGblStrDetalleError`**: Detalle técnico del error (si ocurre).

## Dependencias
*   `pyodbc`
*   `pandas` (en algunos scripts)
*   `datetime`

## Autor
**Diego Ivan Lopez Ochoa**

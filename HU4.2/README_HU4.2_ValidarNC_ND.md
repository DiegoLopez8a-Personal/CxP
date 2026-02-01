# HU4.2 - Validación Automática de Notas Crédito y Débito (RPA)

## 📄 Descripción General
Este script (`HU4.2_ValidarNC_ND.py`) es un componente de automatización diseñado para la plataforma **Rocketbot**. Su función principal es validar técnica y financieramente las Notas Crédito (NC) y Notas Débito (ND) almacenadas en la base de datos intermedia `[CxP].[DocumentsProcessing]`.

El script realiza cruces de información contra Facturas (FV), valida reglas de negocio (NITs, códigos tributarios, fechas) y genera trazabilidad detallada en tablas espejo y reportes de novedades en Excel.

**Versión:** 4.0 (Estandarizada ZPAF)
**Entorno:** Python 3.x (Integrado en Rocketbot)

---

## 🛠️ Requisitos y Dependencias

### Librerías Python
El script utiliza las siguientes librerías estándar y de terceros:
* `pandas` y `numpy`: Manipulación de datos y cálculos.
* `pyodbc`: Conexión a SQL Server.
* `openpyxl`: Generación y manipulación de reportes Excel.
* `datetime`, `dateutil`: Manejo de fechas y plazos.
* `json`, `ast`: Parsing de configuración.

### Base de Datos (SQL Server)
El script interactúa con las siguientes tablas:
1.  **Origen/Destino:** `[CxP].[DocumentsProcessing]` (Tabla principal de documentos).
2.  **Trazabilidad NC:** `[CxP].[Comparativa_NC]` (Detalle ítem por ítem de las validaciones de NC).
3.  **Trazabilidad ND:** `[CxP].[Comparativa_ND]` (Detalle ítem por ítem de las validaciones de ND).

> **Nota:** El script crea automáticamente las tablas comparativas si no existen, o las limpia (`TRUNCATE`) al inicio de cada ejecución.

---

## ⚙️ Configuración (Entrada)

El script espera recibir una variable de Rocketbot llamada `vLocDicConfig` con un JSON o Diccionario que contenga:

```json
{
  "ServidorBaseDatos": "IP_O_HOSTNAME",
  "NombreBaseDatos": "NOMBRE_BD",
  "UsuarioBaseDatos": "USER",
  "ClaveBaseDatos": "PASSWORD",
  "PlazoMaximoRetoma": 120,
  "RutaBaseReporteNC": "\\\\172.16.250.222\\BOT_Validacion_FV_NC_ND_CXP",
  "NombreReporteNC": "Reporte_Novedades_NC"
}
```

## 🚀 Flujo de Ejecución

### **Inicialización y Limpieza**

Establece conexión a BD (Soporta Autenticación SQL y Windows/Trusted).

Ejecuta TRUNCATE en las tablas [CxP].[Comparativa_NC] y [CxP].[Comparativa_ND] para iniciar con un lienzo limpio.

### **Procesamiento de Notas Crédito (NC)**

#### **Carga Inicial:** 
Lee las NC pendientes y realiza una inserción masiva (Snapshot) en la tabla comparativa con estado "PENDIENTE".

#### **Carga de Facturas:** 
Carga en memoria las Facturas (FV) de los últimos 2 meses para realizar el cruce.

#### **Regla de Retoma:** 

Verifica si la NC ha superado el PlazoMaximoRetoma (ej. 120 días). Si lo excede, se marca como NO EXITOSO.

#### **Validaciones de Datos:** 

1. Verifica la existencia y formato de:
Nombre y NIT del Emisor.

2. Receptor (Validación estricta de nombres como 'DIANACORPORACIONSAS' y NIT '860031606').

3. Códigos tributarios (TaxLevelCode).

#### **Lógica de Cruce (Match):**

1. Tipo 20: Si la NC es tipo 20, se valida que existan los campos CUFE/CUDE pero no se exige referencia cruzada.

2. Otros Tipos: Busca la Factura (FV) coincidente por PrefijoYNumero y NIT.

#### **Validación Monetaria: **

Si encuentra la factura, compara el Valor a Pagar con una tolerancia de 0.01.

**Resultado:**

Si cruza y los montos coinciden: ENCONTRADO.

Si no cruza o hay error de datos: CON NOVEDAD.

### **Reporte de Novedades (Excel)**
Si se encuentran NC con estado CON NOVEDAD:

Busca/Crea la carpeta del mes actual (ej: .../2026/01. Enero/INSUMO DE RETORNO).

Genera o actualiza un archivo Excel agregando las filas con ID, NIT y Número de Documento.

### Procesamiento de Notas Débito (ND)
Carga Inicial: Snapshot masivo en [CxP].[Comparativa_ND].

#### **Validaciones:**

Aplica las mismas reglas tributarias y de datos maestros que en las NC.

**Resultado:** Si cumple las validaciones de campos, se marca como EXITOSO.

## 📊 Salidas del Proceso

### **Base de Datos:**

Actualización de estados en [CxP].[DocumentsProcessing] (Columnas: ResultadoFinalAntesEventos, ObservacionesFase_4, etc.).

Llenado detallado de tablas Comparativa_NC y Comparativa_ND con el resultado de cada validación (SI/NO por campo).

## **Archivos:**

Reporte Excel en ruta de red (Solo si hay novedades).

## **Variables Rocketbot:**

**vLocStrResultadoSP**: "True" si finalizó, "False" si hubo error crítico.

**vGblStrDetalleError**: Detalle del error (Traceback) en caso de fallo.

## ⚠️ Notas Técnicas Importantes

**Sin Tildes**: El código fuente está estrictamente sanitizado para no contener tildes ni caracteres especiales (ñ) en comentarios, variables o nombres de columnas internas para evitar conflictos de codificación (Unicode/ASCII).

**Decimales**: La normalización de moneda maneja tanto punto (.) como coma (,) como separadores decimales.

**Performance**: Utiliza executemany para inserciones masivas y pandas vectorizado para filtros, optimizando el tiempo de ejecución.

**Autor**: Diego Ivan Lopez Ochoa Fecha: Enero 2026
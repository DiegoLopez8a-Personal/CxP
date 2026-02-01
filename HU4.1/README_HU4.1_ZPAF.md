# HU4.1 - Validación de Pedidos de Activos Fijos (ZPAF/41)

## 📄 Descripción General

Este componente de automatización (`HU4.1_ZPAF.py`) está diseñado para ejecutarse dentro de **Rocketbot**. Su objetivo es realizar la validación financiera y tributaria de las Órdenes de Compra (OC) marcadas en SAP como **Activos Fijos** (clases de pedido **ZPAF** o **41**).

El script cruza la información extraída de las facturas (XML/OCR), almacenada en la tabla `DocumentsProcessing`, contra el histórico de órdenes de compra en SAP (`HistoricoOrdenesCompra`), asegurando que los montos, impuestos y clasificaciones contables sean correctos antes de la contabilización.

**Versión:** 1.0 (Estandarizada ZPAF)  
**Entorno:** Python 3.x  (Integrado en Rocketbot) 
**Autor:** Diego Ivan Lopez Ochoa

---

## 🚀 Flujo de Ejecución

### 1. Lectura de Candidatos

- Consulta la vista/tabla `[CxP].[HU41_CandidatosValidacion]`.
- Filtra únicamente los registros donde la `ClaseDePedido` sea **ZPAF** o **41**.

### 2. Expansión de Datos

- Los datos provenientes del histórico de SAP se encuentran concatenados (ejemplo: `10|20|30`).
- El script desglosa estos strings en listas manejables para evaluar cada posición de la orden de compra de forma individual.

### 3. Lógica de Coincidencia (Combinatoria)

- El robot intenta encontrar qué combinación de posiciones de la OC suma exactamente el valor de la factura.

**Reglas de comparación por moneda:**

- **Moneda USD:**
  - Factura: `VlrPagarCop`
  - SAP: `PorCalcular`

- **Moneda Local:**
  - Factura: `Valor de la Compra LEA`
  - SAP: `PorCalcular`

**Tolerancia:** Se permite una diferencia máxima de **500** unidades monetarias.

### 4. Validaciones de Negocio

Si se encuentra coincidencia numérica, se ejecutan las siguientes reglas:

| Regla | Descripción |
| --- | --- |
| **TRM** | Compara la tasa de cambio del XML vs SAP con una tolerancia de **0.01**. |
| **Nombre Emisor** | Normaliza los nombres (elimina SAS, LTDA, signos especiales) y compara palabras clave. |
| **Activo Fijo** | El campo debe contener estrictamente **9 dígitos numéricos**. |
| **Capitalizado el** | Este campo debe estar **vacío** (nulo o en blanco). |
| **Indicador Impuestos** | Valida coherencia de grupos:<br>• **Grupo 1:** H4, H5, VP<br>• **Grupo 2:** H6, H7, VP<br><br>No se permite mezclar indicadores del Grupo 1 con el Grupo 2. |
| **Criterio Clasif. 2** | Debe coincidir con el indicador:<br>• H4 / H5 → `0001`<br>• H6 / H7 → `0000` |
| **Cuenta Contable** | La cuenta debe ser estrictamente `2695950020`. |

### 5. Resultado y Trazabilidad

- Actualiza la tabla `[CxP].[DocumentsProcessing]` con el estado final del documento:
  - `EXITOSO`
  - `CON NOVEDAD`
  - `PROCESADO`
- Inserta el detalle de cada validación (ítem por ítem) en la tabla `[dbo].[CxP.Comparativa]`.
- Marca las órdenes procesadas en el histórico para evitar reprocesos.

---

## 🛠️ Requisitos Técnicos

### Librerías Python

- `pandas`, `numpy`
- `pyodbc`
- `itertools`
- `datetime`, `time`
- `re`, `unicodedata`

### Base de Datos

- `[CxP].[HU41_CandidatosValidacion]`
- `[CxP].[DocumentsProcessing]`
- `[CxP].[HistoricoOrdenesCompra]`
- `[dbo].[CxP.Comparativa]`

---

## ⚙️ Configuración (Input)

```json
{
  "ServidorBaseDatos": "IP_O_HOSTNAME",
  "NombreBaseDatos": "NOMBRE_BD",
  "UsuarioBaseDatos": "USER",
  "ClaveBaseDatos": "PASSWORD"
}
```

---

## ⚠️ Notas de Mantenimiento

- Código sin tildes ni caracteres especiales.
- Uso de `zip_longest` para evitar desalineación de listas.
- Observaciones concatenadas para mantener historial.

---

**Autor:** Diego Ivan Lopez Ochoa  
**Fecha:** Enero 2026

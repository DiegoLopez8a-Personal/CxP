# HU4.1 - Validación de Pedidos de Servicios (ZPSA / ZPSS / 43)

## 📄 Descripción General

Este componente de automatización (`HU4.1_ZPSA_ZPSS.py`) está diseñado para ejecutarse dentro de **Rocketbot**. Su función principal es validar técnica y financieramente las **Órdenes de Compra de Servicios** (clases de pedido **ZPSA**, **ZPSS** o **43**) almacenadas en la base de datos intermedia `[CxP].[DocumentsProcessing]`.

El script realiza cruces de información entre los datos extraídos de la factura (XML / OCR) y el histórico de órdenes de compra en SAP (`HistoricoOrdenesCompra`), aplicando reglas de negocio diferenciadas según el tipo de imputación: **Orden**, **Elemento PEP**, **Activo Fijo** o **Gasto General**.

**Versión:** 1.0 (Estandarizada Servicios)  
**Entorno:** Python 3.x (Integrado en Rocketbot)  
**Autor:** Diego Ivan Lopez Ochoa

---

## 🚀 Flujo de Ejecución y Lógica de Negocio

El script implementa un **árbol de decisión jerárquico** para validar los datos de cada documento.

### 1. Validación Matemática (Combinatoria)

- Antes de aplicar reglas de negocio, el robot valida que los montos coincidan.
- Utiliza lógica combinatoria para identificar qué posiciones de la Orden de Compra suman el valor total de la factura.
- **Tolerancia:** Se permite una diferencia máxima de **500** unidades monetarias.

### 2. Rutas de Validación (Árbol de Decisión)

Dependiendo de la estructura de imputación encontrada en SAP, se activa una de las siguientes rutas:

---

### A. Ruta: Tiene Orden (`Orden_hoc`)

#### A.1 Orden 15 (Inicia con `15` y tiene 9 dígitos)

- **Indicador de Impuestos:** Solo permite `H4`, `H5`, `H6`, `H7`, `VP`, `CO`, `IC`, `CR`.
- **Centro de Coste:** Debe estar **vacío**.
- **Cuenta Contable:** Debe ser estrictamente `5199150001`.
- **Clase de Orden:** Valida coherencia (`ZINV` vs `ZADM`) según el indicador de impuestos.

#### A.2 Orden 53 (Inicia con `53` y tiene 8 dígitos – Estadísticas)

- **Centro de Coste:** Debe estar **diligenciado**.

#### A.3 Otras Órdenes

- **Centro de Coste:** Debe estar **vacío**.
- **Cuenta Contable:** Debe ser `5299150099` **o** iniciar con `7` (10 dígitos).

---

### B. Ruta: Tiene Elemento PEP (No tiene Orden)

- **Indicador de Impuestos:** Solo permite `H4`, `H5`, `H6`, `H7`, `VP`, `CO`, `IC`, `CR`.
- **Centro de Coste:** Debe estar **vacío**.
- **Cuenta Contable:** Debe ser estrictamente `5199150001`.
- **Emplazamiento:** Se valida según el indicador de impuestos  
  - Regla: `DCTO_01` vs `GTO_02`.

---

### C. Ruta: Tiene Activo Fijo (No tiene Orden ni PEP)

#### C.1 Activo Diferido (Inicia con `2000`)

- **Indicador de Impuestos:** Solo permite `C1`, `FA`, `VP`, `CO`, `CR`.
- **Centro de Coste:** Debe estar **vacío**.
- **Cuenta Contable:** Debe estar **vacía**.

---

### D. Ruta: Generales (Sin imputación específica)

- **Regla Base:**  
  - Cuenta Contable  
  - Indicador de Impuestos  
  - Centro de Coste  
  
  Todos deben estar **diligenciados**.

- **Validación Cruzada:**  
  Se carga el archivo Excel maestro `Impuestos especiales CXP.xlsx` para validar que el Indicador de Impuestos corresponda al Centro de Coste configurado.

---

## 📊 Salidas del Proceso

### 1. Base de Datos – `[CxP].[DocumentsProcessing]`

- Actualiza el estado final del documento:
  - `EXITOSO`
  - `CON NOVEDAD`
  - `PROCESADO`
- Registra observaciones técnicas detallando el motivo del fallo  
  (ejemplo: *"Centro de Coste diligenciado cuando no debe estarlo"*).

### 2. Trazabilidad – `[dbo].[CxP.Comparativa]`

- Inserta el detalle **ítem por ítem** de cada validación ejecutada (resultado **SI / NO**).

### 3. Histórico – `[CxP].[HistoricoOrdenesCompra]`

- Marca las posiciones de la Orden de Compra como `PROCESADO` para evitar duplicidad en ejecuciones futuras.

---

## ⚙️ Configuración (Entrada)

El script requiere una variable de Rocketbot llamada `vLocDicConfig` con la siguiente estructura JSON:

```json
{
  "ServidorBaseDatos": "IP_O_HOSTNAME",
  "NombreBaseDatos": "NOMBRE_BD",
  "UsuarioBaseDatos": "USER",
  "ClaveBaseDatos": "PASSWORD",
  "RutaImpuestosEspeciales": "C:\\Ruta\\Al\\Archivo\\Impuestos especiales CXP.xlsx"
}
```

---

## ⚠️ Notas Técnicas Importantes

- **Sin tildes:** El código fuente ha sido sanitizado para eliminar tildes y caracteres especiales (como `ñ`), garantizando compatibilidad con SQL Server y entornos Windows con diferentes codificaciones.
- **Zip Longest:** Se utiliza `itertools.zip_longest` para iterar listas (por ejemplo, Indicadores vs Centros de Coste) de forma segura cuando SAP devuelve arreglos de diferente longitud.
- **Concatenación de observaciones:** Las nuevas observaciones se agregan al final del campo existente, preservando el historial completo de validaciones del documento.

---

**Autor:** Diego Ivan Lopez Ochoa  
**Fecha:** Enero 2026

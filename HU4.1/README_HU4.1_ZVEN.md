# HU4.1 - Validación de Pedidos Comercializados (ZVEN / 50)

## 📄 Descripción General

Este componente de automatización (`HU4.1_ZVEN.py`) está diseñado para procesar las **Órdenes de Compra de tipo Comercializados** (clases de pedido **ZVEN** o **50**) dentro de la plataforma **Rocketbot**.

A diferencia de otros flujos de la HU4.1, este bot no depende únicamente de la validación contra SAP, sino que utiliza un **Archivo Maestro de Comercializados** externo como punto principal de decisión para determinar si un pedido puede ser procesado o debe quedar en espera.

**Versión:** 1.0 (Integración Maestro Comercializados)  
**Entorno:** Python 3.x (Integrado en Rocketbot)  
**Autor:** Diego Ivan Lopez Ochoa

---

## 🚀 Flujo de Ejecución y Lógica de Negocio

El robot sigue un flujo **secuencial y determinístico** para cada registro procesado.

### 1. Búsqueda en Maestro (Punto de Decisión)

El bot busca la combinación de **Número de Orden de Compra (OC)** y **Número de Factura** dentro del archivo Excel **"Maestro de Comercializados"**.

#### Escenario A: No se encuentra en el Maestro

- **Acción:** El registro se considera **no habilitado** para validación.
- **Gestión de Archivos:**  
  Los archivos asociados (PDF / XML) se mueven a la carpeta de **En Espera** (`.../INSUMO`).
- **Estado Final en BD:** `EN ESPERA - COMERCIALIZADOS`
- **Observación registrada:**  
  *"No se encuentran datos de la orden de compra y factura en el archivo Maestro de Comercializados"*.

#### Escenario B: Se encuentra en el Maestro

- **Acción:** El registro continúa con la validación financiera y técnica.
- **Carga de Datos:**  
  Se extraen del Excel maestro las posiciones, valores unitarios y valores en moneda extranjera (ME) esperados para la validación.

---

### 2. Validaciones de Negocio (Aplica solo si existe en Maestro)

Una vez confirmado que el registro existe en el Maestro, se ejecutan las siguientes validaciones cruzadas entre **Factura (XML / OCR)**, **SAP** y **Maestro de Comercializados**:

| Validación | Descripción |
| --- | --- |
| **Coincidencia de Valor** | Suma los valores del Maestro (unitario o ME) y los compara con el valor total de la factura. **Tolerancia:** 500. |
| **Coincidencia de Posiciones** | Valida que las posiciones indicadas en el Maestro existan en el histórico de SAP. |
| **TRM** | Para moneda extranjera (USD), compara la Tasa de Cambio entre XML y SAP. **Tolerancia:** 0.01. |
| **Cantidad y Precio** | Compara línea por línea la cantidad y el precio unitario entre Factura y SAP. |
| **Nombre Emisor** | Normaliza y compara el nombre del proveedor entre XML y SAP. |

---

### 3. Resultado del Proceso

- **EXITOSO:**  
  Todas las validaciones cruzan correctamente.  
  **Estado:** `PROCESADO`

- **CON NOVEDAD:**  
  Alguna validación falla (TRM, precio, cantidad, nombre, etc.).  
  **Estado:** `CON NOVEDAD - COMERCIALIZADOS`

---

## 🛠️ Requisitos de Insumos (Archivos Excel)

### 1. Maestro de Comercializados

Debe contener obligatoriamente las siguientes columnas:

- `OC`
- `FACTURA`
- `VALOR TOTAL OC`
- `POSICION`
- `POR CALCULAR (VALOR UNITARIO)`
- `POR CALCULAR (ME)`

### 2. Asociación Cuenta – Indicador

Archivo utilizado para validaciones contables auxiliares.  
Debe contener la hoja **"Grupo cuentas agrupacion provee"** con las columnas:

- `CTA MAYOR`
- `NOMBRE CUENTA`
- `TIPO RET.`
- `IND.RETENCION`
- `AGRUPACION CODIGO`

---

## ⚙️ Configuración (Entrada)

El script requiere la variable de Rocketbot `vLocDicConfig` con la siguiente estructura JSON:

```json
{
  "ServidorBaseDatos": "IP_SERVIDOR",
  "NombreBaseDatos": "NOMBRE_BD",
  "UsuarioBaseDatos": "USER",
  "ClaveBaseDatos": "PASS",
  "RutaInsumosComercializados": "C:\\Ruta\\Maestro_Comercializados.xlsx",
  "RutaInsumoAsociacion": "C:\\Ruta\\Asociacion_Cuentas.xlsx",
  "CarpetaDestinoComercializados": "C:\\Ruta\\Destino_En_Espera"
}
```

---

## 📊 Salidas del Proceso

### Base de Datos – `[CxP].[DocumentsProcessing]`

- Actualiza:
  - `EstadoFinalFase_4`
  - `ObservacionesFase_4`
  - `ResultadoFinalAntesEventos`
- Si el registro queda en espera, actualiza la columna `RutaArchivo` con la nueva ubicación.
- Persiste información del maestro en campos como:
  - `Posicion_Comercializado`
  - `Valor_a_pagar_Comercializado`
  - entre otros.

### Trazabilidad – `[dbo].[CxP.Comparativa]`

- Inserta el detalle de cada validación ejecutada.
- Para registros en espera, almacena la observación de **no encontrado en Maestro**.

### Gestión de Archivos

- Mueve físicamente los archivos (PDF / XML) cuando el registro queda en estado **EN ESPERA**.

---

**Autor:** Diego Ivan Lopez Ochoa  
**Fecha:** Enero 2026

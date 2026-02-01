# ZPRE_ValidarUSD.py

## 📄 Descripción General

Este script se encarga de la **validación de montos en moneda local (COP)** para pedidos de tipo **ZPRE** (Recepción de Servicios) y **45** que son facturados en **Moneda Extranjera (USD)**.

El objetivo es asegurar que la conversión de moneda y los valores base en SAP sean coherentes con el valor en pesos reportado en la factura digital. Compara el valor `PorCalcular` (interpretado como el valor convertido en SAP) contra el valor `VlrPagarCop` (valor explícito en pesos del XML).

**Autor:** Diego Ivan Lopez Ochoa

---

## 🚀 Flujo de Ejecución

1.  **Selección de Candidatos:**
    *   Consulta `[CxP].[HU41_CandidatosValidacion]`.
    *   Filtra pedidos `ZPRE`, `45`.
    *   **Filtro Crítico:** Filtra registros donde `Moneda_hoc` contenga **"USD"**.
2.  **Validación de Montos:**
    *   Suma `PorCalcular_hoc` (SAP).
    *   Suma `VlrPagarCop_dp` (XML - Valor Pesos).
    *   Calcula diferencia absoluta.
3.  **Resultado:**
    *   **Diferencia <= Tolerancia (500 COP):** APROBADO.
    *   **Diferencia > Tolerancia:** CON NOVEDAD.
        *   Observación: *"No se encuentra coincidencia del Valor a pagar COP de la factura"*.
        *   Actualiza estado en BD y marca ítems en comparativa.

---

## 🛠️ Detalles Técnicos

### Variables de Entrada (RocketBot)

*   `vLocDicConfig`:
    *   `Tolerancia`: Margen de error (Default: 500).

### Variables de Salida (RocketBot)

*   `vLocStrResultadoSP`: `True` / `False`.
*   `vLocStrResumenSP`: Resumen.

### Importancia de VlrPagarCop

En facturas internacionales, el campo `VlrPagarCop` es vital porque representa la obligación legal en moneda local. SAP puede tener un valor estimado basado en una TRM promedio o del día anterior, por lo que esta validación asegura que la diferencia entre la estimación de SAP y la realidad de la factura no supere un umbral aceptable.

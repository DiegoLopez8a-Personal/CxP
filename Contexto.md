Archivo que contiene el contexto de lo que se muestra en las Historias de usuario pero que realmente hacen enfoque a otra cosa.

# Contexto general:
- Al momento de referirse a las pocisiones dentro de las historias de usuario, se hace referencia a cada uno de los objetos que puedan aparecer en los campos del registros dentro de la tabla [CxP].[HU41_CadidatosValidacion] en esta tabla los campos pueden tener varios valores y estar separados por "|" entonces si un campo tiene 4 valores separados por "|" se entendera que tiee 4 pocisiones.

- Al momento de generar los registros en la tabla [dbo].[CxP.Comparativa] se deben verificar cuantos Itemsa con la misma descripcion ya se encuentran creados, por ejemplo si se identifican que hay 4 pocisiones y ya se encuentran 2 Items creados para "Clase de Impuesto" (por ejemplo), entonces solo sera necesario crear 2 mas y agregar los valores dentro de las columnas que se indican.

- Tener en cuenta que, en CADA VALIDACION cuyo resultado sea "CON
NOVEDAD" se deberá validar si el campo "forma_de_pago_dp" es igual a 01 o 1 y
adicionar la palabra CONTADO en caso de que, SI SEA IGUAL, ejemplo "CON
NOVEDAD CONTADO". Si NO es igual a 01 o 1 seria CON NOVEDAD

- Ten presente que los campos pueden estar NULL o vacios dependiendo de la validacion esto podria ser un caso malo o bueno.

# Revision del flujo de el Script HU4.1 ZPAF

La idea es que revisemos el script que esta realizado en python y en caso de requerirse hacer modificaciones se realicen, si no es necesario realizar modificaciones entonces simplemente Dejar el Script completamente documentado, con comentarios entre lineas y con un docstring detallado en estilo google.

## Historia de usuario.
1. Si corresponde a ZPAF:
i. Nota:
1. Actualmente corresponde a OC 41 = Pedido Activos Fijos.
2. Tener en cuenta que, en CADA VALIDACION cuyo resultado sea "CON
NOVEDAD" se deberá validar si el campo "PaymentMeans" es igual a 01 y
adicionar la palabra CONTADO en caso de que, SI SEA IGUAL, ejemplo "CON
NOVEDAD CONTADO". Si NO es igual a 01, deja la marca como se indica en
cada paso.
a. Validar si el campo "Moneda" del HISTORICO DE ORDENES DE COMPRA para la OC y NIT
consultadas, contiene el dato USD en la o las posiciones encontradas (si cuenta con varias).
i. Si cuenta con el dato = USD:
1. Validar COINCIDENCIA de los campos valor_a_pagar_dp de BD CXP vs “PorCalcular_hoc”
de HISTORICO DE ORDENES DE COMPRA, teniendo como rango de variación
permitido 500 por encima y por debajo del valor informado en el histórico, según
los siguientes escenarios:
a. Cuando el resultado de consulta de OC y NIT en el HISTORICO DE ORDENES DE
COMPRA arroje solo 1 Posición:
i. Si se encuentra COINCIDENCIA de VlrPagarCop exitosamente:
1. Se deberá marcar la posición en el HISTORICO DE ORDENES DE
COMPRA con la marca PROCESADO para que no sea tomado para
siguientes facturas.
2. Generar trazabilidad en tabla comparativa, de la siguiente manera:
a. COLUMNA ITEM (se crean los siguientes datos por cada
posición encontrada en el HISTORICO DE ORDENES DE
COMPRA):
i. Crear el item Posición
ii. Crear el item Valor PorCalcular_hoc SAP
iii. Crear el item Tipo NIF
iv. Crear el item Acreedor
v. Crear el item Fec.Doc
vi. Crear el item Fec.Reg
vii. Crear el item Fecha. cont gasto
viii. Crear el item Indicador impuestos
ix. Crear el item Texto breve
x. Crear el item Clase de impuesto
xi. Crear el item Cuenta
xii. Crear el item Ciudad proveedor
xiii. Crear el item DOC.FI.ENTRADA
xiv. Crear el item CTA 26
b. COLUMNA VALOR XML:
i. Línea de Item "LineExtensionAmount": se diligencia
con el dato del campo Valor a pagar que se encuentra
en BD CXP.
ii. Línea de Item "VlrPagarCop": se diligencia con el dato
del campo VlrPagarCop que se encuentra en BD CXP.
c. COLUMNA VALOR ORDEN DE COMPRA:
i. Línea de Item "VlrPagarCop": se diligencia con el dato
del campo PorCalcular_hoc de la posición que se
encuentra en el HISTORICO DE ORDENES DE COMPRA.
ii. Las líneas de item Posición: se diligencian con el
dato Posición de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA.
iii. Las líneas de item Valor PorCalcular_hoc SAP: se
diligencian con el dato PorCalcular_hoc de acuerdo con las
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 1/19
posiciones encontradas en el HISTÓRICO DE ÓRDENES
DE COMPRA.
iv. Las líneas de item Tipo NIF: se diligencian con el
dato Tipo NIF de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
v. Las líneas de item Acreedor: se diligencian con el
dato Acreedor de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
vi. Las líneas de item Fec.Doc: se diligencian con el
dato Fec.Doc de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
vii. Las líneas de item Fec.Reg: se diligencian con el
dato Fec.Reg de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
viii. Las líneas de item Fecha. cont gasto: se diligencian
con el dato Fecha. cont gasto de acuerdo con las
posiciones encontradas en el HISTÓRICO DE ÓRDENES
DE COMPRA
ix. Las líneas de item Indicador impuestos: se diligencian
con el dato Indicador impuestos de acuerdo con las
posiciones encontradas en el HISTÓRICO DE ÓRDENES
DE COMPRA
x. Las líneas de item Texto breve: se diligencian con el
dato Texto breve de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
xi. Las líneas de item Clase de impuesto: se diligencian
con el dato Clase de impuesto de acuerdo con las
posiciones encontradas en el HISTÓRICO DE ÓRDENES
DE COMPRA
xii. Las líneas de item Cuenta: se diligencian con el
dato Cuenta de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
xiii. Las líneas de item Ciudad proveedor: se diligencian
con el dato Ciudad proveedor de acuerdo con las
posiciones encontradas en el HISTÓRICO DE ÓRDENES
DE COMPRA
xiv. Las líneas de item DOC.FI.ENTRADA: se diligencian
con el dato DOC.FI.ENTRADA de acuerdo con las
posiciones encontradas en el HISTÓRICO DE ÓRDENES
DE COMPRA
xv. Las líneas de item CTA 26: se diligencian con el
dato CTA 26 de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
d. COLUMNA APROBADO:
i. Línea de Item "VlrPagarCop": se diligencia con la
marca igual a SI.
3. Se deberá continuar con la siguiente VALIDACION.
ii. Si NO se encuentra COINCIDENCIA de VlrPagarCop exitosamente: 
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 2/19
1. Se deberá marcar registro en BD CXP como:
a. Estado Fase 4 - VALIDACION DATOS DE
FACTURACIÓN: Exitoso.
b. Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN: Diligenciar "No se encuentra coincidencia
del Valor a pagar de la factura", conservando las
observaciones previas que cuente el campo, separando por
comas, pero primando la última observación que se realiza.
c. Resultado Final registro antes de Eventos: CON NOVEDAD.
2. Generar trazabilidad en tabla comparativa, de la siguiente manera:
a. COLUMNA VALOR XML:
i. Línea de Item "LineExtensionAmount": se diligencia
con el dato del campo Valor a pagar que se encuentra
en BD CXP.
ii. Línea de Item "VlrPagarCop": se diligencia con el dato
del campo VlrPagarCop que se encuentra en BD CXP.
iii. Línea de Item "Observaciones": se diligencia con el
dato del campo Observaciones Fase 4 - VALIDACION
DATOS DE FACTURACIÓN que se encuentra en BD
CXP, ejemplo: No se encuentra coincidencia del
Valor a pagar de la factura.
b. COLUMNA VALOR ORDEN DE COMPRA:
i. Línea de Item "VlrPagarCop": se diligencia con
el dato NO ENCONTRADO
c. COLUMNA APROBADO:
i. Línea de Item "VlrPagarCop": se diligencia con la
marca igual a NO.
d. COLUMNA ESTADO:
i. se diligencia con el dato del campo Resultado Final
registro antes de Eventos que se encuentra en BD
CXP, ejemplo: CON NOVEDAD.
3. Se deberá continuar las validaciones con el siguiente registro de la
misma Clase de pedido, si NO existen más registros de la misma
clase, validar los registros con la siguiente Clase de pedido.
b. Cuando el resultado de consulta de OC y NIT en el HISTORICO DE ORDENES DE
COMPRA arroje más de 1 Posición:
i. Se combina los valores del campo "PorCalcular_hoc" que existan
correspondientes al Nit y Orden de compra consultada hasta encontrar el
valor del campo VlrPagarCop del registro en BD CXP.
1. Si se encuentra COINCIDENCIA de Valor a Pagar exitosamente:
1. Se deberá marcar las posiciones en el HISTORICO DE
ORDENES DE COMPRA con la marca PROCESADO para que
no sea tomado para siguientes facturas.
2. Generar trazabilidad en tabla comparativa, de la siguiente
manera:
i. COLUMNA ITEM (se crean los siguientes datos por cada
posición encontrada en el HISTORICO DE ORDENES DE
COMPRA):
1. Crear los item Posición según la cantidad de
items "Posición" que se encuentren
en el HISTORICO DE ORDENES DE
COMPRA.
2. Crear los item Valor PorCalcular_hoc SAP según la
cantidad de items "Posición" que se encuentren
en la tabla comparativa.
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 3/19
3. Crear los item Tipo NIF según la cantidad de
items "Posición" que se encuentren en la tabla
comparativa.
4. Crear los item Acreedor según la cantidad de
items "Posición" que se encuentren en la tabla
comparativa.
5. Crear los item Fec.Doc según la cantidad de
items "Posición" que se encuentren en la tabla
comparativa.
6. Crear los item Fec.Reg según la cantidad de
items "Posición" que se encuentren en la tabla
comparativa.
7. Crear los item Fecha. cont gasto según la
cantidad de items "Posición" que se encuentren
en la tabla comparativa.
8. Crear los item Indicador impuestos según la
cantidad de items "Posición" que se encuentren
en la tabla comparativa.
9. Crear los item Texto breve según la cantidad de
items "Posición" que se encuentren en la tabla
comparativa.
10. Crear los item Clase de impuesto según la
cantidad de items "Posición" que se encuentren
en la tabla comparativa.
11. Crear los item Cuenta según la cantidad de
items "Posición" que se encuentren en la tabla
comparativa.
12. Crear los item Ciudad proveedor según la
cantidad de items "Posición" que se encuentren
en la tabla comparativa.
13. Crearlos item DOC.FI.ENTRADA según la
cantidad de items "Posición" que se encuentren
en la tabla comparativa.
14. Crear los item CTA 26 según la cantidad de
items "Posición" que se encuentren en la tabla
comparativa.
ii. COLUMNA VALOR XML:
1. Línea de Item "LineExtensionAmount": se
diligencia con el dato del campo Valor a
pagar que se encuentra en BD CXP.
2. Línea de Item "VlrPagarCop": se diligencia con
el dato del campo VlrPagarCop que se
encuentra en BD CXP.
iii. COLUMNA VALOR ORDEN DE COMPRA:
1. Línea de Item "VlrPagarCop": se diligencia con
la sumatoria del dato del campo PorCalcular_hoc de
las posiciones que se encuentran en
el HISTORICO DE ORDENES DE COMPRA.
2. Las líneas de item Posición: se diligencian con el
dato Posición de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA.
3. Las líneas de item Valor PorCalcular_hoc SAP: se
diligencian con el dato PorCalcular_hoc de acuerdo
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 4/19
con las posiciones encontradas en el HISTÓRICO
DE ÓRDENES DE COMPRA.
4. Las líneas de item Tipo NIF: se diligencian con el
dato Tipo NIF de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
5. Las líneas de item Acreedor: se diligencian con
el dato Acreedor de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
6. Las líneas de item Fec.Doc: se diligencian con el
dato Fec.Doc de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
7. Las líneas de item Fec.Reg: se diligencian con el
dato Fec.Reg de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
8. Las líneas de item Fecha. cont gasto: se
diligencian con el dato Fecha. cont gasto de
acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
9. Las líneas de item Indicador impuestos: se
diligencian con el dato Indicador impuestos de
acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
10. Las líneas de item Texto breve: se diligencian
con el dato Texto breve de acuerdo con las
posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA
11. Las líneas de item Clase de impuesto: se
diligencian con el dato Clase de impuesto de
acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
12. Las líneas de item Cuenta: se diligencian con el
dato Cuenta de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
13. Las líneas de item Ciudad proveedor: se
diligencian con el dato Ciudad proveedor de
acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
14. Las líneas de item DOC.FI.ENTRADA: se
diligencian con el dato DOC.FI.ENTRADA de
acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
15. Las líneas de item CTA 26: se diligencian con el
dato CTA 26 de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA
iv. COLUMNA APROBADO:
1. Línea de Item "VlrPagarCop": se diligencia con
la marca igual a SI.
3. Se deberá continuar con la siguiente VALIDACION.
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 5/19
2. Si NO se encuentra
COINCIDENCIA de VlrPagarCop exitosamente: 
a. Se deberá marcar registro en BD CXP como:
i. Estado Fase 4 - VALIDACION DATOS DE
FACTURACIÓN: Exitoso.
ii. Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN: Diligenciar "No se encuentra
coincidencia del Valor a pagar de la
factura", conservando las observaciones previas que
cuente el campo, separando por comas, pero primando
la última observación que se realiza.
iii. Resultado Final registro antes de Eventos: CON
NOVEDAD.
b. Generar trazabilidad en tabla comparativa, de la siguiente
manera:
i. COLUMNA VALOR XML:
1. Línea de Item "LineExtensionAmount": se
diligencia con el dato del campo Valor a
pagar que se encuentra en BD CXP.
2. Línea de Item "VlrPagarCop": se diligencia con
el dato del campo VlrPagarCop que se
encuentra en BD CXP.
3. Línea de Item "Observaciones": se diligencia
con el dato del campo Observaciones Fase 4 -
VALIDACION DATOS DE FACTURACIÓN que se
encuentra en BD CXP, ejemplo: No se
encuentra coincidencia del Valor a pagar de
la factura.
ii. COLUMNA VALOR ORDEN DE COMPRA:
1. Línea de Item "VlrPagarCop": se diligencia con
el dato NO ENCONTRADO
iii. COLUMNA APROBADO:
1. Línea de Item "VlrPagarCop": se diligencia con
la marca igual a NO.
iv. COLUMNA ESTADO:
1. se diligencia con el dato del campo Resultado
Final registro antes de Eventos que se
encuentra en BD CXP, ejemplo: CON NOVEDAD.
c. Se deberá continuar las validaciones con el siguiente registro
de la misma Clase de pedido, si NO existen más registros
de la misma clase, validar los registros con la siguiente Clase
de pedido.
ii. Si NO cuenta con el dato = USD:
1. Continuar con el paso b "Validar COINCIDENCIA de los campos Valor a Pagar de BD CXP
vs PorCalcular_hoc de HISTORICO DE ORDENES DE COMPRA".
b. Validar COINCIDENCIA de los campos Valor a Pagar de BD CXP vs PorCalcular_hoc de HISTORICO
DE ORDENES DE COMPRA, teniendo como rango de variación permitido 500 por encima y por
debajo del valor informado en el histórico, según los siguientes escenarios:
i. Cuando el resultado de consulta de OC y NIT en el HISTORICO DE ORDENES DE COMPRA
arroje solo 1 Posición:
1. Si se encuentra COINCIDENCIA de Valor a Pagar exitosamente:
a. Se deberá marcar la posición en el HISTORICO DE ORDENES DE COMPRA con la
marca PROCESADO para que no sea tomado para siguientes facturas.
b. Generar trazabilidad en tabla comparativa, de la siguiente manera:
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 6/19
i. COLUMNA ITEM (se crean los siguientes datos por cada posición encontrada
en el HISTORICO DE ORDENES DE COMPRA):
1. Crear el item Posición
2. Crear el item Valor PorCalcular_hoc SAP
3. Crear el item Tipo NIF
4. Crear el item Acreedor
5. Crear el item Fec.Doc
6. Crear el item Fec.Reg
7. Crear el item Fecha. cont gasto
8. Crear el item Indicador impuestos
9. Crear el item Texto breve
10. Crear el item Clase de impuesto
11. Crear el item Cuenta
12. Crear el item Ciudad proveedor
13. Crear el item DOC.FI.ENTRADA
14. Crear el item CTA 26
15. Crear el item Activo fijo
16. Crear el item Capitalizado el
17. Crear el item Criterio clasif. 2
ii. COLUMNA VALOR XML:
1. Línea de Item "LineExtensionAmount": se diligencia con el dato
del campo Valor a Pagar que se encuentra en BD CXP,
ejemplo: 18871381. Si ya existe omitir.
iii. COLUMNA VALOR ORDEN DE COMPRA:
1. Línea de Item "LineExtensionAmount": se diligencia con el dato
del campo PorCalcular_hoc de la posición que se encuentra en
el HISTORICO DE ORDENES DE COMPRA, ejemplo: 18871381. Si ya
existe omitir.
2. Las líneas de item Posición: se diligencian con el dato Posición de
acuerdo con las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA.
3. Las líneas de item Valor PorCalcular_hoc SAP: se diligencian con el
dato PorCalcular_hoc de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA.
4. Las líneas de item Tipo NIF: se diligencian con el dato Tipo NIF de
acuerdo con las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA
5. Las líneas de item Acreedor: se diligencian con el dato Acreedor de
acuerdo con las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA
6. Las líneas de item Fec.Doc: se diligencian con el dato Fec.Doc de
acuerdo con las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA
7. Las líneas de item Fec.Reg: se diligencian con el dato Fec.Reg de
acuerdo con las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA
8. Las líneas de item Fecha. cont gasto: se diligencian con el
dato Fecha. cont gasto de acuerdo con las posiciones encontradas
en el HISTÓRICO DE ÓRDENES DE COMPRA
9. Las líneas de item Indicador impuestos: se diligencian con el
dato Indicador impuestos de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA
10. Las líneas de item Texto breve: se diligencian con el dato Texto
breve de acuerdo con las posiciones encontradas en el HISTÓRICO
DE ÓRDENES DE COMPRA
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 7/19
11. Las líneas de item Clase de impuesto: se diligencian con el
dato Clase de impuesto de acuerdo con las posiciones encontradas
en el HISTÓRICO DE ÓRDENES DE COMPRA
12. Las líneas de item Cuenta: se diligencian con el dato Cuenta de
acuerdo con las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA
13. Las líneas de item Ciudad proveedor: se diligencian con el
dato Ciudad proveedor de acuerdo con las posiciones encontradas
en el HISTÓRICO DE ÓRDENES DE COMPRA
14. Las líneas de item DOC.FI.ENTRADA: se diligencian con el
dato DOC.FI.ENTRADA de acuerdo con las posiciones encontradas
en el HISTÓRICO DE ÓRDENES DE COMPRA
15. Las líneas de item CTA 26: se diligencian con el dato CTA 26 de
acuerdo con las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA
16. Las líneas de item Activo fijo: se diligencian con el dato Activo
fijo de acuerdo con las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA
17. Las líneas de item Capitalizado el: se diligencian con el
dato Capitalizado el de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
18. Las líneas de item Criterio clasif. 2: se diligencian con el
dato Criterio clasif. 2 de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
iv. COLUMNA APROBADO:
1. Línea de Item "LineExtensionAmount": se diligencia con la marca
igual a SI.
c. Se deberá continuar con la siguiente VALIDACION.
2. Si NO se encuentra COINCIDENCIA de Valor a Pagar exitosamente: 
1. Se deberá marcar registro en BD CXP como:
a. Estado Fase 4 - VALIDACION DATOS DE FACTURACIÓN: Exitoso.
b. Observaciones Fase 4 - VALIDACION DATOS DE FACTURACIÓN:
Diligenciar "No se encuentra coincidencia del Valor a pagar de la
factura", conservando las observaciones previas que cuente el campo,
separando por comas, pero primando la última observación que se realiza.
c. Resultado Final registro antes de Eventos: CON NOVEDAD.
2. Generar trazabilidad en tabla comparativa, de la siguiente manera:
a. COLUMNA VALOR XML:
i. Línea de Item "LineExtensionAmount": se diligencia con el dato
del campo Valor a Pagar que se encuentra en BD CXP,
ejemplo: 18871381. Si ya existe omitir.
ii. Línea de Item "Observaciones": se diligencia con el dato del
campo Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN que se encuentra en BD CXP, ejemplo: No se
encuentra coincidencia del Valor a pagar de la factura.
b. COLUMNA VALOR ORDEN DE COMPRA:
i. Línea de Item "LineExtensionAmount": se diligencia con
el dato NO ENCONTRADO
c. COLUMNA APROBADO:
i. Línea de Item "LineExtensionAmount": se diligencia con la marca
igual a NO.
d. COLUMNA ESTADO:
i. se diligencia con el dato del campo Resultado Final registro antes
de Eventos que se encuentra en BD CXP, ejemplo: CON NOVEDAD.
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 8/19
3. Se deberá continuar las validaciones con el siguiente registro de la misma Clase
de pedido, si NO existen más registros de la misma clase, validar los registros
con la siguiente Clase de pedido.
ii. Cuando el resultado de consulta de OC y NIT en el HISTORICO DE ORDENES DE COMPRA
arroje más de 1 Posición:
1. Se combina los valores del campo "PorCalcular_hoc" que existan correspondientes
al Nit y Orden de compra consultada hasta encontrar el valor del campo Valor a Pagar
(LineExtensionAmount currencyID).
a. Ejemplo: 
i. 
Image
ii. 
Image
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 9/19
iii. 
Image
1. Si se encuentra COINCIDENCIA de Valor a Pagar exitosamente:
a. Se deberá marcar las posiciones en el HISTORICO DE ORDENES DE
COMPRA con la marca PROCESADO para que no sea tomado para
siguientes facturas.
b. Generar trazabilidad en tabla comparativa, de la siguiente manera:
i. COLUMNA ITEM (se crean los siguientes datos por cada posición
encontrada en el HISTORICO DE ORDENES DE COMPRA):
1. Crear los item Posición según la cantidad de items "Posición"
que se encuentren en el HISTORICO DE ORDENES DE
COMPRA.
2. Crear los item Valor PorCalcular_hoc SAP según la cantidad de
items "Posición" que se encuentren en la tabla comparativa.
3. Crear los item Tipo NIF según la cantidad de items "Posición"
que se encuentren en la tabla comparativa.
4. Crear los item Acreedor según la cantidad de items
"Posición" que se encuentren en la tabla comparativa.
5. Crear los item Fec.Doc según la cantidad de items "Posición"
que se encuentren en la tabla comparativa.
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 10/19
6. Crear los item Fec.Reg según la cantidad de items "Posición"
que se encuentren en la tabla comparativa.
7. Crear los item Fecha. cont gasto según la cantidad de items
"Posición" que se encuentren en la tabla comparativa.
8. Crear los item Indicador impuestos según la cantidad de
items "Posición" que se encuentren en la tabla comparativa.
9. Crear los item Texto breve según la cantidad de items
"Posición" que se encuentren en la tabla comparativa.
10. Crear los item Clase de impuesto según la cantidad de items
"Posición" que se encuentren en la tabla comparativa.
11. Crear los item Cuenta según la cantidad de items "Posición"
que se encuentren en la tabla comparativa.
12. Crear los item Ciudad proveedor según la cantidad de items
"Posición" que se encuentren en la tabla comparativa.
13. Crearlos item DOC.FI.ENTRADA según la cantidad de items
"Posición" que se encuentren en la tabla comparativa.
14. Crear los item CTA 26 según la cantidad de items "Posición"
que se encuentren en la tabla comparativa.
15. Crear los item Activo fijo según la cantidad de items
"Posición" que se encuentren en la tabla comparativa.
16. Crear los item Capitalizado el según la cantidad de items
"Posición" que se encuentren en la tabla comparativa.
17. Crear los item Criterio clasif. 2 según la cantidad de items
"Posición" que se encuentren en la tabla comparativa.
ii. COLUMNA VALOR XML:
1. Línea de Item "LineExtensionAmount": se diligencia con
el dato del campo Valor a Pagar que se encuentra en BD
CXP, ejemplo: 18871381. Si ya existe omitir.
iii. COLUMNA VALOR ORDEN DE COMPRA:
1. Línea de Item "LineExtensionAmount": se diligencia con la
sumatoria del dato del campo PorCalcular_hoc de las
posiciones que se encuentran en el HISTORICO DE ORDENES
DE COMPRA, ejemplo: 18871381. Si ya existe omitir.
2. Las líneas de item Posición: se diligencian con el
dato Posición de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA.
3. Las líneas de item Valor PorCalcular_hoc SAP: se diligencian con
el dato PorCalcular_hoc de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA.
4. Las líneas de item Tipo NIF: se diligencian con el dato Tipo
NIF de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
5. Las líneas de item Acreedor: se diligencian con el
dato Acreedor de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
6. Las líneas de item Fec.Doc: se diligencian con el
dato Fec.Doc de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
7. Las líneas de item Fec.Reg: se diligencian con el
dato Fec.Reg de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
8. Las líneas de item Fecha. cont gasto: se diligencian con el
dato Fecha. cont gasto de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 11/19
9. Las líneas de item Indicador impuestos: se diligencian con el
dato Indicador impuestos de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA
10. Las líneas de item Texto breve: se diligencian con el
dato Texto breve de acuerdo con las posiciones encontradas
en el HISTÓRICO DE ÓRDENES DE COMPRA
11. Las líneas de item Clase de impuesto: se diligencian con el
dato Clase de impuesto de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA
12. Las líneas de item Cuenta: se diligencian con el
dato Cuenta de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
13. Las líneas de item Ciudad proveedor: se diligencian con el
dato Ciudad proveedor de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA
14. Las líneas de item DOC.FI.ENTRADA: se diligencian con el
dato DOC.FI.ENTRADA de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA
15. Las líneas de item CTA 26: se diligencian con el dato CTA
26 de acuerdo con las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA
16. Las líneas de item Activo fijo: se diligencian con el
dato Activo fijo de acuerdo con las posiciones encontradas
en el HISTÓRICO DE ÓRDENES DE COMPRA
17. Las líneas de item Capitalizado el: se diligencian con el
dato Capitalizado el de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA
18. Las líneas de item Criterio clasif. 2: se diligencian con el
dato Criterio clasif. 2 de acuerdo con las posiciones
encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA
iv. COLUMNA APROBADO:
1. Línea de Item "LineExtensionAmount": se diligencia con la
marca igual a SI.
c. Se deberá continuar con la siguiente VALIDACION.
2. Si NO se encuentra COINCIDENCIA de Valor a Pagar exitosamente:
a. Se deberá marcar registro en BD CXP como:
1. Estado Fase 4 - VALIDACION DATOS DE FACTURACIÓN: Exitoso.
2. Observaciones Fase 4 - VALIDACION DATOS DE FACTURACIÓN:
Diligenciar "No se encuentra coincidencia del Valor a pagar de
la factura", conservando las observaciones previas que cuente el
campo, separando por comas, pero primando la última observación
que se realiza.
3. Resultado Final registro antes de Eventos: CON NOVEDAD.
b. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA VALOR XML:
i. Línea de Item "LineExtensionAmount": se diligencia con
el dato del campo Valor a Pagar que se encuentra en BD
CXP, ejemplo: 18871381. Si ya existe omitir.
ii. Línea de Item "Observaciones": se diligencia con el dato del
campo Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN que se encuentra en BD CXP, ejemplo: No se
encuentra coincidencia del Valor a pagar de la factura.
2. COLUMNA VALOR ORDEN DE COMPRA:
i. Línea de Item "LineExtensionAmount": se diligencia con
el dato NO ENCONTRADO
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 12/19
3. COLUMNA APROBADO:
i. Línea de Item "LineExtensionAmount": se diligencia con la
marca igual a NO.
4. COLUMNA ESTADO:
i. se diligencia con el dato del campo Resultado Final registro
antes de Eventos que se encuentra en BD CXP,
ejemplo: CON NOVEDAD.
c. Se deberá continuar las validaciones con el siguiente registro de la
misma Clase de pedido, si NO existen más registros de la misma clase,
validar los registros con la siguiente Clase de pedido.
c. Validar el valor del campo TRM de la primera posición del HISTORICO DE ORDENES DE
COMPRA vs el campo “CalculationRate” del registro en la BD CXP.
i. Si los valores validados NO COINCIDEN:
1. Se deberá marcar registro en BD CXP como:
a. Observaciones Fase 4 - VALIDACION DATOS DE FACTURACIÓN: Diligenciar "No
se encuentra coincidencia en el campo TRM de la factura vs la información
reportada en SAP", conservando las observaciones previas que cuente el
campo, separando por comas, pero primando la última observación que se
realiza.
b. Resultado Final registro antes de Eventos: CON NOVEDAD.
2. Generar trazabilidad en tabla comparativa, de la siguiente manera:
a. COLUMNA ITEM:
i. Crear el o los item "TRM" según la cantidad de items "Posición" que se
encuentren en la tabla comparativa.
b. COLUMNA VALOR XML:
i. La o las líneas de item "TRM": se diligencia con el dato del
campo CalculationRate que se encuentra en BD CXP.
ii. Línea de Item "Observaciones": se diligencia con el dato del
campo Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN que se encuentra en BD CXP.
c. COLUMNA VALOR ORDEN DE COMPRA:
i. La o las líneas de item "TRM": se diligencia con el dato del
campo TRM que se encuentra en HISTORICO DE ORDENES DE
COMPRA.
d. COLUMNA APROBADO:
i. La o las líneas de item "TRM": se diligencia con la marca igual a NO.
e. COLUMNA ESTADO:
i. se diligencia con el dato del campo Resultado Final registro antes de
Eventos que se encuentra en BD CXP, ejemplo: CON NOVEDAD.
3. Se deberá continuar con la siguiente VALIDACION.
ii. Si los valores validados COINCIDEN:
1. Generar trazabilidad en tabla comparativa, de la siguiente manera:
a. COLUMNA ITEM:
i. Crear el o los item "TRM" según la cantidad de items "Posición" que se
encuentren en la tabla comparativa.
b. COLUMNA VALOR XML:
i. La o las líneas de item "TRM": se diligencia con el dato del
campo CalculationRate que se encuentra en BD CXP.
c. COLUMNA VALOR ORDEN DE COMPRA:
i. La o las líneas de item "TRM": se diligencia con el dato del
campo TRM que se encuentra en BD CXP.
d. COLUMNA APROBADO:
i. La o las líneas de item "TRM": se diligencia con la marca igual a SI.
2. Se deberá continuar con la siguiente VALIDACION.
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 13/19
d. Hallar coincidencia del valor del campo "Nombre Emisor" del registro en BD CXP que está
siendo procesado vs el campo "N. Proveedor" del HISTORICO DE ORDENES DE COMPRA por
primera posición identificada, teniendo en cuenta las siguientes reglas:
1. Para los nombres de compañías:
a. Considerar las variaciones en Mayúsculas y minúsculas en los nombres de
compañías comparadas.
b. Considerar las variaciones en puntuación de en los nombres de compañías
comparadas, por ejemplo:
i. SAS:
1. Se debe limpiar comas, puntos y espacios de la expresión S A S.
2. Puntos y espacios en expresión S.A.S. de la siguiente manera:
1. S.A.S.
2. S. A. S.
3. SAS
4. S A S
5. S, A. S.
6. S,A.S.
ii. LTDA:
1. Se debe limpiar comas, puntos y espacios de la expresión LTDA.
2. Puntos y espacios en expresión Ltda de la siguiente manera:
1. Limitada
2. Ltda
3. Ltda.
4. Ltda,
iii. S EN C:
1. Se debe limpiar comas, puntos y espacios de la expresión S EN C.
2. Puntos y espacios en expresión Comandita de la siguiente manera:
a. S. EN C.
b. S EN C
c. S EN C A
2. Para los nombres de personas:
i. Considerar las variaciones en Mayúsculas y minúsculas de los nombres
comparados.
ii. Si los nombres comparados, NO COINCIDEN en el orden original en que se
presentan los datos desde la BD CXP, se deberá aplicar la conversión del dato de
la siguiente manera:
1. El dato de las 2 últimas posiciones del nombre, se trasladan a primera y
segunda posición en la expresión.
a. Ejemplo:
i. Dato en BD CXP: ALEXANDER LOZANO CALDERON
ii. Dato después de convertido: LOZANO CALDERON
ALEXANDER
1. Nota:
a. Para realizar la conversión detallada previamente,
se parte de la definición entregada por el usuario
que el dato del Nombre Emisor del xml que se
encuentra en BD CXP corresponde al
orden Nombres + Apellidos.
b. Si se desea tomar el dato del nombre para hacer
la conversión directamente del HISTORICO DE
ORDENES DE COMPRA, entonces el orden
presentado es Apellidos + nombres. Para lo cual
se deberá tomar las primeras 2 posiciones y
llevarlas al final de la expresión.
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 14/19
i. Si NO SE ENCUENTRA COINCIDENCIA en la comparación de los datos después de aplicadas
las reglas de variación:
a. Se deberá marcar registro en BD CXP como:
1. Observaciones Fase 4 - VALIDACION DATOS DE FACTURACIÓN: Diligenciar "No
se encuentra coincidencia en Nombre Emisor de la factura vs la
información reportada en SAP", conservando las observaciones previas que
cuente el campo, separando por comas, pero primando la última observación
que se realiza.
2. Resultado Final registro antes de Eventos: CON NOVEDAD.
b. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA VALOR XML:
1. Línea de Item "Nombre emisor": se diligencia con el dato del
campo Nombre emisor que se encuentra en BD CXP, ejemplo: DIANA A S
A S. Si ya existe omitir.
2. Línea de Item "Observaciones": se diligencia con el dato del
campo Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN que se encuentra en BD CXP.
2. COLUMNA VALOR ORDEN DE COMPRA:
1. Línea de Item "Nombre emisor": se diligencia con el dato del campo N.
Proveedor que se encuentra en HISTORICO DE ORDENES DE COMPRA.
3. COLUMNA APROBADO:
1. Línea de Item "Nombre emisor": se diligencia con la marca igual a NO.
4. COLUMNA ESTADO:
1. se diligencia con el dato del campo Resultado Final registro antes de
Eventos que se encuentra en BD CXP, ejemplo: CON NOVEDAD.
c. Continuar con la siguiente VALIDACION.
ii. Si SE ENCUENTRA COINCIDENCIA en la comparación de los datos después de aplicadas las
reglas de variación:
1. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA VALOR XML:
1. Línea de Item "Nombre emisor": se diligencia con el dato del
campo Nombre emisor que se encuentra en BD CXP, ejemplo: DIANA A S
A S. Si ya existe omitir.
2. COLUMNA VALOR ORDEN DE COMPRA:
1. Línea de Item "Nombre emisor": se diligencia con el dato del campo N.
Proveedor que se encuentra en HISTORICO DE ORDENES DE COMPRA.
3. COLUMNA APROBADO:
1. Línea de Item "Nombre emisor": se diligencia con la marca igual a SI.
2. Continuar con la siguiente VALIDACION.
e. Validar el cumplimiento de las siguientes reglas de negocio en la información obtenida del
HISTÓRICO DE ORDENES DE COMPRA para el registro FV que se está procesando por cada
posición encontrada.
i. Campo Activo fijo: Este dato debe estar diligenciado en todas las posiciones y corresponder a
un dato de 9 dígitos.
a. Si se encuentra diligenciado y corresponde a un dato de 9 dígitos:
1. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA APROBADO:
1. Línea de Item "Activo fijo": se diligencia con la marca igual a SI en
la o las posiciones encontradas en el HISTÓRICO DE ÓRDENES DE
COMPRA.
2. Continuar con la siguiente VALIDACION.
b. Si NO se encuentra diligenciado y/o NO corresponde a un dato de 9 dígitos:
1. Se deberá marcar registro en BD CXP como:
1. Observaciones Fase 4 - VALIDACION DATOS DE FACTURACIÓN:
Diligenciar "Pedido corresponde a ZPAF pero campo "Activo
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 15/19
fijo" NO se encuentra diligenciado y/o NO corresponde a un dato de 9
dígitos", conservando las observaciones previas que cuente el campo,
separando por comas, pero primando la última observación que se realiza.
2. Resultado Final registro antes de Eventos: CON NOVEDAD.
2. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA VALOR XML:
1. Línea de Item "Observaciones": se diligencia con el dato del
campo Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN que se encuentra en BD CXP.
2. COLUMNA APROBADO:
1. Línea de Item "Activo fijo": se diligencia con la marca igual
a NO en la o las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA donde falla regla y SI en las que si haya
cumplido (si aplica).
3. COLUMNA ESTADO:
1. se diligencia con el dato del campo Resultado Final registro antes
de Eventos que se encuentra en BD CXP, ejemplo: CON NOVEDAD.
3. Continuar con la siguiente VALIDACION.
ii. Campo Capitalizado el: Este dato NUNCA debe estar diligenciado.
a. Si NO se encuentra diligenciado:
i. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA APROBADO:
a. Línea de Item "Capitalizado el": se diligencia con la marca igual
a SI en la o las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA.
ii. Continuar con la siguiente VALIDACION.
b. Si se encuentra diligenciado:
i. Se deberá marcar registro en BD CXP como:
1. Observaciones Fase 4 - VALIDACION DATOS DE FACTURACIÓN:
Diligenciar "Pedido corresponde a ZPAF (Activo fijo) pero
campo "Capitalizado el" se encuentra diligenciado cuando NUNCA debe
estarlo", conservando las observaciones previas que cuente el campo,
separando por comas, pero primando la última observación que se realiza.
2. Resultado Final registro antes de Eventos: CON NOVEDAD.
ii. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA VALOR XML:
a. Línea de Item "Observaciones": se diligencia con el dato del
campo Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN que se encuentra en BD CXP.
2. COLUMNA APROBADO:
a. Línea de Item "Capitalizado el": se diligencia con la marca igual a
NO en la o las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA donde falla regla y SI en las que si haya
cumplido (si aplica).
3. COLUMNA ESTADO:
a. se diligencia con el dato del campo Resultado Final registro antes
de Eventos que se encuentra en BD CXP, ejemplo: CON NOVEDAD.
iii. Continuar con la siguiente VALIDACION.
iii. Campo Indicador impuestos: Este dato debe estar diligenciado con uno de los
indicadores "H4", "H5", "H6", "H7" o "VP" por posición encontrada para el registro FV. En los
casos donde exista más de una posición asociada al registro, los valores del campo pueden
repetirse o combinarse según los siguientes escenarios permitidos, sin importar el orden en
que aparezcan:
a. ESCENARIO 1: Grupo productores de renta (productos de producción):
1. "H4"
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 16/19
2. "H5"
3. "VP"
4.  o cualquier combinación entre ellos, por ejemplo: "H4" y "VP", "H4",
"H5" y "VP", etc.
b. ESCENARIO 2: Grupo No productores de renta (equipos de oficina, muebles y
enseres):
i. "H6"
ii. "H7"
iii. "VP"
iv. o cualquier combinación entre ellos, por ejemplo: "H6" y "VP", "H6",
"H7" y "VP", etc.
1. Nota: No se permite mezclar indicadores de ambos grupos (por
ejemplo, "H4" con "H6").
1. Si se encuentra diligenciado y corresponde a las combinaciones permitidas según cada
escenario detallado previamente:
a. Generar trazabilidad en tabla comparativa, de la siguiente manera:
i. COLUMNA APROBADO:
1. Línea de Item "Indicador impuestos": se diligencia con la marca
igual a SI en la o las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA.
ii. Continuar con la siguiente VALIDACION.
2. Si NO se encuentra diligenciado y/o dato diligenciado NO corresponde a las
combinaciones permitidas según cada escenario detallado previamente:
a. Se deberá marcar registro en BD CXP como:
i. Observaciones Fase 4 - VALIDACION DATOS DE FACTURACIÓN: Diligenciar
las siguientes observaciones que apliquen según el caso, conservando las
observaciones previas que cuente el campo, separando por comas, pero
primando la última observación que se realiza:
1. "Pedido corresponde a ZPAF pero campo "Indicador impuestos"
NO se encuentra diligenciado"
2. "Pedido corresponde a ZPAF pero campo "Indicador
impuestos" NO corresponde alguna de las opciones "H4", "H5",
"H6", "H7" o "VP" en pedido de Activos fijos"
3. "Pedido corresponde a ZPAF pero campo "Indicador
impuestos" NO se encuentra aplicado correctamente"
ii. Resultado Final registro antes de Eventos: CON NOVEDAD.
b. Generar trazabilidad en tabla comparativa, de la siguiente manera:
i. COLUMNA VALOR XML:
1. Línea de Item "Observaciones": se diligencia con el dato del
campo Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN que se encuentra en BD CXP.
ii. COLUMNA APROBADO:
1. Línea de Item "Indicador impuestos": se diligencia con la marca
igual a NO en la o las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA donde falla regla y SI en las que si haya
cumplido (si aplica).
iii. COLUMNA ESTADO:
1. se diligencia con el dato del campo Resultado Final registro antes
de Eventos que se encuentra en BD CXP, ejemplo: CON NOVEDAD.
c. Continuar con la siguiente VALIDACION.
3. Campo Criterio clasif. 2: Este campo debe estar diligenciado con un solo valor por
cada posición encontrada del registro tipo FV de acuerdo con los valores presentes en
el campo Indicador impuestos, aplicando las siguientes reglas:
i. Si en alguna posición del registro se encuentra el valor H4 o H5:
1. Criterio clasif. 2 = 0001
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 17/19
ii. Si en alguna posición del registro se encuentra el valor H6 o H7:
1. Criterio clasif. 2 = 0000
iii. Si en alguna posición del registro se encuentra el valor VP:
1. Criterio clasif. 2 = 0001 o 0000
a. Si se encuentra diligenciado y corresponde a las reglas indicadas:
i. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA APROBADO:
1. Línea de Item "Criterio clasif. 2": se diligencia con la marca
igual a SI en la o las posiciones encontradas en el HISTÓRICO
DE ÓRDENES DE COMPRA.
ii. Continuar con la siguiente VALIDACION.
b. Si NO se encuentra diligenciado y/o dato diligenciado NO corresponde a las
reglas indicadas:
i. Se deberá marcar registro en BD CXP como:
1. Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN: Diligenciar las siguientes observaciones que
apliquen según el caso, conservando las observaciones previas que
cuente el campo, separando por comas, pero primando la última
observación que se realiza:
1. "Pedido corresponde a ZPAF pero campo "Criterio clasif."
2 NO se encuentra diligenciado"
2. "Pedido corresponde a ZPAF pero campo "Criterio clasif."
2 NO se encuentra aplicado correctamente según reglas "H4
y H5 = 0001", "H6 y H7 = 0000"o "VP = 0001 o 0000""
2. Resultado Final registro antes de Eventos: CON NOVEDAD.
ii. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA VALOR XML:
a. Línea de Item "Observaciones": se diligencia con el dato del
campo Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN que se encuentra en BD CXP.
2. COLUMNA APROBADO:
1. Línea de Item "Criterio clasif. 2": se diligencia con la marca
igual a NO en la o las posiciones encontradas en
el HISTÓRICO DE ÓRDENES DE COMPRA donde falla
regla y SI en las que si haya cumplido (si aplica).
3. COLUMNA ESTADO:
1. se diligencia con el dato del campo Resultado Final registro
antes de Eventos que se encuentra en BD CXP,
ejemplo: CON NOVEDAD.
iii. Continuar con la siguiente VALIDACION.
4. Campo Cuenta: debe estar diligenciado y el dato debe ser "2695950020", en todas las
posiciones encontradas en el HISTÓRICO DE ÓRDENES DE COMPRA.
a. Si cuenta es igual:
i. Generar trazabilidad en tabla comparativa, de la siguiente manera:
1. COLUMNA APROBADO:
a. Línea de Item "Cuenta": se diligencia con la marca igual
a SI en la o las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA.
ii. Se deberá continuar las validaciones con el siguiente registro de la
misma Clase de pedido, si NO existen más registros de la misma clase,
validar los registros con la siguiente Clase de pedido en el siguiente
paso
b. Si cuenta NO es igual:
i. Se deberá marcar registro en BD CXP como:
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 18/19
1. Observaciones Fase 4 - VALIDACION DATOS DE FACTURACIÓN:
Diligenciar "Pedido corresponde a ZPAF, pero Campo "Cuenta" NO
corresponde a 2695950020", conservando las observaciones previas
que cuente el campo, separando por comas, pero primando la
última observación que se realiza.
2. Resultado Final registro antes de Eventos: CON NOVEDAD.
ii. Generar trazabilidad en tabla comparativa, de la siguiente manera: 
1. COLUMNA VALOR XML:
1. Línea de Item "Observaciones": se diligencia con el dato del
campo Observaciones Fase 4 - VALIDACION DATOS DE
FACTURACIÓN que se encuentra en BD CXP.
2. COLUMNA APROBADO:
a. Línea de Item "Cuenta": se diligencia con la marca igual
a NO en la o las posiciones encontradas en el HISTÓRICO DE
ÓRDENES DE COMPRA donde falla regla y SI en las que si
haya cumplido (si aplica).
3. COLUMNA ESTADO:
a. se diligencia con el dato del campo Resultado Final registro
antes de Eventos que se encuentra en BD CXP,
ejemplo: CON NOVEDAD.
iii. Se deberá continuar las validaciones con el siguiente registro de la
misma Clase de pedido, si NO existen más registros de la misma clase,
validar los registros con la siguiente Clase de pedido en el siguiente
paso.
12/1/26, 2:07 p.m. CXP Epic P Backlog - Boards
https://dev.azure.com/Gerencia2NV/Grupo Diana/_backlogs/backlog/CXP/Epic P?workitem=42748 19/19



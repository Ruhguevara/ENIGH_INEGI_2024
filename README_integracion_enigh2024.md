# Integración ENIGH 2024 Nueva Serie

Pipeline de unificación de los 17 conjuntos de datos de la ENIGH 2024 NS del INEGI,
para el modelo de estimación de ingresos.

## Archivos

| Archivo | Qué es |
|---|---|
| `enigh2024_pipeline.py` | Módulo con las funciones de lectura, tipado, catálogos, validación de grano y joins instrumentados |
| `01_integracion_enigh2024.ipynb` | Notebook paso a paso: inventario → esquema → carga → grano → integración → validación → exportación |
| `README_integracion_enigh2024.md` | Este archivo |

## Antes de correr: Git LFS

Los 194 CSV del repositorio están bajo Git LFS (`.gitattributes` tiene
`*.csv filter=lfs`). Un `git clone` normal deja **punteros de 130 bytes**, no datos.

```bash
git lfs install
git clone https://github.com/Ruhguevara/ENIGH_INEGI_2024.git
cd ENIGH_INEGI_2024
git lfs pull
du -sh DAT/          # debe dar ~880 MB
```

La celda de inventario del notebook detecta punteros sin resolver y aborta con un
mensaje explícito.

### Advertencia de cuota

El repositorio pesa 880 MB en objetos LFS. La cuota gratuita de GitHub es de
**1 GB de almacenamiento y 1 GB de ancho de banda al mes**. Con un solo `git lfs pull`
completo ya consumes el 88 % del ancho de banda mensual; el segundo clon del mes
va a fallar.

Dos salidas, según cómo quieras trabajar:

1. **Sacar `gastoshogar` del repo.** Solo ese archivo son 579 MB, el 66 % del total,
   y este pipeline no lo necesita: `concentradohogar` ya trae `gasto_mon` y todos
   sus componentes trimestralizados. Sin él, el repositorio baja a ~300 MB.
2. **No versionar los datos.** Dejar en el repo solo diccionarios, catálogos,
   metadatos y modelos ER (menos de 1 MB en total, sin LFS) y un script que
   descargue los microdatos desde el sitio del INEGI a una carpeta ignorada por git.
   Es la opción que escala mejor si vas a agregar ENIGH 2022, 2020 y 2018.

## Requisitos

```bash
pip install "pandas>=2.0" numpy pyarrow
```

Pico de memoria estimado: ~4 GB, saltando `gastoshogar`.

## Configuración

Una sola línea al inicio del notebook:

```python
RUTA_DAT = Path("~/repos/ENIGH_INEGI_2024/DAT").expanduser()
```

## Qué produce

El pipeline **no aplana las 17 tablas en un solo CSV**. La ENIGH es un modelo
relacional con cinco granos distintos (vivienda, hogar, persona, trabajo, detalle);
aplanarlo genera un producto cartesiano sin significado estadístico — una persona
con 2 trabajos y 8 claves de ingreso produciría 16 filas para la misma unidad de
observación.

El resultado son dos tablas analíticas con grano declarado y verificado:

| Salida | Grano | Filas esperadas | Uso |
|---|---|---|---|
| `enigh2024_hogar.parquet` | un hogar | ~91 400 | Ingreso del hogar, deciles, calibración |
| `enigh2024_persona.parquet` | un integrante | ~308 000 | **Estimador de ingreso a nivel persona** |
| `enigh2024_ingresos_clave.parquet` | persona × clave | ~1.5 M | Composición del ingreso |
| `enigh2024_negocios.parquet` | negocio del hogar | — | Ingreso de independientes |
| `enigh2024_celdas_diseno.parquet` | entidad × tam_loc × est_socio | ≤512 | Prior geográfico con CV |
| `enigh2024_validacion.parquet` | variable | 10 | Contraste contra cifras publicadas |
| `enigh2024_diccionario_final.csv` | columna | ~150 | Documentación del entregable |

## Puerta de aceptación

La sección 11 del notebook contrasta el resultado contra las cifras publicadas por
el INEGI en la *Presentación de resultados ENIGH 2024* (julio 2025), en pesos de 2024:

| Variable | Valor publicado |
|---|---|
| Ingreso corriente promedio trimestral por hogar | 77 864 |
| Ingreso por trabajo | 51 099 |
| Remuneraciones por trabajo subordinado | 43 665 |
| Transferencias | 13 799 |
| Estimación del alquiler | 9 066 |
| Hogar urbano / rural | 85 550 / 48 004 |
| Coeficiente de Gini | 0.391 |
| Razón decil X / decil I | 14.06 |

Los deciles se calculan con el criterio del INEGI: el hogar que cruza el umbral se
reparte entre dos deciles en lugar de asignarse entero a uno.

**Si estas cifras no se reproducen dentro del 1 %, el dataset no debe usarse.**

## Decisiones de diseño que conviene conocer

**Todo se lee como texto y se castea con el diccionario del INEGI.** No hay una sola
lista de columnas hardcodeada. El tipado sale de `diccionario_de_datos/`, el
etiquetado de `catalogos/`. El módulo sirve igual para ENIGH 2016-2022 cambiando la
constante `SUFIJO`.

**Las llaves nunca son numéricas.** `folioviv` leído como entero pierde el cero
inicial y todas las entidades 01 a 09 dejan de unir, en silencio. El notebook lo
verifica con un `assert` explícito.

**Cada join se reporta.** `unir()` imprime filas antes y después, número de columnas
nuevas, porcentaje de filas con pareja y detección de fan-out. Además usa
`validate=` de pandas, así que un join `1:1` sobre una tabla que en realidad es `1:N`
levanta `MergeError` en lugar de multiplicar filas sin avisar.

**Los catálogos son por tabla, no globales.** `si_no.csv` de `trabajos` y `si_no.csv`
de `hogares` pueden diferir. Se cargan y aplican por carpeta.

**Las columnas heredadas del hogar se prefijan con `hog_`.** Replicar `ing_cor` en
cada integrante y luego modelarlo como si fuera atributo individual es un error
frecuente; el prefijo lo hace visible.

**El desglose de ingreso por rubro se deriva de las descripciones del catálogo**,
no de rangos de claves. Las reglas están en `ep.REGLAS_RUBRO_INGRESO` y el notebook
imprime el mapeo completo más las claves que ninguna regla clasificó, para que las
revises. La autoridad para agregados de hogar sigue siendo `concentradohogar`.

**Los errores estándar usan conglomerados últimos.** La función
`ee_conglomerados_ultimos()` estima la varianza entre UPM dentro de estrato, que es
como el INEGI calcula las precisiones publicadas. Un error estándar calculado como
si la muestra fuera aleatoria simple está subestimado entre 1.1 y 1.9 veces, dado
que el efecto de diseño por entidad va de 1.17 a 3.73.

## Advertencia metodológica

La ENIGH tiene representatividad **nacional y por entidad federativa, con cortes
urbano y rural**. No la tiene a nivel municipio. `ubica_geo` trae la clave municipal
y sirve como llave de cruce, pero 105 718 viviendas repartidas sobre ~2 470
municipios no permiten estimar nada a ese nivel.

La unidad geográfica correcta para este pipeline es la celda
`entidad × tam_loc × est_socio`, y solo aquellas cuyo coeficiente de variación quede
por debajo del 30 % según el semáforo del INEGI.

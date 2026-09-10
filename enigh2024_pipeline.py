"""
enigh2024_pipeline
==================
Lectura, tipado, validación e integración de los microdatos de la ENIGH 2024
Nueva Serie (INEGI), tal como vienen distribuidos en 17 carpetas
``conjunto_de_datos_<tabla>_enigh2024_ns/``.

Principios de diseño
--------------------
1. **Nada se hardcodea.** El tipado sale del ``diccionario_de_datos`` que el
   INEGI publica junto a cada tabla; el etiquetado sale de los ``catalogos``.
   El módulo funciona igual para ENIGH 2016-2022 cambiando ``SUFIJO``.
2. **Las llaves siempre son texto.** ``folioviv``, ``foliohog``, ``numren``,
   ``id_trabajo``, ``clave``, ``ubica_geo``... se fuerzan a ``str`` aunque el
   diccionario las marque numéricas. Perder los ceros a la izquierda de
   ``folioviv`` rompe todas las uniones y es el error más frecuente al leer
   ENIGH con pandas.
3. **Cada join se reporta.** ``unir()`` imprime filas antes/después, cobertura
   y detección de fan-out. Un join silencioso es un join no auditado.
4. **Nada se sobrescribe.** Los CSV originales solo se leen.
"""

from __future__ import annotations

import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

__all__ = [
    "TABLAS", "LLAVES_TEXTO", "NO_ESPECIFICADO",
    "Rutas", "ruta_datos", "ruta_diccionario", "ruta_catalogos", "inventario",
    "leer_csv_crudo", "leer_diccionario", "leer_catalogos", "leer_tabla",
    "perfilar", "validar_llave", "unir", "etiquetar",
    "desglosar_ubica_geo", "clasificar_claves_ingreso",
    "agregar_ingresos_persona", "seleccionar_trabajo_principal",
    "agregar_gastoshogar", "resumen_memoria",
]

# --------------------------------------------------------------------------
# Constantes del proyecto
# --------------------------------------------------------------------------

SUFIJO = "enigh2024_ns"

#: Código INEGI de "no especificado" en variables carácter.
NO_ESPECIFICADO = "&"

#: Columnas que SIEMPRE se leen como texto, sin importar lo que diga el
#: diccionario de datos. Son llaves o claves catalogadas con ceros a la
#: izquierda significativos.
LLAVES_TEXTO = {
    "folioviv", "foliohog", "numren", "id_trabajo", "clave", "tipoact",
    "numprod", "codigo", "cosecha", "destino", "ubica_geo", "tam_loc",
    "est_socio", "est_dis", "upm", "entidad", "sinco", "scian", "clas_emp",
    "tam_emp", "educa_jefe", "clase_hog", "sexo", "sexo_jefe", "tipo_gasto",
    "mes_dia", "parentesco",
}


@dataclass(frozen=True)
class Tabla:
    """Metadatos de una de las 17 tablas de la ENIGH."""
    nombre: str          # nombre corto, p.ej. "concentradohogar"
    nivel: str           # vivienda | hogar | persona | trabajo | detalle
    llaves: tuple        # llaves que definen el grano teórico
    descripcion: str


#: Registro de las 17 tablas. ``llaves`` es el grano teórico según el modelo
#: entidad-relación del INEGI; el notebook lo verifica contra los datos reales.
TABLAS: dict[str, Tabla] = {t.nombre: t for t in [
    Tabla("viviendas", "vivienda", ("folioviv",),
          "Características de las viviendas, ubicación y diseño muestral"),
    Tabla("hogares", "hogar", ("folioviv", "foliohog"),
          "Acceso a la alimentación, equipamiento, vehículos, hábitos de consumo"),
    Tabla("concentradohogar", "hogar", ("folioviv", "foliohog"),
          "Tabla resumen: ingresos y gastos construidos y trimestralizados"),
    Tabla("gastoshogar", "detalle", ("folioviv", "foliohog", "clave"),
          "Gasto monetario y no monetario del hogar, formato largo"),
    Tabla("erogaciones", "detalle", ("folioviv", "foliohog", "clave"),
          "Erogaciones financieras y de capital del hogar"),
    Tabla("gastotarjetas", "detalle", ("folioviv", "foliohog", "clave"),
          "Gasto del hogar cubierto con tarjeta de crédito bancaria o comercial"),
    Tabla("poblacion", "persona", ("folioviv", "foliohog", "numren"),
          "Características sociodemográficas y ocupacionales de cada integrante"),
    Tabla("ingresos", "detalle", ("folioviv", "foliohog", "numren", "clave"),
          "Ingresos y percepciones de capital por persona y clave, formato largo"),
    Tabla("ingresos_jcf", "detalle", ("folioviv", "foliohog", "numren", "clave"),
          "Ingresos de Jóvenes Construyendo el Futuro (tabla de referencia)"),
    Tabla("gastospersona", "detalle", ("folioviv", "foliohog", "numren", "clave"),
          "Gasto en educación, transporte público y remuneraciones en especie"),
    Tabla("trabajos", "trabajo", ("folioviv", "foliohog", "numren", "id_trabajo"),
          "Condición de actividad de personas de 12 o más años, SINCO y SCIAN"),
    Tabla("noagro", "trabajo", ("folioviv", "foliohog", "numren", "id_trabajo"),
          "Negocios industriales, comerciales y de servicios del hogar"),
    Tabla("noagroimportes", "detalle",
          ("folioviv", "foliohog", "numren", "id_trabajo", "clave"),
          "Gastos de operación de los negocios no agrícolas"),
    Tabla("agro", "trabajo",
          ("folioviv", "foliohog", "numren", "id_trabajo", "tipoact"),
          "Negocios agrícolas, forestales, pecuarios, de pesca y caza"),
    Tabla("agroproductos", "detalle",
          ("folioviv", "foliohog", "numren", "id_trabajo", "tipoact",
           "numprod", "codigo", "cosecha"),
          "Productos de los negocios agrícolas del hogar"),
    Tabla("agroconsumo", "detalle",
          ("folioviv", "foliohog", "numren", "id_trabajo", "tipoact",
           "numprod", "codigo", "cosecha", "destino"),
          "Destino, cantidad y valor de los productos del negocio del hogar"),
    Tabla("agrogasto", "detalle",
          ("folioviv", "foliohog", "numren", "id_trabajo", "tipoact", "clave"),
          "Gasto que realiza el negocio agropecuario del hogar"),
]}


# --------------------------------------------------------------------------
# Rutas
# --------------------------------------------------------------------------

class Rutas:
    """Resuelve rutas dentro de la carpeta DAT/ del repositorio."""

    def __init__(self, dat: str | os.PathLike):
        self.dat = Path(dat).expanduser().resolve()
        if not self.dat.is_dir():
            raise FileNotFoundError(
                f"No existe la carpeta DAT: {self.dat}\n"
                "Apunta RUTA_DAT a la carpeta DAT/ del repo ENIGH_INEGI_2024."
            )

    def carpeta(self, tabla: str) -> Path:
        return self.dat / f"conjunto_de_datos_{tabla}_{SUFIJO}"

    def datos(self, tabla: str) -> Path:
        return (self.carpeta(tabla) / "conjunto_de_datos"
                / f"conjunto_de_datos_{tabla}_{SUFIJO}.csv")

    def diccionario(self, tabla: str) -> Path:
        return (self.carpeta(tabla) / "diccionario_de_datos"
                / f"diccionario_datos_{tabla}_{SUFIJO}.csv")

    def catalogos(self, tabla: str) -> Path:
        return self.carpeta(tabla) / "catalogos"

    def indice(self) -> Path:
        return self.dat / f"0_indice_tablas_{SUFIJO}.csv"


# Atajos funcionales, útiles en celdas sueltas del notebook
def ruta_datos(rutas: Rutas, tabla: str) -> Path:
    return rutas.datos(tabla)


def ruta_diccionario(rutas: Rutas, tabla: str) -> Path:
    return rutas.diccionario(tabla)


def ruta_catalogos(rutas: Rutas, tabla: str) -> Path:
    return rutas.catalogos(tabla)


def inventario(rutas: Rutas) -> pd.DataFrame:
    """Verifica que las 17 carpetas y sus 4 subcarpetas existan, y mide tamaños.

    Devuelve un DataFrame con una fila por tabla. Es el primer control del
    pipeline: si aquí falta algo, no tiene sentido seguir.
    """
    filas = []
    for nombre, meta in TABLAS.items():
        d, dic, cat = rutas.datos(nombre), rutas.diccionario(nombre), rutas.catalogos(nombre)
        mb = round(d.stat().st_size / 1e6, 2) if d.exists() else np.nan
        # Un puntero de Git LFS pesa ~130 bytes y empieza con "version https://git-lfs"
        lfs = False
        if d.exists() and d.stat().st_size < 1000:
            with open(d, "rb") as fh:
                lfs = fh.read(24).startswith(b"version https://git-lfs")
        filas.append({
            "tabla": nombre,
            "nivel": meta.nivel,
            "llaves_teoricas": " + ".join(meta.llaves),
            "datos_ok": d.exists() and not lfs,
            "puntero_lfs": lfs,
            "diccionario_ok": dic.exists(),
            "n_catalogos": len(list(cat.glob("*.csv"))) if cat.is_dir() else 0,
            "mb": mb,
            "descripcion": meta.descripcion,
        })
    inv = pd.DataFrame(filas).sort_values("mb", ascending=False, na_position="last")
    return inv.reset_index(drop=True)


# --------------------------------------------------------------------------
# Lectura robusta
# --------------------------------------------------------------------------

_ENCODINGS = ("utf-8", "utf-8-sig", "latin-1")


def leer_csv_crudo(path: str | os.PathLike, **kwargs) -> pd.DataFrame:
    """Lee un CSV probando codificaciones, todo como texto.

    El INEGI mezcla UTF-8 y Latin-1 entre archivos y entre ediciones. Leer todo
    como ``str`` y castear después con el diccionario evita que pandas infiera
    mal los tipos (el caso clásico: ``folioviv`` convertido a int64).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    ultimo = None
    for enc in _ENCODINGS:
        try:
            return pd.read_csv(
                path, dtype=str, encoding=enc, keep_default_na=False,
                na_values=[""], low_memory=False, **kwargs
            )
        except UnicodeDecodeError as e:
            ultimo = e
    raise UnicodeDecodeError(*ultimo.args)  # pragma: no cover


def _norm(s: str) -> str:
    """minúsculas, sin acentos, sin espacios ni signos: para casar encabezados."""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", s.lower())


def leer_diccionario(rutas: Rutas, tabla: str) -> pd.DataFrame:
    """Devuelve el diccionario de datos normalizado.

    Columnas de salida: ``variable``, ``etiqueta``, ``longitud``, ``tipo``
    (``C``/``N``), ``rango``, ``catalogo``.

    Los diccionarios del INEGI traen filas de portada antes del encabezado
    real; la función localiza la fila que contiene "NEMONICO" y la usa como
    header.
    """
    crudo = leer_csv_crudo(rutas.diccionario(tabla), header=None)
    fila_header = None
    for i in range(min(15, len(crudo))):
        vals = [_norm(v) for v in crudo.iloc[i].tolist() if pd.notna(v)]
        if any(v in ("nemonico", "mnemonico", "nombredelcampo") for v in vals):
            fila_header = i
            break
    if fila_header is None:
        fila_header = 0

    dic = crudo.iloc[fila_header + 1:].copy()
    dic.columns = [_norm(c) for c in crudo.iloc[fila_header].tolist()]
    dic = dic.loc[:, [c for c in dic.columns if c and c != "nan"]]

    def col(*cands, default=None):
        for c in cands:
            if c in dic.columns:
                return dic[c]
        return pd.Series([default] * len(dic), index=dic.index)

    out = pd.DataFrame({
        "variable": col("nemonico", "mnemonico", "nombredelcampo").astype(str).str.strip().str.lower(),
        "etiqueta": col("nombredelcampo", "descripcion", "etiqueta", default=""),
        "longitud": pd.to_numeric(col("longitud"), errors="coerce"),
        "tipo": col("tipo", "tipodedato").astype(str).str.strip().str.upper().str[:1],
        "rango": col("rango", "rangoclave", default=""),
        "catalogo": col("catalogo", "nombredelcatalogo", default=""),
    })
    out = out[out["variable"].str.match(r"^[a-z_][a-z0-9_]*$", na=False)]
    out = out.drop_duplicates("variable").reset_index(drop=True)
    out["tabla"] = tabla
    return out


def leer_catalogos(rutas: Rutas, tabla: str) -> dict[str, pd.DataFrame]:
    """Carga todos los catálogos de una tabla como ``{nombre: DataFrame}``.

    Cada catálogo del INEGI es un CSV de dos columnas: clave y descripción,
    a veces con filas de portada. La función normaliza a ``clave`` /
    ``descripcion``.
    """
    cats: dict[str, pd.DataFrame] = {}
    carpeta = rutas.catalogos(tabla)
    if not carpeta.is_dir():
        return cats
    for f in sorted(carpeta.glob("*.csv")):
        try:
            crudo = leer_csv_crudo(f, header=None)
        except Exception:
            continue
        # localiza la primera fila con 2+ celdas no vacías tras la portada
        inicio = 0
        for i in range(min(10, len(crudo))):
            fila = [v for v in crudo.iloc[i].tolist() if pd.notna(v) and str(v).strip()]
            if len(fila) >= 2 and _norm(fila[0]) not in ("catalogo", "catalogos"):
                inicio = i
                break
        cat = crudo.iloc[inicio:, :2].copy()
        cat.columns = ["clave", "descripcion"]
        # si la primera fila es encabezado ("CLAVE", "DESCRIPCION"), se descarta
        if _norm(cat.iloc[0, 0]) in ("clave", "cve", "codigo") or \
           _norm(cat.iloc[0, 1]) in ("descripcion", "descripciones"):
            cat = cat.iloc[1:]
        cat = cat.dropna(subset=["clave"])
        cat["clave"] = cat["clave"].astype(str).str.strip()
        cat["descripcion"] = cat["descripcion"].astype(str).str.strip()
        cats[f.stem.lower()] = cat.reset_index(drop=True)
    return cats


def leer_tabla(
    rutas: Rutas,
    tabla: str,
    columnas: Sequence[str] | None = None,
    tipar: bool = True,
    limpiar_ne: bool = True,
    verbose: bool = True,
) -> pd.DataFrame:
    """Carga una tabla de la ENIGH con tipos derivados del diccionario.

    Parameters
    ----------
    columnas : subconjunto de columnas a leer. Reduce memoria de forma drástica
        en ``gastoshogar`` (579 MB) y ``poblacion`` (93 MB).
    tipar : castea a numérico las variables que el diccionario marca ``N``,
        salvo las de :data:`LLAVES_TEXTO`.
    limpiar_ne : convierte ``&`` a ``NaN`` en variables carácter.
    """
    path = rutas.datos(tabla)
    df = leer_csv_crudo(path, usecols=list(columnas) if columnas else None)
    df.columns = [c.strip().lower() for c in df.columns]
    n0, m0 = df.shape

    dic = leer_diccionario(rutas, tabla).set_index("variable")
    numericas, caracter = [], []
    for c in df.columns:
        if c in LLAVES_TEXTO:
            caracter.append(c)
        elif c in dic.index and str(dic.loc[c, "tipo"]).startswith("N"):
            numericas.append(c)
        else:
            caracter.append(c)

    if limpiar_ne:
        for c in caracter:
            df[c] = df[c].replace(NO_ESPECIFICADO, np.nan)

    if tipar:
        for c in numericas:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if verbose:
        print(f"  {tabla:<18} {n0:>9,} filas x {m0:>3} cols   "
              f"[{len(numericas)} num / {len(caracter)} car]   "
              f"{path.stat().st_size/1e6:>7.1f} MB en disco   "
              f"{df.memory_usage(deep=True).sum()/1e6:>7.1f} MB en RAM")
    return df


# --------------------------------------------------------------------------
# Perfilado y control de calidad
# --------------------------------------------------------------------------

def perfilar(df: pd.DataFrame, nombre: str, llaves: Sequence[str] | None = None) -> pd.Series:
    """Resumen compacto de una tabla: filas, columnas, memoria, nulos, grano."""
    r = {
        "tabla": nombre,
        "filas": len(df),
        "columnas": df.shape[1],
        "mb_ram": round(df.memory_usage(deep=True).sum() / 1e6, 1),
        "pct_celdas_nulas": round(100 * df.isna().to_numpy().mean(), 2),
        "cols_100pct_nulas": int((df.isna().mean() == 1).sum()),
    }
    if llaves:
        presentes = [k for k in llaves if k in df.columns]
        r["llaves_presentes"] = " + ".join(presentes)
        r["llave_unica"] = bool(not df.duplicated(presentes).any()) if presentes else None
        r["n_llaves_dup"] = int(df.duplicated(presentes).sum()) if presentes else None
    return pd.Series(r)


def validar_llave(df: pd.DataFrame, llaves: Sequence[str], nombre: str = "") -> bool:
    """Verifica unicidad del grano y lo reporta. Devuelve True si es única."""
    faltan = [k for k in llaves if k not in df.columns]
    if faltan:
        print(f"  [!] {nombre}: faltan columnas llave {faltan}")
        return False
    dup = df.duplicated(list(llaves)).sum()
    if dup == 0:
        print(f"  [ok] {nombre}: grano único en ({' + '.join(llaves)}) — {len(df):,} filas")
        return True
    print(f"  [!] {nombre}: {dup:,} filas duplicadas en ({' + '.join(llaves)}) "
          f"→ la tabla es de grano más fino de lo esperado, NO unir 1:1")
    return False


def unir(
    izq: pd.DataFrame,
    der: pd.DataFrame,
    on: Sequence[str],
    how: str = "left",
    nombre: str = "",
    validar: str | None = "m:1",
    sufijos: tuple = ("", "_dup"),
) -> pd.DataFrame:
    """Join instrumentado: reporta tamaños antes/después, cobertura y fan-out.

    ``validar='m:1'`` exige que la tabla derecha tenga grano único en ``on``;
    si no lo tiene, pandas levanta MergeError en lugar de multiplicar filas en
    silencio. Ese es exactamente el fallo que queremos que sea ruidoso.
    """
    on = list(on)
    n_izq, n_der = len(izq), len(der)
    cols_izq = izq.shape[1]

    res = izq.merge(der, on=on, how=how, validate=validar,
                    suffixes=sufijos, indicator="_match")

    nuevas = [c for c in res.columns if c not in izq.columns and c != "_match"]
    # cobertura real: filas de la izquierda que encontraron pareja en la derecha
    cobertura = 100 * res["_match"].isin(["both", "right_only"]).mean()
    sin_pareja = int((res["_match"] == "left_only").sum())
    res = res.drop(columns="_match")

    delta = len(res) - n_izq
    flag = "" if delta == 0 else f"  <-- CAMBIO DE FILAS {delta:+,} (fan-out)"
    print(f"  {nombre or 'join':<30} izq {n_izq:>9,} x{cols_izq:<4} + der {n_der:>9,} "
          f"= {len(res):>9,} x{res.shape[1]:<4} | +{len(nuevas)} cols | "
          f"pareja {cobertura:5.1f}% ({sin_pareja:,} sin match){flag}")

    dup = [c for c in res.columns if c.endswith(sufijos[1]) and sufijos[1]]
    if dup:
        print(f"      [!] columnas duplicadas por nombre: {dup[:8]}")
    return res


def etiquetar(
    df: pd.DataFrame,
    catalogos: dict[str, pd.DataFrame],
    mapeo: dict[str, str],
    sufijo: str = "_desc",
) -> pd.DataFrame:
    """Agrega columnas descriptivas a partir de los catálogos del INEGI.

    ``mapeo`` es ``{columna_del_df: nombre_del_catalogo}``.
    """
    out = df.copy()
    for col, cat_name in mapeo.items():
        if col not in out.columns:
            print(f"      [skip] columna ausente: {col}")
            continue
        cat = catalogos.get(cat_name.lower())
        if cat is None:
            print(f"      [skip] catálogo ausente: {cat_name}")
            continue
        m = dict(zip(cat["clave"], cat["descripcion"]))
        out[col + sufijo] = out[col].map(m)
        sin_match = out[col].notna() & out[col + sufijo].isna()
        if sin_match.any():
            faltantes = sorted(out.loc[sin_match, col].dropna().unique())[:6]
            print(f"      [!] {col}: {sin_match.sum():,} valores sin match en "
                  f"'{cat_name}' (ej. {faltantes})")
    return out


def desglosar_ubica_geo(df: pd.DataFrame, col: str = "ubica_geo") -> pd.DataFrame:
    """Separa ``ubica_geo`` (5 caracteres) en entidad, municipio y clave INEGI.

    Recordatorio metodológico: la ENIGH tiene representatividad a nivel entidad
    (con corte urbano/rural), NO a nivel municipio. ``cve_mun`` sirve como llave
    de cruce y para agregar a niveles superiores, no para estimar.
    """
    out = df.copy()
    s = out[col].astype(str).str.strip().str.zfill(5)
    out["cve_ent"] = s.str[:2]
    out["cve_mun"] = s.str[2:5]
    out["cve_geo"] = s                       # entidad + municipio, 5 dígitos
    return out


# --------------------------------------------------------------------------
# Transformaciones específicas del modelo de ingresos
# --------------------------------------------------------------------------

#: Reglas de clasificación de las claves de ingreso en los cinco rubros de la
#: Nueva Serie, más las percepciones financieras. Se aplican sobre la
#: DESCRIPCIÓN del catálogo ``ingresos_cat.csv``, no sobre el código, para no
#: depender de rangos de claves que cambian entre ediciones.
#: El orden importa: la primera regla que casa, gana.
REGLAS_RUBRO_INGRESO: list[tuple[str, str]] = [
    ("percepciones_financieras",
     r"retiro de inversion|venta de (?:casas|terrenos|joyas|vehiculos|maquinaria|animales)|"
     r"prestamo|herencia|loteria|seguro de vida|otras percepciones|"
     r"recuperacion de prestamo|deposito de ahorro"),
    ("transferencias",
     r"jubilacion|pension|beca|donativo|indemnizacion|programa|beneficio|"
     r"otros paises|remesa|ayuda|adultos mayores|discapacidad|apoyo"),
    ("renta_propiedad",
     r"alquiler|arrendamiento|renta de|interes|cooperativa|sociedad|utilidad|"
     r"regalia|dividendo|marca|patente"),
    ("trabajo_independiente",
     r"negocio|independiente|cuenta propia|actividad agricola|actividad pecuaria|"
     r"cria de animales|pesca|recoleccion|autoconsumo"),
    ("trabajo_subordinado",
     r"sueldo|salario|jornal|destajo|hora extra|comision|propina|aguinaldo|"
     r"reparto de utilidades|especie|remuneracion|primas|trabajo subordinado|"
     r"gratificacion"),
    ("otros_trabajos", r"otros trabajos|otro trabajo"),
]


def clasificar_claves_ingreso(cat_ingresos: pd.DataFrame) -> pd.DataFrame:
    """Asigna un rubro a cada clave de ingreso usando el catálogo del INEGI.

    Devuelve ``clave``, ``descripcion``, ``rubro``. Las claves que no casan con
    ninguna regla quedan como ``sin_clasificar`` y DEBEN revisarse a mano: el
    notebook las imprime. Esta clasificación es una conveniencia para el
    análisis a nivel persona; la autoridad para los agregados por hogar sigue
    siendo ``concentradohogar``.
    """
    out = cat_ingresos.copy()
    desc = (out["descripcion"].astype(str)
            .apply(lambda s: unicodedata.normalize("NFKD", s))
            .apply(lambda s: "".join(c for c in s if not unicodedata.combining(c)))
            .str.lower())
    out["rubro"] = "sin_clasificar"
    for rubro, patron in REGLAS_RUBRO_INGRESO:
        pendiente = out["rubro"] == "sin_clasificar"
        out.loc[pendiente & desc.str.contains(patron, regex=True, na=False), "rubro"] = rubro
    return out[["clave", "descripcion", "rubro"]]


def agregar_ingresos_persona(
    ingresos: pd.DataFrame,
    clasificacion: pd.DataFrame | None = None,
    col_valor: str = "ing_tri",
) -> pd.DataFrame:
    """Pasa la tabla ``ingresos`` de formato largo a una fila por persona.

    Salida: llaves de persona + ``ing_tri_total`` + una columna por rubro
    (``ing_tri_trabajo_subordinado``, ...) + ``n_claves_ingreso``.
    """
    llaves = ["folioviv", "foliohog", "numren"]
    df = ingresos.copy()
    df[col_valor] = pd.to_numeric(df[col_valor], errors="coerce").fillna(0.0)

    base = (df.groupby(llaves, as_index=False)
              .agg(ing_tri_total=(col_valor, "sum"),
                   n_claves_ingreso=("clave", "nunique")))

    if clasificacion is not None:
        m = dict(zip(clasificacion["clave"], clasificacion["rubro"]))
        df["rubro"] = df["clave"].map(m).fillna("sin_clasificar")
        ancho = (df.pivot_table(index=llaves, columns="rubro", values=col_valor,
                                aggfunc="sum", fill_value=0.0)
                   .add_prefix("ing_tri_")
                   .reset_index())
        base = base.merge(ancho, on=llaves, how="left", validate="1:1")
    return base


def seleccionar_trabajo_principal(
    trabajos: pd.DataFrame, col_id: str = "id_trabajo"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separa el trabajo principal del resto y resume la actividad secundaria.

    En la ENIGH ``id_trabajo`` ordena los trabajos de la persona; el principal
    es el de menor valor. Devuelve ``(principal, resumen_multiempleo)``.
    """
    llaves = ["folioviv", "foliohog", "numren"]
    t = trabajos.copy()
    t["_orden"] = pd.to_numeric(t[col_id], errors="coerce")
    t = t.sort_values(llaves + ["_orden"])

    principal = t.drop_duplicates(llaves, keep="first").drop(columns="_orden")
    resumen = (t.groupby(llaves, as_index=False)
                 .agg(n_trabajos=(col_id, "nunique")))
    resumen["multiempleo"] = resumen["n_trabajos"] > 1
    return principal.reset_index(drop=True), resumen


def agregar_gastoshogar(
    rutas: Rutas,
    chunksize: int = 500_000,
    cols: Sequence[str] = ("folioviv", "foliohog", "gasto_tri", "gas_nm_tri"),
) -> pd.DataFrame:
    """Agrega ``gastoshogar`` (579 MB) a nivel hogar leyendo por bloques.

    Nota práctica: para modelar ingreso NO necesitas esta tabla —
    ``concentradohogar`` ya trae ``gasto_mon`` y todos sus componentes
    trimestralizados. Esta función existe para reconciliar y para análisis
    de gasto por clave, no para el pipeline base.
    """
    path = rutas.datos("gastoshogar")
    acc = []
    for chunk in pd.read_csv(path, dtype=str, usecols=list(cols), encoding="utf-8",
                             chunksize=chunksize, keep_default_na=False,
                             na_values=[""], low_memory=False):
        chunk.columns = [c.strip().lower() for c in chunk.columns]
        for c in ("gasto_tri", "gas_nm_tri"):
            if c in chunk.columns:
                chunk[c] = pd.to_numeric(chunk[c], errors="coerce").fillna(0.0)
        acc.append(chunk.groupby(["folioviv", "foliohog"], as_index=False).sum(numeric_only=True))
    out = (pd.concat(acc, ignore_index=True)
             .groupby(["folioviv", "foliohog"], as_index=False)
             .sum(numeric_only=True))
    return out.rename(columns={"gasto_tri": "gastoshogar_tri",
                               "gas_nm_tri": "gastoshogar_nm_tri"})


def resumen_memoria(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Tabla de uso de memoria por DataFrame cargado."""
    filas = [{"objeto": k, "filas": len(v), "columnas": v.shape[1],
              "mb_ram": round(v.memory_usage(deep=True).sum() / 1e6, 1)}
             for k, v in dfs.items()]
    out = pd.DataFrame(filas).sort_values("mb_ram", ascending=False)
    return pd.concat([out, pd.DataFrame([{"objeto": "TOTAL", "filas": np.nan,
                                          "columnas": np.nan,
                                          "mb_ram": out["mb_ram"].sum()}])],
                     ignore_index=True)

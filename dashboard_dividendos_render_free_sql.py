
"""
Dashboard interactivo en Dash + Plotly para analizar dividendos, cobre y producción.

Qué hace:
- Lee la base desde Excel o SQL.
- Calcula dividendos reales, cobre real y valor de cobre fino neto real.
- Muestra tendencias, correlaciones y dispersión.
- Permite refresco automático.
- Si el backend es SQL, permite cargar un mes manualmente desde la interfaz
  y hacer upsert por mes usando la columna fecha.
- Puede autocompletar IPC, cobre y USD/CLP con fuentes públicas más actuales.

Uso rápido:
1) Probar con Excel
   python dashboard_dividendos.py

2) Crear una base SQL inicial desde el Excel
   python dashboard_dividendos.py --bootstrap-sql

3) Correr el dashboard leyendo desde SQL
   DATA_BACKEND=sql python dashboard_dividendos.py

4) Cargar un mes manualmente a SQL desde la interfaz
   - Cambia a backend SQL
   - Usa el formulario mensual del dashboard

5) Actualizar SQL desde línea de comandos
   python dashboard_dividendos.py --upsert-file ruta/al/archivo.xlsx --backend sql
"""

from __future__ import annotations

import argparse
import base64
import io
import os
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
from dash import Dash, dcc, html, Input, Output, State, no_update, callback_context, dash_table
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# AG Grid es opcional. Si no está instalado, el dashboard usa dash_table.
try:
    import dash_ag_grid as dag  # type: ignore
    HAS_AG_GRID = True
except Exception:
    dag = None
    HAS_AG_GRID = False


# -------------------------------------------------------------------
# Configuración
# -------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_EXCEL_PATH = os.path.join(BASE_DIR, "panel_mensual_mina_chile_2021_2026_2.xlsx")


@dataclass
class Config:
    backend: str = os.getenv("DATA_BACKEND", "excel").lower()
    excel_path: str = os.getenv("EXCEL_PATH", DEFAULT_EXCEL_PATH)
    excel_sheet: str = os.getenv("EXCEL_SHEET", "panel_regresion")
    db_url: str = (
        os.getenv("SQLALCHEMY_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or f"sqlite:///{os.path.join(BASE_DIR, 'mina_dashboard.db')}"
    )
    sql_table: str = os.getenv("SQL_TABLE", "panel_mensual_mina_dashboard")
    auto_bootstrap_sql_if_empty: bool = os.getenv("AUTO_BOOTSTRAP_SQL_IF_EMPTY", "false").lower() == "true"
    auto_refresh_seconds: int = int(os.getenv("AUTO_REFRESH_SECONDS", "300"))
    http_timeout_seconds: int = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
    findic_base_url: str = os.getenv("FINDIC_BASE_URL", "https://findic.cl/api")
    bcch_ipc_general_url: str = os.getenv(
        "BCCH_IPC_GENERAL_URL",
        "https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/CAP_PRECIOS/MN_CAP_PRECIOS/IPC_G_2023",
    )
    yahoo_chart_base_url: str = os.getenv("YAHOO_CHART_BASE_URL", "https://query1.finance.yahoo.com/v8/finance/chart")
    yahoo_copper_symbol: str = os.getenv("YAHOO_COPPER_SYMBOL", "HG=F")
    yahoo_fx_symbol: str = os.getenv("YAHOO_FX_SYMBOL", "CLP=X")
    fred_copper_series_id: str = os.getenv("FRED_COPPER_SERIES_ID", "PCOPPUSDM")
    fred_fx_series_id: str = os.getenv("FRED_FX_SERIES_ID", "CCUSMA02CLM618N")
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8050"))
    debug: bool = os.getenv("DEBUG", "true").lower() == "true"


CFG = Config()
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}
LIBRAS_POR_TONELADA_METRICA = 2204.62262185
MESES_ES = {
    "Ene": 1,
    "Feb": 2,
    "Mar": 3,
    "Abr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Ago": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dic": 12,
}


# -------------------------------------------------------------------
# Utilidades generales
# -------------------------------------------------------------------

def serie(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series(np.nan, index=df.index, dtype="float64")


def fmt_es_num(valor: Optional[float], decimales: int = 1) -> str:
    if valor is None or pd.isna(valor):
        return "NA"
    txt = f"{valor:,.{decimales}f}"
    return txt.replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_es_moneda_mm(valor: Optional[float]) -> str:
    if valor is None or pd.isna(valor):
        return "NA"
    return f"{fmt_es_num(valor / 1_000_000, 1)} MM CLP"


def fmt_es_pct(valor: Optional[float], decimales: int = 1) -> str:
    if valor is None or pd.isna(valor):
        return "NA"
    return f"{fmt_es_num(100 * valor, decimales)}%"


def idx_base_100(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    base = s.dropna()
    if base.empty or float(base.iloc[0]) == 0:
        return pd.Series(np.nan, index=s.index, dtype="float64")
    return 100 * s / float(base.iloc[0])


def rolling_safe(s: pd.Series, ventana: int) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return s.rolling(window=ventana, min_periods=1).mean()


def resumen_crecimiento_anual(df: pd.DataFrame, col: str, agregacion: str) -> tuple[str, str]:
    aux = pd.DataFrame(
        {
            "fecha": pd.to_datetime(df["fecha"], errors="coerce"),
            "valor": pd.to_numeric(serie(df, col), errors="coerce"),
        }
    ).dropna(subset=["fecha", "valor"])

    if aux.empty:
        return "NA", "Sin datos"

    aux["anio"] = aux["fecha"].dt.year
    aux["mes"] = aux["fecha"].dt.month

    if agregacion == "sum":
        anual = aux.groupby("anio", as_index=False).agg(
            valor_anual=("valor", "sum"),
            meses=("mes", "nunique"),
        )
    elif agregacion == "mean":
        anual = aux.groupby("anio", as_index=False).agg(
            valor_anual=("valor", "mean"),
            meses=("mes", "nunique"),
        )
    else:
        raise ValueError(f"Agregacion no soportada: {agregacion}")

    anual = anual.loc[anual["meses"] == 12].copy()
    if anual.shape[0] < 2:
        return "NA", "Requiere 2 anos completos"

    anual["crecimiento_anual"] = anual["valor_anual"].pct_change()
    anual["crecimiento_anual"] = anual["crecimiento_anual"].replace([np.inf, -np.inf], np.nan)

    comparaciones = int(anual["crecimiento_anual"].notna().sum())
    promedio = anual["crecimiento_anual"].mean(skipna=True)
    if comparaciones == 0 or pd.isna(promedio):
        return "NA", "Base anual no comparable"

    anio_inicio = int(anual["anio"].min())
    anio_fin = int(anual["anio"].max())
    etiqueta = "comparacion" if comparaciones == 1 else "comparaciones"
    return fmt_es_pct(promedio), f"Años completos {anio_inicio}-{anio_fin} ({comparaciones} {etiqueta})"


def resumen_anual_valor(df: pd.DataFrame, col: str) -> pd.DataFrame:
    aux = pd.DataFrame(
        {
            "fecha": pd.to_datetime(df["fecha"], errors="coerce"),
            "valor": pd.to_numeric(serie(df, col), errors="coerce"),
        }
    ).dropna(subset=["fecha", "valor"])

    if aux.empty:
        return pd.DataFrame(columns=["anio", "total_anual", "promedio_mensual", "meses"])

    aux["anio"] = aux["fecha"].dt.year
    aux["mes"] = aux["fecha"].dt.month
    return aux.groupby("anio", as_index=False).agg(
        total_anual=("valor", "sum"),
        promedio_mensual=("valor", "mean"),
        meses=("mes", "nunique"),
    )


def resumen_promedio_anual(df: pd.DataFrame, col: str) -> tuple[str, str]:
    anual = resumen_anual_valor(df, col)
    if anual.empty:
        return "NA", "Sin datos"

    promedio = pd.to_numeric(anual["total_anual"], errors="coerce").mean(skipna=True)
    if pd.isna(promedio):
        return "NA", "Sin datos"

    anios = int(anual["anio"].nunique())
    etiqueta = "año" if anios == 1 else "años"
    return fmt_es_moneda_mm(promedio), f"Promedio de {anios} {etiqueta} del rango filtrado"


def leer_archivo_tabular(ruta: str, sheet_name: str = "panel_regresion") -> pd.DataFrame:
    ext = os.path.splitext(ruta)[1].lower()
    if ext in [".xlsx", ".xlsm", ".xls"]:
        try:
            return pd.read_excel(ruta, sheet_name=sheet_name)
        except Exception:
            return pd.read_excel(ruta)
    if ext == ".csv":
        return pd.read_csv(ruta)
    raise ValueError(f"Formato no soportado: {ext}")


def leer_upload(contents: str, filename: str, sheet_name: str = "panel_regresion") -> pd.DataFrame:
    _, content_string = contents.split(",", 1)
    decoded = base64.b64decode(content_string)
    nombre = (filename or "").lower()

    if nombre.endswith((".xlsx", ".xlsm", ".xls")):
        buffer = io.BytesIO(decoded)
        try:
            return pd.read_excel(buffer, sheet_name=sheet_name)
        except Exception:
            buffer.seek(0)
            return pd.read_excel(buffer)

    if nombre.endswith(".csv"):
        texto = decoded.decode("utf-8")
        return pd.read_csv(io.StringIO(texto))

    raise ValueError("Solo se aceptan archivos .xlsx, .xls o .csv")


# -------------------------------------------------------------------
# Preparación de datos
# -------------------------------------------------------------------

def preparar_dataframe(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "fecha" not in df.columns:
        if {"anio", "mes"}.issubset(df.columns):
            df["fecha"] = pd.to_datetime(
                dict(year=pd.to_numeric(df["anio"], errors="coerce"),
                     month=pd.to_numeric(df["mes"], errors="coerce"),
                     day=1),
                errors="coerce"
            )
        else:
            raise ValueError("No encuentro la columna 'fecha' ni el par 'anio'/'mes'.")

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    for col in df.columns:
        if col != "fecha":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("fecha").reset_index(drop=True)

    # Garantiza columnas clave aunque no vengan en el archivo
    columnas_esperadas = [
        "dividendo_total_nominal", "ipc", "cobre_usd_metric_ton", "fx_clp_usd",
        "anio", "mes", "dry_tons", "grade", "regalia",
        "dividendo_real_base_ultimo_ipc",
        "cobre_real_clp_metric_ton_base_ultimo_ipc"
    ]
    for col in columnas_esperadas:
        if col not in df.columns:
            df[col] = np.nan

    # Si anio y mes faltan o vienen incompletos, los recrea
    df["anio"] = df["fecha"].dt.year
    df["mes"] = df["fecha"].dt.month

    ipc_base = pd.to_numeric(df["ipc"], errors="coerce").dropna()
    if ipc_base.empty:
        raise ValueError("La columna ipc está vacía. No puedo construir series reales.")
    ipc_base = float(ipc_base.iloc[-1])

    # Dividendos reales
    dividendo_real_archivo = pd.to_numeric(serie(df, "dividendo_real_base_ultimo_ipc"), errors="coerce")
    dividendo_real_reconstruido = (
        pd.to_numeric(serie(df, "dividendo_total_nominal"), errors="coerce") * ipc_base /
        pd.to_numeric(serie(df, "ipc"), errors="coerce")
    )
    df["dividendo_real"] = dividendo_real_archivo.fillna(dividendo_real_reconstruido)

    # Precio real del cobre
    precio_cobre_real_reconstruido = (
        pd.to_numeric(serie(df, "cobre_usd_metric_ton"), errors="coerce") *
        pd.to_numeric(serie(df, "fx_clp_usd"), errors="coerce") *
        ipc_base / pd.to_numeric(serie(df, "ipc"), errors="coerce")
    )
    cobre_real_archivo = pd.to_numeric(serie(df, "cobre_real_clp_metric_ton_base_ultimo_ipc"), errors="coerce")
    df["cobre_real_clp_ton"] = cobre_real_archivo.fillna(precio_cobre_real_reconstruido)
    df["precio_cobre_real_clp_ton"] = precio_cobre_real_reconstruido

    # Producción y valor fino neto
    df["cobre_fino_ton"] = (
        pd.to_numeric(serie(df, "dry_tons"), errors="coerce") *
        pd.to_numeric(serie(df, "grade"), errors="coerce") / 100
    )

    regalia = pd.to_numeric(serie(df, "regalia"), errors="coerce")
    df["valor_cobre_fino_bruto_real"] = df["cobre_fino_ton"] * df["precio_cobre_real_clp_ton"]
    df["valor_cobre_fino_neto_real"] = np.where(
        regalia.notna(),
        df["valor_cobre_fino_bruto_real"] * (1 - regalia),
        np.nan
    )

    # Escalas útiles
    df["dividendo_real_mm"] = df["dividendo_real"] / 1_000_000
    df["valor_cobre_fino_neto_real_mm"] = df["valor_cobre_fino_neto_real"] / 1_000_000
    df["valor_cobre_fino_bruto_real_mm"] = df["valor_cobre_fino_bruto_real"] / 1_000_000
    df["precio_cobre_real_clp_ton_mm"] = df["precio_cobre_real_clp_ton"] / 1_000_000

    # Índices base 100
    df["indice_dividendo_real"] = idx_base_100(df["dividendo_real"])
    df["indice_cobre_real"] = idx_base_100(df["cobre_real_clp_ton"])
    df["indice_valor_cobre_fino"] = idx_base_100(df["valor_cobre_fino_neto_real"])
    # Variable simple de cobre bajo usando promedio del mes t y t-1
    cobre_prom_2m = (df["cobre_real_clp_ton"] + df["cobre_real_clp_ton"].shift(1)) / 2
    q25 = cobre_prom_2m.dropna().quantile(0.25) if cobre_prom_2m.dropna().shape[0] > 0 else np.nan
    df["cobre_prom_2m"] = cobre_prom_2m
    df["cobre_bajo_q25_calc"] = np.where(
        cobre_prom_2m.notna(),
        (cobre_prom_2m <= q25).astype(int),
        np.nan
    )

    # Ratios para revisar rarezas
    df["ratio_div_sobre_valor_fino"] = df["dividendo_real"] / df["valor_cobre_fino_neto_real"]
    df["dividendo_lag1"] = df["dividendo_real"].shift(1)
    df["cobre_real_lag1"] = df["cobre_real_clp_ton"].shift(1)
    df["valor_fino_lag1"] = df["valor_cobre_fino_neto_real"].shift(1)
    df["valor_fino_prom_2m"] = (
        df[["valor_cobre_fino_neto_real", "valor_fino_lag1"]]
        .mean(axis=1, skipna=True)
    )

    df["fecha_label"] = df["fecha"].dt.strftime("%Y-%m")

    return df


# -------------------------------------------------------------------
# SQL
# -------------------------------------------------------------------

def normalizar_db_url(db_url: str) -> str:
    """Normaliza URLs de Postgres de servicios externos para SQLAlchemy.

    Algunas plataformas entregan URLs con el prefijo postgres://, pero
    SQLAlchemy espera postgresql://. Si tu proveedor exige SSL y la URL no
    trae sslmode, define DB_SSLMODE=require en Render.
    """
    if not db_url:
        raise ValueError("Falta configurar DATABASE_URL o SQLALCHEMY_DATABASE_URL.")

    db_url = db_url.strip()
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    sslmode = os.getenv("DB_SSLMODE", "").strip()
    if sslmode and db_url.startswith(("postgresql://", "postgresql+psycopg2://")) and "sslmode=" not in db_url:
        separador = "&" if "?" in db_url else "?"
        db_url = f"{db_url}{separador}sslmode={sslmode}"

    return db_url


def get_engine(db_url: str):
    return create_engine(
        normalizar_db_url(db_url),
        future=True,
        pool_pre_ping=True,
        pool_recycle=300,
    )


def cargar_desde_excel(excel_path: str, sheet_name: str) -> pd.DataFrame:
    df = leer_archivo_tabular(excel_path, sheet_name=sheet_name)
    return preparar_dataframe(df)


def cargar_desde_sql(db_url: str, table_name: str) -> pd.DataFrame:
    engine = get_engine(db_url)
    try:
        df = pd.read_sql_table(table_name, engine)
    except Exception:
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    return preparar_dataframe(df)


def guardar_en_sql(df: pd.DataFrame, db_url: str, table_name: str) -> pd.DataFrame:
    engine = get_engine(db_url)
    df_out = preparar_dataframe(df)
    df_out.to_sql(table_name, engine, if_exists="replace", index=False, method="multi")
    return df_out


def bootstrap_excel_a_sql(excel_path: str, sheet_name: str, db_url: str, table_name: str) -> pd.DataFrame:
    df = cargar_desde_excel(excel_path, sheet_name)
    engine = get_engine(db_url)
    df.to_sql(table_name, engine, if_exists="replace", index=False, method="multi")
    return df


def upsert_dataframe_sql(df_nuevo_raw: pd.DataFrame, db_url: str, table_name: str) -> pd.DataFrame:
    nuevo = preparar_dataframe(df_nuevo_raw)
    engine = get_engine(db_url)

    try:
        actual = pd.read_sql_table(table_name, engine)
        actual = preparar_dataframe(actual)
    except Exception:
        actual = pd.DataFrame()

    combinado = pd.concat([actual, nuevo], ignore_index=True)

    # Si llega una corrección para un mes ya existente, se queda con el último registro cargado
    if "fecha" not in combinado.columns:
        raise ValueError("No encuentro la columna fecha para hacer el upsert.")

    combinado["fecha"] = pd.to_datetime(combinado["fecha"], errors="coerce")
    combinado["_fecha_mes"] = combinado["fecha"].dt.to_period("M")
    combinado["_orden_carga"] = np.arange(len(combinado))
    combinado = (
        combinado.sort_values(["_fecha_mes", "_orden_carga"])
        .drop_duplicates(subset=["_fecha_mes"], keep="last")
        .drop(columns=["_fecha_mes", "_orden_carga"])
    )
    combinado = preparar_dataframe(combinado)
    combinado.to_sql(table_name, engine, if_exists="replace", index=False, method="multi")
    return combinado


def upsert_archivo_mensual(ruta_archivo: str, db_url: str, table_name: str, sheet_name: str) -> pd.DataFrame:
    df_nuevo = leer_archivo_tabular(ruta_archivo, sheet_name=sheet_name)
    return upsert_dataframe_sql(df_nuevo, db_url, table_name)


def cargar_datos(config: Config) -> pd.DataFrame:
    if config.backend == "excel":
        return cargar_desde_excel(config.excel_path, config.excel_sheet)
    if config.backend == "sql":
        try:
            return cargar_desde_sql(config.db_url, config.sql_table)
        except Exception:
            if config.auto_bootstrap_sql_if_empty:
                return bootstrap_excel_a_sql(
                    config.excel_path,
                    config.excel_sheet,
                    config.db_url,
                    config.sql_table,
                )
            raise
    raise ValueError("backend debe ser 'excel' o 'sql'")


def normalizar_fecha_mes(fecha: str | pd.Timestamp) -> pd.Timestamp:
    fecha_ts = pd.to_datetime(fecha, errors="coerce")
    if pd.isna(fecha_ts):
        raise ValueError("La fecha del registro no es válida.")
    return fecha_ts.to_period("M").to_timestamp()


def parsear_mes_es(texto: object) -> pd.Timestamp:
    texto_limpio = str(texto).strip()
    match = re.fullmatch(r"([A-Za-z]{3})\.(\d{4})", texto_limpio)
    if not match:
        return pd.NaT

    mes = MESES_ES.get(match.group(1).title())
    if mes is None:
        return pd.NaT

    return pd.Timestamp(year=int(match.group(2)), month=mes, day=1)


def descargar_json_publico(url: str, timeout_seconds: int) -> dict:
    response = requests.get(url, timeout=timeout_seconds, headers=HTTP_HEADERS)
    response.raise_for_status()
    return response.json()


def descargar_html_publico(url: str, timeout_seconds: int) -> str:
    response = requests.get(url, timeout=timeout_seconds, headers=HTTP_HEADERS)
    response.raise_for_status()
    return response.text


def resumir_promedio_mensual(fecha_mes: pd.Timestamp, fecha_final: pd.Timestamp, observaciones: int) -> str:
    return (
        f"promedio diario de {fecha_mes.strftime('%Y-%m')} "
        f"con {observaciones} observaciones hasta {fecha_final.strftime('%Y-%m-%d')}"
    )


def promedio_diario_para_mes(
    serie: pd.DataFrame,
    fecha_mes: str | pd.Timestamp,
    nombre_serie: str,
) -> tuple[float, pd.Timestamp, pd.Timestamp, int]:
    fecha_ref = normalizar_fecha_mes(fecha_mes)
    fecha_fin_mes = fecha_ref + pd.offsets.MonthEnd(1)

    datos_mes = serie.loc[(serie["fecha"] >= fecha_ref) & (serie["fecha"] <= fecha_fin_mes)].copy()
    if datos_mes.empty:
        ultima_fecha = serie["fecha"].max() if not serie.empty else pd.NaT
        if pd.notna(ultima_fecha):
            raise ValueError(
                f"{nombre_serie} no tiene datos para {fecha_ref.strftime('%Y-%m')}. "
                f"Ultima fecha disponible: {pd.Timestamp(ultima_fecha).strftime('%Y-%m-%d')}."
            )
        raise ValueError(f"{nombre_serie} no devolvio observaciones utilizables.")

    return (
        float(datos_mes["valor"].mean()),
        pd.Timestamp(datos_mes["fecha"].min()),
        pd.Timestamp(datos_mes["fecha"].max()),
        int(datos_mes.shape[0]),
    )


def descargar_serie_findic(codigo: str, timeout_seconds: int, base_url: str) -> pd.DataFrame:
    url = f"{base_url.rstrip('/')}/{codigo}"
    payload = descargar_json_publico(url, timeout_seconds)
    serie = pd.DataFrame(payload.get("serie", []))
    if serie.empty:
        raise ValueError(f"findic no devolvio datos para {codigo}.")

    serie["fecha"] = pd.to_datetime(serie["fecha"], errors="coerce")
    serie["valor"] = pd.to_numeric(serie["valor"], errors="coerce")
    serie = serie.loc[serie["fecha"].notna() & serie["valor"].notna(), ["fecha", "valor"]].copy()
    if serie.empty:
        raise ValueError(f"findic no devolvio valores utilizables para {codigo}.")

    return serie.sort_values("fecha").reset_index(drop=True)


def obtener_valor_findic_para_mes(
    codigo: str,
    fecha_mes: str | pd.Timestamp,
    timeout_seconds: int,
    base_url: str,
    factor: float = 1.0,
) -> tuple[float, pd.Timestamp, pd.Timestamp, int]:
    serie = descargar_serie_findic(codigo, timeout_seconds, base_url)
    serie["valor"] = serie["valor"] * factor
    return promedio_diario_para_mes(serie, fecha_mes, f"findic/{codigo}")


def descargar_serie_yahoo(
    symbol: str,
    fecha_mes: str | pd.Timestamp,
    timeout_seconds: int,
    base_url: str,
) -> pd.DataFrame:
    fecha_ref = normalizar_fecha_mes(fecha_mes)
    fecha_fin_mes = fecha_ref + pd.offsets.MonthEnd(1) + pd.Timedelta(days=1)
    period1 = int(pd.Timestamp(fecha_ref).tz_localize("UTC").timestamp())
    period2 = int(pd.Timestamp(fecha_fin_mes).tz_localize("UTC").timestamp())
    symbol_encoded = quote(symbol, safe="")
    url = (
        f"{base_url.rstrip('/')}/{symbol_encoded}"
        f"?period1={period1}&period2={period2}&interval=1d&includePrePost=false&events=div%2Csplits"
    )
    payload = descargar_json_publico(url, timeout_seconds)
    resultados = payload.get("chart", {}).get("result") or []
    if not resultados:
        raise ValueError(f"Yahoo Finance no devolvio datos para {symbol}.")

    resultado = resultados[0]
    timestamps = resultado.get("timestamp") or []
    quote_data = ((resultado.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote_data.get("close") or []
    if not timestamps or not closes:
        raise ValueError(f"Yahoo Finance no devolvio cierres para {symbol}.")

    serie = pd.DataFrame(
        {
            "fecha": pd.to_datetime(timestamps, unit="s", utc=True).tz_localize(None),
            "valor": pd.to_numeric(closes, errors="coerce"),
        }
    )
    serie["fecha"] = serie["fecha"].dt.normalize()
    serie = serie.loc[serie["fecha"].notna() & serie["valor"].notna(), ["fecha", "valor"]].copy()
    if serie.empty:
        raise ValueError(f"Yahoo Finance no devolvio valores utilizables para {symbol}.")

    return serie.sort_values("fecha").reset_index(drop=True)


def obtener_valor_yahoo_para_mes(
    symbol: str,
    fecha_mes: str | pd.Timestamp,
    timeout_seconds: int,
    base_url: str,
    factor: float = 1.0,
) -> tuple[float, pd.Timestamp, pd.Timestamp, int]:
    serie = descargar_serie_yahoo(symbol, fecha_mes, timeout_seconds, base_url)
    serie["valor"] = serie["valor"] * factor
    return promedio_diario_para_mes(serie, fecha_mes, f"Yahoo Finance/{symbol}")


def descargar_tabla_html(url: str, timeout_seconds: int) -> pd.DataFrame:
    html = descargar_html_publico(url, timeout_seconds)
    tablas = pd.read_html(io.StringIO(html), decimal=",", thousands=".")
    if not tablas:
        raise ValueError(f"No pude leer tablas desde {url}.")
    return tablas[0]


def descargar_serie_bcch_ipc_general(url: str, timeout_seconds: int) -> pd.DataFrame:
    tabla = descargar_tabla_html(url, timeout_seconds)
    if "Serie" not in tabla.columns:
        raise ValueError("La tabla del Banco Central no contiene la columna Serie.")

    fila = tabla.loc[tabla["Serie"].astype(str).str.strip().eq("IPC General")]
    if fila.empty:
        raise ValueError("No encontre la fila 'IPC General' en la tabla del Banco Central.")

    fila = fila.iloc[0]
    registros = []
    for columna in tabla.columns:
        fecha_col = parsear_mes_es(columna)
        if pd.isna(fecha_col):
            continue

        valor = pd.to_numeric(fila[columna], errors="coerce")
        if pd.notna(valor):
            registros.append({"fecha": fecha_col, "valor": float(valor)})

    if not registros:
        raise ValueError("No pude construir la serie de IPC General desde el Banco Central.")

    return pd.DataFrame(registros).sort_values("fecha").reset_index(drop=True)


def descargar_serie_fred(series_id: str, timeout_seconds: int) -> pd.DataFrame:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    out = pd.read_csv(io.StringIO(response.text))

    if out.shape[1] < 2:
        raise ValueError(f"No pude leer la serie FRED {series_id}.")

    out = out.rename(columns={out.columns[0]: "fecha", out.columns[1]: "valor"})
    out["fecha"] = pd.to_datetime(out["fecha"], errors="coerce")
    out["valor"] = pd.to_numeric(out["valor"], errors="coerce")
    out = out.loc[out["fecha"].notna() & out["valor"].notna()].copy()

    if out.empty:
        raise ValueError(f"La serie FRED {series_id} no devolvió valores utilizables.")

    return out.sort_values("fecha").reset_index(drop=True)


def obtener_valor_fred_para_mes(series_id: str, fecha_mes: str | pd.Timestamp, timeout_seconds: int) -> tuple[float, pd.Timestamp]:
    serie = descargar_serie_fred(series_id, timeout_seconds)
    fecha_ref = normalizar_fecha_mes(fecha_mes)

    exacto = serie.loc[serie["fecha"] == fecha_ref]
    if not exacto.empty:
        fila = exacto.iloc[-1]
        return float(fila["valor"]), pd.Timestamp(fila["fecha"])

    historico = serie.loc[serie["fecha"] <= fecha_ref]
    if historico.empty:
        raise ValueError(f"No encontré datos FRED para {series_id} hasta {fecha_ref.strftime('%Y-%m')}.")

    fila = historico.iloc[-1]
    return float(fila["valor"]), pd.Timestamp(fila["fecha"])


def obtener_cobre_para_mes(fecha_mes: str | pd.Timestamp, config: Config) -> dict[str, object]:
    errores = []
    fecha_ref = normalizar_fecha_mes(fecha_mes)

    try:
        valor, fecha_inicio, fecha_final, observaciones = obtener_valor_findic_para_mes(
            "libra_cobre",
            fecha_ref,
            config.http_timeout_seconds,
            config.findic_base_url,
            factor=LIBRAS_POR_TONELADA_METRICA,
        )
        return {
            "valor": valor,
            "fecha": fecha_final,
            "fuente": "findic",
            "detalle": resumir_promedio_mensual(fecha_ref, fecha_final, observaciones),
        }
    except Exception as exc:
        errores.append(f"findic: {exc}")

    try:
        valor, fecha_inicio, fecha_final, observaciones = obtener_valor_yahoo_para_mes(
            config.yahoo_copper_symbol,
            fecha_ref,
            config.http_timeout_seconds,
            config.yahoo_chart_base_url,
            factor=LIBRAS_POR_TONELADA_METRICA,
        )
        return {
            "valor": valor,
            "fecha": fecha_final,
            "fuente": "Yahoo Finance",
            "detalle": resumir_promedio_mensual(fecha_ref, fecha_final, observaciones),
        }
    except Exception as exc:
        errores.append(f"Yahoo Finance: {exc}")

    try:
        valor, fecha = obtener_valor_fred_para_mes(
            config.fred_copper_series_id,
            fecha_ref,
            config.http_timeout_seconds,
        )
        return {
            "valor": valor,
            "fecha": fecha,
            "fuente": "FRED",
            "detalle": f"valor mensual publicado para {pd.Timestamp(fecha).strftime('%Y-%m')}",
        }
    except Exception as exc:
        errores.append(f"FRED: {exc}")

    raise ValueError("No pude obtener cobre automaticamente. " + " | ".join(errores))


def obtener_fx_para_mes(fecha_mes: str | pd.Timestamp, config: Config) -> dict[str, object]:
    errores = []
    fecha_ref = normalizar_fecha_mes(fecha_mes)

    try:
        valor, fecha_inicio, fecha_final, observaciones = obtener_valor_findic_para_mes(
            "dolar",
            fecha_ref,
            config.http_timeout_seconds,
            config.findic_base_url,
        )
        return {
            "valor": valor,
            "fecha": fecha_final,
            "fuente": "findic",
            "detalle": resumir_promedio_mensual(fecha_ref, fecha_final, observaciones),
        }
    except Exception as exc:
        errores.append(f"findic: {exc}")

    try:
        valor, fecha_inicio, fecha_final, observaciones = obtener_valor_yahoo_para_mes(
            config.yahoo_fx_symbol,
            fecha_ref,
            config.http_timeout_seconds,
            config.yahoo_chart_base_url,
        )
        return {
            "valor": valor,
            "fecha": fecha_final,
            "fuente": "Yahoo Finance",
            "detalle": resumir_promedio_mensual(fecha_ref, fecha_final, observaciones),
        }
    except Exception as exc:
        errores.append(f"Yahoo Finance: {exc}")

    try:
        valor, fecha = obtener_valor_fred_para_mes(
            config.fred_fx_series_id,
            fecha_ref,
            config.http_timeout_seconds,
        )
        return {
            "valor": valor,
            "fecha": fecha,
            "fuente": "FRED",
            "detalle": f"valor mensual publicado para {pd.Timestamp(fecha).strftime('%Y-%m')}",
        }
    except Exception as exc:
        errores.append(f"FRED: {exc}")

    raise ValueError("No pude obtener USD/CLP automaticamente. " + " | ".join(errores))


def obtener_ipc_para_mes(
    fecha_mes: str | pd.Timestamp,
    config: Config,
    permitir_ultimo_disponible: bool = False,
) -> dict[str, object]:
    fecha_ref = normalizar_fecha_mes(fecha_mes)
    serie = descargar_serie_bcch_ipc_general(config.bcch_ipc_general_url, config.http_timeout_seconds)
    exacto = serie.loc[serie["fecha"] == fecha_ref]

    if not exacto.empty:
        fila = exacto.iloc[-1]
        return {
            "ipc": float(fila["valor"]),
            "fecha_ipc": pd.Timestamp(fila["fecha"]),
            "fuente_ipc": "Banco Central de Chile",
            "detalle_ipc": f"IPC General base 2023=100 para {fecha_ref.strftime('%Y-%m')}",
            "es_rezago": False,
            "mes_solicitado": fecha_ref,
        }

    historico = serie.loc[serie["fecha"] <= fecha_ref]
    ultima_fecha = historico["fecha"].max() if not historico.empty else (serie["fecha"].max() if not serie.empty else pd.NaT)
    if permitir_ultimo_disponible and not historico.empty:
        fila = historico.sort_values("fecha").iloc[-1]
        return {
            "ipc": float(fila["valor"]),
            "fecha_ipc": pd.Timestamp(fila["fecha"]),
            "fuente_ipc": "Banco Central de Chile",
            "detalle_ipc": (
                f"IPC General base 2023=100 de {pd.Timestamp(fila['fecha']).strftime('%Y-%m')} "
                f"usado como ultimo dato disponible para {fecha_ref.strftime('%Y-%m')}"
            ),
            "es_rezago": True,
            "mes_solicitado": fecha_ref,
        }

    if pd.notna(ultima_fecha):
        raise ValueError(
            f"Banco Central no tiene IPC General para {fecha_ref.strftime('%Y-%m')}. "
            f"Ultimo mes disponible: {pd.Timestamp(ultima_fecha).strftime('%Y-%m')}."
        )
    raise ValueError("Banco Central no devolvio la serie de IPC General.")


def construir_prompt_ipc_rezago(ipc_info: dict[str, object], accion: str) -> str:
    mes_solicitado = pd.Timestamp(ipc_info["mes_solicitado"]).strftime("%Y-%m")
    mes_disponible = pd.Timestamp(ipc_info["fecha_ipc"]).strftime("%Y-%m")
    etiqueta_accion = "guardar el registro" if accion == "guardar" else "completar el formulario"
    return {
        "mensaje": (
            f"No hay IPC General para {mes_solicitado}. "
            f"Ultimo mes disponible: {mes_disponible} con valor {fmt_es_num(float(ipc_info['ipc']), 2)}. "
            f"Quieres continuar y {etiqueta_accion} usando ese dato?"
        )
    }["mensaje"]


def obtener_mercado_para_mes(fecha_mes: str | pd.Timestamp, config: Config) -> dict[str, object]:
    cobre = obtener_cobre_para_mes(fecha_mes, config)
    fx = obtener_fx_para_mes(fecha_mes, config)
    return {
        "cobre_usd_metric_ton": float(cobre["valor"]),
        "fx_clp_usd": float(fx["valor"]),
        "fecha_cobre": pd.Timestamp(cobre["fecha"]),
        "fecha_fx": pd.Timestamp(fx["fecha"]),
        "fuente_cobre": str(cobre["fuente"]),
        "fuente_fx": str(fx["fuente"]),
        "detalle_cobre": str(cobre["detalle"]),
        "detalle_fx": str(fx["detalle"]),
    }


def construir_registro_manual(
    fecha: str,
    dividendo_total_nominal: Optional[float],
    ipc: Optional[float],
    dry_tons: Optional[float],
    regalia: Optional[float],
    grade: Optional[float],
    cobre_usd_metric_ton: Optional[float],
    fx_clp_usd: Optional[float],
    config: Config,
    permitir_ipc_rezago: bool = False,
) -> tuple[pd.DataFrame, str]:
    fecha_mes = normalizar_fecha_mes(fecha)

    faltantes = []
    for nombre, valor in [
        ("dividendo nominal", dividendo_total_nominal),
        ("toneladas secas", dry_tons),
        ("regalia", regalia),
        ("ley", grade),
    ]:
        if valor is None or pd.isna(valor):
            faltantes.append(nombre)

    if faltantes:
        raise ValueError("Faltan campos obligatorios: " + ", ".join(faltantes) + ".")

    mensajes_auto = []

    if ipc is None or pd.isna(ipc):
        ipc_info = obtener_ipc_para_mes(
            fecha_mes,
            config,
            permitir_ultimo_disponible=permitir_ipc_rezago,
        )
        ipc = float(ipc_info["ipc"])
        mensajes_auto.append(
            f"IPC desde {ipc_info['fuente_ipc']} ({ipc_info['detalle_ipc']})"
        )

    if cobre_usd_metric_ton is None or pd.isna(cobre_usd_metric_ton) or fx_clp_usd is None or pd.isna(fx_clp_usd):
        mercado = obtener_mercado_para_mes(fecha_mes, config)
        if cobre_usd_metric_ton is None or pd.isna(cobre_usd_metric_ton):
            cobre_usd_metric_ton = float(mercado["cobre_usd_metric_ton"])
            mensajes_auto.append(
                f"cobre desde {mercado['fuente_cobre']} ({mercado['detalle_cobre']})"
            )
        if fx_clp_usd is None or pd.isna(fx_clp_usd):
            fx_clp_usd = float(mercado["fx_clp_usd"])
            mensajes_auto.append(
                f"USD/CLP desde {mercado['fuente_fx']} ({mercado['detalle_fx']})"
            )

    mensaje_mercado = ""
    if mensajes_auto:
        mensaje_mercado = " Se completaron automaticamente " + "; ".join(mensajes_auto) + "."

    regalia_valor = float(regalia)
    if regalia_valor > 1:
        regalia_valor = regalia_valor / 100

    fila = pd.DataFrame(
        [
            {
                "fecha": fecha_mes,
                "dividendo_total_nominal": float(dividendo_total_nominal),
                "ipc": float(ipc),
                "dry_tons": float(dry_tons),
                "regalia": regalia_valor,
                "grade": float(grade),
                "cobre_usd_metric_ton": float(cobre_usd_metric_ton),
                "fx_clp_usd": float(fx_clp_usd),
            }
        ]
    )
    return fila, mensaje_mercado


def upsert_registro_manual(
    fecha: str,
    dividendo_total_nominal: Optional[float],
    ipc: Optional[float],
    dry_tons: Optional[float],
    regalia: Optional[float],
    grade: Optional[float],
    cobre_usd_metric_ton: Optional[float],
    fx_clp_usd: Optional[float],
    db_url: str,
    table_name: str,
    config: Config,
    permitir_ipc_rezago: bool = False,
) -> tuple[pd.DataFrame, str]:
    fila_nueva, mensaje_mercado = construir_registro_manual(
        fecha=fecha,
        dividendo_total_nominal=dividendo_total_nominal,
        ipc=ipc,
        dry_tons=dry_tons,
        regalia=regalia,
        grade=grade,
        cobre_usd_metric_ton=cobre_usd_metric_ton,
        fx_clp_usd=fx_clp_usd,
        config=config,
        permitir_ipc_rezago=permitir_ipc_rezago,
    )

    engine = get_engine(db_url)
    try:
        actual = pd.read_sql_table(table_name, engine)
    except Exception:
        actual = pd.DataFrame()

    if not actual.empty and "fecha" in actual.columns:
        actual["fecha"] = pd.to_datetime(actual["fecha"], errors="coerce")
        mascara_mes = actual["fecha"].dt.to_period("M") == fila_nueva.loc[0, "fecha"].to_period("M")
        if mascara_mes.any():
            base = actual.loc[mascara_mes].sort_values("fecha").tail(1).copy()
            for col, valor in fila_nueva.iloc[0].items():
                base[col] = valor
            fila_nueva = base

    df_final = upsert_dataframe_sql(fila_nueva, db_url, table_name)
    return df_final, mensaje_mercado


# -------------------------------------------------------------------
# Modelo simple de anomalías
# -------------------------------------------------------------------

def ajustar_modelo_anomalias(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work = work.sort_values("fecha").reset_index(drop=True)

    work["tendencia"] = np.arange(len(work), dtype=float)
    work["dividendo_lag1"] = work["dividendo_real"].shift(1)
    work["valor_fino_lag1"] = work["valor_cobre_fino_neto_real"].shift(1)
    work["valor_fino_prom_2m"] = work[["valor_cobre_fino_neto_real", "valor_fino_lag1"]].mean(axis=1, skipna=True)

    # Modelo extendido si hay datos de producción
    usa_produccion = work["valor_cobre_fino_neto_real"].notna().sum() >= 12

    X = pd.DataFrame({"constante": 1.0}, index=work.index)
    X["ln_cobre_real"] = np.log(work["cobre_real_clp_ton"].clip(lower=1))
    X["ln_dividendo_lag1"] = np.log1p(work["dividendo_lag1"].clip(lower=0))
    X["tendencia"] = work["tendencia"]

    if usa_produccion:
        X["ln_valor_fino_prom_2m"] = np.log(work["valor_fino_prom_2m"].clip(lower=1))

    y = np.log1p(work["dividendo_real"].clip(lower=0))

    mask = np.isfinite(y)
    for col in X.columns:
        mask &= np.isfinite(X[col])

    if mask.sum() < max(10, len(X.columns) + 4):
        work["pred_dividendo_real"] = np.nan
        work["residuo_modelo"] = np.nan
        work["z_residuo"] = np.nan
        work["anomalia"] = False
        work["modelo_usado"] = "sin_modelo"
        return work

    X_fit = X.loc[mask].to_numpy(dtype=float)
    y_fit = y.loc[mask].to_numpy(dtype=float)

    beta, *_ = np.linalg.lstsq(X_fit, y_fit, rcond=None)

    pred_log = np.full(shape=len(work), fill_value=np.nan, dtype=float)
    mask_pred = np.isfinite(X).all(axis=1).to_numpy()
    pred_log[mask_pred] = X.loc[mask_pred].to_numpy(dtype=float) @ beta

    work["pred_dividendo_real"] = np.expm1(pred_log)
    work["residuo_modelo"] = work["dividendo_real"] - work["pred_dividendo_real"]

    resid = work["residuo_modelo"].copy()
    media = resid.mean(skipna=True)
    desvio = resid.std(skipna=True, ddof=0)
    if pd.isna(desvio) or desvio == 0:
        work["z_residuo"] = np.nan
    else:
        work["z_residuo"] = (resid - media) / desvio

    work["anomalia"] = work["z_residuo"].abs() >= 1.5
    work["modelo_usado"] = "extendido_con_produccion" if usa_produccion else "base_sin_produccion"

    return work


# -------------------------------------------------------------------
# Gráficos
# -------------------------------------------------------------------

AZUL_OSCURO = "#143B4D"
AZUL_PRIMARIO = "#146C94"
AZUL_MEDIO = "#2F80C9"
AZUL_CLARO = "#CFE7FA"
AZUL_SUAVE = "#EEF6FD"
AZUL_TINTA = "#17212B"
AZUL_MUTED = "#61707A"
AZUL_BORDE = "#D7E4E6"
AZUL_PANEL = "#FAFCFD"
AZUL_FONDO = "#F4F7F8"
AZUL_ACENTO_1 = "#1B8FD1"
AZUL_ACENTO_2 = "#5EB4E7"
AZUL_ACENTO_3 = "#8FD3FF"
AZUL_ACENTO_4 = "#0A5E86"
PALETA_AZUL = [AZUL_PRIMARIO, AZUL_ACENTO_1, AZUL_MEDIO, AZUL_ACENTO_2, AZUL_ACENTO_4, AZUL_OSCURO]
FONT_FAMILY = "'Trebuchet MS', 'Segoe UI', sans-serif"


def aplicar_estilo_figura(
    fig: go.Figure,
    titulo: str,
    xaxis_title: str = "Fecha",
    yaxis_title: Optional[str] = None,
    hovermode: str = "x unified",
    legend_title: str = "Serie",
) -> go.Figure:
    fig.update_layout(
        template="plotly_white",
        title={
            "text": titulo,
            "x": 0.01,
            "xanchor": "left",
            "font": {"size": 20, "color": AZUL_TINTA},
            "pad": {"b": 40},
        },
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor=AZUL_PANEL,
        font={"family": FONT_FAMILY, "color": AZUL_TINTA},
        colorway=PALETA_AZUL,
        hovermode=hovermode,
        transition={"duration": 3500, "easing": "cubic-in-out"},
        legend={
            "title": {"text": legend_title},
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0,
        },
        hoverlabel={"bgcolor": "white", "bordercolor": AZUL_BORDE, "font": {"family": FONT_FAMILY}},
        margin=dict(l=40, r=20, t=116, b=46),
    )
    fig.update_xaxes(
        title_text=xaxis_title,
        showgrid=True,
        gridcolor=AZUL_BORDE,
        zeroline=False,
        linecolor=AZUL_BORDE,
        ticks="outside",
        tickcolor=AZUL_BORDE,
        showspikes=True,
        spikecolor=AZUL_MEDIO,
        spikethickness=1,
        spikedash="dot",
        spikemode="across",
        spikesnap="cursor",
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=AZUL_BORDE,
        zeroline=False,
        linecolor=AZUL_BORDE,
        ticks="outside",
        tickcolor=AZUL_BORDE,
        showspikes=True,
        spikecolor=AZUL_MEDIO,
        spikethickness=1,
        spikedash="dot",
    )
    if yaxis_title is not None:
        fig.update_yaxes(title_text=yaxis_title)
    return fig

def grafico_dividendos(df: pd.DataFrame, ventana_ma: int) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df["fecha"],
            y=df["dividendo_real_mm"],
            name="Dividendo real",
            opacity=0.72,
            marker=dict(color=AZUL_CLARO, line=dict(color=AZUL_MEDIO, width=1)),
            hovertemplate="%{x|%Y-%m}<br>%{y:,.1f} MM CLP<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["fecha"],
            y=rolling_safe(df["dividendo_real_mm"], ventana_ma),
            mode="lines+markers",
            name=f"Promedio móvil {ventana_ma}m",
            line=dict(width=3.2, color=AZUL_OSCURO, shape="spline"),
            marker=dict(size=5, color=AZUL_OSCURO, line=dict(color="white", width=1)),
            connectgaps=True,
            hovertemplate="%{x|%Y-%m}<br>%{y:,.1f} MM CLP<extra></extra>",
        )
    )

    return aplicar_estilo_figura(
        fig,
        titulo=f"Dividendos reales y promedio móvil de {ventana_ma} meses",
        yaxis_title="MM CLP reales",
    )


def grafico_indices(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["fecha"],
            y=df["indice_dividendo_real"],
            mode="lines+markers",
            name="Dividendos reales",
            line=dict(width=3, color=AZUL_OSCURO, shape="spline"),
            marker=dict(size=5, color=AZUL_OSCURO, line=dict(color="white", width=1)),
            connectgaps=True,
            hovertemplate="%{x|%Y-%m}<br>Índice %{y:,.1f}<extra></extra>",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df["fecha"],
            y=df["indice_cobre_real"],
            mode="lines+markers",
            name="Cobre real",
            line=dict(width=2.5, color=AZUL_PRIMARIO, shape="spline"),
            marker=dict(size=5, color=AZUL_PRIMARIO, line=dict(color="white", width=1)),
            connectgaps=True,
            hovertemplate="%{x|%Y-%m}<br>Índice %{y:,.1f}<extra></extra>",
        )
    )

    if df["indice_valor_cobre_fino"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=df["fecha"],
                y=df["indice_valor_cobre_fino"],
                mode="lines+markers",
                name="Valor cobre fino neto real",
                line=dict(width=2.5, color=AZUL_ACENTO_1, shape="spline"),
                marker=dict(size=5, color=AZUL_ACENTO_1, line=dict(color="white", width=1)),
                connectgaps=True,
                hovertemplate="%{x|%Y-%m}<br>Índice %{y:,.1f}<extra></extra>",
            )
        )

    return aplicar_estilo_figura(
        fig,
        titulo="Índices base 100 de dividendos, cobre y valor fino",
        yaxis_title="Índice base 100",
    )


def grafico_produccion(df: pd.DataFrame) -> go.Figure:
    hay_dry_tons = df["dry_tons"].notna().any()
    hay_grade = df["grade"].notna().any()
    ancho_barra_ms = 26 * 24 * 60 * 60 * 1000

    if hay_dry_tons and hay_grade:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            row_heights=[0.68, 0.32],
        )
        fila_dry_tons = 1
        fila_grade = 2
    else:
        fig = make_subplots(rows=1, cols=1)
        fila_dry_tons = 1
        fila_grade = 1

    if hay_dry_tons:
        fig.add_trace(
            go.Bar(
                x=df["fecha"],
                y=df["dry_tons"],
                name="Toneladas secas",
                width=ancho_barra_ms,
                opacity=1.0,
                marker=dict(color=AZUL_PRIMARIO, line=dict(color=AZUL_OSCURO, width=1.3)),
                hovertemplate="%{x|%Y-%m}<br>%{y:,.0f} toneladas secas<extra></extra>",
            ),
            row=fila_dry_tons,
            col=1,
        )

    if hay_grade:
        fig.add_trace(
            go.Scatter(
                x=df["fecha"],
                y=df["grade"],
                mode="lines+markers",
                name="Ley (%)",
                line=dict(width=2.6, color=AZUL_ACENTO_2, shape="spline"),
                marker=dict(size=7, color=AZUL_ACENTO_2, line=dict(color="white", width=1.1)),
                connectgaps=True,
                hovertemplate="%{x|%Y-%m}<br>%{y:,.2f}%<extra></extra>",
            ),
            row=fila_grade,
            col=1,
        )

    aplicar_estilo_figura(fig, titulo="Produccion y ley", yaxis_title=None)
    fig.update_layout(bargap=0.04)

    if hay_dry_tons:
        fig.update_yaxes(title_text="Toneladas secas", row=fila_dry_tons, col=1)
    if hay_grade:
        fig.update_yaxes(title_text="Ley (%)", row=fila_grade, col=1)

    return fig


def grafico_dispersion(df: pd.DataFrame, variable_x: str) -> go.Figure:
    nombres = {
        "cobre_real_clp_ton": "Cobre real (MM CLP por tonelada)",
        "valor_cobre_fino_neto_real": "Valor cobre fino neto real (MM CLP)",
        "cobre_fino_ton": "Toneladas de cobre fino",
    }
    etiquetas = {
        "cobre_real_clp_ton": "cobre real",
        "valor_cobre_fino_neto_real": "valor cobre fino neto",
        "cobre_fino_ton": "toneladas de cobre fino",
    }
    divisores_x = {
        "cobre_real_clp_ton": 1_000_000,
        "valor_cobre_fino_neto_real": 1_000_000,
        "cobre_fino_ton": 1,
    }
    etiquetas_hover_x = {
        "cobre_real_clp_ton": "MM CLP/t",
        "valor_cobre_fino_neto_real": "MM CLP",
        "cobre_fino_ton": "ton",
    }

    aux = df.copy().sort_values("fecha").reset_index(drop=True)
    divisor_x = divisores_x.get(variable_x, 1)
    aux["x_plot"] = aux[variable_x] / divisor_x
    aux["y_plot"] = aux["dividendo_real"] / 1_000_000
    aux = aux.loc[aux["x_plot"].notna() & aux["y_plot"].notna()].copy()

    fig = go.Figure()

    if aux.empty:
        return aplicar_estilo_figura(
            fig,
            titulo="No hay observaciones suficientes para la dispersion",
            xaxis_title=nombres.get(variable_x, variable_x),
            yaxis_title="Dividendo real (MM CLP)",
            hovermode="closest",
        )

    fig.add_trace(
        go.Scatter(
            x=aux["x_plot"],
            y=aux["y_plot"],
            mode="markers",
            name="Meses",
            text=aux["fecha"].dt.strftime("%Y-%m"),
            customdata=np.stack([aux["anio"], aux["mes"]], axis=1),
            marker=dict(size=11, color=AZUL_ACENTO_1, opacity=0.86, line=dict(color="white", width=1.4)),
            hovertemplate=(
                "Fecha: %{text}<br>"
                f"X: %{{x:,.2f}} {etiquetas_hover_x.get(variable_x, '')}<br>"
                "Dividendo: %{y:,.1f} MM CLP<extra></extra>"
            ),
        )
    )

    if len(aux) >= 3:
        coef = np.polyfit(aux["x_plot"].astype(float), aux["y_plot"].astype(float), 1)
        x_line = np.linspace(float(aux["x_plot"].min()), float(aux["x_plot"].max()), 100)
        y_line = coef[0] * x_line + coef[1]
        fig.add_trace(
            go.Scatter(
                x=x_line,
                y=y_line,
                mode="lines",
                name="Tendencia simple",
                line=dict(width=2.8, color=AZUL_OSCURO, dash="dot"),
                hoverinfo="skip",
            )
        )

    x_min = float(aux["x_plot"].min())
    x_max = float(aux["x_plot"].max())
    y_min = float(aux["y_plot"].min())
    y_max = float(aux["y_plot"].max())
    x_pad = (x_max - x_min) * 0.06 if x_max > x_min else max(abs(x_max) * 0.06, 1)
    y_pad = (y_max - y_min) * 0.12 if y_max > y_min else max(abs(y_max) * 0.12, 1)
    fig.update_xaxes(range=[x_min - x_pad, x_max + x_pad])
    fig.update_yaxes(range=[max(0, y_min - y_pad), y_max + y_pad])

    return aplicar_estilo_figura(
        fig,
        titulo=f"Relacion entre dividendos y {etiquetas.get(variable_x, variable_x)}",
        xaxis_title=nombres.get(variable_x, variable_x),
        yaxis_title="Dividendo real (MM CLP)",
        hovermode="closest",
    )


def grafico_correlaciones(df: pd.DataFrame, max_lag: int = 3) -> go.Figure:
    filas = []
    for lag in range(max_lag + 1):
        corr_cobre = df["dividendo_real"].corr(df["cobre_real_clp_ton"].shift(lag))
        filas.append({"rezago": f"Rezago {lag}", "serie": "Cobre real", "corr": corr_cobre})

        corr_valor = df["dividendo_real"].corr(df["valor_cobre_fino_neto_real"].shift(lag))
        filas.append({"rezago": f"Rezago {lag}", "serie": "Valor cobre fino neto", "corr": corr_valor})

    cdf = pd.DataFrame(filas)

    fig = px.bar(
        cdf,
        x="rezago",
        y="corr",
        color="serie",
        barmode="group",
        template="plotly_white",
        title="Correlacion simple de dividendos con cobre y valor fino por rezago",
        labels={"corr": "Correlación", "rezago": "Rezago"},
        color_discrete_map={
            "Cobre real": AZUL_PRIMARIO,
            "Valor cobre fino neto": AZUL_ACENTO_2,
        },
    )
    fig.update_traces(marker_line_width=0, opacity=0.88)
    return aplicar_estilo_figura(
        fig,
        titulo="Correlacion simple de dividendos con cobre y valor fino por rezago",
        xaxis_title="Rezago",
        yaxis_title="Correlacion",
    )


def grafico_modelo(df_modelo: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df_modelo["fecha"],
            y=df_modelo["dividendo_real"] / 1_000_000,
            mode="lines+markers",
            name="Observado",
            line=dict(width=3),
            hovertemplate="%{x|%Y-%m}<br>%{y:,.1f} MM CLP<extra></extra>",
        )
    )

    if df_modelo["pred_dividendo_real"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=df_modelo["fecha"],
                y=df_modelo["pred_dividendo_real"] / 1_000_000,
                mode="lines",
                name="Esperado por modelo",
                line=dict(width=2, dash="dot"),
                hovertemplate="%{x|%Y-%m}<br>%{y:,.1f} MM CLP<extra></extra>",
            )
        )

    fig.update_layout(
        template="plotly_white",
        title="Dividendos observados vs dividendo esperado por modelo simple",
        yaxis_title="MM CLP reales",
        xaxis_title="Fecha",
        hovermode="x unified",
        legend_title="Serie",
        margin=dict(l=40, r=20, t=60, b=40),
    )
    return fig


def grafico_residuos(df_modelo: pd.DataFrame) -> go.Figure:
    aux = df_modelo.copy()
    aux["color"] = np.where(aux["anomalia"], "Atípico", "Normal")

    fig = px.bar(
        aux,
        x="fecha",
        y="z_residuo",
        color="color",
        template="plotly_white",
        title="Residuos estandarizados del modelo",
        labels={"z_residuo": "Residuo estandarizado", "fecha": "Fecha"},
        hover_data={"fecha_label": True, "residuo_modelo": ":,.0f"},
    )
    fig.add_hline(y=1.5, line_dash="dot")
    fig.add_hline(y=-1.5, line_dash="dot")
    fig.update_layout(hovermode="x unified", margin=dict(l=40, r=20, t=60, b=40))
    return fig


# -------------------------------------------------------------------
# Tablas
# -------------------------------------------------------------------

def tabla_interactiva(df: pd.DataFrame, table_id: str, page_size: int = 10):
    df_show = df.copy()

    if HAS_AG_GRID:
        columnas = [
            {
                "field": c,
                "headerName": c.replace("_", " ").title(),
                "sortable": True,
                "filter": True,
                "resizable": True,
                "minWidth": 140,
                "wrapHeaderText": True,
                "autoHeaderHeight": True,
            }
            for c in df_show.columns
        ]
        return html.Div(
            dag.AgGrid(
                id=table_id,
                rowData=df_show.to_dict("records"),
                columnDefs=columnas,
                defaultColDef={
                    "sortable": True,
                    "filter": True,
                    "resizable": True,
                    "minWidth": 140,
                    "wrapHeaderText": True,
                    "autoHeaderHeight": True,
                },
                dashGridOptions={"pagination": False, "animateRows": True},
                style={"height": "560px", "width": "100%", "borderRadius": "8px"},
                className="ag-theme-alpine"
            ),
            style={"width": "100%", "maxWidth": "100%", "minWidth": 0, "overflowX": "auto", "overflowY": "hidden"},
        )

    return dash_table.DataTable(
        id=table_id,
        data=df_show.to_dict("records"),
        columns=[{"name": c.replace("_", " ").title(), "id": c} for c in df_show.columns],
        page_action="none",
        sort_action="native",
        filter_action="native",
        fixed_rows={"headers": True},
        style_table={
            "overflowX": "auto",
            "overflowY": "auto",
            "maxWidth": "100%",
            "minWidth": 0,
            "maxHeight": "560px",
            "borderRadius": "8px",
        },
        style_cell={
            "textAlign": "left",
            "padding": "10px 12px",
            "fontFamily": FONT_FAMILY,
            "fontSize": 13,
            "color": AZUL_TINTA,
            "backgroundColor": "white",
            "border": f"1px solid {AZUL_BORDE}",
            "minWidth": "120px",
            "width": "140px",
            "maxWidth": "240px",
            "whiteSpace": "normal",
            "height": "auto",
        },
        style_header={
            "fontWeight": "bold",
            "backgroundColor": AZUL_SUAVE,
            "color": AZUL_OSCURO,
            "border": f"1px solid {AZUL_BORDE}",
            "whiteSpace": "normal",
            "height": "auto",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": AZUL_PANEL},
        ],
    )


def construir_tabla_anomalias(df_modelo: pd.DataFrame):
    aux = df_modelo.copy()
    aux = aux.loc[aux["pred_dividendo_real"].notna()].copy()
    aux["abs_z"] = aux["z_residuo"].abs()
    aux = aux.sort_values(["abs_z", "fecha"], ascending=[False, False]).head(15)

    cols = [
        "fecha_label",
        "dividendo_real_mm",
        "pred_dividendo_real",
        "residuo_modelo",
        "z_residuo",
        "ratio_div_sobre_valor_fino",
        "dry_tons",
        "grade",
        "regalia",
        "modelo_usado"
    ]
    cols = [c for c in cols if c in aux.columns]
    out = aux[cols].copy()

    if "pred_dividendo_real" in out.columns:
        out["pred_dividendo_real_mm"] = aux["pred_dividendo_real"] / 1_000_000
        out = out.drop(columns=["pred_dividendo_real"])
        out = out.rename(columns={"pred_dividendo_real_mm": "pred_dividendo_real_mm"})

    if "residuo_modelo" in out.columns:
        out["residuo_modelo_mm"] = aux["residuo_modelo"] / 1_000_000
        out = out.drop(columns=["residuo_modelo"])

    rename = {
        "fecha_label": "fecha",
        "dividendo_real_mm": "dividendo_real_mm",
        "z_residuo": "z_residuo",
        "ratio_div_sobre_valor_fino": "ratio_div_sobre_valor_fino",
        "dry_tons": "dry_tons",
        "grade": "grade",
        "regalia": "regalia",
        "modelo_usado": "modelo_usado",
    }
    out = out.rename(columns=rename)

    for c in ["dividendo_real_mm", "pred_dividendo_real_mm", "residuo_modelo_mm", "dry_tons", "grade", "z_residuo"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(2)

    if "regalia" in out.columns:
        out["regalia"] = (100 * pd.to_numeric(out["regalia"], errors="coerce")).round(2)

    if "ratio_div_sobre_valor_fino" in out.columns:
        out["ratio_div_sobre_valor_fino"] = (100 * pd.to_numeric(out["ratio_div_sobre_valor_fino"], errors="coerce")).round(2)

    return tabla_interactiva(out, table_id="tabla_anomalias", page_size=10)


def construir_tabla_datos(df: pd.DataFrame):
    cols = [
        "fecha_label",
        "dividendo_total_nominal",
        "dividendo_real_mm",
        "cobre_real_clp_ton",
        "dry_tons",
        "grade",
        "regalia",
        "cobre_fino_ton",
        "valor_cobre_fino_neto_real_mm",
        "ratio_div_sobre_valor_fino"
    ]
    cols = [c for c in cols if c in df.columns]
    out = df[cols].copy()

    out = out.rename(columns={
        "fecha_label": "fecha",
        "dividendo_total_nominal": "dividendo nominal (clp)",
        "dividendo_real_mm": "dividendo real (mm clp)",
        "cobre_real_clp_ton": "cobre real (clp por tonelada)",
        "dry_tons": "toneladas secas",
        "grade": "ley (%)",
        "regalia": "regalia (%)",
        "cobre_fino_ton": "toneladas de cobre fino",
        "valor_cobre_fino_neto_real_mm": "valor cobre fino neto real (mm clp)",
        "ratio_div_sobre_valor_fino": "ratio dividendo / valor fino (%)",
    })

    if "regalia (%)" in out.columns:
        out["regalia (%)"] = (100 * pd.to_numeric(out["regalia (%)"], errors="coerce")).round(2)
    if "ratio dividendo / valor fino (%)" in out.columns:
        out["ratio dividendo / valor fino (%)"] = (100 * pd.to_numeric(out["ratio dividendo / valor fino (%)"], errors="coerce")).round(2)

    for c in out.columns:
        if c != "fecha":
            out[c] = pd.to_numeric(out[c], errors="coerce").round(2)

    return tabla_interactiva(out, table_id="tabla_datos", page_size=12)


# -------------------------------------------------------------------
# Resumen y filtros
# -------------------------------------------------------------------

def filtrar_df(df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
    out = df.copy()
    out["fecha"] = pd.to_datetime(out["fecha"])
    if start_date:
        out = out.loc[out["fecha"] >= pd.to_datetime(start_date)]
    if end_date:
        out = out.loc[out["fecha"] <= pd.to_datetime(end_date)]
    return out.sort_values("fecha").reset_index(drop=True)


# -------------------------------------------------------------------
# Layout
# -------------------------------------------------------------------

CARD_STYLE = {
    "background": "linear-gradient(180deg, rgba(255,255,255,0.99) 0%, rgba(250,252,253,0.99) 100%)",
    "border": f"1px solid {AZUL_BORDE}",
    "borderRadius": "8px",
    "padding": "24px",
    "boxShadow": "0 16px 36px rgba(23, 33, 43, 0.09)",
    "minWidth": 0,
}

PAGE_STYLE = {
    "maxWidth": "1480px",
    "margin": "0 auto",
    "padding": "28px 24px 40px",
    "fontFamily": FONT_FAMILY,
    "background": "linear-gradient(180deg, #eef5fb 0%, #fbfcfd 42%, #f2f7fb 100%)",
    "overflowX": "hidden",
}

HEADER_STYLE = {
    "background": f"linear-gradient(135deg, {AZUL_OSCURO} 0%, {AZUL_PRIMARIO} 56%, {AZUL_ACENTO_2} 100%)",
    "borderRadius": "8px",
    "padding": "34px 36px",
    "boxShadow": "0 22px 48px rgba(23, 33, 43, 0.18)",
    "marginBottom": "28px",
    "color": "white",
}

HEADER_SOURCE_STYLE = {
    "display": "inline-flex",
    "alignItems": "center",
    "padding": "8px 14px",
    "borderRadius": "8px",
    "backgroundColor": "rgba(255,255,255,0.14)",
    "border": "1px solid rgba(255,255,255,0.18)",
    "fontSize": "13px",
    "marginTop": "16px",
}

CONTROL_BOX_STYLE = {
    "display": "grid",
    "gridTemplateColumns": "repeat(auto-fit, minmax(240px, 1fr))",
    "gap": "18px",
    "marginBottom": "24px",
}

KPI_GRID_STYLE = {
    "display": "grid",
    "gridTemplateColumns": "repeat(auto-fit, minmax(210px, 1fr))",
    "gap": "18px",
    "marginBottom": "28px",
}

ACTION_ROW_STYLE = {
    "display": "flex",
    "flexWrap": "wrap",
    "gap": "12px",
    "marginBottom": "28px",
}

BUTTON_PRIMARY_STYLE = {
    "padding": "12px 18px",
    "borderRadius": "8px",
    "border": "none",
    "background": f"linear-gradient(135deg, {AZUL_OSCURO} 0%, {AZUL_PRIMARIO} 68%, {AZUL_ACENTO_1} 100%)",
    "color": "white",
    "fontFamily": FONT_FAMILY,
    "fontWeight": "bold",
    "boxShadow": "0 10px 24px rgba(20, 108, 148, 0.20)",
    "transition": "transform 160ms ease, box-shadow 220ms ease, opacity 180ms ease, filter 180ms ease",
    "cursor": "pointer",
}

BUTTON_SECONDARY_STYLE = {
    "padding": "12px 18px",
    "borderRadius": "8px",
    "border": f"1px solid {AZUL_BORDE}",
    "backgroundColor": "white",
    "color": AZUL_OSCURO,
    "fontFamily": FONT_FAMILY,
    "fontWeight": "bold",
    "boxShadow": "0 8px 20px rgba(23, 33, 43, 0.07)",
    "transition": "transform 160ms ease, box-shadow 220ms ease, opacity 180ms ease, filter 180ms ease",
    "cursor": "pointer",
}

BOTON_PRIMARIO_CLASE = "boton-accion boton-primario"
BOTON_SECUNDARIO_CLASE = "boton-accion boton-secundario"
BOTON_AUTO_TEXT = "Completar IPC, cobre y USD/CLP"
BOTON_GUARDAR_TEXT = "Guardar mes en SQL"
GRAPH_CONFIG = {"displayModeBar": False, "responsive": True}
GRAPH_ANIMATION_OPTIONS = {
    "frame": {"duration": 4250, "redraw": False},
    "transition": {"duration": 3500, "easing": "cubic-in-out"},
}


def clase_boton_exito(clase_base: str, referencia: Optional[int]) -> str:
    if not referencia:
        return clase_base
    sufijo = "boton-exito-a" if referencia % 2 == 0 else "boton-exito-b"
    return f"{clase_base} {sufijo}"

FORM_GRID_STYLE = {
    "display": "grid",
    "gridTemplateColumns": "repeat(auto-fit, minmax(min(100%, 190px), 1fr))",
    "gap": "16px",
    "marginBottom": "18px",
    "width": "100%",
    "maxWidth": "100%",
    "alignItems": "start",
    "minWidth": 0,
}

FORM_LABEL_STYLE = {
    "fontSize": 13,
    "fontWeight": "bold",
    "color": AZUL_OSCURO,
    "marginBottom": "8px",
}

FORM_INPUT_STYLE = {
    "width": "100%",
    "padding": "12px 14px",
    "borderRadius": "8px",
    "border": f"1px solid {AZUL_BORDE}",
    "backgroundColor": "white",
    "fontFamily": FONT_FAMILY,
    "fontSize": 14,
    "color": AZUL_TINTA,
}

TAB_STYLE = {
    "padding": "14px 18px",
    "border": "none",
    "backgroundColor": "rgba(255,255,255,0)",
    "color": AZUL_MUTED,
    "fontWeight": "bold",
    "fontFamily": FONT_FAMILY,
}

TAB_SELECTED_STYLE = {
    "padding": "14px 18px",
    "border": "none",
    "backgroundColor": "white",
    "color": AZUL_OSCURO,
    "fontWeight": "bold",
    "borderRadius": "8px",
    "boxShadow": "0 12px 28px rgba(23, 33, 43, 0.08)",
}

UPLOAD_STYLE = {
    "width": "100%",
    "height": "76px",
    "lineHeight": "76px",
    "borderWidth": "1px",
    "borderStyle": "dashed",
    "borderColor": AZUL_MEDIO,
    "borderRadius": "8px",
    "textAlign": "center",
    "backgroundColor": AZUL_PANEL,
    "color": AZUL_OSCURO,
    "marginBottom": "10px",
}


def tarjeta_kpi(titulo: str, valor_id: str, subtitulo_id: str):
    return html.Div(
        [
            html.Div(titulo, style={"fontSize": 13, "letterSpacing": 0, "textTransform": "uppercase", "color": AZUL_MUTED, "marginBottom": "10px"}),
            html.Div(id=valor_id, style={"fontSize": 34, "fontWeight": "bold", "color": AZUL_OSCURO, "marginBottom": "6px"}),
            html.Div(id=subtitulo_id, style={"fontSize": 13, "color": AZUL_MUTED})
        ],
        style={**CARD_STYLE, "padding": "22px 24px"},
        className="metric-card",
    )


def bloque_control(titulo: str, componente):
    return html.Div(
        [
            html.Div(titulo, style={"fontSize": 13, "fontWeight": "bold", "color": AZUL_OSCURO, "marginBottom": "12px"}),
            componente,
        ],
        style={**CARD_STYLE, "padding": "18px 20px"},
        className="control-card",
    )


def tarjeta_grafico(graph_id: str):
    return html.Div(
        [
            dcc.Graph(
                id=graph_id,
                config=GRAPH_CONFIG,
                animate=True,
                animation_options=GRAPH_ANIMATION_OPTIONS,
                className="animated-graph",
            )
        ],
        style=CARD_STYLE,
        className="chart-card",
    )


def campo_formulario(titulo: str, componente):
    return html.Div(
        [
            html.Div(titulo, style=FORM_LABEL_STYLE),
            componente,
        ],
        style={"minWidth": 0, "width": "100%"},
    )


def build_app(config: Config) -> Dash:
    app = Dash(__name__, title="Dividendos, cobre y producción")

    app.layout = html.Div(
        [
            dcc.Store(id="store-datos"),
            dcc.Store(id="store-pendiente-ipc"),
            dcc.Interval(id="intervalo-recarga", interval=config.auto_refresh_seconds * 1000, n_intervals=0),
            dcc.ConfirmDialog(id="confirmar-ipc-rezago"),

            html.Div(
                [
                    html.Div(
                        "Panel de seguimiento",
                        style={"fontSize": 13, "letterSpacing": 0, "textTransform": "uppercase", "fontWeight": "bold", "opacity": 0.85},
                    ),
                    html.H1(
                        "Dividendos, cobre y producción",
                        style={"fontSize": "44px", "lineHeight": "1.05", "margin": "10px 0 14px"},
                    ),
                    html.P(
                        "Dividendos reales, producción y cobre fino en una lectura mensual.",
                        style={"fontSize": "16px", "maxWidth": "760px", "margin": 0, "opacity": 0.92},
                    ),
                    html.Div(id="texto-fuente", style=HEADER_SOURCE_STYLE),
                ],
                style=HEADER_STYLE,
                className="dashboard-header",
            ),

            html.Div(
                [
                    bloque_control(
                        "Rango de fechas",
                        dcc.DatePickerRange(id="rango-fechas", display_format="YYYY-MM-DD"),
                    ),
                    bloque_control(
                        "Promedio móvil",
                        dcc.Slider(
                            id="ventana-ma",
                            min=3,
                            max=12,
                            step=1,
                            value=6,
                            marks={i: str(i) for i in range(3, 13)},
                        ),
                    ),
                    bloque_control(
                        "Variable de comparación",
                        dcc.Dropdown(
                            id="variable-x",
                            options=[
                                {"label": "Valor cobre fino neto real", "value": "valor_cobre_fino_neto_real"},
                                {"label": "Cobre real (CLP por tonelada)", "value": "cobre_real_clp_ton"},
                                {"label": "Toneladas de cobre fino", "value": "cobre_fino_ton"},
                            ],
                            value="valor_cobre_fino_neto_real",
                            clearable=False
                        ),
                    ),
                ],
                style=CONTROL_BOX_STYLE
            ),

            html.Div(
                [
                    html.Button("Recargar datos", id="boton-recargar", n_clicks=0, style=BUTTON_PRIMARY_STYLE, className=BOTON_PRIMARIO_CLASE),
                    html.Button("Descargar CSV filtrado", id="boton-descargar", n_clicks=0, style=BUTTON_SECONDARY_STYLE, className=BOTON_SECUNDARIO_CLASE),
                ],
                style=ACTION_ROW_STYLE
            ),

            dcc.Download(id="download-filtrado"),

            html.Div(
                [
                    tarjeta_kpi("Promedio dividendo real", "kpi1-valor", "kpi1-sub"),
                    tarjeta_kpi("Último dividendo real", "kpi2-valor", "kpi2-sub"),
                    tarjeta_kpi("Último valor cobre fino neto", "kpi3-valor", "kpi3-sub"),
                    tarjeta_kpi("Meses con producción", "kpi4-valor", "kpi4-sub"),
                ],
                style=KPI_GRID_STYLE
            ),

            html.Div(
                [
                    html.Div(
                        "Crecimiento anual promedio",
                        style={"fontSize": 13, "letterSpacing": 0, "textTransform": "uppercase", "fontWeight": "bold", "color": AZUL_MUTED},
                    ),
                    html.Div(
                        [
                            tarjeta_kpi("Crec. anual dividendo real", "kpi5-valor", "kpi5-sub"),
                            tarjeta_kpi("Crec. anual tons secas", "kpi6-valor", "kpi6-sub"),
                            tarjeta_kpi("Crec. anual ley", "kpi7-valor", "kpi7-sub"),
                        ],
                        style=KPI_GRID_STYLE,
                    ),
                ],
                style={"display": "grid", "gap": "14px", "paddingTop": "18px"},
            ),

            dcc.Loading(
                type="circle",
                color=AZUL_MEDIO,
                children=dcc.Tabs(
                    [
                    dcc.Tab(
                        label="Tendencias",
                        style=TAB_STYLE,
                        selected_style=TAB_SELECTED_STYLE,
                        children=[
                            html.Div(
                                [
                                    tarjeta_grafico("grafico-dividendos"),
                                    tarjeta_grafico("grafico-indices"),
                                ],
                                style={"display": "grid", "gap": "20px", "paddingTop": "22px"},
                            ),
                        ],
                    ),
                    dcc.Tab(
                        label="Producción y relación",
                        style=TAB_STYLE,
                        selected_style=TAB_SELECTED_STYLE,
                        children=[
                            html.Div(
                                [
                                    tarjeta_grafico("grafico-produccion"),
                                    tarjeta_grafico("grafico-dispersion"),
                                    tarjeta_grafico("grafico-correlaciones"),
                                ],
                                style={"display": "grid", "gap": "20px", "paddingTop": "22px"},
                            ),
                        ],
                    ),
                    dcc.Tab(
                        label="Datos",
                        style=TAB_STYLE,
                        selected_style=TAB_SELECTED_STYLE,
                        children=[
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.H4("Carga manual mensual", style={"marginTop": 0, "marginBottom": "10px", "color": AZUL_OSCURO}),
                                            html.Div(
                                                "Guarda un solo mes en la base SQL. Si el mes ya existe, se actualiza. "
                                                "IPC, cobre y USD/CLP se pueden completar automaticamente desde fuentes publicas mas actuales y luego puedes corregirlos antes de guardar.",
                                                style={"fontSize": 12, "color": AZUL_MUTED, "marginBottom": "18px"}
                                            ),
                                            html.Div(
                                                [
                                                    campo_formulario(
                                                        "Mes",
                                                        dcc.DatePickerSingle(
                                                            id="form-fecha",
                                                            display_format="YYYY-MM-DD",
                                                            first_day_of_week=1,
                                                        ),
                                                    ),
                                                    campo_formulario(
                                                        "Dividendo nominal (CLP)",
                                                        dcc.Input(id="form-dividendo-nominal", type="number", debounce=True, style=FORM_INPUT_STYLE),
                                                    ),
                                                    campo_formulario(
                                                        "IPC",
                                                        dcc.Input(id="form-ipc", type="number", debounce=True, style=FORM_INPUT_STYLE),
                                                    ),
                                                    campo_formulario(
                                                        "Toneladas secas",
                                                        dcc.Input(id="form-dry-tons", type="number", debounce=True, style=FORM_INPUT_STYLE),
                                                    ),
                                                    campo_formulario(
                                                        "Regalia (% o proporción)",
                                                        dcc.Input(id="form-regalia", type="number", debounce=True, style=FORM_INPUT_STYLE),
                                                    ),
                                                    campo_formulario(
                                                        "Ley (%)",
                                                        dcc.Input(id="form-grade", type="number", debounce=True, style=FORM_INPUT_STYLE),
                                                    ),
                                                    campo_formulario(
                                                        "Cobre USD por tonelada métrica",
                                                        dcc.Input(id="form-cobre-usd", type="number", debounce=True, style=FORM_INPUT_STYLE),
                                                    ),
                                                    campo_formulario(
                                                        "USD/CLP",
                                                        dcc.Input(id="form-fx-clp", type="number", debounce=True, style=FORM_INPUT_STYLE),
                                                    ),
                                                ],
                                                style=FORM_GRID_STYLE,
                                            ),
                                            html.Div(
                                                [
                                                    html.Button(BOTON_AUTO_TEXT, id="boton-autocompletar-mercado", n_clicks=0, style=BUTTON_SECONDARY_STYLE, className=BOTON_SECUNDARIO_CLASE),
                                                    html.Button(BOTON_GUARDAR_TEXT, id="boton-guardar-manual", n_clicks=0, style=BUTTON_PRIMARY_STYLE, className=BOTON_PRIMARIO_CLASE),
                                                ],
                                                style={"display": "flex", "flexWrap": "wrap", "gap": "12px", "marginBottom": "12px"},
                                            ),
                                            html.Div(id="mensaje-mercado", style={"fontSize": 13, "color": AZUL_MUTED, "marginBottom": "8px"}),
                                            html.Div(id="mensaje-formulario", style={"fontSize": 13, "color": AZUL_OSCURO}),
                                        ],
                                        style={**CARD_STYLE, "overflow": "hidden"}
                                    ),
                                    html.Div(
                                        [
                                            html.H4("Base filtrada", style={"marginTop": 0, "marginBottom": "14px", "color": AZUL_OSCURO}),
                                            html.Div(id="contenedor-tabla-datos", style={"width": "100%", "maxWidth": "100%", "minWidth": 0}),
                                        ],
                                        style={**CARD_STYLE, "overflow": "hidden"}
                                    ),
                                ],
                                style={"display": "grid", "gap": "20px", "paddingTop": "22px", "minWidth": 0}
                            ),
                        ],
                    ),
                    ],
                    parent_style={"marginBottom": "0"},
                    style={"backgroundColor": "transparent"},
                ),
            ),
        ],
        style=PAGE_STYLE,
        className="dashboard-shell",
    )

    # ---------------------------------------------------------------
    # Callback 1: carga inicial, recarga y guardado manual a SQL
    # ---------------------------------------------------------------
    @app.callback(
        Output("store-datos", "data"),
        Output("texto-fuente", "children"),
        Output("mensaje-formulario", "children"),
        Output("confirmar-ipc-rezago", "displayed"),
        Output("confirmar-ipc-rezago", "message"),
        Output("store-pendiente-ipc", "data"),
        Output("boton-guardar-manual", "children"),
        Output("boton-guardar-manual", "className"),
        Input("intervalo-recarga", "n_intervals"),
        Input("boton-recargar", "n_clicks"),
        Input("boton-guardar-manual", "n_clicks"),
        Input("confirmar-ipc-rezago", "submit_n_clicks"),
        State("form-fecha", "date"),
        State("form-dividendo-nominal", "value"),
        State("form-ipc", "value"),
        State("form-dry-tons", "value"),
        State("form-regalia", "value"),
        State("form-grade", "value"),
        State("form-cobre-usd", "value"),
        State("form-fx-clp", "value"),
        State("store-pendiente-ipc", "data"),
        prevent_initial_call=False
    )
    def cargar_o_actualizar(
        _n_intervals,
        _n_clicks,
        _n_guardar,
        confirmar_ipc,
        fecha_manual,
        dividendo_nominal,
        ipc,
        dry_tons,
        regalia,
        grade,
        cobre_usd,
        fx_clp,
        pendiente_ipc,
    ):
        mensaje_formulario = ""
        confirm_displayed = no_update
        confirm_message = no_update
        nuevo_pendiente = no_update
        boton_guardar_texto = BOTON_GUARDAR_TEXT
        boton_guardar_clase = BOTON_PRIMARIO_CLASE

        def mensaje_fuente(df_local: pd.DataFrame) -> str:
            fuente = "Excel" if config.backend == "excel" else f"SQL | tabla: {config.sql_table}"
            return (
                f"Fuente: {fuente} | filas: {len(df_local)} | "
                f"última fecha: {df_local['fecha'].max().strftime('%Y-%m')} | "
                f"refresco automático: cada {config.auto_refresh_seconds} segundos"
            )

        try:
            triggered = callback_context.triggered
            trigger_id = None
            if triggered:
                trigger_id = triggered[0]["prop_id"].split(".")[0]

            if trigger_id == "confirmar-ipc-rezago":
                if not pendiente_ipc or pendiente_ipc.get("accion") != "guardar":
                    return (
                        no_update,
                        no_update,
                        no_update,
                        no_update,
                        no_update,
                        no_update,
                        BOTON_GUARDAR_TEXT,
                        BOTON_PRIMARIO_CLASE,
                    )

                df, mensaje_mercado = upsert_registro_manual(
                    fecha=pendiente_ipc.get("fecha_manual"),
                    dividendo_total_nominal=pendiente_ipc.get("dividendo_nominal"),
                    ipc=pendiente_ipc.get("ipc"),
                    dry_tons=pendiente_ipc.get("dry_tons"),
                    regalia=pendiente_ipc.get("regalia"),
                    grade=pendiente_ipc.get("grade"),
                    cobre_usd_metric_ton=pendiente_ipc.get("cobre_usd"),
                    fx_clp_usd=pendiente_ipc.get("fx_clp"),
                    db_url=config.db_url,
                    table_name=config.sql_table,
                    config=config,
                    permitir_ipc_rezago=True,
                )
                mensaje_formulario = (
                    f"Registro guardado para {normalizar_fecha_mes(pendiente_ipc.get('fecha_manual')).strftime('%Y-%m')}. "
                    f"Base SQL actualizada hasta {df['fecha'].max().strftime('%Y-%m')}."
                    f"{mensaje_mercado}"
                )
                confirm_displayed = False
                confirm_message = ""
                nuevo_pendiente = None
                boton_guardar_texto = "Guardado"
                boton_guardar_clase = clase_boton_exito(BOTON_PRIMARIO_CLASE, confirmar_ipc)
                return (
                    df.to_json(date_format="iso", orient="split"),
                    mensaje_fuente(df),
                    mensaje_formulario,
                    confirm_displayed,
                    confirm_message,
                    nuevo_pendiente,
                    boton_guardar_texto,
                    boton_guardar_clase,
                )

            if trigger_id == "boton-guardar-manual":
                if config.backend != "sql":
                    mensaje_formulario = "La carga manual solo está habilitada cuando el backend es SQL."
                    df = cargar_datos(config)
                else:
                    if ipc is None or pd.isna(ipc):
                        ipc_info = obtener_ipc_para_mes(
                            fecha_manual,
                            config,
                            permitir_ultimo_disponible=True,
                        )
                        if bool(ipc_info.get("es_rezago")):
                            mensaje_formulario = "Confirma si quieres continuar usando el último IPC publicado."
                            confirm_displayed = True
                            confirm_message = construir_prompt_ipc_rezago(ipc_info, "guardar")
                            nuevo_pendiente = {
                                "accion": "guardar",
                                "fecha_manual": fecha_manual,
                                "dividendo_nominal": dividendo_nominal,
                                "ipc": ipc,
                                "dry_tons": dry_tons,
                                "regalia": regalia,
                                "grade": grade,
                                "cobre_usd": cobre_usd,
                                "fx_clp": fx_clp,
                            }
                            boton_guardar_texto = "Confirmar IPC"
                            return (
                                no_update,
                                no_update,
                                mensaje_formulario,
                                confirm_displayed,
                                confirm_message,
                                nuevo_pendiente,
                                boton_guardar_texto,
                                BOTON_PRIMARIO_CLASE,
                            )

                    df, mensaje_mercado = upsert_registro_manual(
                        fecha=fecha_manual,
                        dividendo_total_nominal=dividendo_nominal,
                        ipc=ipc,
                        dry_tons=dry_tons,
                        regalia=regalia,
                        grade=grade,
                        cobre_usd_metric_ton=cobre_usd,
                        fx_clp_usd=fx_clp,
                        db_url=config.db_url,
                        table_name=config.sql_table,
                        config=config,
                    )
                    mensaje_formulario = (
                        f"Registro guardado para {normalizar_fecha_mes(fecha_manual).strftime('%Y-%m')}. "
                        f"Base SQL actualizada hasta {df['fecha'].max().strftime('%Y-%m')}."
                        f"{mensaje_mercado}"
                    )
                    confirm_displayed = False
                    confirm_message = ""
                    nuevo_pendiente = None
                    boton_guardar_texto = "Guardado"
                    boton_guardar_clase = clase_boton_exito(BOTON_PRIMARIO_CLASE, _n_guardar)
            else:
                df = cargar_datos(config)

            return (
                df.to_json(date_format="iso", orient="split"),
                mensaje_fuente(df),
                mensaje_formulario,
                confirm_displayed,
                confirm_message,
                nuevo_pendiente,
                boton_guardar_texto,
                boton_guardar_clase,
            )

        except Exception as e:
            return (
                no_update,
                f"Error al cargar datos: {e}",
                f"Error: {e}",
                False,
                "",
                None,
                BOTON_GUARDAR_TEXT,
                BOTON_PRIMARIO_CLASE,
            )

    # ---------------------------------------------------------------
    # Callback 2: autocompleta IPC, cobre y USD/CLP
    # ---------------------------------------------------------------
    @app.callback(
        Output("form-ipc", "value"),
        Output("form-cobre-usd", "value"),
        Output("form-fx-clp", "value"),
        Output("mensaje-mercado", "children"),
        Output("confirmar-ipc-rezago", "displayed", allow_duplicate=True),
        Output("confirmar-ipc-rezago", "message", allow_duplicate=True),
        Output("store-pendiente-ipc", "data", allow_duplicate=True),
        Output("boton-autocompletar-mercado", "children"),
        Output("boton-autocompletar-mercado", "className"),
        Input("boton-autocompletar-mercado", "n_clicks"),
        Input("confirmar-ipc-rezago", "submit_n_clicks"),
        State("form-fecha", "date"),
        State("store-pendiente-ipc", "data"),
        running=[
            (Output("boton-autocompletar-mercado", "disabled"), True, False),
        ],
        prevent_initial_call=True
    )
    def autocompletar_mercado(_n_clicks, confirmar_ipc, fecha_manual, pendiente_ipc):
        boton_texto = BOTON_AUTO_TEXT
        boton_clase = BOTON_SECUNDARIO_CLASE

        trigger_id = callback_context.triggered[0]["prop_id"].split(".")[0] if callback_context.triggered else None

        if trigger_id == "confirmar-ipc-rezago":
            if not pendiente_ipc or pendiente_ipc.get("accion") != "autocompletar":
                return no_update, no_update, no_update, no_update, no_update, no_update, no_update, BOTON_AUTO_TEXT, BOTON_SECUNDARIO_CLASE
            fecha_manual = pendiente_ipc.get("fecha_manual")

        if not fecha_manual:
            return no_update, no_update, no_update, "Selecciona primero el mes que quieres cargar.", False, "", None, BOTON_AUTO_TEXT, BOTON_SECUNDARIO_CLASE

        try:
            permitir_rezago = trigger_id == "confirmar-ipc-rezago"
            ipc_info = obtener_ipc_para_mes(
                fecha_manual,
                config,
                permitir_ultimo_disponible=True,
            )
            if bool(ipc_info.get("es_rezago")) and not permitir_rezago:
                return (
                    no_update,
                    no_update,
                    no_update,
                    "Confirma si quieres usar el último IPC disponible para completar el formulario.",
                    True,
                    construir_prompt_ipc_rezago(ipc_info, "autocompletar"),
                    {"accion": "autocompletar", "fecha_manual": fecha_manual},
                    "Confirmar IPC",
                    BOTON_SECUNDARIO_CLASE,
                )

            mercado = obtener_mercado_para_mes(fecha_manual, config)
            mensaje = (
                "Valores completados automaticamente. "
                f"IPC: {fmt_es_num(float(ipc_info['ipc']), 2)} "
                f"({ipc_info['detalle_ipc']}; {ipc_info['fuente_ipc']}). "
                f"Cobre: {fmt_es_num(float(mercado['cobre_usd_metric_ton']), 2)} USD/tm "
                f"({mercado['detalle_cobre']}; {mercado['fuente_cobre']}). "
                f"USD/CLP: {fmt_es_num(float(mercado['fx_clp_usd']), 2)} "
                f"({mercado['detalle_fx']}; {mercado['fuente_fx']})."
            )
            boton_texto = "Valores listos"
            referencia_exito = confirmar_ipc if trigger_id == "confirmar-ipc-rezago" else _n_clicks
            boton_clase = clase_boton_exito(BOTON_SECUNDARIO_CLASE, referencia_exito)
            return (
                float(ipc_info["ipc"]),
                float(mercado["cobre_usd_metric_ton"]),
                float(mercado["fx_clp_usd"]),
                mensaje,
                False,
                "",
                None,
                boton_texto,
                boton_clase,
            )
        except Exception as e:
            return (
                no_update,
                no_update,
                no_update,
                f"No pude completar los valores automaticamente: {e}",
                False,
                "",
                None,
                BOTON_AUTO_TEXT,
                BOTON_SECUNDARIO_CLASE,
            )

    # ---------------------------------------------------------------
    # Callback 3: sugiere el siguiente mes para la carga manual
    # ---------------------------------------------------------------
    @app.callback(
        Output("form-fecha", "date"),
        Input("store-datos", "data"),
        State("form-fecha", "date"),
        prevent_initial_call=False
    )
    def sugerir_fecha_formulario(data_json, fecha_actual):
        if fecha_actual or not data_json:
            return fecha_actual

        df = pd.read_json(io.StringIO(data_json), orient="split")
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        if df["fecha"].dropna().empty:
            return None

        sugerida = df["fecha"].max() + pd.offsets.MonthBegin(1)
        return sugerida.date().isoformat()

    # ---------------------------------------------------------------
    # Callback 4: actualiza rango de fechas cuando cambian los datos
    # ---------------------------------------------------------------
    @app.callback(
        Output("rango-fechas", "min_date_allowed"),
        Output("rango-fechas", "max_date_allowed"),
        Output("rango-fechas", "start_date"),
        Output("rango-fechas", "end_date"),
        Input("store-datos", "data"),
        State("rango-fechas", "start_date"),
        State("rango-fechas", "end_date"),
        prevent_initial_call=False
    )
    def actualizar_rango(data_json, start_actual, end_actual):
        if not data_json:
            return None, None, None, None

        df = pd.read_json(io.StringIO(data_json), orient="split")
        df["fecha"] = pd.to_datetime(df["fecha"])
        min_fecha = df["fecha"].min()
        max_fecha = df["fecha"].max()

        start_final = pd.to_datetime(start_actual) if start_actual else min_fecha
        end_final = pd.to_datetime(end_actual) if end_actual else max_fecha

        start_final = max(start_final, min_fecha)
        end_final = min(end_final, max_fecha)

        return (
            min_fecha.date().isoformat(),
            max_fecha.date().isoformat(),
            start_final.date().isoformat(),
            end_final.date().isoformat(),
        )

    # ---------------------------------------------------------------
    # Callback 5: actualiza KPIs, gráficos y tablas
    # ---------------------------------------------------------------
    @app.callback(
        Output("kpi1-valor", "children"),
        Output("kpi1-sub", "children"),
        Output("kpi2-valor", "children"),
        Output("kpi2-sub", "children"),
        Output("kpi3-valor", "children"),
        Output("kpi3-sub", "children"),
        Output("kpi4-valor", "children"),
        Output("kpi4-sub", "children"),
        Output("kpi5-valor", "children"),
        Output("kpi5-sub", "children"),
        Output("kpi6-valor", "children"),
        Output("kpi6-sub", "children"),
        Output("kpi7-valor", "children"),
        Output("kpi7-sub", "children"),
        Output("grafico-dividendos", "figure"),
        Output("grafico-indices", "figure"),
        Output("grafico-produccion", "figure"),
        Output("grafico-dispersion", "figure"),
        Output("grafico-correlaciones", "figure"),
        Output("contenedor-tabla-datos", "children"),
        Input("store-datos", "data"),
        Input("rango-fechas", "start_date"),
        Input("rango-fechas", "end_date"),
        Input("ventana-ma", "value"),
        Input("variable-x", "value"),
        prevent_initial_call=False
    )
    def actualizar_dashboard(data_json, start_date, end_date, ventana_ma, variable_x):
        vacio = aplicar_estilo_figura(go.Figure(), titulo="Sin datos", yaxis_title=None)

        if not data_json:
            return (
                "NA", "", "NA", "", "NA", "", "NA", "", "NA", "", "NA", "", "NA", "",
                vacio, vacio, vacio, vacio, vacio,
                html.Div("Sin datos"),
            )

        df = pd.read_json(io.StringIO(data_json), orient="split")
        df["fecha"] = pd.to_datetime(df["fecha"])
        df = preparar_dataframe(df)
        dff = filtrar_df(df, start_date, end_date)

        if dff.empty:
            return (
                "NA", "", "NA", "", "NA", "", "NA", "", "NA", "", "NA", "", "NA", "",
                vacio, vacio, vacio, vacio, vacio,
                html.Div("Sin datos"),
            )

        # KPIs
        prom_div = dff["dividendo_real"].mean(skipna=True)
        ult_div = dff["dividendo_real"].dropna().iloc[-1] if dff["dividendo_real"].notna().any() else np.nan
        ult_fino = dff["valor_cobre_fino_neto_real"].dropna().iloc[-1] if dff["valor_cobre_fino_neto_real"].notna().any() else np.nan
        n_prod = int(dff["valor_cobre_fino_neto_real"].notna().sum())
        n_total = int(len(dff))

        kpi1_val = fmt_es_moneda_mm(prom_div)
        kpi1_sub = "Promedio del rango filtrado"

        kpi2_val = fmt_es_moneda_mm(ult_div)
        kpi2_sub = f"Último mes: {dff['fecha'].max().strftime('%Y-%m')}"

        kpi3_val = fmt_es_moneda_mm(ult_fino) if pd.notna(ult_fino) else "NA"
        kpi3_sub = "Valor cobre fino neto real"

        kpi4_val = f"{n_prod} / {n_total}"
        kpi4_sub = "Meses con datos de producción"

        # Gráficos
        kpi5_val, kpi5_sub = resumen_crecimiento_anual(dff, "dividendo_real", "sum")
        kpi6_val, kpi6_sub = resumen_crecimiento_anual(dff, "dry_tons", "sum")
        kpi7_val, kpi7_sub = resumen_crecimiento_anual(dff, "grade", "mean")

        fig1 = grafico_dividendos(dff, int(ventana_ma))
        fig2 = grafico_indices(dff)
        fig3 = grafico_produccion(dff)
        fig4 = grafico_dispersion(dff, variable_x=variable_x)
        fig5 = grafico_correlaciones(dff)
        tabla_datos = construir_tabla_datos(dff)

        return (
            kpi1_val, kpi1_sub, kpi2_val, kpi2_sub,
            kpi3_val, kpi3_sub, kpi4_val, kpi4_sub,
            kpi5_val, kpi5_sub, kpi6_val, kpi6_sub, kpi7_val, kpi7_sub,
            fig1, fig2, fig3, fig4, fig5, tabla_datos
        )

    # ---------------------------------------------------------------
    # Callback 6: descarga de CSV filtrado
    # ---------------------------------------------------------------
    @app.callback(
        Output("download-filtrado", "data"),
        Input("boton-descargar", "n_clicks"),
        State("store-datos", "data"),
        State("rango-fechas", "start_date"),
        State("rango-fechas", "end_date"),
        prevent_initial_call=True
    )
    def descargar_csv(_n_clicks, data_json, start_date, end_date):
        if not data_json:
            return no_update

        df = pd.read_json(io.StringIO(data_json), orient="split")
        df["fecha"] = pd.to_datetime(df["fecha"])
        df = preparar_dataframe(df)
        dff = filtrar_df(df, start_date, end_date)

        nombre = f"dashboard_filtrado_{dff['fecha'].min().strftime('%Y%m')}_{dff['fecha'].max().strftime('%Y%m')}.csv"
        return dcc.send_data_frame(dff.to_csv, nombre, index=False)

    return app


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dashboard de dividendos, cobre y producción")
    parser.add_argument("--backend", choices=["excel", "sql"], default=CFG.backend)
    parser.add_argument("--excel-path", default=CFG.excel_path)
    parser.add_argument("--sheet", default=CFG.excel_sheet)
    parser.add_argument("--db-url", default=CFG.db_url)
    parser.add_argument("--table", default=CFG.sql_table)
    parser.add_argument("--host", default=CFG.host)
    parser.add_argument("--port", type=int, default=CFG.port)
    parser.add_argument("--debug", action="store_true", default=CFG.debug)
    parser.add_argument("--bootstrap-sql", action="store_true")
    parser.add_argument("--upsert-file", default=None)
    return parser.parse_args()


RENDER_APP = build_app(CFG)
server = RENDER_APP.server


def main() -> None:
    args = parse_args()

    config = Config(
        backend=args.backend,
        excel_path=args.excel_path,
        excel_sheet=args.sheet,
        db_url=args.db_url,
        sql_table=args.table,
        auto_bootstrap_sql_if_empty=CFG.auto_bootstrap_sql_if_empty,
        auto_refresh_seconds=CFG.auto_refresh_seconds,
        http_timeout_seconds=CFG.http_timeout_seconds,
        findic_base_url=CFG.findic_base_url,
        bcch_ipc_general_url=CFG.bcch_ipc_general_url,
        yahoo_chart_base_url=CFG.yahoo_chart_base_url,
        yahoo_copper_symbol=CFG.yahoo_copper_symbol,
        yahoo_fx_symbol=CFG.yahoo_fx_symbol,
        fred_copper_series_id=CFG.fred_copper_series_id,
        fred_fx_series_id=CFG.fred_fx_series_id,
        host=args.host,
        port=args.port,
        debug=args.debug
    )

    if args.bootstrap_sql:
        df = bootstrap_excel_a_sql(config.excel_path, config.excel_sheet, config.db_url, config.sql_table)
        print(
            f"Base SQL creada o reemplazada correctamente.\n"
            f"Tabla: {config.sql_table}\n"
            f"Filas: {len(df)}\n"
            f"Última fecha: {df['fecha'].max().strftime('%Y-%m')}"
        )
        return

    if args.upsert_file:
        if config.backend != "sql":
            raise ValueError("Para usar --upsert-file el backend debe ser sql.")
        df = upsert_archivo_mensual(args.upsert_file, config.db_url, config.sql_table, config.excel_sheet)
        print(
            f"Archivo cargado correctamente en SQL.\n"
            f"Tabla: {config.sql_table}\n"
            f"Filas totales: {len(df)}\n"
            f"Última fecha: {df['fecha'].max().strftime('%Y-%m')}"
        )
        return

    app = RENDER_APP if config == CFG else build_app(config)

    # Compatibilidad simple entre versiones de Dash
    if hasattr(app, "run"):
        app.run(host=config.host, port=config.port, debug=config.debug)
    else:
        app.run_server(host=config.host, port=config.port, debug=config.debug)


if __name__ == "__main__":
    main()

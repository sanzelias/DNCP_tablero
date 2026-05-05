from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cache"


st.set_page_config(
    page_title="Tablero DNCP Paraguay",
    layout="wide",
)


@st.cache_data(show_spinner=False)
def load_parquet(relative_path: str) -> pd.DataFrame:
    path = CACHE / relative_path
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)


def format_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "0"
    return f"{value:,.0f}".replace(",", ".")


def format_money(value: float | int | None) -> str:
    return f"Gs. {format_number(value)}"


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def filter_by_year(df: pd.DataFrame, years: tuple[int, int] | None) -> pd.DataFrame:
    if years is None or "anio" not in df.columns or df.empty:
        return df
    start, end = years
    return df[df["anio"].between(start, end, inclusive="both")]


def filter_by_ruc(df: pd.DataFrame, ruc_query: str, column: str = "ruc") -> pd.DataFrame:
    if not ruc_query or column not in df.columns or df.empty:
        return df
    token = normalize_text(ruc_query).replace(".", "")
    values = df[column].fillna("").astype(str).str.lower().str.replace(".", "", regex=False)
    return df[values.str.contains(token, regex=False)]


def filter_contains(df: pd.DataFrame, column: str, query: str) -> pd.DataFrame:
    if not query or column not in df.columns or df.empty:
        return df
    token = normalize_text(query)
    return df[df[column].fillna("").astype(str).str.lower().str.contains(token, regex=False)]


def metric_row(values: list[tuple[str, str]]) -> None:
    columns = st.columns(len(values))
    for column, (label, value) in zip(columns, values):
        column.metric(label, value)


def prepare_time_series(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    if df.empty or date_column not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out[date_column] = pd.to_datetime(out[date_column], errors="coerce")
    out = out.dropna(subset=[date_column]).sort_values(date_column)
    return out.set_index(date_column)


def show_missing_data(error: Exception) -> None:
    st.error("No se encontraron los archivos Parquet necesarios en la carpeta cache/.")
    st.exception(error)
    st.stop()


try:
    conv_anual = load_parquet("convocatorias/evolucion_anual.parquet")
    conv_mensual = load_parquet("convocatorias/evolucion_mensual.parquet")
    modalidades = load_parquet("convocatorias/modalidades.parquet")
    top_entidades = load_parquet("convocatorias/top_entidades.parquet")
    licitaciones = load_parquet("convocatorias/licitaciones_full.parquet")

    adj_anual = load_parquet("adjudicaciones/evolucion_anual.parquet")
    adj_mensual = load_parquet("adjudicaciones/evolucion_mensual.parquet")
    top_proveedores = load_parquet("adjudicaciones/top_proveedores.parquet")
    items = load_parquet("adjudicaciones/items_detalle.parquet")
    comparacion = load_parquet("adjudicaciones/comparacion_precios.parquet")
except Exception as exc:
    show_missing_data(exc)


st.title("Tablero de Contrataciones Publicas de Paraguay")
st.caption(
    "Prototipo de analisis sobre datos publicos de contrataciones. "
    "No constituye informacion oficial ni prueba legal."
)

all_years = sorted(
    {
        int(year)
        for frame in (conv_anual, adj_anual, licitaciones, items, comparacion)
        if "anio" in frame.columns
        for year in frame["anio"].dropna().unique()
    }
)
valid_years = [year for year in all_years if 2010 <= year <= 2026]
default_range = (min(valid_years), max(valid_years)) if valid_years else None

with st.sidebar:
    st.header("Filtros")
    year_range = None
    if default_range:
        year_range = st.slider(
            "Rango de anos",
            min_value=default_range[0],
            max_value=default_range[1],
            value=default_range,
        )
    ruc_query = st.text_input("Buscar por RUC del proveedor", placeholder="80004886-5")
    entity_query = st.text_input("Buscar entidad", placeholder="Ministerio, Municipalidad...")
    supplier_query = st.text_input("Buscar proveedor", placeholder="Nombre del proveedor")


conv_anual_f = filter_by_year(conv_anual, year_range)
adj_anual_f = filter_by_year(adj_anual, year_range)
licitaciones_f = filter_contains(filter_by_year(licitaciones, year_range), "entidad", entity_query)
items_f = filter_by_ruc(filter_by_year(items, year_range), ruc_query)
items_f = filter_contains(items_f, "entidad", entity_query)
items_f = filter_contains(items_f, "proveedor", supplier_query)
top_proveedores_f = filter_by_ruc(top_proveedores, ruc_query)
top_proveedores_f = filter_contains(top_proveedores_f, "proveedor", supplier_query)
comparacion_f = filter_contains(filter_by_year(comparacion, year_range), "entidad", entity_query)
comparacion_f = filter_contains(comparacion_f, "proveedor_mas_frecuente", supplier_query)

metric_row(
    [
        ("Llamados", format_number(conv_anual_f["cantidad"].sum())),
        ("Monto estimado", format_money(conv_anual_f["monto"].sum())),
        ("Adjudicaciones", format_number(adj_anual_f["cantidad"].sum())),
        ("Monto adjudicado", format_money(adj_anual_f["monto"].sum())),
    ]
)

metric_row(
    [
        ("Items adjudicados", format_number(len(items_f))),
        ("Proveedores", format_number(items_f["proveedor"].nunique())),
        ("Entidades", format_number(items_f["entidad"].nunique())),
        ("Alertas de precio", format_number(len(comparacion_f))),
    ]
)

tab_conv, tab_adj, tab_items, tab_alertas = st.tabs(
    ["Convocatorias", "Adjudicaciones", "Detalle de items", "Alertas de precios"]
)

with tab_conv:
    left, right = st.columns([2, 1])
    with left:
        st.subheader("Evolucion anual de convocatorias")
        chart_data = conv_anual_f.sort_values("anio").set_index("anio")[["cantidad", "monto"]]
        st.line_chart(chart_data)
    with right:
        st.subheader("Modalidades")
        st.dataframe(modalidades.sort_values("cantidad", ascending=False), use_container_width=True)

    left, right = st.columns([1, 2])
    with left:
        st.subheader("Top entidades")
        st.dataframe(top_entidades, use_container_width=True, hide_index=True)
    with right:
        st.subheader("Buscador de licitaciones")
        search = st.text_input("Texto en titulo o identificador", key="licitaciones_search")
        shown = licitaciones_f.copy()
        if search:
            mask = (
                shown["titulo"].fillna("").astype(str).str.lower().str.contains(search.lower(), regex=False)
                | shown["id_llamado"].fillna("").astype(str).str.lower().str.contains(search.lower(), regex=False)
                | shown["ocid"].fillna("").astype(str).str.lower().str.contains(search.lower(), regex=False)
            )
            shown = shown[mask]
        columns = [
            "id_llamado",
            "estado",
            "titulo",
            "monto_estimado",
            "fecha_publicacion",
            "fecha_cierre",
            "modalidad",
            "entidad",
        ]
        st.dataframe(shown[columns].head(500), use_container_width=True, hide_index=True)

with tab_adj:
    left, right = st.columns([2, 1])
    with left:
        st.subheader("Evolucion anual de adjudicaciones")
        chart_data = adj_anual_f.sort_values("anio").set_index("anio")[["cantidad", "monto"]]
        st.line_chart(chart_data)
    with right:
        st.subheader("Top proveedores")
        st.dataframe(top_proveedores_f.head(20), use_container_width=True, hide_index=True)

    monthly = prepare_time_series(adj_mensual, "mes")
    if not monthly.empty:
        st.subheader("Evolucion mensual")
        st.line_chart(monthly[["cantidad", "monto"]])

with tab_items:
    st.subheader("Items adjudicados")
    item_text = st.text_input("Buscar descripcion o clasificacion", key="items_search")
    shown_items = items_f
    if item_text:
        mask = (
            shown_items["descripcion"].fillna("").astype(str).str.lower().str.contains(item_text.lower(), regex=False)
            | shown_items["clasificacion"].fillna("").astype(str).str.lower().str.contains(item_text.lower(), regex=False)
        )
        shown_items = shown_items[mask]

    metric_row(
        [
            ("Registros filtrados", format_number(len(shown_items))),
            ("Monto de items", format_money(shown_items["monto_item"].sum())),
            ("Precio unitario mediano", format_money(shown_items["precio_unitario"].median())),
            ("Unidades distintas", format_number(shown_items["unidad"].nunique())),
        ]
    )
    st.dataframe(
        shown_items[
            [
                "anio",
                "fecha_adjudicacion",
                "entidad",
                "proveedor",
                "ruc",
                "descripcion",
                "cantidad",
                "unidad",
                "precio_unitario",
                "monto_item",
            ]
        ].head(1000),
        use_container_width=True,
        hide_index=True,
    )

with tab_alertas:
    st.subheader("Comparacion de precios")
    nivel_options = sorted(comparacion_f["nivel_alerta"].dropna().unique())
    selected_levels = st.multiselect("Nivel de alerta", nivel_options, default=nivel_options)
    shown_alerts = comparacion_f
    if selected_levels:
        shown_alerts = shown_alerts[shown_alerts["nivel_alerta"].isin(selected_levels)]

    metric_row(
        [
            ("Comparaciones", format_number(len(shown_alerts))),
            ("Catalogos", format_number(shown_alerts["codigo_catalogo"].nunique())),
            ("Entidades", format_number(shown_alerts["entidad"].nunique())),
            ("Sobreprecio medio", f"{shown_alerts['sobreprecio_pct'].mean():.1f}%" if not shown_alerts.empty else "0%"),
        ]
    )

    st.bar_chart(shown_alerts["nivel_alerta"].value_counts())
    st.dataframe(
        shown_alerts[
            [
                "nivel_alerta",
                "anio",
                "entidad",
                "nombre_catalogo",
                "proveedor_mas_frecuente",
                "precio_promedio_ent",
                "precio_mediano",
                "sobreprecio_pct",
                "cantidad_compras",
                "unidad",
            ]
        ]
        .sort_values(["sobreprecio_pct", "cantidad_compras"], ascending=False)
        .head(1000),
        use_container_width=True,
        hide_index=True,
    )

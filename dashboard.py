from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cache"
APP_VERSION = "2026.06.29-ux"

MONEY_COLUMNS = {
    "monto",
    "monto_estimado",
    "monto_adjudicado",
    "monto_item",
    "monto_total",
    "presupuesto",
    "precio_unitario",
    "precio_promedio_ent",
    "precio_mediano",
    "precio_mediano_ent",
    "precio_promedio",
}


@dataclass(frozen=True)
class FilterState:
    years: tuple[int, int] | None
    entity: str
    supplier: str
    ruc: str
    modalities: list[str]
    alert_levels: list[str]
    positive_alerts_only: bool


st.set_page_config(
    page_title="Tablero DNCP Paraguay",
    page_icon="DNCP",
    layout="wide",
    initial_sidebar_state="expanded",
)


def inject_css() -> None:
    st.markdown(
        """
        <style>
        :root {
            --brand: #0B5D3B;
            --brand-dark: #073B28;
            --brand-soft: #E8F3EE;
            --accent: #C8A24A;
            --ink: #1F2937;
            --muted: #64748B;
            --line: #D8E1DD;
            --surface: #FFFFFF;
            --bg: #F5F7F6;
            --danger: #B42318;
            --warn: #B7791F;
        }

        html, body, [class*="css"] {
            font-family: "Inter", "Segoe UI", Arial, sans-serif;
            color: var(--ink);
        }

        .stApp {
            background: var(--bg);
        }

        section[data-testid="stSidebar"] {
            background: #EEF4F1;
            border-right: 1px solid var(--line);
        }

        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3 {
            color: var(--brand-dark);
        }

        div[data-testid="stMetric"] {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 14px 16px;
            min-height: 104px;
        }

        div[data-testid="stMetricLabel"] {
            color: var(--muted);
            font-size: 0.82rem;
        }

        div[data-testid="stMetricValue"] {
            color: var(--ink);
            font-size: clamp(1.25rem, 2.4vw, 1.95rem);
            line-height: 1.1;
        }

        .hero {
            background: linear-gradient(135deg, #073B28 0%, #0B5D3B 62%, #486B48 100%);
            color: #FFFFFF;
            border-radius: 8px;
            padding: 26px 28px;
            margin-bottom: 16px;
        }

        .hero h1 {
            font-size: clamp(1.9rem, 3vw, 3rem);
            line-height: 1.05;
            margin: 0 0 8px 0;
            letter-spacing: 0;
        }

        .hero p {
            color: #DCEBE5;
            max-width: 980px;
            margin: 0;
            font-size: 1rem;
        }

        .hero-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 18px;
        }

        .pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            border: 1px solid rgba(255, 255, 255, 0.28);
            border-radius: 999px;
            padding: 6px 10px;
            color: #F8FAFC;
            background: rgba(255, 255, 255, 0.1);
            font-size: 0.84rem;
        }

        .filter-strip {
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 12px 14px;
            background: var(--surface);
            margin: 8px 0 18px 0;
            color: var(--muted);
            font-size: 0.92rem;
        }

        .section-title {
            margin: 12px 0 8px 0;
        }

        .section-title h2,
        .section-title h3 {
            margin-bottom: 2px;
            color: var(--ink);
            letter-spacing: 0;
        }

        .section-title p {
            margin: 0;
            color: var(--muted);
        }

        .insight-box {
            background: #FFFDF5;
            border: 1px solid #E8D9A6;
            border-left: 4px solid var(--accent);
            border-radius: 8px;
            padding: 14px 16px;
            color: #3B3A2A;
        }

        .risk-box {
            background: #FFF7F6;
            border: 1px solid #F1C7C1;
            border-left: 4px solid var(--danger);
            border-radius: 8px;
            padding: 14px 16px;
            color: #42221E;
        }

        .trace-box {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 14px 16px;
            color: var(--muted);
        }

        .small-note {
            color: var(--muted);
            font-size: 0.86rem;
            margin-top: 4px;
        }

        .footer {
            border-top: 1px solid var(--line);
            color: var(--muted);
            font-size: 0.85rem;
            margin-top: 26px;
            padding-top: 12px;
        }

        div[data-testid="stTabs"] button {
            font-weight: 600;
        }

        @media (max-width: 760px) {
            .hero {
                padding: 22px 18px;
            }
            .hero-meta {
                display: grid;
            }
            div[data-testid="stMetric"] {
                min-height: 90px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def load_parquet(relative_path: str) -> pd.DataFrame:
    path = CACHE / relative_path
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_sources() -> dict[str, pd.DataFrame]:
    return {
        "conv_anual": load_parquet("convocatorias/evolucion_anual.parquet"),
        "conv_mensual": load_parquet("convocatorias/evolucion_mensual.parquet"),
        "modalidades": load_parquet("convocatorias/modalidades.parquet"),
        "top_entidades": load_parquet("convocatorias/top_entidades.parquet"),
        "licitaciones": load_parquet("convocatorias/licitaciones_full.parquet"),
        "adj_anual": load_parquet("adjudicaciones/evolucion_anual.parquet"),
        "adj_mensual": load_parquet("adjudicaciones/evolucion_mensual.parquet"),
        "top_proveedores": load_parquet("adjudicaciones/top_proveedores.parquet"),
        "items": load_parquet("adjudicaciones/items_detalle.parquet"),
        "comparacion": load_parquet("adjudicaciones/comparacion_precios.parquet"),
        "red_actores": load_parquet("adjudicaciones/red_actores.parquet"),
        "catalogo_ruc": load_parquet("adjudicaciones/catalogo_ruc.parquet"),
    }


def show_missing_data(error: Exception) -> None:
    st.error("No se encontraron los archivos Parquet necesarios en la carpeta cache/.")
    st.exception(error)
    st.stop()


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def clean_alert_label(value: object) -> str:
    text = str(value).strip()
    for symbol in ("🔴", "🟠", "🟡", "🔵", "✅", "⚠️"):
        text = text.replace(symbol, "")
    return " ".join(text.split())


def format_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "0"
    return f"{value:,.0f}".replace(",", ".")


def format_percent(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "0,0%"
    return f"{value:,.1f}%".replace(",", "X").replace(".", ",").replace("X", ".")


def format_decimal(value: float, digits: int = 1) -> str:
    return f"{value:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_money(value: float | int | None, compact: bool = True) -> str:
    if value is None or pd.isna(value):
        return "Gs. 0"
    value = float(value)
    if compact:
        abs_value = abs(value)
        if abs_value >= 1_000_000_000_000:
            return f"Gs. {format_decimal(value / 1_000_000_000_000)} bill."
        if abs_value >= 1_000_000_000:
            return f"Gs. {format_decimal(value / 1_000_000_000)} mil mill."
        if abs_value >= 1_000_000:
            return f"Gs. {format_decimal(value / 1_000_000)} mill."
    return f"Gs. {format_number(value)}"


def format_date(value: object) -> str:
    if pd.isna(value):
        return ""
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    return parsed.strftime("%Y-%m-%d")


def filter_by_year(df: pd.DataFrame, years: tuple[int, int] | None) -> pd.DataFrame:
    if years is None or "anio" not in df.columns or df.empty:
        return df
    start, end = years
    return df[df["anio"].between(start, end, inclusive="both")]


def filter_contains(df: pd.DataFrame, column: str, query: str) -> pd.DataFrame:
    if not query or column not in df.columns or df.empty:
        return df
    token = normalize_text(query)
    values = df[column].fillna("").astype(str).str.lower()
    return df[values.str.contains(token, regex=False)]


def filter_by_ruc(df: pd.DataFrame, ruc_query: str, column: str = "ruc") -> pd.DataFrame:
    if not ruc_query or column not in df.columns or df.empty:
        return df
    token = normalize_text(ruc_query).replace(".", "")
    values = df[column].fillna("").astype(str).str.lower().str.replace(".", "", regex=False)
    return df[values.str.contains(token, regex=False)]


def filter_by_modalities(df: pd.DataFrame, modalities: list[str]) -> pd.DataFrame:
    if not modalities or df.empty:
        return df
    column = "modalidad_detalle" if "modalidad_detalle" in df.columns else "modalidad"
    if column not in df.columns:
        return df
    return df[df[column].fillna("").isin(modalities)]


def apply_filters(data: dict[str, pd.DataFrame], filters: FilterState) -> dict[str, pd.DataFrame]:
    licitaciones = filter_by_year(data["licitaciones"], filters.years)
    licitaciones = filter_contains(licitaciones, "entidad", filters.entity)
    licitaciones = filter_by_modalities(licitaciones, filters.modalities)

    conv_anual = filter_by_year(data["conv_anual"], filters.years)
    adj_anual = filter_by_year(data["adj_anual"], filters.years)

    items = filter_by_year(data["items"], filters.years)
    items = filter_contains(items, "entidad", filters.entity)
    items = filter_contains(items, "proveedor", filters.supplier)
    items = filter_by_ruc(items, filters.ruc)

    comparacion = filter_by_year(data["comparacion"], filters.years)
    comparacion = filter_contains(comparacion, "entidad", filters.entity)
    comparacion = filter_contains(comparacion, "proveedor_mas_frecuente", filters.supplier)
    if filters.alert_levels and "nivel_alerta_limpio" in comparacion.columns:
        comparacion = comparacion[comparacion["nivel_alerta_limpio"].isin(filters.alert_levels)]
    if filters.positive_alerts_only and "sobreprecio_pct" in comparacion.columns:
        comparacion = comparacion[comparacion["sobreprecio_pct"] > 0]

    red_actores = data["red_actores"]
    red_actores = filter_contains(red_actores, "entidad", filters.entity)
    red_actores = filter_contains(red_actores, "proveedor", filters.supplier)
    red_actores = filter_by_ruc(red_actores, filters.ruc)

    return {
        "licitaciones": licitaciones,
        "conv_anual": conv_anual,
        "adj_anual": adj_anual,
        "items": items,
        "comparacion": comparacion,
        "red_actores": red_actores,
    }


def available_years(frames: Iterable[pd.DataFrame]) -> list[int]:
    years: set[int] = set()
    for frame in frames:
        if "anio" in frame.columns:
            years.update(int(year) for year in frame["anio"].dropna().unique())
    return sorted(year for year in years if 2010 <= year <= 2026)


def prepare_monthly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "mes" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["fecha"] = pd.to_datetime(out["mes"].astype(str), format="%Y-%m", errors="coerce")
    out = out.dropna(subset=["fecha"]).sort_values("fecha")
    return out.set_index("fecha")


def prepare_annual_chart(df: pd.DataFrame, value_column: str) -> pd.DataFrame:
    if df.empty or "anio" not in df.columns or value_column not in df.columns:
        return pd.DataFrame()
    out = df[["anio", value_column]].copy()
    out = out.sort_values("anio").set_index("anio")
    return out.rename(columns={value_column: "valor"})


def aggregate_annual(df: pd.DataFrame, value_column: str) -> pd.DataFrame:
    if df.empty or "anio" not in df.columns or value_column not in df.columns:
        return pd.DataFrame(columns=["anio", "cantidad", "monto"])
    return (
        df.groupby("anio", dropna=True)
        .agg(cantidad=(value_column, "count"), monto=(value_column, "sum"))
        .reset_index()
        .sort_values("anio")
    )


def entity_options(df: pd.DataFrame, limit: int = 80) -> list[str]:
    if "entidad" not in df.columns or df.empty:
        return []
    counts = df["entidad"].dropna().astype(str).value_counts().head(limit)
    return counts.index.tolist()


def supplier_options(df: pd.DataFrame, limit: int = 80) -> list[str]:
    if "proveedor" not in df.columns or df.empty:
        return []
    counts = df["proveedor"].dropna().astype(str).value_counts().head(limit)
    return counts.index.tolist()


def render_sidebar(data: dict[str, pd.DataFrame], years: list[int]) -> FilterState:
    modality_values = sorted(
        data["licitaciones"]["modalidad_detalle"].dropna().astype(str).unique().tolist()
        if "modalidad_detalle" in data["licitaciones"].columns
        else data["licitaciones"]["modalidad"].dropna().astype(str).unique().tolist()
    )
    alert_values = sorted(data["comparacion"]["nivel_alerta_limpio"].dropna().unique().tolist())

    with st.sidebar:
        st.markdown("## Panel de filtros")
        st.caption("Los filtros impactan KPIs, graficos, tablas y alertas.")

        if years:
            selected_years = st.slider(
                "Cobertura temporal",
                min_value=min(years),
                max_value=max(years),
                value=(min(years), max(years)),
                help="Filtra por ano declarado en los archivos procesados.",
            )
        else:
            selected_years = None

        st.markdown("### Busqueda")
        entity_pick = st.selectbox(
            "Entidad frecuente",
            [""] + entity_options(data["licitaciones"]),
            format_func=lambda value: "Todas las entidades" if value == "" else value,
        )
        entity_text = st.text_input("Entidad contiene", placeholder="Ministerio, Municipalidad...")

        supplier_pick = st.selectbox(
            "Proveedor frecuente",
            [""] + supplier_options(data["items"]),
            format_func=lambda value: "Todos los proveedores" if value == "" else value,
        )
        supplier_text = st.text_input("Proveedor contiene", placeholder="Nombre o razon social")
        ruc_text = st.text_input("RUC contiene", placeholder="80004886-5")

        st.markdown("### Contratacion")
        selected_modalities = st.multiselect(
            "Modalidad",
            modality_values,
            placeholder="Todas las modalidades",
        )

        st.markdown("### Alertas")
        selected_alerts = st.multiselect(
            "Nivel de alerta",
            alert_values,
            placeholder="Todos los niveles",
        )
        positive_only = st.toggle("Solo sobreprecio positivo", value=False)

        st.markdown("---")
        st.caption(f"Version {APP_VERSION}")

    return FilterState(
        years=selected_years,
        entity=entity_text.strip() or entity_pick,
        supplier=supplier_text.strip() or supplier_pick,
        ruc=ruc_text,
        modalities=selected_modalities,
        alert_levels=selected_alerts,
        positive_alerts_only=positive_only,
    )


def render_hero(years: list[int], file_count: int) -> None:
    coverage = f"{min(years)}-{max(years)}" if years else "sin rango"
    st.markdown(
        f"""
        <div class="hero">
            <h1>Observatorio de Contrataciones Publicas de Paraguay</h1>
            <p>
                Tablero ejecutivo para explorar llamados, adjudicaciones, proveedores y senales de precios
                con datos publicos procesados desde cache Parquet. Es una herramienta analitica; no constituye
                informacion oficial ni prueba legal.
            </p>
            <div class="hero-meta">
                <span class="pill">Cobertura {coverage}</span>
                <span class="pill">{file_count} archivos Parquet</span>
                <span class="pill">Fuente: datos publicos DNCP / OCDS</span>
                <span class="pill">Version {APP_VERSION}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def active_filter_text(filters: FilterState) -> str:
    bits: list[str] = []
    if filters.years:
        bits.append(f"Anos {filters.years[0]}-{filters.years[1]}")
    if filters.entity:
        bits.append(f"Entidad contiene: {filters.entity}")
    if filters.supplier:
        bits.append(f"Proveedor contiene: {filters.supplier}")
    if filters.ruc:
        bits.append(f"RUC contiene: {filters.ruc}")
    if filters.modalities:
        bits.append(f"Modalidades: {len(filters.modalities)}")
    if filters.alert_levels:
        bits.append(f"Alertas: {', '.join(filters.alert_levels)}")
    if filters.positive_alerts_only:
        bits.append("Solo sobreprecio positivo")
    return " | ".join(bits) if bits else "Sin filtros activos"


def render_filter_strip(filters: FilterState, filtered: dict[str, pd.DataFrame]) -> None:
    st.markdown(
        f"""
        <div class="filter-strip">
            <strong>Filtros activos:</strong> {active_filter_text(filters)}
            <br>
            <span>
                Resultado actual: {format_number(len(filtered["licitaciones"]))} llamados,
                {format_number(len(filtered["items"]))} items adjudicados y
                {format_number(len(filtered["comparacion"]))} comparaciones de precio.
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi_row(values: list[tuple[str, str, str | None]]) -> None:
    columns = st.columns(len(values))
    for column, (label, value, help_text) in zip(columns, values):
        column.metric(label, value, help=help_text)


def data_frame(
    df: pd.DataFrame,
    columns: list[str],
    rename: dict[str, str],
    limit: int = 500,
    sort_by: str | list[str] | None = None,
    ascending: bool | list[bool] = False,
) -> None:
    if df.empty:
        st.info("No hay registros para los filtros actuales.")
        return
    shown = df.copy()
    if sort_by is not None:
        shown = shown.sort_values(sort_by, ascending=ascending)
    existing = [col for col in columns if col in shown.columns]
    shown = shown[existing].head(limit)
    for col in existing:
        if col in MONEY_COLUMNS:
            shown[col] = shown[col].map(lambda value: format_money(value, compact=False))
        elif "fecha" in col:
            shown[col] = shown[col].map(format_date)
        elif col == "sobreprecio_pct":
            shown[col] = shown[col].map(format_percent)
    shown = shown.rename(columns=rename)
    st.dataframe(shown, use_container_width=True, hide_index=True)


def render_section_title(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="section-title">
            <h2>{title}</h2>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def top_entities_from_licitaciones(licitaciones: pd.DataFrame) -> pd.DataFrame:
    if licitaciones.empty:
        return pd.DataFrame(columns=["entidad", "cantidad", "monto_estimado"])
    return (
        licitaciones.groupby("entidad", dropna=True)
        .agg(cantidad=("id_llamado", "count"), monto_estimado=("monto_estimado", "sum"))
        .reset_index()
        .sort_values(["cantidad", "monto_estimado"], ascending=False)
    )


def top_suppliers_from_items(items: pd.DataFrame) -> pd.DataFrame:
    if items.empty:
        return pd.DataFrame(columns=["proveedor", "ruc", "cantidad", "monto_item"])
    return (
        items.groupby(["proveedor", "ruc"], dropna=False)
        .agg(cantidad=("descripcion", "count"), monto_item=("monto_item", "sum"))
        .reset_index()
        .sort_values("monto_item", ascending=False)
    )


def alert_distribution(comparacion: pd.DataFrame) -> pd.DataFrame:
    if comparacion.empty:
        return pd.DataFrame(columns=["nivel", "cantidad"])
    return (
        comparacion["nivel_alerta_limpio"]
        .value_counts()
        .rename_axis("nivel")
        .reset_index(name="cantidad")
        .sort_values("cantidad", ascending=False)
    )


def render_summary(filtered: dict[str, pd.DataFrame]) -> None:
    licitaciones = filtered["licitaciones"]
    items = filtered["items"]
    comparacion = filtered["comparacion"]
    positive_alerts = comparacion[comparacion["sobreprecio_pct"] > 0] if "sobreprecio_pct" in comparacion.columns else comparacion

    kpi_row(
        [
            ("Llamados", format_number(len(licitaciones)), "Registros de convocatorias filtrados."),
            ("Monto estimado", format_money(licitaciones["monto_estimado"].sum()), "Suma de llamados filtrados."),
            ("Items adjudicados", format_number(len(items)), "Items adjudicados que cumplen los filtros actuales."),
            ("Monto adjudicado", format_money(items["monto_item"].sum()), "Suma de montos de items adjudicados filtrados."),
        ]
    )
    kpi_row(
        [
            ("Proveedores", format_number(items["proveedor"].nunique()), "Proveedores unicos en items filtrados."),
            ("Entidades", format_number(items["entidad"].nunique()), "Entidades unicas en items filtrados."),
            ("Alertas de precio", format_number(len(comparacion)), "Comparaciones de precio para revision analitica."),
            ("Sobreprecio positivo", format_number(len(positive_alerts)), "Comparaciones filtradas con sobreprecio mayor a cero."),
        ]
    )

    left, middle, right = st.columns([1.2, 1, 1])
    with left:
        render_section_title("Lectura rapida", "Senales principales bajo los filtros actuales.")
        if items.empty:
            st.markdown('<div class="insight-box">No hay items adjudicados para el filtro actual.</div>', unsafe_allow_html=True)
        else:
            top_supplier = top_suppliers_from_items(items).head(1)
            supplier_name = top_supplier.iloc[0]["proveedor"] if not top_supplier.empty else "sin proveedor"
            supplier_amount = top_supplier.iloc[0]["monto_item"] if not top_supplier.empty else 0
            st.markdown(
                f"""
                <div class="insight-box">
                    El proveedor con mayor monto filtrado es <strong>{supplier_name}</strong>
                    con {format_money(supplier_amount)}. El filtro actual deja
                    <strong>{format_number(len(positive_alerts))}</strong> comparaciones con sobreprecio positivo
                    para revisar con criterio tecnico.
                </div>
                """,
                unsafe_allow_html=True,
            )
    with middle:
        render_section_title("Top entidades", "Por cantidad de llamados.")
        top_entities = top_entities_from_licitaciones(licitaciones).head(8)
        if top_entities.empty:
            st.info("Sin entidades para mostrar.")
        else:
            st.bar_chart(top_entities.set_index("entidad")["cantidad"], height=280)
    with right:
        render_section_title("Alertas", "Distribucion por nivel.")
        dist = alert_distribution(comparacion)
        if dist.empty:
            st.info("Sin alertas para mostrar.")
        else:
            st.bar_chart(dist.set_index("nivel")["cantidad"], height=280)


def render_convocatorias(data: dict[str, pd.DataFrame], filtered: dict[str, pd.DataFrame]) -> None:
    licitaciones = filtered["licitaciones"]
    conv_anual = aggregate_annual(licitaciones, "monto_estimado")

    render_section_title(
        "Llamados y convocatorias",
        "Evolucion, modalidades y buscador de llamados. Los montos y cantidades se muestran separados para evitar lecturas confusas.",
    )

    kpi_row(
        [
            ("Llamados filtrados", format_number(len(licitaciones)), None),
            ("Presupuesto filtrado", format_money(licitaciones["presupuesto"].sum()) if "presupuesto" in licitaciones else "Gs. 0", None),
            ("Monto estimado", format_money(licitaciones["monto_estimado"].sum()) if "monto_estimado" in licitaciones else "Gs. 0", None),
            ("Modalidades", format_number(licitaciones["modalidad_detalle"].nunique()) if "modalidad_detalle" in licitaciones else "0", None),
        ]
    )

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Cantidad anual")
        chart = prepare_annual_chart(conv_anual, "cantidad")
        if chart.empty:
            st.info("Sin serie anual para mostrar.")
        else:
            st.line_chart(chart, height=260)
    with right:
        st.subheader("Monto anual estimado")
        chart = prepare_annual_chart(conv_anual, "monto")
        if chart.empty:
            st.info("Sin serie anual para mostrar.")
        else:
            st.area_chart(chart, height=260)

    left, right = st.columns([0.85, 1.15])
    with left:
        st.subheader("Modalidades")
        if licitaciones.empty:
            st.info("Sin modalidades para el filtro actual.")
        else:
            modality_col = "modalidad_detalle" if "modalidad_detalle" in licitaciones.columns else "modalidad"
            modalities = (
                licitaciones.groupby(modality_col)
                .agg(cantidad=("id_llamado", "count"), monto_estimado=("monto_estimado", "sum"))
                .reset_index()
                .sort_values("cantidad", ascending=False)
            )
            data_frame(
                modalities,
                [modality_col, "cantidad", "monto_estimado"],
                {modality_col: "Modalidad", "cantidad": "Llamados", "monto_estimado": "Monto estimado"},
                limit=20,
            )
    with right:
        st.subheader("Buscador de llamados")
        search = st.text_input("Buscar en titulo, ID u OCID", key="licitaciones_search")
        shown = licitaciones.copy()
        if search:
            token = search.lower()
            mask = (
                shown["titulo"].fillna("").astype(str).str.lower().str.contains(token, regex=False)
                | shown["id_llamado"].fillna("").astype(str).str.lower().str.contains(token, regex=False)
                | shown["ocid"].fillna("").astype(str).str.lower().str.contains(token, regex=False)
            )
            shown = shown[mask]
        data_frame(
            shown,
            [
                "id_llamado",
                "estado",
                "titulo",
                "entidad",
                "modalidad_detalle",
                "monto_estimado",
                "fecha_publicacion",
                "fecha_cierre",
            ],
            {
                "id_llamado": "ID llamado",
                "estado": "Estado",
                "titulo": "Titulo",
                "entidad": "Entidad",
                "modalidad_detalle": "Modalidad",
                "monto_estimado": "Monto estimado",
                "fecha_publicacion": "Publicacion",
                "fecha_cierre": "Cierre",
            },
            limit=350,
            sort_by="fecha_publicacion",
            ascending=False,
        )


def render_adjudicaciones(filtered: dict[str, pd.DataFrame]) -> None:
    items = filtered["items"]
    adj_anual = aggregate_annual(items, "monto_item")

    render_section_title(
        "Adjudicaciones y proveedores",
        "Concentracion por proveedor, entidades adjudicantes y detalle de items adjudicados.",
    )

    kpi_row(
        [
            ("Items filtrados", format_number(len(items)), None),
            ("Monto de items", format_money(items["monto_item"].sum()), None),
            ("Precio unitario mediano", format_money(items["precio_unitario"].median()), None),
            ("Unidades distintas", format_number(items["unidad"].nunique()), None),
        ]
    )

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Cantidad anual adjudicada")
        chart = prepare_annual_chart(adj_anual, "cantidad")
        if chart.empty:
            st.info("Sin serie anual para mostrar.")
        else:
            st.line_chart(chart, height=260)
    with right:
        st.subheader("Monto anual adjudicado")
        chart = prepare_annual_chart(adj_anual, "monto")
        if chart.empty:
            st.info("Sin serie anual para mostrar.")
        else:
            st.area_chart(chart, height=260)

    top_suppliers = top_suppliers_from_items(items)
    left, right = st.columns([0.9, 1.1])
    with left:
        st.subheader("Proveedores por monto")
        if top_suppliers.empty:
            st.info("Sin proveedores para mostrar.")
        else:
            chart_data = top_suppliers.head(12).set_index("proveedor")["monto_item"]
            st.bar_chart(chart_data, height=320)
    with right:
        st.subheader("Ranking de proveedores")
        data_frame(
            top_suppliers,
            ["proveedor", "ruc", "cantidad", "monto_item"],
            {"proveedor": "Proveedor", "ruc": "RUC", "cantidad": "Items", "monto_item": "Monto adjudicado"},
            limit=25,
        )

    st.subheader("Detalle de items")
    item_text = st.text_input("Buscar descripcion o clasificacion", key="items_search")
    shown_items = items
    if item_text:
        token = item_text.lower()
        shown_items = shown_items[
            shown_items["descripcion"].fillna("").astype(str).str.lower().str.contains(token, regex=False)
            | shown_items["clasificacion"].fillna("").astype(str).str.lower().str.contains(token, regex=False)
        ]
    data_frame(
        shown_items,
        [
            "anio",
            "fecha_adjudicacion",
            "entidad",
            "proveedor",
            "ruc",
            "descripcion",
            "clasificacion",
            "cantidad",
            "unidad",
            "precio_unitario",
            "monto_item",
        ],
        {
            "anio": "Ano",
            "fecha_adjudicacion": "Fecha",
            "entidad": "Entidad",
            "proveedor": "Proveedor",
            "ruc": "RUC",
            "descripcion": "Descripcion",
            "clasificacion": "Clasificacion",
            "cantidad": "Cantidad",
            "unidad": "Unidad",
            "precio_unitario": "Precio unitario",
            "monto_item": "Monto item",
        },
        limit=700,
        sort_by="monto_item",
        ascending=False,
    )


def render_alertas(filtered: dict[str, pd.DataFrame]) -> None:
    comparacion = filtered["comparacion"]

    render_section_title(
        "Senales de precios",
        "Comparaciones para revision analitica. Una alerta no implica irregularidad; indica casos que merecen mirar contexto, especificaciones y fuente.",
    )

    positive = comparacion[comparacion["sobreprecio_pct"] > 0] if "sobreprecio_pct" in comparacion.columns else comparacion
    severe = positive[positive["sobreprecio_pct"] >= 50] if "sobreprecio_pct" in positive.columns else positive
    kpi_row(
        [
            ("Comparaciones", format_number(len(comparacion)), None),
            ("Sobreprecio positivo", format_number(len(positive)), None),
            ("Casos >= 50%", format_number(len(severe)), None),
            ("Sobreprecio medio", format_percent(positive["sobreprecio_pct"].mean()) if not positive.empty else "0,0%", None),
        ]
    )

    left, right = st.columns([0.8, 1.2])
    with left:
        st.subheader("Distribucion de niveles")
        dist = alert_distribution(comparacion)
        if dist.empty:
            st.info("Sin alertas para mostrar.")
        else:
            st.bar_chart(dist.set_index("nivel")["cantidad"], height=300)
        st.markdown(
            """
            <div class="risk-box">
                Usar lenguaje prudente: senal, riesgo, comportamiento atipico o caso para revision.
                La tabla no prueba fraude ni incumplimiento.
            </div>
            """,
            unsafe_allow_html=True,
        )
    with right:
        st.subheader("Casos priorizados")
        data_frame(
            comparacion,
            [
                "nivel_alerta_limpio",
                "anio",
                "entidad",
                "nombre_catalogo",
                "proveedor_mas_frecuente",
                "precio_promedio_ent",
                "precio_mediano",
                "sobreprecio_pct",
                "cantidad_compras",
                "unidad",
            ],
            {
                "nivel_alerta_limpio": "Nivel",
                "anio": "Ano",
                "entidad": "Entidad",
                "nombre_catalogo": "Catalogo",
                "proveedor_mas_frecuente": "Proveedor frecuente",
                "precio_promedio_ent": "Precio promedio entidad",
                "precio_mediano": "Precio mediano referencia",
                "sobreprecio_pct": "Sobreprecio",
                "cantidad_compras": "Compras",
                "unidad": "Unidad",
            },
            limit=700,
            sort_by=["sobreprecio_pct", "cantidad_compras"],
            ascending=[False, False],
        )


def render_trazabilidad(data: dict[str, pd.DataFrame], years: list[int]) -> None:
    render_section_title(
        "Fuentes, cobertura y trazabilidad",
        "Estado operativo de los datos incluidos en el despliegue Streamlit.",
    )
    cache_files = sorted(CACHE.rglob("*.parquet"))
    metadata = []
    for path in cache_files:
        frame_name = path.relative_to(ROOT).as_posix()
        stat = path.stat()
        metadata.append(
            {
                "archivo": frame_name,
                "filas": len(pd.read_parquet(path)),
                "tamano_mb": stat.st_size / 1_000_000,
                "modificado": pd.to_datetime(stat.st_mtime, unit="s").strftime("%Y-%m-%d %H:%M"),
            }
        )
    meta_df = pd.DataFrame(metadata)

    coverage = f"{min(years)}-{max(years)}" if years else "sin dato"
    left, right = st.columns([1, 1])
    with left:
        st.markdown(
            f"""
            <div class="trace-box">
                <strong>Cobertura temporal:</strong> {coverage}<br>
                <strong>Archivos Parquet:</strong> {format_number(len(cache_files))}<br>
                <strong>Registros de items:</strong> {format_number(len(data["items"]))}<br>
                <strong>Registros de llamados:</strong> {format_number(len(data["licitaciones"]))}<br>
                <strong>Comparaciones de precios:</strong> {format_number(len(data["comparacion"]))}
            </div>
            """,
            unsafe_allow_html=True,
        )
    with right:
        st.markdown(
            """
            <div class="trace-box">
                <strong>Limitaciones actuales:</strong><br>
                Los Parquet estan versionados para asegurar que Streamlit Cloud arranque sin descargar datos.
                El pipeline completo de regeneracion todavia debe consolidarse con manifiestos de fuente,
                tamanio, fecha y hash.
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.subheader("Inventario de cache")
    st.dataframe(
        meta_df.rename(
            columns={
                "archivo": "Archivo",
                "filas": "Filas",
                "tamano_mb": "Tamano MB",
                "modificado": "Modificado local",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    inject_css()

    try:
        data = load_sources()
    except Exception as exc:
        show_missing_data(exc)

    data["comparacion"] = data["comparacion"].copy()
    data["comparacion"]["nivel_alerta_limpio"] = data["comparacion"]["nivel_alerta"].map(clean_alert_label)

    years = available_years(
        [
            data["conv_anual"],
            data["adj_anual"],
            data["licitaciones"],
            data["items"],
            data["comparacion"],
        ]
    )

    filters = render_sidebar(data, years)
    filtered = apply_filters(data, filters)

    render_hero(years, len(list(CACHE.rglob("*.parquet"))))
    render_filter_strip(filters, filtered)

    tab_summary, tab_calls, tab_awards, tab_alerts, tab_trace = st.tabs(
        [
            "Resumen ejecutivo",
            "Llamados",
            "Adjudicaciones",
            "Alertas de precio",
            "Fuentes",
        ]
    )

    with tab_summary:
        render_summary(filtered)
    with tab_calls:
        render_convocatorias(data, filtered)
    with tab_awards:
        render_adjudicaciones(filtered)
    with tab_alerts:
        render_alertas(filtered)
    with tab_trace:
        render_trazabilidad(data, years)

    st.markdown(
        f"""
        <div class="footer">
            Tablero analitico no oficial. Version {APP_VERSION}. Repositorio GitHub:
            sanzelias/DNCP_tablero. Datos servidos desde cache Parquet versionado.
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()

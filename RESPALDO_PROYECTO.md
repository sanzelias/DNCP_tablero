# 📋 Documento de Respaldo — Tablero de Transparencia en Contrataciones Públicas

**Proyecto:** Dashboard de análisis de contrataciones públicas de Paraguay  
**Fecha:** Marzo 2025  
**URL pública:** [tablerodncppy.streamlit.app](https://tablerodncppy.streamlit.app)  
**Repositorio:** [github.com/sanzelias/DNCP_tablero](https://github.com/sanzelias/DNCP_tablero)

---

## 📁 Estructura del Proyecto

```
dncp-dashboard/
├── dashboard.py          ← App principal Streamlit
├── processor.py          ← Procesa JSON crudos → Parquet
├── downloader.py         ← Descarga datos de la API DNCP
├── requirements.txt      ← Dependencias Python
├── README.md             ← Documentación básica
├── .gitignore
├── cache/                ← Datos Parquet procesados (incluidos en git)
│   ├── adjudicaciones/
│   │   ├── catalogo_ruc.parquet       ← RUC de empresas (ruc, proveedor)
│   │   ├── comparacion_precios.parquet← Análisis de anomalías de precios
│   │   ├── evolucion_anual.parquet
│   │   ├── evolucion_mensual.parquet
│   │   ├── items_detalle.parquet      ← 1M+ ítems con RUC
│   │   ├── muestra.parquet
│   │   ├── red_actores.parquet        ← Relaciones entidad↔proveedor
│   │   └── top_proveedores.parquet    ← Top 20 proveedores con RUC
│   └── convocatorias/
│       ├── evolucion_anual.parquet
│       ├── evolucion_mensual.parquet
│       ├── licitaciones_full.parquet  ← Buscador de licitaciones
│       ├── modalidades.parquet
│       ├── muestra.parquet
│       └── top_entidades.parquet
└── data/                 ← JSON crudos (2.8 GB, NO incluido en git)
    └── 2025/
```

---

## 🖥️ Stack Tecnológico

| Componente | Tecnología |
|---|---|
| Frontend / App | Streamlit ≥ 1.30 |
| Gráficos | Plotly Express + Graph Objects |
| Datos | Pandas + PyArrow (Parquet) |
| Deploy | Streamlit Community Cloud |
| Versionado | GitHub (rama `main`) |
| Fuente de datos | DNCP Paraguay — API OCDS |

**Dependencias (`requirements.txt`):**
```
pandas>=2.0.0
requests>=2.31.0
tqdm>=4.66.0
streamlit>=1.30.0
plotly>=5.18.0
openpyxl>=3.1.0
pyarrow>=14.0.0
```

---

## 🗂️ Tabs del Dashboard

| # | Tab | Descripción |
|---|---|---|
| 1 | 📋 Convocatorias | Evolución anual/mensual, modalidades, top entidades, buscador de licitaciones |
| 2 | 🏆 Adjudicaciones | Top proveedores (con RUC), evolución, muestra |
| 3 | 🔍 Detalle de Ítems | 1M+ ítems filtrables por entidad, año, descripción, RUC |
| 4 | 🚨 Anomalías de Precios | Detección de precios fuera de rango vs mediana del Estado |

---

## 🔢 Filtro Global por RUC

El filtro de RUC está ubicado en el **sidebar izquierdo**, al final del panel.

- **Campo:** "🔢 Buscar por RUC del proveedor"
- **Placeholder:** `ej: 80004886-5 o 80074991`
- **Funciona en:** todos los tabs simultáneamente
  - Tab 1 → muestra aviso informativo
  - Tab 2 → filtra `top_proveedores` por columna `ruc`
  - Tab 3 → filtra `items_detalle` por columna `ruc`
  - Tab 4 → filtra `comparacion_precios` por columna `ruc_proveedor`

---

## 📊 KPIs Globales (hardcodeados + dinámicos)

| KPI | Fuente |
|---|---|
| Total Llamados | `evolucion_anual.parquet` (convocatorias) |
| Monto Estimado | `evolucion_anual.parquet` (convocatorias) |
| Adjudicaciones | `evolucion_anual.parquet` (adjudicaciones) |
| Monto Adjudicado | `evolucion_anual.parquet` (adjudicaciones) |
| Proveedores Únicos | `items_detalle.parquet` (recuento único) |
| Entidades Públicas | `items_detalle.parquet` (recuento único) |
| Ítems Adjudicados | Hardcodeado: 1,033,935 |
| Anomalías detectadas | Hardcodeado: 15,508 (10,769 crítico + 4,739 alto) |

---

## 🔍 Módulo de Anomalías de Precios

- **Archivo:** `comparacion_precios.parquet`
- **Lógica:** Compara precio promedio por entidad vs precio mediano del conjunto
- **Niveles:** 🚨 CRÍTICO (>2×), ⚠️ Alto (>1.5×), 🟡 Moderado, ✅ Normal
- **RUC:** Obtenido via join con `catalogo_ruc.parquet` por `proveedor_mas_frecuente`

---

## 🔄 Flujo de Actualización de Datos

```
1. downloader.py   → descarga JSONs de la API OCDS → data/2025/
2. processor.py    → procesa JSONs → genera Parquets en cache/
3. git add cache/  → commitear los Parquet actualizados
4. git push        → GitHub detecta cambio → Streamlit Cloud redeploya
```

### Comando rápido para actualizar y pushear:
```bash
cd /ruta/al/proyecto
source .venv/bin/activate
python downloader.py
python processor.py
git add cache/
git commit -m "update: datos frescos YYYY-MM"
git push origin master:main
```

---

## 🌐 Deploy en Streamlit Cloud

- **Cuenta:** diegomezapy (GitHub)
- **App URL:** https://tablerodncppy.streamlit.app
- **Rama:** `main`
- **Archivo principal:** `dashboard.py`
- **Redeploy automático:** sí, al detectar cambios en `main`
- **Redeploy manual:** share.streamlit.io → menú de la app → "Reboot app"

### Para pushear sin VSCode (usar token PAT):
```bash
TOKEN="github_pat_XXXX..."  # Generar en github.com/settings/tokens
git -c credential.helper= push \
  "https://sanzelias:${TOKEN}@github.com/sanzelias/DNCP_tablero.git" \
  master:main
```

> ⚠️ El token se usa una sola vez — guardarlo seguro y regenerar desde GitHub si se comparte.

---

## 📌 Decisiones de Diseño Importantes

| Decisión | Detalle |
|---|---|
| **Branding** | "Transparencia Pública · Paraguay" — NO se presenta como producto oficial de la DNCP. La DNCP solo aparece como fuente de datos (CC BY 4.0) |
| **Aviso académico** | Siempre visible: es prototipo de investigación, no prueba legal |
| **cache/ en git** | Los Parquet (~20MB) se incluyen en el repo para que Streamlit Cloud tenga datos sin necesidad de descargarlos |
| **data/ excluida** | Los JSON crudos (2.8 GB) NO están en git — `.gitignore` los excluye |
| **Red de Actores eliminada** | Se quitó por rendimiento; causaba lentitud al calcular NetworkX en cada render |
| **RUC global** | Se movió al sidebar para aplicar a todos los tabs desde un solo lugar |

---

## 🗂️ Ubicaciones en el Sistema

| Recurso | Ruta |
|---|---|
| Código fuente (local) | `/Users/diegobernardomezabogado/.gemini/antigravity/scratch/dncp-dashboard/` |
| Copia en Google Drive | `/Users/diegobernardomezabogado/Library/CloudStorage/GoogleDrive-dmeza.py@gmail.com/Mi unidad/DNCPpy/` |
| Datos JSON crudos | `...scratch/dncp-dashboard/data/2025/` (solo local, 2.8 GB) |
| Repositorio GitHub | https://github.com/sanzelias/DNCP_tablero |
| App en producción | https://tablerodncppy.streamlit.app |

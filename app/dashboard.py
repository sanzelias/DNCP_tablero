
import streamlit as st
import pandas as pd
from pathlib import Path

st.title("Dashboard DNCP")

data_path = Path("data/processed/evolucion_anual.csv")

if not data_path.exists():
    st.warning("No hay datos procesados. Ejecuta el pipeline.")
    st.stop()

df = pd.read_csv(data_path)

if df.empty:
    st.warning("Dataset vacío")
else:
    st.line_chart(df.set_index("anio")["monto_total"])
    st.dataframe(df)

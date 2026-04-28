
import streamlit as st
import pandas as pd

st.title("Dashboard DNCP")

try:
    df = pd.read_csv("output/evolucion_anual.csv")
except:
    st.warning("No hay datos procesados")
    st.stop()

if df.empty:
    st.warning("Dataset vacío")
else:
    st.line_chart(df.set_index("anio")["monto_total"])
    st.dataframe(df)

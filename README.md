# DNCP Tablero

Tablero Streamlit para explorar datos publicos de contrataciones de Paraguay.

La app funciona directamente con los archivos Parquet incluidos en `cache/`, sin descargar datos adicionales en el arranque.

## Ejecutar localmente

```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

Tambien se conserva el punto de entrada alternativo:

```bash
streamlit run app/dashboard.py
```

## Estructura

- `dashboard.py`: aplicacion principal de Streamlit.
- `app/dashboard.py`: wrapper compatible con despliegues que apunten a `app/dashboard.py`.
- `cache/convocatorias/`: indicadores, muestras y buscador de licitaciones.
- `cache/adjudicaciones/`: proveedores, items, evoluciones y alertas de precios.
- `RESPALDO_PROYECTO.md`: documento de contexto del proyecto.

## Despliegue en Streamlit Cloud

Configurar el archivo principal como:

```text
dashboard.py
```

Streamlit instalara las dependencias desde `requirements.txt` y leera los datos versionados en `cache/`.

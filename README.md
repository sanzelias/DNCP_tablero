# DNCP Tablero

Tablero Streamlit para explorar datos publicos de contrataciones de Paraguay.

La app funciona directamente con los archivos Parquet incluidos en `cache/`, sin descargar datos adicionales en el arranque.

URL publica verificada: https://tablero-dashboard-dncp.streamlit.app/

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

El repositorio incluye `runtime.txt` para fijar Python 3.11. Streamlit instalara las dependencias desde `requirements.txt` y leera los datos versionados en `cache/`.

Si Streamlit Cloud esta configurado con otro archivo principal, usar tambien es valido:

```text
app/dashboard.py
```

Ese archivo carga el dashboard principal del repositorio.

## Verificacion operativa

Antes de publicar cambios, ejecutar como minimo:

```bash
python -m py_compile dashboard.py app/dashboard.py downloader.py processor.py run.py src/downloader.py src/processor.py
streamlit run dashboard.py
```

La app publicada puede quedar dormida en Streamlit Cloud por inactividad. Si aparece la pantalla `Zzzz`, usar el boton de despertar y confirmar que renderice el tablero con KPIs y filtros.

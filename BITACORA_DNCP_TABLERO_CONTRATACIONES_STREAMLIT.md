# BITACORA_DNCP_TABLERO_CONTRATACIONES_STREAMLIT

## 2026-06-29 18:59

### Proyecto

* Nombre: DNCP Tablero Streamlit
* Cliente o institucion: Proyecto de analisis de contrataciones publicas Paraguay
* Ruta local: `G:\.shortcut-targets-by-id\1qg2zKQViUr0GmifBOXMKLGLYRg2EqyPB\BIGDATA_PROYECTO_CONTRATACIONES\DNCP_tablero_repo`
* Repositorio: `https://github.com/sanzelias/DNCP_tablero`
* URL publica: `https://tablero-dashboard-dncp.streamlit.app/`
* Responsable: Diego / Codex
* Version: reparacion operativa 2026-06-29

### Objetivo de la intervencion

* Revisar el repositorio local, la rama remota y la app publicada en Streamlit Cloud.
* Verificar si el tablero publico esta realmente operativo.
* Corregir el estado de GitHub si existia riesgo para el redeploy.

### Diagnostico inicial

* La carpeta raiz del proyecto contenia varias copias; el repo Git activo es `DNCP_tablero_repo`.
* El repo local estaba `ahead 1` y luego, tras `git fetch`, quedo `ahead 1, behind 1`.
* `origin/main` tenia un commit remoto de carga manual con archivos duplicados en raiz y contenido cruzado:
  `dashboard.py` contenia el contenido de `.gitignore`, `requirements.txt` contenia el README, `runtime.txt` contenia codigo de `run.py` y `run.py` contenia `RESPALDO_PROYECTO.md`.
* La URL publica mostro primero pantalla de app dormida de Streamlit; tras pulsar "wake up" renderizo el tablero.
* La app local, desde el commit bueno `9cff568`, renderizo correctamente en escritorio y movil.

### Acciones realizadas

* Se retiro `desktop.ini` dentro de `.git/refs` porque rompia `git log` con `fatal: bad object refs/desktop.ini`.
* Se creo la rama `codex/repair-streamlit-dncp-20260629` desde `origin/main`.
* Se restauro el arbol operativo probado desde `9cff5683bc2a45816d6b7d1e4e76e385c2e214d9`.
* Se eliminaron duplicados subidos en raiz que no pertenecen al arbol limpio.
* Se agrego `desktop.ini` a `.gitignore`.
* Se creo esta bitacora local del proyecto.
* Se creo `PROMPTS_DNCP_TABLERO_CONTRATACIONES_2026-06-29.md` como registro de la secuencia de prompts de la intervencion.

### Archivos modificados

* `.gitignore`
* `README.md`
* `RESPALDO_PROYECTO.md`
* `dashboard.py`
* `processor.py`
* `requirements.txt`
* `run.py`
* `runtime.txt`
* `workspace.code-workspace`
* `BITACORA_DNCP_TABLERO_CONTRATACIONES_STREAMLIT.md`
* `PROMPTS_DNCP_TABLERO_CONTRATACIONES_2026-06-29.md`

### Comandos o scripts ejecutados

* `git status --branch --short`
* `git fetch origin`
* `git log --oneline --decorate --graph -n 12 --all`
* `git ls-remote --heads origin`
* `curl.exe -I https://tablero-dashboard-dncp.streamlit.app/`
* `curl.exe -I https://tablerodncppy.streamlit.app/`
* `python -m py_compile dashboard.py app\dashboard.py downloader.py processor.py run.py src\downloader.py src\processor.py`
* `python -m streamlit run dashboard.py --server.headless true --server.port 8507 --server.address 127.0.0.1`
* `npx playwright screenshot ...`
* Validacion de Parquet con `pandas.read_parquet`

### Resultados verificados

* Sintaxis Python validada sin errores.
* 14 archivos Parquet cargados correctamente.
* Datos principales verificados:
  * `items_detalle.parquet`: 1.034.110 filas.
  * `comparacion_precios.parquet`: 52.177 filas.
  * `licitaciones_full.parquet`: 12.738 filas.
* App local verificada por HTTP 200 y captura Playwright.
* App publica verificada despues de despertar en `https://tablero-dashboard-dncp.streamlit.app/`.

### Pruebas realizadas

* Compilacion Python.
* Carga completa de Parquet.
* Render local Streamlit escritorio.
* Render local Streamlit movil.
* Verificacion publica Streamlit Cloud con navegador headless.

### Errores o incidentes

* `desktop.ini` dentro de `.git/refs` rompia operaciones Git.
* La punta remota `origin/main` estaba corrupta por carga manual.
* Streamlit mostro pantalla de app dormida antes de despertar.
* Streamlit local emitio advertencia no fatal de formato de fecha en `prepare_time_series`.

### Soluciones aplicadas

* Limpieza de `desktop.ini` en `.git/refs`.
* Restauracion del arbol operativo probado sobre una rama basada en `origin/main`.
* Eliminacion de duplicados de raiz.
* Registro de bitacora y prompts.

### Pendientes

* Confirmar redeploy automatico de Streamlit Cloud despues del push.
* Si Streamlit no redeploya solo, entrar a Streamlit Cloud y ejecutar "Reboot app".
* Sustituir `downloader.py` y `processor.py` por un pipeline real reproducible contra la fuente DNCP/OCDS; actualmente son esqueletos.
* Crear manifiestos de datos/cache con fecha, fuente, tamanio y hash.
* Corregir la advertencia de fecha especificando formato esperado en `prepare_time_series`.

### Riesgos

* La app publicada puede seguir funcionando por despliegue previo aunque GitHub este roto; por eso se corrigio la punta remota antes de depender de nuevos redeploys.
* Los Parquet estan versionados en Git; esto simplifica Streamlit Cloud pero exige control de tamanio y trazabilidad.
* No hay pipeline completo de regeneracion de cache, por lo que la reproducibilidad de datos aun es parcial.

### Recomendaciones

* Mantener `dashboard.py` como archivo principal de Streamlit Cloud.
* Evitar cargas manuales de archivos sueltos desde GitHub web.
* Publicar solo desde commits revisados localmente.
* Agregar validacion automatica minima: compilacion Python, carga de Parquet y smoke test Streamlit.
* Registrar cada actualizacion de datos con manifiesto y bitacora.

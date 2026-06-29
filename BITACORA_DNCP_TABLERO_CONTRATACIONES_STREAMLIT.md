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

## 2026-06-29 19:20

### Proyecto

* Nombre: DNCP Tablero Streamlit
* Cliente o institucion: Proyecto de analisis de contrataciones publicas Paraguay
* Ruta local: `G:\.shortcut-targets-by-id\1qg2zKQViUr0GmifBOXMKLGLYRg2EqyPB\BIGDATA_PROYECTO_CONTRATACIONES\DNCP_tablero_repo`
* Repositorio: `https://github.com/sanzelias/DNCP_tablero`
* URL publica: `https://tablero-dashboard-dncp.streamlit.app/`
* Responsable: Diego / Codex
* Version: `2026.06.29-ux`

### Objetivo de la intervencion

* Mejorar radicalmente la experiencia de usuario del tablero publicado.
* Crear un panel de filtros claro y persistente.
* Reorganizar la app para que sea comprensible para lectura ejecutiva.

### Diagnostico inicial

* La app era accesible, pero se veia como prototipo tecnico desordenado.
* Los filtros existian de forma limitada y no eran suficientemente visibles.
* Algunos KPIs y graficos usaban agregados globales por anio, por lo que no respondian plenamente a filtros de entidad/proveedor/modalidad.
* La navegacion no separaba bien resumen, llamados, adjudicaciones, alertas y trazabilidad.

### Acciones realizadas

* Se redisenio `dashboard.py` con estructura ejecutiva.
* Se agrego estilo institucional con paleta verde/gris/dorado y tarjetas KPI.
* Se creo panel lateral de filtros por periodo, entidad, proveedor, RUC, modalidad, nivel de alerta y sobreprecio positivo.
* Se agrego franja de filtros activos y conteos resultantes.
* Se reorganizaron las vistas en:
  `Resumen ejecutivo`, `Llamados`, `Adjudicaciones`, `Alertas de precio` y `Fuentes`.
* Se separaron graficos de cantidades y montos para no mezclar escalas.
* Se recalculan series anuales desde datos filtrados para que los filtros afecten KPIs, graficos y tablas.
* Se limpio la lectura de niveles de alerta quitando simbolos en la tabla.
* Se agrego vista de trazabilidad con inventario de archivos Parquet.

### Archivos modificados

* `dashboard.py`
* `README.md`
* `BITACORA_DNCP_TABLERO_CONTRATACIONES_STREAMLIT.md`
* `PROMPTS_DNCP_TABLERO_CONTRATACIONES_2026-06-29.md`

### Comandos o scripts ejecutados

* `python -m py_compile dashboard.py app\dashboard.py`
* `python -m streamlit run dashboard.py --server.headless true --server.port 8508 --server.address 127.0.0.1`
* `npx playwright screenshot --viewport-size="1440,1000" --full-page http://127.0.0.1:8508/ ...`
* `npx playwright screenshot --viewport-size="390,844" --full-page http://127.0.0.1:8508/ ...`
* Prueba Playwright de filtro `Entidad contiene = IPS`.

### Resultados verificados

* La app compila sin errores.
* La app local responde HTTP 200.
* Captura desktop verificada con panel lateral visible, KPIs, resumen ejecutivo y filtros activos.
* Captura movil verificada con sidebar plegable de Streamlit.
* Prueba de filtro `IPS` verificada:
  * llamados: 500;
  * items adjudicados: 65.485;
  * comparaciones de precio: 1.118;
  * monto estimado: `Gs. 20,6 bill.`;
  * monto adjudicado: `Gs. 13,7 bill.`.

### Pruebas realizadas

* Compilacion Python.
* Smoke test HTTP local.
* Render local escritorio.
* Render local movil.
* Navegacion por pestanas `Llamados`, `Adjudicaciones`, `Alertas de precio` y `Fuentes`.
* Prueba funcional de filtro textual.

### Errores o incidentes

* El primer patron selector+texto no disparaba claramente el filtro libre; se separo selector sugerido y texto libre, usando prioridad del texto libre.
* El formato compacto de moneda afectaba el prefijo `Gs.`; se corrigio con formateo decimal separado.

### Soluciones aplicadas

* Filtros persistentes y realmente conectados a indicadores, tablas y graficos.
* KPIs ejecutivos con montos compactos legibles.
* Lenguaje prudente en alertas de precios.
* Vista de fuentes y limitaciones para trazabilidad.

### Pendientes

* Verificar URL publica luego del push y redeploy de Streamlit Cloud.
* Si Streamlit queda dormido o no redeploya, despertar o reiniciar desde Streamlit Cloud.
* Completar pipeline reproducible de datos DNCP/OCDS y manifiestos hash/tamano/fecha.

### Riesgos

* Streamlit Cloud puede demorar en redeployar tras el push.
* El sidebar movil depende del comportamiento nativo de Streamlit; es funcional, pero ocupa gran parte del viewport cuando esta abierto.
* La reproducibilidad de datos sigue limitada por falta de pipeline completo.

### Recomendaciones

* No volver a mezclar montos y cantidades en un mismo grafico.
* Mantener la regla de filtros fan-out: todo filtro debe afectar KPIs, graficos, tablas, alertas y detalle.
* Evitar lenguaje acusatorio en alertas de precios.
* Usar la vista `Fuentes` como punto de control antes de discutir resultados.

# 📊 DNCP Tablero

Dashboard de análisis de datos de contrataciones públicas (DNCP Paraguay).

---

## 🚀 Ver aplicación

Este proyecto utiliza **Streamlit**, por lo que la aplicación interactiva no se ejecuta en GitHub Pages.

👉 Para usar el dashboard:

```bash
pip install -r requirements.txt
python downloader.py --years 2024 2025
python processor.py --years 2024 2025
streamlit run dashboard.py
```

---

## 📂 Estructura del proyecto

```
.
├── downloader.py     # Descarga datos
├── processor.py      # Procesa información
├── dashboard.py      # Visualización (Streamlit)
├── requirements.txt
├── output/
└── docs/             # GitHub Pages (este archivo)
```

---

## ⚠️ Nota importante

GitHub Pages solo muestra contenido estático (HTML, CSS, Markdown).
El dashboard interactivo requiere ejecución local o en la nube.

---

## 🌐 Deploy recomendado

Puedes desplegar la app en:

* Streamlit Cloud
* Render
* Railway

---

## 👨‍💻 Autor

Proyecto de análisis de datos DNCP.

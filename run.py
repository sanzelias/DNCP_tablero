
import os

os.system("python src/downloader.py --years 2024 2025")
os.system("python src/processor.py --years 2024 2025")
os.system("streamlit run app/dashboard.py")

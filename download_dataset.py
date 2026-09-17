"""
download_dataset.py
-------------------
Descarga el Online Retail Dataset desde UCI Machine Learning Repository
y lo convierte de formato Excel (.xlsx) a CSV para uso con PySpark.

Fuente: https://archive.ics.uci.edu/ml/datasets/Online+Retail
"""

import os
import requests
import pandas as pd

# ── Configuración ──────────────────────────────────────────────────────────────
DATA_DIR   = os.path.join(os.path.dirname(__file__), "data")
EXCEL_PATH = os.path.join(DATA_DIR, "online_retail.xlsx")
CSV_PATH   = os.path.join(DATA_DIR, "online_retail.csv")

# URL directa al archivo Excel en el repositorio UCI
DATASET_URL = (
    "https://archive.ics.uci.edu/ml/machine-learning-databases"
    "/00352/Online%20Retail.xlsx"
)

# ── Funciones ──────────────────────────────────────────────────────────────────

def descargar_excel(url: str, destino: str) -> None:
    """Descarga el archivo Excel desde la URL indicada."""
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    print(f"[INFO] Descargando dataset desde:\n       {url}")
    response = requests.get(url, timeout=120, stream=True)
    response.raise_for_status()

    total = int(response.headers.get("content-length", 0))
    descargado = 0

    with open(destino, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            descargado += len(chunk)
            if total:
                pct = descargado / total * 100
                print(f"\r[INFO] Progreso: {pct:5.1f}%", end="", flush=True)

    print(f"\n[OK]  Archivo guardado en: {destino}")


def convertir_a_csv(excel_path: str, csv_path: str) -> None:
    """Lee el Excel con pandas y lo exporta como CSV UTF-8."""
    print("[INFO] Convirtiendo Excel -> CSV...")
    df = pd.read_excel(excel_path, engine="openpyxl", dtype=str)
    print(f"[INFO] Filas leídas: {len(df):,}  |  Columnas: {list(df.columns)}")
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"[OK]  CSV guardado en: {csv_path}")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1. Descargar si no existe el Excel
    if not os.path.exists(EXCEL_PATH):
        descargar_excel(DATASET_URL, EXCEL_PATH)
    else:
        print(f"[INFO] Excel ya existe: {EXCEL_PATH}")

    # 2. Convertir a CSV si no existe
    if not os.path.exists(CSV_PATH):
        convertir_a_csv(EXCEL_PATH, CSV_PATH)
    else:
        print(f"[INFO] CSV ya existe: {CSV_PATH}")

    print("\n[LISTO] Dataset preparado. Ejecuta: python etl_online_retail.py")

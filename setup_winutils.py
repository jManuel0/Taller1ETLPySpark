"""
setup_winutils.py
-----------------
Descarga winutils.exe y hadoop.dll necesarios para ejecutar
PySpark en Windows sin instalar Hadoop completo.
"""

import os
import urllib.request

# Directorio donde se instalarán los binarios de Hadoop
HADOOP_BIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hadoop", "bin")

# Binarios necesarios (Hadoop 3.3.5 compatible con PySpark 3.5.x)
BINARIES = {
    "winutils.exe": (
        "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
    ),
    "hadoop.dll": (
        "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll"
    ),
}


def descargar(url: str, destino: str) -> None:
    nombre = os.path.basename(destino)
    print(f"[INFO] Descargando {nombre}...")
    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as resp, \
         open(destino, "wb") as f:
        f.write(resp.read())
    size_kb = os.path.getsize(destino) / 1024
    print(f"[OK]   {nombre} guardado ({size_kb:.1f} KB)")


if __name__ == "__main__":
    os.makedirs(HADOOP_BIN, exist_ok=True)
    print(f"[INFO] Directorio Hadoop: {HADOOP_BIN}\n")

    for nombre, url in BINARIES.items():
        destino = os.path.join(HADOOP_BIN, nombre)
        if os.path.exists(destino):
            print(f"[SKIP] {nombre} ya existe.")
        else:
            descargar(url, destino)

    hadoop_home = os.path.dirname(HADOOP_BIN)
    print(f"\n[LISTO] winutils instalado.")
    print(f"        HADOOP_HOME = {hadoop_home}")
    print("\n        Ahora ejecuta: python etl_online_retail.py")

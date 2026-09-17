"""
etl_online_retail.py
====================
Taller 1 – ETL con PySpark  |  Online Retail Dataset (UCI)

Operaciones implementadas:
  1.  Lectura de datos            – spark.read.format("csv")
  2.  Selección de columnas       – select()
  3.  Filtrado de datos           – filter() / where()
  4.  Ordenamiento                – orderBy()
  5.  Agregaciones                – sum(), avg(), min(), max(), count()
  6.  Agrupación de datos         – groupBy() + agg()
  7.  Columnas derivadas          – withColumn()
  8.  Uniones                     – join()
  9.  Funciones de ventana        – rank(), row_number()
  10. Exportación de resultados   – write.csv()

Preguntas respondidas:
  Q1  – Número total de facturas
  Q2  – Número de clientes únicos
  Q3  – Ingreso total (Quantity * UnitPrice)
  Q4  – Producto más vendido en cantidad
  Q5  – Cliente con mayor volumen de compra en dinero
  Q6  – Top 5 países (fuera de UK) con más compras
  Q7  – Ticket promedio por factura
  Q8  – Mínimo, máximo y promedio de productos por factura
  Q9  – Mes del año con más ventas
  Q10 – Porcentaje de facturas con devoluciones
"""

import os
import sys

# ==============================================================================
#  CONFIGURACIÓN HADOOP_HOME (requerido en Windows)
# ==============================================================================

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
HADOOP_HOME  = os.path.join(BASE_DIR, "hadoop")
HADOOP_BIN   = os.path.join(HADOOP_HOME, "bin")
DATA_PATH    = os.path.join(BASE_DIR, "data", "online_retail.csv")
OUTPUT_DIR   = os.path.join(BASE_DIR, "output")

# Configurar HADOOP_HOME antes de importar PySpark
if os.path.isdir(HADOOP_HOME):
    os.environ["HADOOP_HOME"]      = HADOOP_HOME
    os.environ["hadoop.home.dir"]  = HADOOP_HOME
    os.environ["PATH"]             = HADOOP_BIN + os.pathsep + os.environ.get("PATH", "")
    print(f"[INFO] HADOOP_HOME configurado: {HADOOP_HOME}")
else:
    print("[WARN] No se encontro hadoop/. Ejecuta: python setup_winutils.py")

# Fijar PYSPARK_PYTHON a Python 3.11 (PySpark 3.5.x no soporta Python 3.13)
# Buscar Python 3.11 via py launcher o ruta directa
import subprocess, shutil

def find_python311() -> str:
    """Devuelve la ruta al ejecutable de Python 3.11."""
    # Intentar con py launcher primero
    try:
        result = subprocess.run(
            ["py", "-3.11", "-c", "import sys; print(sys.executable)"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    # Fallback: buscar en PATH
    for name in ["python3.11", "python3.11.exe"]:
        path = shutil.which(name)
        if path:
            return path
    # Fallback final: usar el actual (puede fallar en 3.13)
    return sys.executable

PYTHON311 = find_python311()
os.environ["PYSPARK_PYTHON"]        = PYTHON311
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON311
print(f"[INFO] PYSPARK_PYTHON configurado: {PYTHON311}")

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

# ==============================================================================
#  HELPERS
# ==============================================================================

def save_csv(df, folder_name: str) -> None:
    """Guarda un DataFrame como CSV en output/<folder_name>/."""
    path = os.path.join(OUTPUT_DIR, folder_name)
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", "true")
       .csv(path))
    print(f"  [OK] Exportado -> output/{folder_name}/")


def banner(title: str) -> None:
    line = "-" * 70
    print(f"\n{line}")
    print(f"  {title}")
    print(line)


# ==============================================================================
#  SPARK SESSION
# ==============================================================================

spark = (SparkSession.builder
         .appName("Taller1_ETL_OnlineRetail")
         .master("local[*]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("\n[OK] SparkSession iniciada correctamente.")
print(f"     Spark version: {spark.version}")

# ==============================================================================
#  1. LECTURA DE DATOS  →  spark.read.format("csv")
# ==============================================================================

banner("PASO 1 – Lectura de datos (spark.read.format('csv'))")

raw_df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("multiLine", "true")
          .option("quote", '"')
          .option("escape", '"')
          .option("encoding", "UTF-8")
          .load(DATA_PATH))

print(f"\n  Filas totales leídas : {raw_df.count():>10,}")
print(f"  Columnas             : {raw_df.columns}")
raw_df.printSchema()
raw_df.show(5, truncate=False)

# ==============================================================================
#  2. SELECCIÓN DE COLUMNAS  →  select()
# ==============================================================================

banner("PASO 2 – Selección de columnas (select())")

selected_df = raw_df.select(
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country"
)

print("\n  Columnas seleccionadas:")
selected_df.printSchema()

# ==============================================================================
#  3. FILTRADO DE DATOS  →  filter() / where()
# ==============================================================================

banner("PASO 3 – Filtrado de datos (filter() / where())")

# Eliminar filas con CustomerID nulo o Quantity / UnitPrice inválidos
clean_df = (selected_df
            .filter(F.col("CustomerID").isNotNull())
            .filter(F.col("Quantity").isNotNull())
            .filter(F.col("UnitPrice").isNotNull())
            .where(F.col("UnitPrice") >= 0))

print(f"\n  Filas después de limpieza: {clean_df.count():>10,}")

# Dataset de ventas normales (sin devoluciones ni facturas canceladas)
sales_df = (clean_df
            .filter(F.col("Quantity") > 0)
            .filter(~F.col("InvoiceNo").startswith("C")))

print(f"  Filas de ventas válidas  : {sales_df.count():>10,}")
sales_df.show(5, truncate=False)

# ==============================================================================
#  7. COLUMNAS DERIVADAS  →  withColumn()
#     (se hace antes de los análisis que dependen de Revenue)
# ==============================================================================

banner("PASO 7 – Columnas derivadas (withColumn())")

# Revenue = Quantity × UnitPrice
sales_df = (sales_df
            .withColumn("Revenue",
                        F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
            .withColumn("InvoiceDate",
                        F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
            .withColumn("Month",
                        F.month(F.col("InvoiceDate")))
            .withColumn("Year",
                        F.year(F.col("InvoiceDate"))))

# También en clean_df (para Q10 sobre devoluciones)
clean_df = (clean_df
            .withColumn("Revenue",
                        F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
            .withColumn("InvoiceDate",
                        F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm")))

print("\n  Schema con columnas derivadas:")
sales_df.printSchema()
sales_df.select("InvoiceNo", "Quantity", "UnitPrice", "Revenue",
                "Month", "Year").show(5)

# ==============================================================================
#  4. ORDENAMIENTO  →  orderBy()
# ==============================================================================

banner("PASO 4 – Ordenamiento (orderBy())")

top_revenue_df = (sales_df
                  .select("InvoiceNo", "StockCode", "Description",
                          "Quantity", "UnitPrice", "Revenue")
                  .orderBy(F.col("Revenue").desc()))

print("\n  Top 10 líneas por mayor ingreso:")
top_revenue_df.show(10, truncate=40)

# ==============================================================================
#  5. AGREGACIONES  →  sum(), avg(), min(), max(), count()
# ==============================================================================

banner("PASO 5 – Agregaciones globales (sum / avg / min / max / count)")

global_agg = sales_df.agg(
    F.count("InvoiceNo").alias("total_lineas"),
    F.countDistinct("InvoiceNo").alias("total_facturas"),
    F.countDistinct("CustomerID").alias("clientes_unicos"),
    F.sum("Revenue").alias("ingreso_total"),
    F.avg("Revenue").alias("ingreso_promedio_linea"),
    F.min("UnitPrice").alias("precio_minimo"),
    F.max("UnitPrice").alias("precio_maximo")
)

print("\n  Resumen global de ventas:")
global_agg.show(truncate=False)

# ==============================================================================
#  6. AGRUPACIÓN DE DATOS  →  groupBy() + agg()
# ==============================================================================

banner("PASO 6 – Agrupación de datos (groupBy() + agg())")

# Ventas por país
by_country = (sales_df
              .groupBy("Country")
              .agg(
                  F.countDistinct("InvoiceNo").alias("num_facturas"),
                  F.sum("Quantity").alias("total_cantidad"),
                  F.round(F.sum("Revenue"), 2).alias("total_revenue")
              )
              .orderBy(F.col("total_revenue").desc()))

print("\n  Ventas agrupadas por país (Top 10):")
by_country.show(10)

# ==============================================================================
#  8. UNIONES  →  join()
# ==============================================================================

banner("PASO 8 – Uniones entre DataFrames (join())")

# DataFrame de clientes con su revenue total
customer_revenue_df = (sales_df
                       .groupBy("CustomerID")
                       .agg(F.round(F.sum("Revenue"), 2)
                            .alias("total_revenue")))

# DataFrame del país de cada cliente (el más frecuente)
customer_country_df = (sales_df
                       .groupBy("CustomerID", "Country")
                       .count()
                       .orderBy("CustomerID", F.col("count").desc())
                       .dropDuplicates(["CustomerID"])
                       .select("CustomerID", "Country"))

# JOIN entre ambos DataFrames
customer_full_df = (customer_revenue_df
                    .join(customer_country_df,
                          on="CustomerID",
                          how="left")
                    .orderBy(F.col("total_revenue").desc()))

print("\n  Top 10 clientes por revenue (con país de origen):")
customer_full_df.show(10)

# ==============================================================================
#  9. FUNCIONES DE VENTANA  →  rank(), row_number()
# ==============================================================================

banner("PASO 9 – Funciones de ventana (rank / row_number)")

# Ranking de productos por cantidad vendida
product_qty_df = (sales_df
                  .groupBy("StockCode", "Description")
                  .agg(F.sum("Quantity").alias("total_cantidad"))
                  .orderBy(F.col("total_cantidad").desc()))

window_product = Window.orderBy(F.col("total_cantidad").desc())

ranked_products_df = (product_qty_df
                      .withColumn("rank",
                                  F.rank().over(window_product))
                      .withColumn("row_number",
                                  F.row_number().over(window_product)))

print("\n  Top 10 productos más vendidos (con ranking):")
ranked_products_df.show(10, truncate=40)

# Ranking de clientes por revenue
window_customer = Window.orderBy(F.col("total_revenue").desc())

ranked_customers_df = (customer_full_df
                       .withColumn("rank",
                                   F.rank().over(window_customer))
                       .withColumn("row_number",
                                   F.row_number().over(window_customer)))

print("\n  Top 10 clientes por revenue (con ranking):")
ranked_customers_df.show(10)

# ==============================================================================
#  PREGUNTAS ANALÍTICAS
# ==============================================================================

# --- Q1: Número total de facturas --------------------------------------------
banner("Q1 – ¿Cuál es el número total de facturas en el dataset?")

total_invoices = (clean_df
                  .select(F.countDistinct("InvoiceNo")
                          .alias("total_facturas")))
total_invoices.show()
save_csv(total_invoices, "q1_total_invoices")

# --- Q2: Número de clientes únicos -------------------------------------------
banner("Q2 – ¿Cuál es el número de clientes únicos?")

unique_customers = (clean_df
                    .select(F.countDistinct("CustomerID")
                            .alias("clientes_unicos")))
unique_customers.show()
save_csv(unique_customers, "q2_unique_customers")

# --- Q3: Ingreso total (Quantity * UnitPrice) ---------------------------------
banner("Q3 – ¿Cuál es el ingreso total (Quantity * UnitPrice)?")

total_revenue = (sales_df
                 .agg(F.round(F.sum("Revenue"), 2)
                      .alias("ingreso_total_GBP")))
total_revenue.show()
save_csv(total_revenue, "q3_total_revenue")

# --- Q4: Producto más vendido en cantidad -------------------------------------
banner("Q4 – ¿Qué producto fue el más vendido en cantidad?")

top_product = (sales_df
               .groupBy("StockCode", "Description")
               .agg(F.sum("Quantity").alias("total_cantidad"))
               .orderBy(F.col("total_cantidad").desc())
               .limit(1))
top_product.show(truncate=False)
save_csv(top_product, "q4_top_product")

# --- Q5: Cliente con mayor volumen de compra en dinero ------------------------
banner("Q5 – ¿Cuál es el cliente con mayor volumen de compra en dinero?")

top_customer = (customer_full_df
                .orderBy(F.col("total_revenue").desc())
                .limit(1))
top_customer.show()
save_csv(top_customer, "q5_top_customer")

# --- Q6: Top 5 países fuera de UK --------------------------------------------
banner("Q6 – ¿Cuáles son los 5 países que más compran fuera de Reino Unido?")

top_countries = (sales_df
                 .filter(F.col("Country") != "United Kingdom")
                 .groupBy("Country")
                 .agg(
                     F.round(F.sum("Revenue"), 2).alias("total_revenue"),
                     F.sum("Quantity").alias("total_cantidad"),
                     F.countDistinct("InvoiceNo").alias("num_facturas")
                 )
                 .orderBy(F.col("total_revenue").desc())
                 .limit(5))
top_countries.show()
save_csv(top_countries, "q6_top_countries")

# --- Q7: Ticket promedio por factura -----------------------------------------
banner("Q7 – ¿Cuál es el ticket promedio por factura?")

invoice_totals = (sales_df
                  .groupBy("InvoiceNo")
                  .agg(F.round(F.sum("Revenue"), 2)
                       .alias("total_factura")))

avg_ticket = (invoice_totals
              .agg(F.round(F.avg("total_factura"), 2)
                   .alias("ticket_promedio_GBP")))
avg_ticket.show()
save_csv(avg_ticket, "q7_avg_ticket")

# --- Q8: Mínimo, máximo y promedio de productos por factura ------------------
banner("Q8 – ¿Cuál es el mínimo, máximo y promedio de productos por factura?")

products_per_invoice = (sales_df
                        .groupBy("InvoiceNo")
                        .agg(F.countDistinct("StockCode")
                             .alias("productos_distintos")))

stats_products = (products_per_invoice
                  .agg(
                      F.min("productos_distintos").alias("minimo"),
                      F.max("productos_distintos").alias("maximo"),
                      F.round(F.avg("productos_distintos"), 2).alias("promedio")
                  ))
stats_products.show()
save_csv(stats_products, "q8_products_per_invoice")

# --- Q9: Mes del año con más ventas ------------------------------------------
banner("Q9 – ¿Qué mes del año tuvo más ventas?")

monthly_sales = (sales_df
                 .groupBy("Year", "Month")
                 .agg(
                     F.round(F.sum("Revenue"), 2).alias("total_revenue"),
                     F.sum("Quantity").alias("total_cantidad"),
                     F.countDistinct("InvoiceNo").alias("num_facturas")
                 )
                 .orderBy(F.col("total_revenue").desc()))

top_month = monthly_sales.limit(10)
top_month.show()
save_csv(monthly_sales, "q9_monthly_sales")

# --- Q10: Porcentaje de facturas con devoluciones -----------------------------
banner("Q10 – ¿Cuál es el porcentaje de facturas con devoluciones?")

total_inv_count = clean_df.select(
    F.countDistinct("InvoiceNo")).collect()[0][0]

# Facturas con al menos una línea de devolución (Quantity < 0 o InvoiceNo con C)
returns_df = (clean_df
              .filter(
                  (F.col("Quantity") < 0) |
                  (F.col("InvoiceNo").startswith("C"))
              ))

returns_inv_count = returns_df.select(
    F.countDistinct("InvoiceNo")).collect()[0][0]

pct_returns = round(returns_inv_count / total_inv_count * 100, 2)

print(f"\n  Total facturas           : {total_inv_count:,}")
print(f"  Facturas con devolución  : {returns_inv_count:,}")
print(f"  Porcentaje devoluciones  : {pct_returns}%")

return_pct_df = spark.createDataFrame([
    (total_inv_count, returns_inv_count, pct_returns)
], ["total_facturas", "facturas_con_devolucion", "porcentaje_devolucion"])

return_pct_df.show()
save_csv(return_pct_df, "q10_return_pct")

# ==============================================================================
#  EXPORTACIONES ADICIONALES
# ==============================================================================

banner("EXPORTACIONES ADICIONALES")

# Ventas por país (completo)
save_csv(by_country, "ventas_por_pais")

# Ranking de productos
save_csv(ranked_products_df, "ranking_productos")

# Ranking de clientes
save_csv(ranked_customers_df, "ranking_clientes")

# ==============================================================================
#  RESUMEN FINAL
# ==============================================================================

banner("RESUMEN FINAL DE RESULTADOS")

q1_val = total_invoices.collect()[0][0]
q2_val = unique_customers.collect()[0][0]
q3_val = total_revenue.collect()[0][0]
q4_row = top_product.collect()[0]
q5_row = top_customer.collect()[0]
q6_rows = top_countries.collect()
q7_val = avg_ticket.collect()[0][0]
q8_row = stats_products.collect()[0]
q9_row = monthly_sales.collect()[0]

print(f"""
  Q1  – Total de facturas              : {q1_val:,}
  Q2  – Clientes únicos                : {q2_val:,}
  Q3  – Ingreso total (£)              : £{q3_val:,.2f}
  Q4  – Producto más vendido           : {q4_row['Description']} ({q4_row['total_cantidad']:,} uds)
  Q5  – Mejor cliente (ID)             : {q5_row['CustomerID']} — £{q5_row['total_revenue']:,.2f}
  Q6  – Top países (ex-UK)             : {', '.join([r['Country'] for r in q6_rows])}
  Q7  – Ticket promedio (£)            : £{q7_val:,.2f}
  Q8  – Productos/factura (min/max/avg): {q8_row['minimo']} / {q8_row['maximo']} / {q8_row['promedio']}
  Q9  – Mes con más ventas             : {q9_row['Year']}-{q9_row['Month']:02d} — £{q9_row['total_revenue']:,.2f}
  Q10 – % facturas con devolución      : {pct_returns}%
""")

spark.stop()
print("[DONE] ETL completado exitosamente. Resultados en ./output/")

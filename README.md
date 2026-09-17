# Taller 1 – ETL con PySpark 🚀

## Online Retail Dataset (UCI Machine Learning Repository)

Proceso ETL completo con Apache PySpark sobre transacciones de ventas de un retailer en línea del Reino Unido (2010–2011).

## 📋 Descripción

Aplicación de operaciones clave de Spark para explorar y analizar el **Online Retail Dataset** de UCI, incluyendo selección, filtros, agregaciones, agrupaciones, joins y funciones de ventana.

## 📁 Estructura del Proyecto

```
Taller1 ETL_PySpark/
├── data/                         # Dataset (descargar con download_dataset.py)
│   └── online_retail.csv
├── output/                       # Resultados exportados como CSV
│   ├── q1_total_invoices/
│   ├── q2_unique_customers/
│   ├── q3_total_revenue/
│   ├── q4_top_product/
│   ├── q5_top_customer/
│   ├── q6_top_countries/
│   ├── q7_avg_ticket/
│   ├── q8_products_per_invoice/
│   ├── q9_monthly_sales/
│   ├── q10_return_pct/
│   ├── ventas_por_pais/
│   ├── ranking_productos/
│   └── ranking_clientes/
├── etl_online_retail.py          # Script principal PySpark
├── download_dataset.py           # Descarga y prepara el dataset
├── requirements.txt              # Dependencias Python
├── conclusiones.md               # Análisis y conclusiones
└── README.md                     # Este archivo
```

## 🔧 Requisitos

- Python 3.8+
- Java 8 o 11 (requerido por Spark)
- Apache Spark 3.5.x

## ⚙️ Instalación

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Descargar el dataset
python download_dataset.py

# 3. Ejecutar el ETL
python etl_online_retail.py
```

## 📊 Operaciones PySpark Implementadas

| # | Operación | Método Spark |
|---|-----------|-------------|
| 1 | Lectura de datos | `spark.read.format("csv")` |
| 2 | Selección de columnas | `select()` |
| 3 | Filtrado de datos | `filter()` / `where()` |
| 4 | Ordenamiento | `orderBy()` |
| 5 | Agregaciones | `sum()`, `avg()`, `min()`, `max()`, `count()` |
| 6 | Agrupación | `groupBy()` + `agg()` |
| 7 | Columnas derivadas | `withColumn()` |
| 8 | Uniones | `join()` |
| 9 | Funciones de ventana | `rank()`, `row_number()` |
| 10 | Exportación | `write.csv()` |

## ❓ Preguntas Respondidas (Resultados Reales)

| # | Pregunta | Resultado |
|---|----------|-----------|
| Q1 | ¿Numero total de facturas? | **22,190** |
| Q2 | ¿Clientes unicos? | **4,372** |
| Q3 | ¿Ingreso total? | **GBP 8,911,407.90** |
| Q4 | ¿Producto mas vendido? | **PAPER CRAFT , LITTLE BIRDIE** (80,995 uds) |
| Q5 | ¿Cliente con mayor compra? | **CustomerID 14646** — GBP 280,206.02 (Netherlands) |
| Q6 | ¿Top 5 paises ex-UK? | **Netherlands, EIRE, Germany, France, Australia** |
| Q7 | ¿Ticket promedio/factura? | **GBP 480.76** |
| Q8 | ¿Productos/factura (min/max/avg)? | **1 / 541 / 20.93** |
| Q9 | ¿Mes con mas ventas? | **Noviembre 2011** — GBP 1,161,817.38 |
| Q10 | ¿% facturas con devoluciones? | **16.47%** (3,654 de 22,190) |

## 📌 Dataset

- **Fuente:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/Online+Retail)
- **Registros:** ~541,909 transacciones
- **Periodo:** Diciembre 2010 – Diciembre 2011
- **País:** Reino Unido (negocio de artículos de regalo)

## 📄 Licencia

Proyecto académico — Taller 1 ETL con PySpark.

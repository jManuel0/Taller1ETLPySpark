# Conclusiones – Taller 1: ETL con PySpark
## Online Retail Dataset (UCI Machine Learning Repository)

**Materia:** Procesamiento de Datos a Gran Escala
**Dataset:** Online Retail — transacciones de un minorista en linea del Reino Unido (dic 2010 – dic 2011)
**Herramienta:** Apache Spark 3.5.3 + PySpark | Python 3.11

---

## 1. Descripcion del Dataset

El dataset contiene **541,909 registros** de transacciones de ventas con 8 atributos:

| Columna | Tipo | Descripcion |
|---------|------|-------------|
| InvoiceNo | String | Identificador unico de factura (prefijo 'C' = cancelacion) |
| StockCode | String | Codigo unico de producto |
| Description | String | Nombre del producto |
| Quantity | Integer | Cantidad vendida (negativo = devolucion) |
| InvoiceDate | Timestamp | Fecha y hora de la transaccion |
| UnitPrice | Double | Precio unitario en libras esterlinas (GBP) |
| CustomerID | String | Identificador del cliente |
| Country | String | Pais del cliente |

---

## 2. Resultados por Pregunta

### Q1 – Numero total de facturas
- **Resultado: 22,190 facturas unicas**
- De las 541,909 lineas del dataset, existen 22,190 transacciones distintas (incluyendo cancelaciones).
- Tras limpiar registros sin CustomerID se obtiene este conteo limpio.

### Q2 – Numero de clientes unicos
- **Resultado: 4,372 clientes unicos**
- Solo se cuentan registros con CustomerID valido. El dataset original tiene ~25% de registros sin ID de cliente, lo que indica compras anonimas o errores de captura.

### Q3 – Ingreso total (Quantity x UnitPrice)
- **Resultado: GBP 8,911,407.90**
- Calculado unicamente sobre transacciones validas (Quantity > 0, sin cancelaciones).
- Equivale a aproximadamente 11 millones de dolares USD en el periodo 2010-2011.

### Q4 – Producto mas vendido en cantidad
- **Resultado: PAPER CRAFT , LITTLE BIRDIE (StockCode: 23843) con 80,995 unidades**
- Es un articulo de manualidades de bajo costo y alto volumen, tipico de clientes mayoristas.
- El segundo lugar fue MEDIUM CERAMIC TOP STORAGE JAR con ~77,916 unidades.

### Q5 – Cliente con mayor volumen de compra en dinero
- **Resultado: CustomerID 14646 — GBP 280,206.02 (Pais: Netherlands)**
- Este cliente individualmente concentra el 3.1% del ingreso total del negocio.
- Su ubicacion en Paises Bajos y el volumen de compra sugieren un distribuidor mayorista europeo.

### Q6 – Top 5 paises fuera de Reino Unido

| Posicion | Pais | Revenue (GBP) | Cantidad | Facturas |
|----------|------|---------------|----------|----------|
| 1 | Netherlands | 285,446.34 | 200,937 | 95 |
| 2 | EIRE (Irlanda) | 265,545.90 | 140,525 | 260 |
| 3 | Germany | 228,867.14 | 119,263 | 457 |
| 4 | France | 209,024.05 | 111,472 | 389 |
| 5 | Australia | 138,521.31 | 84,209 | 57 |

- Paises Bajos lidera en revenue a pesar de tener solo 95 facturas, lo que indica tickets muy altos (compras mayoristas grandes).
- Alemania e Irlanda tienen mas facturas pero tickets menores, perfil mas de cliente minorista.

### Q7 – Ticket promedio por factura
- **Resultado: GBP 480.76 por factura**
- Este alto ticket promedio confirma que la mayoria de los clientes son mayoristas o distribuidores, no consumidores finales.

### Q8 – Minimo, maximo y promedio de productos por factura

| Metrica | Valor |
|---------|-------|
| Minimo | 1 producto distinto |
| Maximo | 541 productos distintos |
| Promedio | 20.93 productos distintos |

- La gran diferencia entre min (1) y max (541) refleja la heterogeneidad de la base de clientes: desde compras individuales hasta pedidos mayoristas masivos.

### Q9 – Mes del año con mas ventas

| Posicion | Año-Mes | Revenue (GBP) | Cantidad | Facturas |
|----------|---------|---------------|----------|----------|
| 1 | Noviembre 2011 | 1,161,817.38 | 681,888 | 2,658 |
| 2 | Octubre 2011 | 1,039,318.79 | 593,908 | 1,929 |
| 3 | Septiembre 2011 | 952,838.38 | 544,899 | 1,756 |

- **Noviembre 2011** fue el mes pico con GBP 1,161,817 — el 13% del ingreso anual total en un solo mes.
- El patron Q4 (oct-nov-dic) concentra las mayores ventas por la temporada navidena de articulos de regalo.

### Q10 – Porcentaje de facturas con devoluciones
- **Resultado: 16.47% de las facturas tienen devoluciones**
- De 22,190 facturas totales, 3,654 incluyen al menos una linea de devolucion o cancelacion.
- Este porcentaje es elevado para el sector retail y sugiere oportunidades de mejora en la gestion de inventario y politicas de devolucion.

---

## 3. Conclusiones Generales

1. **Modelo de negocio B2B dominante:** El ticket promedio de GBP 480.76 y los pedidos con hasta 541 SKUs distintos confirman que el negocio opera principalmente como mayorista, no como minorista de consumo final.

2. **Alta concentracion de revenue:** El cliente top (ID 14646) genera el 3.1% del ingreso total. Los top 10 clientes probablemente concentran mas del 25% de las ventas — un riesgo de dependencia que debe gestionarse.

3. **Estacionalidad marcada en Q4:** Noviembre 2011 triplica el revenue de meses bajos. El negocio debe preparar inventario y logistica con anticipacion para octubre-diciembre.

4. **Mercados internacionales clave:** Aunque UK domina, Paises Bajos (GBP 285K con solo 95 facturas) es el mercado internacional mas rentable por transaccion. Representa alta oportunidad de expansion.

5. **Tasa de devolucion critica (16.47%):** Casi 1 de cada 6 facturas tiene devoluciones. Es necesario identificar que productos o categorias generan mas devoluciones para tomar accion correctiva.

6. **Calidad de datos mejorable:** El 25% de transacciones sin CustomerID limita el analisis de comportamiento de clientes. Se recomienda implementar captura obligatoria del ID en el sistema de ventas.

7. **PySpark escala correctamente:** El procesamiento de 541,909 registros se completo en ~2 minutos en modo local (single machine). En un cluster distribuido, este tiempo se reduciria proporcionalmente al numero de nodos.

---

## 4. Operaciones PySpark Implementadas

| Operacion | Metodo Spark | Donde se usa |
|-----------|-------------|--------------|
| Lectura de datos | `spark.read.format("csv")` | Carga inicial del dataset |
| Seleccion de columnas | `select()` | Reduccion a 8 columnas relevantes |
| Filtrado | `filter()` / `where()` | Limpieza de nulls, devoluciones, negativos |
| Ordenamiento | `orderBy()` | Rankings de revenue y cantidad |
| Agregaciones | `sum(), avg(), min(), max(), count()` | Todas las preguntas analiticas |
| Agrupacion | `groupBy() + agg()` | Analisis por pais, cliente, producto |
| Col. derivadas | `withColumn()` | Revenue = Qty * Price, Month, Year |
| Uniones | `join()` | Customer revenue + pais de origen |
| Ventanas | `rank(), row_number()` | Ranking de productos y clientes |
| Exportacion | `write.csv()` | 13 archivos CSV de resultados |

---

## 5. Tecnologias Utilizadas

| Componente | Version |
|------------|---------|
| Python | 3.11.x |
| Apache Spark / PySpark | 3.5.3 |
| pandas | 3.0.5 |
| openpyxl | 3.1.5 |
| Java (JDK) | 11+ |

---

## 6. Repositorio GitHub

> **URL:** _[Insertar URL del repositorio aqui]_

### Estructura del repositorio:
```
Taller1 ETL_PySpark/
├── data/
│   └── online_retail.csv          (541,909 filas)
├── output/
│   ├── q1_total_invoices/         22,190 facturas
│   ├── q2_unique_customers/       4,372 clientes
│   ├── q3_total_revenue/          GBP 8,911,407.90
│   ├── q4_top_product/            PAPER CRAFT LITTLE BIRDIE
│   ├── q5_top_customer/           ID 14646 - GBP 280,206
│   ├── q6_top_countries/          NL, IE, DE, FR, AU
│   ├── q7_avg_ticket/             GBP 480.76
│   ├── q8_products_per_invoice/   1 / 541 / 20.93
│   ├── q9_monthly_sales/          Noviembre 2011
│   ├── q10_return_pct/            16.47%
│   ├── ventas_por_pais/
│   ├── ranking_productos/
│   └── ranking_clientes/
├── hadoop/
│   └── bin/
│       ├── winutils.exe
│       └── hadoop.dll
├── etl_online_retail.py
├── download_dataset.py
├── setup_winutils.py
├── requirements.txt
├── conclusiones.md
└── README.md
```

---

*Taller elaborado para la materia de Procesamiento de Datos a Gran Escala — 2026*

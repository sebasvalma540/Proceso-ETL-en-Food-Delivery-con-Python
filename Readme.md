# 🍔 Pipeline ETL & Market Intelligence — Food Delivery (USA)

Pipeline ETL completo en **Python + SQL** que transforma **~900K registros crudos** de restaurantes y menús en una base SQLite lista para análisis, y responde preguntas de negocio de una startup de Food Delivery que quiere expandirse en EE. UU.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-150458?style=flat-square&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-013243?style=flat-square&logo=numpy&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=flat-square&logo=sqlite&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)

## 📑 Contenido

1. [Contexto de negocio](#-contexto-de-negocio)
2. [Datasets y arquitectura](#-datasets-y-arquitectura)
3. [Fase 1 — Extract](#-fase-1--extract-extracción-y-diagnóstico)
4. [Fase 2 — Transform](#-fase-2--transform-limpieza-e-ingeniería-de-características)
5. [Fase 3 — Load](#-fase-3--load-carga-a-sqlite)
6. [Fase 4 — Business Intelligence](#-fase-4--business-intelligence-reporte-al-ceo)
7. [Hallazgos clave](#-hallazgos-clave)
8. [Cómo ejecutarlo](#️-cómo-ejecutarlo)

---
## 👤 Autor

**Sebastian Valentin** — Estudiante de Ingeniería Industrial, UPC

[![GitHub](https://img.shields.io/badge/GitHub-sebasvalma540-181717?style=flat-square&logo=github)](https://github.com/sebasvalma540)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/sebastian-valentin-malpartida-11664428a/)

---

## 📌 Contexto de negocio

Una startup de Food Delivery quiere expandirse en Estados Unidos. Antes de lanzar la app y decidir en qué ciudades invertir, el directorio necesita saber:

- 🏙️ En qué ciudades hay más restaurantes consolidados.
- 💲 Cómo se distribuyen los rangos de precios del mercado.
- 🍽️ Cuánto cuestan en promedio los platos según la ciudad.
- ⭐ Si un restaurante más caro garantiza una mejor experiencia (puntaje).

---

## 🗂️ Datasets y arquitectura

| Archivo | Descripción | Filas |
|---|---|---|
| `restaurants.csv` | Catálogo de restaurantes: dirección completa, puntaje, número de calificaciones, categoría y rango de precios (`$`). | 63,469 |
| `restaurant-menus.csv` | Detalle de cada ítem vendido: categoría, nombre, descripción y precio en texto (ej. `14.0 USD`). | 836,350 (muestra usada) |


```
 CSV crudos   ──► 📥 EXTRACT  ─► 🧹 TRANSFORM     ──► 💾 LOAD  ──► 📊 BI
 (restaurants,    (pandas)        (limpieza, split,    (SQLite)     (consultas SQL)
  menus)                           mapeo, merge)
```

| Etapa | Entrada | Salida |
|---|---|---|
| 📥 Extract | 2 fuentes CSV | `df_menus` (836,350 × 5) y `df_restaurantes` (63,469 × 11) |
| 🧹 Transform | datos crudos con nulos, texto y direcciones anidadas | `df_master` (65,768 × 19) |
| 💾 Load | `df_master` | tabla `master_food_data` en `delivery_insights.db` |
| 📊 BI | tabla SQL | 4 respuestas para el CEO |

---

## 📥 Fase 1 — EXTRACT: Extracción y diagnóstico

### 1.1 Librerías

```python
from pathlib import Path   # rutas de carpetas y archivos
import pandas as pd        # DataFrames
import numpy as np         # cálculos numéricos
```

### 1.2 Lectura de los menús (`df_menus`)

Se leen **todos** los CSV de la carpeta con `Path.glob()`, se validan por extensión y se concatenan en un solo DataFrame. `ignore_index=True` vuelve a numerar las filas desde 0 para evitar índices repetidos.

```python
DATOS = Path("ruta/a/restaurant-menus.csv")
archivos = DATOS.glob("*")

lista_df = []
for archivo in archivos:
    if archivo.suffix == ".csv":
        df = pd.read_csv(archivo, sep=",")
        lista_df.append(df)
    else:
        print("no es archivo tipo csv")

df_menus = pd.concat(lista_df, ignore_index=True)
```

### 1.3 Lectura de restaurantes (`df_restaurantes`)

```python
df_restaurantes = pd.read_csv("ruta/a/restaurants.csv", sep=",")
```

### 1.4 Data profiling

```python
df_menus.info()            # tipos de dato y memoria
df_menus.shape             # (filas, columnas)
df_menus.isnull().sum()    # nulos por columna

df_restaurantes.info()
df_restaurantes.shape
df_restaurantes.isnull().sum()
```

| DataFrame | Dimensiones | Hallazgos principales |
|---|---|---|
| `df_menus` | 836,350 × 5 | 200,324 nulos en `description`, 1 nulo en `name`; `price` viene como texto. |
| `df_restaurantes` | 63,469 × 11 | 28,167 nulos en `score` y `ratings`; 10,617 en `price_range`; nulos menores en `category`, `full_address` y `zip_code`. |

---

## 🧹 Fase 2 — TRANSFORM: Limpieza e ingeniería de características

### 2.1 Limpieza de `df_restaurantes`

**💲 Mapeo de rangos de precio.** Los símbolos `$` se traducen a texto legible y los nulos quedan como `sin categoria`.

```python
def mapeo_precios(fila):
    if fila["price_range"] == "$":
        return "Económico"
    elif fila["price_range"] == "$$":
        return "Moderadamente caro"
    elif fila["price_range"] == "$$$":
        return "Caro"
    elif fila["price_range"] == "$$$$":
        return "Muy caro"
    else:
        return "sin categoria"

df_restaurantes["price_range"] = df_restaurantes.apply(mapeo_precios, axis=1)
```

**📍 Desanidado de ubicación.** La columna `full_address` trae calle, ciudad, estado y código postal en un solo texto. Se separa con `.str.split()` y se elimina la columna repetida.

```python
df_restaurantes[["street", "city", "zip_state", "zip_code2"]] = \
    df_restaurantes["full_address"].str.split(",", n=3, expand=True)

df_restaurantes = df_restaurantes.drop(columns="zip_code2")
```

**✂️ Limpieza de espacios ocultos.**

```python
df_restaurantes["city"] = df_restaurantes["city"].str.strip()
df_restaurantes["street"] = df_restaurantes["street"].str.strip()
df_restaurantes["zip_state"] = df_restaurantes["zip_state"].str.strip()
```

**⭐ Filtro de calidad.** Se descartan restaurantes sin puntaje o con puntaje 0.

```python
df_restaurantes = df_restaurantes[
    df_restaurantes["score"].notna() & (df_restaurantes["score"] != 0)
]
```

**🔡 Estandarización de texto.**

```python
df_restaurantes["category"] = df_restaurantes["category"].str.lower()
```

Resultado de la limpieza (ejemplo):

| name | score | ratings | category | price_range | street | city | zip_state |
|---|---|---|---|---|---|---|---|
| Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | Económico | 4756 S 27th St | Milwaukee | WI |
| Buffalo Wild Wings (8171 S Howell Ave) | 4.4 | 128 | wings | Moderadamente caro | 8171 S Howell Ave | Oak Creek | WI |
| Taqueria El Palmar | 4.7 | 115 | mexican, new mexican, latin american | Económico | 145 W Main St | Waukesha | WI |

### 2.2 Limpieza de `df_menus`

**💵 Casteo de precios.** El precio llega como texto (`"14.0 USD"`). Se elimina `USD` y se convierte a `float`.

```python
df_menus["price"] = df_menus["price"].apply(
    lambda x: float(x.replace("USD", "").strip())
)
```

**🚫 Filtro de precios válidos.** Se conservan solo los ítems con precio no nulo **y** distinto de cero.

```python
df_menus = df_menus[df_menus["price"].notna() & (df_menus["price"] != 0)]
```

Resultado:

| restaurant_id | category | name | price |
|---|---|---|---|
| 4208 | Lunch Sandwiches | BLT Cheeseburger | 14.0 |
| 4208 | Lunch Sandwiches | Thai Short Rib Wrap | 16.0 |
| 4208 | Lunch Salads | Burrata Salad | 16.0 |

### 2.3 Optimización y cruce

**Control de memoria.** Para trabajar solo con restaurantes consolidados, se filtran los que tienen **más de 100 calificaciones**: de decenas de miles quedan **8,702** restaurantes.

```python
df_restaurantes = df_restaurantes[df_restaurantes["ratings"] > 100]
```

**🔗 INNER JOIN.** Se cruzan restaurantes y menús por el ID del restaurante. El `inner` deja solo los IDs presentes en ambas tablas.

```python
df_master = pd.merge(
    df_restaurantes,
    df_menus,
    left_on="id",
    right_on="restaurant_id",
    how="inner"
)
```

```python
df_restaurantes.shape   # (8702, 14)
df_menus.shape          # (836350, 5)
df_master.shape         # (65768, 19)
```

| DataFrame | Dimensiones finales |
|---|---|
| `df_restaurantes` (filtrado) | 8,702 × 14 |
| `df_menus` | 836,350 × 5 |
| `df_master` | 65,768 × 19 |

---

## 💾 Fase 3 — LOAD: Carga a SQLite

Se crea la conexión con la base de datos local (si `delivery_insights.db` no existe, se crea sola) y se carga `df_master` como tabla `master_food_data`.

```python
import sqlite3

con = sqlite3.connect("delivery_insights.db")

df_master.to_sql("master_food_data", con, if_exists="replace", index=False)
# → 65,768 filas cargadas
```

- `if_exists="replace"`: si la tabla ya existía, se borra y se recrea.
- `index=False`: no guarda el índice de pandas como columna.

**Verificación de la carga** leyendo desde SQL:

```python
query = """
    SELECT *
    FROM master_food_data
"""
pd.read_sql(query, con).head()
```

---

## 📊 Fase 4 — Business Intelligence: Reporte al CEO

Todas las consultas se ejecutan sobre la base SQLite con `pd.read_sql(query, con)`.

> ℹ️ Cada fila de `master_food_data` es un **ítem de menú**, por lo que los conteos de las preguntas 1 y 2 son registros (ítems) y no restaurantes únicos.

### 1️⃣ Penetración geográfica: ¿qué ciudades tienen más presencia?

```python
query = """
SELECT city, COUNT(1) AS cantidad_restaurantes
FROM master_food_data
GROUP BY city
ORDER BY COUNT(1) DESC
LIMIT 5
"""
pd.read_sql(query, con)
```

| Ciudad | Registros |
|---|---|
| Milwaukee | 12,278 |
| Seattle | 9,723 |
| Lynnwood | 3,720 |
| Bothell | 2,993 |
| Everett | 2,856 |

💡 **Conclusión:** Milwaukee y Seattle concentran la mayor oferta consolidada, muy por encima del resto.

### 2️⃣ Oportunidad de precio: ¿cómo se distribuye el rango de precios?

Incluye una *window function* (`SUM(COUNT(*)) OVER()`) para calcular el porcentaje sobre el total.

```python
query = """
SELECT price_range,
       COUNT(1) AS total_por_categoria,
       ROUND(COUNT(1) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS porcentaje_por_categoria
FROM master_food_data
GROUP BY price_range
ORDER BY total_por_categoria DESC
"""
pd.read_sql(query, con)
```

| Rango de precios | Registros | % |
|---|---|---|
| Económico | 44,767 | 68.07 |
| Moderadamente caro | 15,370 | 23.37 |
| sin categoria | 5,322 | 8.09 |
| Caro | 309 | 0.47 |

💡 **Conclusión:** la idea de que el mercado es caro no se sostiene: cerca del 68% es económico y solo el 0.47% es "Caro". No aparece ningún "Muy caro".

### 3️⃣ Estrategia de menú: ¿en qué ciudad top es más caro comer?

```python
query = """
SELECT city, AVG(price) AS precio_promedio
FROM master_food_data
WHERE city IN ('Milwaukee', 'Seattle', 'Lynnwood')
GROUP BY city
ORDER BY AVG(price) DESC
"""
pd.read_sql(query, con)
```

| Ciudad | Precio promedio (USD) |
|---|---|
| Seattle | 11.90 |
| Lynnwood | 10.50 |
| Milwaukee | 9.06 |

💡 **Conclusión:** Seattle es la ciudad más cara para comer (≈ USD 11.90 por plato), unos USD 2.84 más que Milwaukee.

### 4️⃣ Correlación precio-calidad: ¿más caro significa mejor experiencia?

Se calcula el puntaje promedio por restaurante y rango de precios, con un índice de referencia por categoría (`CASE`).

```python
query = """
SELECT name_x,
       price_range,
       CASE price_range
           WHEN 'sin categoria'      THEN 10
           WHEN 'Económico'          THEN 20
           WHEN 'Moderadamente caro' THEN 30
           WHEN 'Caro'               THEN 40
           ELSE 0
       END AS Puntaje_referencia_categoria,
       AVG(score) AS Puntaje_por_encuesta
FROM master_food_data
GROUP BY name_x, price_range
ORDER BY AVG(score) DESC
"""
pd.read_sql(query, con)
```

Resultado (671 filas, extremos):

| Restaurante | Rango | Referencia | Puntaje |
|---|---|---|---|
| Colectivo Prospect | Económico | 20 | 5.0 |
| Teriyaki Plus | Económico | 20 | 5.0 |
| 7-Eleven (14501 Juanita-Woodinville) | Económico | 20 | 4.9 |
| Arashi Ramen (Ballard) | Moderadamente caro | 30 | 4.9 |
| … | … | … | … |
| Checkers (3431 W. Fond du Lac Avenue) | Económico | 20 | 3.8 |
| Fryerz (W Fond du Lac Ave) | sin categoria | 10 | 3.8 |
| Popeyes Louisiana Kitchen (2399 W North Ave) | Económico | 20 | 3.2 |

💡 **Conclusión:** los mejores puntajes (4.9–5.0) aparecen tanto en restaurantes *Económico* como *Moderadamente caro*, y los peores también son mayormente económicos. Un precio más alto **no garantiza** mejor puntaje.

---

## 💡 Hallazgos clave

| # | Pregunta | Resultado |
|---|---|---|
| 1 | Ciudades con más presencia | **Milwaukee** y **Seattle** lideran |
| 2 | Distribución de precios | **68%** *Económico*, 23% *Moderadamente caro*, 0.5% *Caro* → el mercado **no** es caro |
| 3 | Precio promedio por ciudad | **Seattle** es la más cara (**USD 11.90**) |
| 4 | Precio vs. calidad | **Más caro no garantiza mejor experiencia** |

---

## ▶️ Cómo ejecutarlo

1. Clona el repositorio:
   ```bash
   git clone https://github.com/sebasvalma540/Proceso-ETL-en-Food-Delivery-con-Python.git
   ```
2. Instala las dependencias:
   ```bash
   pip install pandas numpy jupyter
   ```
3. Ajusta las rutas de los CSV en el cuaderno (`DATOS` y la lectura de `restaurants.csv`) a tu entorno local.
4. Abre `Proyect_ETL.ipynb` y ejecuta las celdas en orden.

```
├── restaurant-menus.csv     # Fuente: menús
├── restaurants.csv          # Fuente: restaurantes
├── Proyect_ETL.ipynb        # Pipeline completo
├── delivery_insights.db     # Base SQLite generada
└── README.md
```

---





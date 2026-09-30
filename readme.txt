Día 1 · Capa Bronze con PySpark
Objetivo de hoy: simular la llegada de un archivo CSV de ventas "sucio", leerlo con PySpark tal como viene, agregarle datos de auditoría y guardarlo como capa Bronze en formato Parquet.

Tiempo: ~1.5 horas · Dónde: Google Colab con tu cuenta personal (no instalas nada en tu computadora).

Cómo usar este notebook: ejecuta las celdas en orden, de arriba hacia abajo, con Shift + Enter. Lee el texto antes de cada celda: explica qué hace y qué deberías ver.

Recordatorio: ¿qué es la arquitectura Medallion?
Capa	Qué guarda	Regla de oro
Bronze	Los datos tal como llegaron	No se corrige nada, solo se agrega auditoría
Silver	Datos limpios y validados	Tipos correctos, sin duplicados, sin basura
Gold	Datos listos para negocio	Agregados, métricas, reportes
Hoy solo hacemos Bronze. Los errores que metemos a propósito los limpiaremos mañana en Silver.

Paso 1 · Preparar PySpark
Colab ya trae Java instalado, así que solo hay que preparar PySpark.

Colab trae también una librería llamada dataproc-spark-connect que choca con PySpark y provoca errores al crear la sesión. Por eso primero la desinstalamos junto con el PySpark que viene de fábrica, y luego instalamos una versión limpia: PySpark 3.5.1.

Si Colab te muestra un aviso de "Restart session", acéptalo y vuelve a ejecutar desde esta celda.

Celda de código 1
# Quitar la librería que genera conflicto y el PySpark preinstalado
!pip uninstall -y dataproc-spark-connect pyspark

# Instalar una versión limpia de PySpark
!pip install pyspark==3.5.1 -q
Paso 2 · Crear la sesión de Spark
La SparkSession es la puerta de entrada a Spark: todo lo que hagas pasa por ella.

appName: nombre de tu aplicación (aparece en logs).
master("local[*]"): corre Spark en esta máquina usando todos los núcleos disponibles.
Deberías ver: Spark 3.5.1 listo.

Celda de código 2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("medallion")
    .master("local[*]")
    .getOrCreate()
)

print("Spark", spark.version, "listo")
Paso 3 · Simular la llegada de un archivo CSV "sucio"
En la vida real los datos llegan de sistemas externos y siempre traen errores. Aquí creamos un CSV con 8 ventas y metemos estos errores a propósito:

Venta	Error plantado	Lo arreglamos en
3	Tienda con espacios de más: " Sur "	Silver
4	Precio negativo: -4500	Silver
5	Fecha en otro formato: 02/09/2026	Silver
6	Cliente vacío	Silver
7	Cantidad escrita en texto: dos	Silver
2 (repetida)	Registro duplicado	Silver
El archivo se guarda en /content/raw/ventas_raw.csv (el disco temporal de Colab).

Celda de código 3
import os

os.makedirs("/content/raw", exist_ok=True)

csv_crudo = """id_venta,tienda,producto,cantidad,precio,fecha,cliente
1,Centro,Laptop,1,15000,2026-09-01,Ana
2,Norte,Mouse,3,250,2026-09-01,Luis
3,  Sur ,Teclado,2,800,2026-09-02,Carla
4,Centro,Monitor,1,-4500,2026-09-02,Pedro
5,Norte,Laptop,2,15000,02/09/2026,Sofia
6,Sur,Mouse,1,250,2026-09-03,
7,Centro,Audífonos,dos,600,2026-09-03,Marta
2,Norte,Mouse,3,250,2026-09-01,Luis
"""

ruta_raw = "/content/raw/ventas_raw.csv"
with open(ruta_raw, "w", encoding="utf-8") as archivo:
    archivo.write(csv_crudo)

print("Archivo creado en:", ruta_raw)
print(open(ruta_raw, encoding="utf-8").read())
Paso 4 · Leer el CSV con Spark (ingesta)
Opciones importantes: - header=True: la primera fila son los nombres de las columnas. - inferSchema=False: todas las columnas se leen como texto (string).

¿Por qué todo como texto? Porque en Bronze no queremos que Spark "adivine" tipos ni descarte valores raros como dos o 02/09/2026. Guardamos exactamente lo que llegó. La conversión de tipos se hace en Silver.

Deberías ver 8 filas y un esquema donde todo dice string.

Celda de código 4
df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("encoding", "UTF-8")
    .csv(ruta_raw)
)

df_raw.printSchema()
df_raw.show(truncate=False)
print("Filas leídas:", df_raw.count())
Paso 5 · Agregar columnas de auditoría
Bronze no corrige datos, pero sí registra de dónde y cuándo llegaron. Esto es clave cuando algo falla y necesitas rastrear el origen:

fecha_ingesta: momento exacto en que cargamos el archivo.
fuente: nombre del archivo de origen.
Celda de código 5
df_bronze = (
    df_raw
    .withColumn("fecha_ingesta", F.current_timestamp())
    .withColumn("fuente", F.lit("ventas_raw.csv"))
)

df_bronze.show(truncate=False)
Paso 6 · Guardar la capa Bronze en Parquet
Parquet es el formato estándar en data engineering: guarda por columnas, comprime muy bien y conserva el esquema. Es lo que usan por debajo Iceberg, Delta Lake y la mayoría de los data lakes.

mode("overwrite"): si ya existe, lo reemplaza. Así puedes volver a ejecutar el notebook sin errores.
Celda de código 6
ruta_bronze = "/content/bronze/ventas"

df_bronze.write.mode("overwrite").parquet(ruta_bronze)

print("Bronze guardado en:", ruta_bronze)
!ls -la /content/bronze/ventas
Paso 7 · Verificar lo que guardamos
Nunca confíes en que se guardó bien: léelo de vuelta. Deberías ver 8 filas y 9 columnas (7 originales + 2 de auditoría).

Celda de código 7
df_check = spark.read.parquet(ruta_bronze)

print("Filas en Bronze:", df_check.count())
print("Columnas:", len(df_check.columns))
df_check.printSchema()
df_check.show(truncate=False)
Paso 8 · Detectar los problemas (preparación para Silver)
Aquí no corregimos nada, solo medimos. Es el primer paso de cualquier limpieza: saber qué está mal.

cast("double") y cast("int") intentan convertir texto a número; si no pueden (como con dos), devuelven null. Así detectamos valores inválidos.

Celda de código 8
print("1) Ventas duplicadas (mismo id_venta):")
df_check.groupBy("id_venta").count().filter(F.col("count") > 1).show()

print("2) Precios negativos:")
df_check.filter(F.col("precio").cast("double") < 0).show(truncate=False)

print("3) Cantidades que no son número:")
df_check.filter(F.col("cantidad").cast("int").isNull()).show(truncate=False)

print("4) Clientes vacíos:")
df_check.filter(F.col("cliente").isNull()).show(truncate=False)

print("5) Tiendas con espacios de más:")
df_check.filter(F.col("tienda") != F.trim(F.col("tienda"))).show(truncate=False)

print("6) Fechas que no tienen formato AAAA-MM-DD:")
df_check.filter(~F.col("fecha").rlike(r"^\d{4}-\d{2}-\d{2}$")).show(truncate=False)
Si todo salió bien, cada una de las 6 revisiones encontró exactamente 1 problema. Esos son los que resolveremos mañana en la capa Silver.

Paso 9 · Guardar tu trabajo en GitHub (15 min)
Si no tienes cuenta, crea una en github.com con tu correo personal.
Crea un repositorio nuevo llamado data-ai-portfolio, público, y marca Add a README file.
En Colab: Archivo → Guardar una copia en GitHub.
Autoriza a Colab, elige el repo data-ai-portfolio y guarda como 01_bronze.ipynb.
En GitHub, edita el README.md y pega el contenido del archivo README que viene junto a este notebook.
Importante: los archivos en /content se borran cuando Colab se desconecta. No pasa nada: el código está en GitHub y en 2 minutos regeneras todo ejecutando las celdas otra vez.

✅ Checklist del día
☐ PySpark funcionando
☐ CSV crudo creado con 6 errores
☐ Bronze guardado en Parquet con auditoría
☐ 6 problemas detectados
☐ Notebook guardado en GitHub

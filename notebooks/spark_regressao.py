#%%
pip install pyspark
# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Regressão com Spark") \
    .getOrCreate()

spark
# %%
caminho = r'C:\Users\Administrador\Desktop\Alura_PysPark\data\imoveis.json'
dados = spark.read.json(
    caminho,
    multiLine=False
)
# %%
dados.show(truncate=False)
# %%
dados.count()
# %%
dados
# %%
dados.printSchema()
# %%
dados\
        .select('ident.CustomerID', 'listing.*')\
        .show(truncate=False)

# %%

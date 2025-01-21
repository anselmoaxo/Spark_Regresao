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
dados\
    .select('ident.customerID', 'listing.types.*', 'listing.features.*', 'listing.address.*', 'listing.prices.price', 'listing.prices.tax.*')\
    .show(truncate=False)
# %%
dados\
        .select('ident.customerID', 'listing.types.*', 'listing.features.*', 'listing.address.*', 'listing.prices.price', 'listing.prices.tax.*')\
        .drop('location', 'neighborhood','totalAreas')\
        .show(truncate=False)
# %%
dataset = dados\
        .select('ident.customerID', 'listing.types.*', 'listing.features.*', 'listing.address.*', 'listing.prices.price', 'listing.prices.tax.*')\
        .drop('location', 'neighborhood','totalAreas')
# %%
dataset
# %%

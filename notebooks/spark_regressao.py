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
# Tratando os Dados
# %%
dataset.printSchema()
# %%
from pyspark.sql.types import DoubleType, IntegerType
# %%
dataset\
        .withColumn('usableAreas', dataset['usableAreas'].cast(IntegerType()))\
        .withColumn('price', dataset['price'].cast(DoubleType()))\
        .withColumn('condo', dataset['condo'].cast(DoubleType()))\
        .withColumn('iptu', dataset['iptu'].cast(DoubleType()))\
        .printSchema()
# %%
# %%
dataset = dataset\
        .withColumn('usableAreas', dataset['usableAreas'].cast(IntegerType()))\
        .withColumn('price', dataset['price'].cast(DoubleType()))\
        .withColumn('condo', dataset['condo'].cast(DoubleType()))\
        .withColumn('iptu', dataset['iptu'].cast(DoubleType()))
# %%
dataset
# %%
dataset.show()
# %%
dataset\
    .select('usage')\
    .groupby('usage')\
    .count()\
    .show()
# %%
dataset = dataset\
    .select('*')\
    .where('usage == "Residencial"')
# %%
dataset\
    .select('unit')\
    .groupby('unit')\
    .count()\
    .show()
# %%
dataset\
    .select('zone')\
    .groupby('zone')\
    .count()\
    .show()
# %%
from pyspark.sql import functions as f
# %%

# Databricks notebook source
# MAGIC %md
# MAGIC #Verificando se os dados foram montados e se o acesso está correto

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados/inbound'))

# COMMAND ----------

# MAGIC %md
# MAGIC #Lendo os dados da camada inbound

# COMMAND ----------

path = 'dbfs:/mnt/dados/inbound'
dados = spark.read.json(path)

# COMMAND ----------

display(dados)

# COMMAND ----------

dados = dados.drop('usuario', 'imagens')
display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC #criando uma coluna de id

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

dados = dados.withColumn("id", col('anuncio.id'))
display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC #Salvando na camada bronze

# COMMAND ----------

path = '/mnt/dados/bronze/dataset-imoveis'
dados.write.format("delta").mode("Overwrite").save(path)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/dados/bronze/dataset-imoveis'))

# COMMAND ----------



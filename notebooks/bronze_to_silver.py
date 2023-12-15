# Databricks notebook source
# MAGIC %md
# MAGIC #Verificando se os dados foram montados e se o acesso está correto

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados/bronze'))

# COMMAND ----------

path = 'dbfs:/mnt/dados/bronze/dataset-imoveis'
dados = spark.read.format("delta").load(path)

# COMMAND ----------

display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformando os campos de json em coluna

# COMMAND ----------

display(dados.select('anuncio.*'))

# COMMAND ----------

display(dados.select('anuncio.*', 'anuncio.endereco.*'))

# COMMAND ----------

dados = dados.select('anuncio.*', 'anuncio.endereco.*')

# COMMAND ----------

dados.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #Removendo as colunas

# COMMAND ----------

dados = dados.drop("caracteristicas", "endereco")

# COMMAND ----------

dados.columns

# COMMAND ----------

path = '/mnt/dados/silver/dataset-imoveis'
dados.write.format("delta").mode("Overwrite").save(path)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/dados/silver/dataset-imoveis'))

# COMMAND ----------



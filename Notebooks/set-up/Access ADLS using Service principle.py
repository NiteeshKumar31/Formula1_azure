# Databricks notebook source
# MAGIC %md 
# MAGIC ## Acess Azure Data lake using Service principle
# MAGIC
# MAGIC 1. Register new application in microsoft infra id and create a secret
# MAGIC 2. Fetch the clientid, tenantid and secretid from overview tab
# MAGIC 3. Create a keyvault and store these keys
# MAGIC 4. Create a secret scope in databricks using valut URL and Resource ID
# MAGIC 5. Assign the secret keys to variables
# MAGIC 3. Read circuits file content

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope", key='client-id')
tenant_id = dbutils.secrets.get(scope="formula1-scope", key='tenant-id')
client_secret = dbutils.secrets.get(scope="formula1-scope", key='client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl31.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl31.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl31.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl31.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl31.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://test@formula1dl31.dfs.core.windows.net/circuits (1).csv"))

# COMMAND ----------

display(spark.read.format(header= True).csv("abfss://test@formula1dl31.dfs.core.windows.net/circuits (1).csv"))

# COMMAND ----------


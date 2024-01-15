# Databricks notebook source
# MAGIC %md 
# MAGIC ## Mount Azure Data lake using Service principle
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 2. Explore other file system utilities related to mount
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope="formula1-scope", key='client-id')
    tenant_id = dbutils.secrets.get(scope="formula1-scope", key='tenant-id')
    client_secret = dbutils.secrets.get(scope="formula1-scope", key='client-secret')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formula1dl31', 'gold')

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope", key='client-id')
tenant_id = dbutils.secrets.get(scope="formula1-scope", key='tenant-id')
client_secret = dbutils.secrets.get(scope="formula1-scope", key='client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl31/test"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl31/test/circuits (1).csv"))

# COMMAND ----------

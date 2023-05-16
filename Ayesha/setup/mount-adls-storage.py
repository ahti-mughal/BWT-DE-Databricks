# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1-scope")

# COMMAND ----------

dbutils.secrets.get(scope="formula1-scope",key="data-bricks-client-ID")

# COMMAND ----------

for x in dbutils.secrets.get(scope="formula1-scope",key="data-bricks-client-ID"):
    print(x)

# COMMAND ----------

storage_account_name = "correctformula1"
client_ID = dbutils.secrets.get(scope="formula1-scope",key="data-bricks-client-ID")
tenant_ID = dbutils.secrets.get(scope="formula1-scope",key="data-bricks-tenant-ID")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="data-bricks-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type" : "OAuth",
"fs.azure.account.oauth.provider.type" : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id" : f"{client_ID}",
"fs.azure.account.oauth2.client.secret" : f"{client_secret}",
"fs.azure.account.oauth2.client.endpoint" : f"https://login.microsoftonline.com/{tenant_ID}/oauth2/token"
} 

# COMMAND ----------

container_name ="raw"
dbutils.fs.mount(
    source= f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs= configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/correctformula1/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
    
    dbutils.fs.mount(
    source= f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs= configs)

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

dbutils.fs.unmount("/mnt/correctformula1/raw")

# COMMAND ----------

dbutils.fs.unmount("/mnt/correctformula1/processed")

# COMMAND ----------


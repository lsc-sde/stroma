# Databricks notebook source
# DBTITLE 1,Import Libraries
import sqlmesh
import os

# COMMAND ----------
# DBTITLE 1,Get Secrets
os.environ['DATABRICKS_STATE_DB_HOST'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "ConnectionStringTo-SqlMeshStateDatabase")
os.environ['DATABRICKS_STATE_DB_USER'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "UsernameTo-SqlMeshStateDatabase")
os.environ['DATABRICKS_STATE_DB_PASSWORD'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "PasswordTo-SqlMeshStateDatabase")
os.environ['DATABRICKS_SERVER_HOSTNAME'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "UrlTo-DatabricksWorkspace")
os.environ['DATABRICKS_HTTP_PATH'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "SqlEndpointTo-DatabricksWorkspace")
os.environ['DATABRICKS_ACCESS_TOKEN'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "TokenTo-DatabricksAPI")

os.environ['AZURE_SQL_SERVER_STATE_HOST'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "ConnectionStringTo-AzureSqlDbStateDatabase")
os.environ['AZURE_SQL_SERVER_STATE_USER'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "UsernameTo-AzureSqlDbStateDatabase")
os.environ['AZURE_SQL_SERVER_STATE_PASSWORD'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "PasswordTo-AzureSqlDbStateDatabase")
os.environ['AZURE_SQL_STATE_DATABASE'] = dbutils.secrets.get(scope = "AzureKeyVaultSecrets", key = "DatabaseTo-AzureSqlDbStateDatabase")

# COMMAND ----------
# DBTITLE 1,Set Environment Variables
os.environ['ENABLED_GATEWAYS']="databricks"
os.environ['DEFAULT_GATEWAY']='databricks'
os.environ['STATE_SCHEMA']="stroma"
os.environ['DATABRICKS_CATALOG']="rde_development"
os.environ['DATABRICKS_CATALOG_SOURCE']="dbw-lsc-uks-dataprd-dlz-01"
os.environ['DATABRICKS_SCHEMA_BASE']="lth_omop"
os.environ['DATABRICKS_SCHEMA_VOCAB']="lth_omop"
os.environ['DATABRICKS_CONCURRENT_TASKS']='8'
os.environ['DATABRICKS_STATE_DB_PORT']='5432'
os.environ['DATABRICKS_STATE_DB_DATABASE']="sqlmesh_stroma"
os.environ['AZURE_SQL_SERVER_STATE_PORT']='1433'

print('Environment variables set.')

# COMMAND ----------
# DBTITLE 1,Set Context
# MAGIC %context ../stroma/

# COMMAND ----------
# DBTITLE 1,Run Plan
# MAGIC %plan prod

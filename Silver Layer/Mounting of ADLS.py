# Databricks notebook source
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "4645191d-8348-48e3-bdb8-2a83cc9c5d0d",
    "fs.azure.account.oauth2.client.secret":"abY8Q~tI5d4B96E4d5MWhsiymdmErGXc-DuubcIU",
    "fs.azure.account.oauth2.client.endpoint":"https://login.microsoftonline.com/87f050b0-4ec1-410d-81d7-97b2c1e92b04/oauth2/v2.0/token"
}

dbutils.fs.mount(
    source = "abfss://bronze@datalakehousegen23.dfs.core.windows.net/",
    mount_point="/mnt/datalakehousegen23/bronze",
    extra_configs = configs
)

dbutils.fs.mount(
    source = "abfss://silver@datalakehousegen23.dfs.core.windows.net/",
    mount_point="/mnt/datalakehousegen23/silver",
    extra_configs = configs
)

dbutils.fs.mount(
    source = "abfss://gold@datalakehousegen23.dfs.core.windows.net/",
    mount_point="/mnt/datalakehousegen23/gold",
    extra_configs = configs
)


display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/datalakehousegen23/silver")
dbutils.fs.unmount("/mnt/datalakehousegen23/gold")



  
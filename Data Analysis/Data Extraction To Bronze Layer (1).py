# Databricks notebook source
#Access Keys Normally Hidden via Secrets
storage_account_name = "lalalandstorages"
storage_account_access_key = "/2QrnuJZBZ78IPzYCK3GgiE3xNcY0oTur/M7fzkVyNbFhovvuMZqbx6ibvEijhaeYCyigFhY9gNW+AStpBLDxw=="

#Set Spark Configuration
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

#Setup
path = "wasbs://newcontainer@lalalandstorages.blob.core.windows.net/"
lists = dbutils.fs.ls(path)
filenames = []

#Iterate to get list of file names
num = 0
for i in lists:
    filenames.append(lists[num][1])
    num += 1

# COMMAND ----------

#Iterate to load Data into Bronze
file_type = "csv"

for i in filenames:
    file_location = f"wasbs://newcontainer@lalalandstorages.blob.core.windows.net/{i}"
    #remove suffix ".csv"
    i = i[0:-4]
    #Create DF
    df = spark.read.format(file_type).option("header","true")\
    .option("inferschema","true").load(file_location)
    #Write to Bronze Overwrite for simplicity purposes
    df.write.mode("overwrite").format("parquet").saveAsTable(f"bronze.{i}")

# Databricks notebook source
# MAGIC %md
# MAGIC # Lets look at Ingest
# MAGIC
# MAGIC In the previous examples, we downloaded Biodiversity Data and boundary conditions from online sources. Now lets look at a few more:
# MAGIC
# MAGIC 1. Ingesting Raw Files to Bronze
# MAGIC 2. Create a table with the Wizard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC To start off, we'll first create our lab schema

# COMMAND ----------

from lab_common import LabContext
labContext = LabContext(spark)
labContext.setupLab("ingest")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manually Ingesting Raw Files to Bronze
# MAGIC
# MAGIC The first step is to think about where to store these uploaded files. Traditionally when using the cloud you had to:
# MAGIC
# MAGIC 1. Find an object storage container
# MAGIC 2. Create access keys for this path
# MAGIC 3. Managed data at the file level.
# MAGIC
# MAGIC With Unity Catalog and Databricks, we can make use of a new feature called "Volumes". These are a shortcut to Object Storage like a "G:" in Windows.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME raw_files;

# COMMAND ----------

# MAGIC %md
# MAGIC We can now see the newly created volume in the catalog though you may need to click refresh.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uploading Raw Files
# MAGIC You typically put raw files into a volume in a few different ways:
# MAGIC
# MAGIC 1. You may have them landed automatically by a tool like Azure Data Factory
# MAGIC 2. You may download them with a script, then store them.
# MAGIC 3. You may manually upload them to the Volume or to a table with our Wizard.
# MAGIC
# MAGIC Lets start by downloading some fish data, so we have something to upload (hand-draulic)
# MAGIC
# MAGIC Save this to your local computer (right click "save"): <a href="https://gist.githubusercontent.com/tomdyson/de3b46612fdbbe6a449a4cd08a0041c0/raw/a8bbb8a2d45f320ee1116aec68a20634c29066ba/fish.json" target="fish">fish.json</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload File to Volume
# MAGIC
# MAGIC We can now find the volume, right click it and upload it.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <img src="https://github.com/davidglevy/public-sector-bootcamp/raw/main/images/upload%20to%20volume.png" width="500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### View the Raw Data
# MAGIC
# MAGIC

# COMMAND ----------


labBasePath = f"/Volumes/{labContext.catalog}/ingest/raw_files/fish.json"
print(labBasePath)

df = spark.sql(f"SELECT name, rating_description, image, description FROM json.`{labBasePath}`")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest to Bronze
# MAGIC

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("fish_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingesting Files with the Wizard
# MAGIC
# MAGIC The other way to ingest raw files is to upload them using the wizard. We'll do this with the "add data" feature.
# MAGIC
# MAGIC Right click this Pokemon CSV and click "Save As": <a href="https://gist.githubusercontent.com/armgilles/194bcff35001e7eb53a2a8b441e8b2c6/raw/92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv" >pokemon.csv</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now we ingest via the wizard
# MAGIC
# MAGIC 1. Click Catalog on the left
# MAGIC 2. Click "Add Data"
# MAGIC
# MAGIC <img src="https://github.com/davidglevy/public-sector-bootcamp/raw/main/images/add_data.png" width="300"/>
# MAGIC
# MAGIC <br />
# MAGIC <br />
# MAGIC
# MAGIC 3. Click "Create New Table"
# MAGIC
# MAGIC <img src="https://github.com/davidglevy/public-sector-bootcamp/raw/main/images/create_or_modify.png" width="500" />
# MAGIC
# MAGIC <br />
# MAGIC <br />
# MAGIC 4. Once you are here, create the new table in your ingest schema.
# MAGIC

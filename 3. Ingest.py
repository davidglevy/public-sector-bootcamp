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
# MAGIC
# MAGIC

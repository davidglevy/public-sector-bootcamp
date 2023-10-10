# Databricks notebook source
# MAGIC %md
# MAGIC ## Take a Look at Your New Car
# MAGIC
# MAGIC We'll use this notebook to show off some of the features we have. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## First Run Setup
# MAGIC We can import python code via modules, and we use this to setup the lab:
# MAGIC
# MAGIC 1. This will use the current users username to create their own catalog
# MAGIC 2. We will first drop the catalog if it exists
# MAGIC 3. We will then create a new catalog and schema for this lab
# MAGIC 4. We set this catalog as the default.

# COMMAND ----------

from lab_common import LabContext
labContext = LabContext(spark)
labContext.setupLab("introduction")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() || '.' || current_schema() AS lab_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Some Tables
# MAGIC
# MAGIC We'll now create some tables using static data. Ordinarily this isn't how we would create a table, there's more likely a source to ingest from.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE people_bronze (
# MAGIC   id bigint,
# MAGIC   dob_text string,
# MAGIC   name string,
# MAGIC   fav_colour string
# MAGIC );
# MAGIC
# MAGIC INSERT INTO people_bronze
# MAGIC (id, dob_text, name, fav_colour)
# MAGIC VALUES
# MAGIC (1, '1980/01/01','David', 'pink'),
# MAGIC (2, '1982/02/01','Shirley', 'Pink'),
# MAGIC (3, '1981/03/01','Peter', 'blue'),
# MAGIC (4, '1984/06/01','Greg', 'Orange'),
# MAGIC (5, '1979/02/01','Wendy', 'orange'),
# MAGIC (6, '1978/11/01','Vinh', 'BROWN'),
# MAGIC (7, '1910/01/01','Alexia', 'Gray'),
# MAGIC (8, '1994/10/01','Ottoline', 'Blue'),
# MAGIC (9, '1994/12/01','Alejandro', 'Yellow');

# COMMAND ----------

# MAGIC %md
# MAGIC # Example Data Manipulation Language (DML) operations

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Create-Table-As (CTAS) for Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE people_silver
# MAGIC AS
# MAGIC SELECT id, to_date(dob_text, 'yyyy/MM/dd') AS dob, name, lower(fav_colour) AS fav_colour
# MAGIC FROM people_bronze;

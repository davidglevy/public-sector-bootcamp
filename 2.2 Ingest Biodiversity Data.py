# Databricks notebook source
# MAGIC %md
# MAGIC Content from here: https://github.com/BNHM/jupyter/blob/master/BNHM-integrateData-2020.ipynb
# MAGIC
# MAGIC Run the cell below to setup the notebook (_shift+return in cell to run_ or Press Run button in the menu)
# MAGIC Do you see a number in the left margin of the cell below? If so, click on _Kernel->Restart and Clear Output_

# COMMAND ----------

# MAGIC %pip install pygbif

# COMMAND ----------

#from db_logging import setup_logging
#logger = setup_logging()

# COMMAND ----------

# MAGIC %md
# MAGIC # ESPM / IB 105 Natural History Museums and Data Science
# MAGIC
# MAGIC The goal of this notebook is to access and integrate diverse data sets to visualize correlations and discover patterns to address questions of species’ responses to environmental change. We will use programmatic tools to show how to use Berkeley resources such as the biodiversity data from biocollections and online databases, field stations, climate models, and other environmental data.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # Download from GBIF API<a id='gbif'></a>
# MAGIC
# MAGIC The Global Biodiversity Information Facility has created an API that we can use to get data about different species at the [GBIF Web API](http://www.gbif.org/developer/summary).
# MAGIC
# MAGIC You can think of a Web API call as a fancy URL, what do you think the end of this URL means:
# MAGIC
# MAGIC http://api.gbif.org/v1/occurrence/search?year=1800,1899
# MAGIC
# MAGIC If you're guessing that it limits the search to the years 1800-1899, you're right! Go ahead and click the URL above. You should see something like this:
# MAGIC
# MAGIC ```
# MAGIC {"offset":0,"limit":20,"endOfRecords":false,"count":5711947,"results":[{"key":14339704,"datasetKey":"857aa892-f762-11e1-a439-00145eb45e9a","publishingOrgKey":"6bcc0290-6e76-11db-bcd5-b8a03c50a862","publishingCountry":"FR","protocol":"BIOCASE","lastCrawled":"2013-09-07T07:06:34.000+0000","crawlId":1,"extensions":{},"basisOfRecord":"OBSERVATION","taxonKey":2809968,"kingdomKey":6,"phylumKey":7707728,"classKey":196,"orderKey":1169,"familyKey":7689,"genusKey":2849312,"speciesKey":2809968,"scientificName":"Orchis militaris L.","kingdom":"Plantae","phylum":"Tracheophyta","order":"Asparagales","family":"Orchidaceae","genus":"Orchis","species":"Orchis 
# MAGIC ```
# MAGIC
# MAGIC It might look like a mess, but it's not! This is actually very structured data, and can easily be put into a table like format, though often programmers don't do this because it's just as easy to keep it as is.
# MAGIC
# MAGIC You might be able to pick out the curly braces `{` and think this it's a dictionary. You'd be right, except in this format we call it [JSON](https://en.wikipedia.org/wiki/JSON).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## *Argia arioides*
# MAGIC
# MAGIC ![argia arioides](https://upload.wikimedia.org/wikipedia/commons/thumb/b/bd/Argia_agrioides-Male-1.jpg/220px-Argia_agrioides-Male-1.jpg)
# MAGIC
# MAGIC When performing data analysis, it is always important to define a question that you seek the answer to. *The goal of finding the answer to this question will ultimately drive the queries and analysis styles you choose to use/write.*
# MAGIC
# MAGIC For this example, we are going to ask: **where have [*Argia agrioides*](https://www.google.com/search?q=Argia+agrioides&rlz=1C1CHBF_enUS734US734&source=lnms&tbm=isch&sa=X&ved=0ahUKEwji9t29kNTWAhVBymMKHWJ-ANcQ_AUICygC&biw=1536&bih=694) (the California Dancer dragonfly) been documented? Are there records at any of our field stations?**
# MAGIC
# MAGIC The code to ask the API has already been written for us! This is often the case with programming, someone has already written the code, so we don't have to. We'll just set up the `GBIFRequest` object and assign that to the variable `req`, short for "request":

# COMMAND ----------

from pygbif import species as species
from pygbif import occurrences as occ

#logger = logging.getLogger()
#logger.disabled = True

# COMMAND ----------

# MAGIC %md
# MAGIC First, get GBIF backbone taxonomic keys

# COMMAND ----------


species_name = 'Argia agrioides'
name_backbone = species.name_backbone(species_name)
print(name_backbone)


key = name_backbone['usageKey']

# COMMAND ----------

# MAGIC %md
# MAGIC Then, get a count of occurrence records for each taxon, and pull out number of records found for each taxon

# COMMAND ----------

records = occ.search(taxonKey = key, limit=1000)


# COMMAND ----------

count = records["count"]
print(f"We had {count} results")
results = records["results"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrames
# MAGIC
# MAGIC JSON is great, but it might be conceptually easier to make this a table. We'll use the popular [`pandas`](http://pandas.pydata.org/) Python library. In `pandas`, a DataFrame is a table that has several convenient features. For example, we can access the columns of the table like we would `dict`ionaries, and we can also treat the columns and rows themselves as Python `list`s.

# COMMAND ----------

# Remove the key extensions to remove complexity
for result in results:
    result.pop('extensions', None)
    result.pop('gadm', None)
    result.pop('issues', None)
    result.pop('identifiers', None)
    result.pop('media', None)
    result.pop('facts', None)
    result.pop('relations', None)
    result.pop('recordedByIDs', None)
    result.pop('identifiedByIDs', None)

records_df = spark.createDataFrame(results)
display(records_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the Data
# MAGIC
# MAGIC NB: Ignore the extra logging, the GBIF library does some odd things to our default logger.
# MAGIC

# COMMAND ----------

from lab_common import LabContext
labContext = LabContext(spark)
labContext.setupLab("analytics_biodiversity")

records_df.write.mode("overwrite").saveAsTable("occurences")

# Databricks notebook source
# MAGIC %md
# MAGIC Content from here: https://github.com/BNHM/jupyter/blob/master/BNHM-integrateData-2020.ipynb
# MAGIC
# MAGIC Run the cell below to setup the notebook (_shift+return in cell to run_ or Press Run button in the menu)
# MAGIC Do you see a number in the left margin of the cell below? If so, click on _Kernel->Restart and Clear Output_

# COMMAND ----------

# MAGIC %pip install shapely folium

# COMMAND ----------

from lab_common import LabContext
labContext = LabContext(spark)
labContext.setupLab("analytics_biodiversity", delete=False)

# COMMAND ----------

import os
import time
import folium
from datetime import datetime
from shapely.geometry import Point, mapping
from shapely.geometry.polygon import Polygon
import matplotlib as mpl
from matplotlib.collections import PatchCollection
import matplotlib.pyplot as plot
import numpy as np
import pandas as pd
import requests
from shapely import geometry as sg, wkt
import json
import random
from IPython.core.display import display, HTML
from scripts.espm import *
import ipywidgets as widgets
%matplotlib inline
plot.style.use('seaborn')

# COMMAND ----------

# MAGIC %md
# MAGIC # Natural History Museums and Data Science
# MAGIC The goal of this notebook is to access and integrate diverse data sets to visualize correlations and discover patterns to address questions of species’ responses to environmental change. We will use programmatic tools to show how to use Berkeley resources such as the biodiversity data from biocollections and online databases, field stations, climate models, and other environmental data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets Have a look at our data
# MAGIC
# MAGIC Now that we have stored our data in our table "occurences" we can query it like any other.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT collectionCode, count(*)
# MAGIC FROM occurences
# MAGIC GROUP BY ALL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refine the Data
# MAGIC Since we have nulls in the data field "collectionCode", we will now remove it to our silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE occurences_silver
# MAGIC AS
# MAGIC SELECT * EXCEPT (collectionCode), lower(collectionCode) AS collectionCode
# MAGIC FROM occurences
# MAGIC WHERE collectionCode IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT collectionCode, count(*)
# MAGIC FROM occurences_silver
# MAGIC GROUP BY ALL;

# COMMAND ----------

# MAGIC %md
# MAGIC Since each column (or row) above can be thought of as a `list`, that means we can use list functions to interact with them! One such function is the `len` function to get the number of elements in a `list`:

# COMMAND ----------

records_df = spark.sql("SELECT * FROM occurences_silver")


print(f"Column count {len(records_df.columns)}")
print(f"Record count: {records_df.count()}")

# COMMAND ----------

records_pdf = records_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC How many columns are there? (hint: there are 118) How many records/rows? (hint: it's the other value)! That's a lot of information. What variables do we have in the columns?

# COMMAND ----------

records_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC We can use two methods from `pandas` to do a lot more. The `value_counts()` method will tabulate the frequency of the row value in a column, and the `plot.barh()` will plot us a horizontal bar chart:

# COMMAND ----------

type(records_pdf)

# COMMAND ----------



records_pdf['country'].value_counts()

# COMMAND ----------

records_pdf['country'].value_counts().plot.barh()

# COMMAND ----------

records_pdf['county'].value_counts().plot.barh()

# COMMAND ----------

records_pdf['basisOfRecord'].value_counts().plot.barh()

# COMMAND ----------

records_pdf['institutionCode'].value_counts().plot.barh()

# COMMAND ----------

# MAGIC %md
# MAGIC The `groupby()` method allows us to count based one column based on another, and then color the bar chart differently depending on a variable of our choice:

# COMMAND ----------

records_pdf.groupby(["collectionCode", "basisOfRecord"])['basisOfRecord'].count()

# COMMAND ----------

records_pdf.groupby(["collectionCode", "basisOfRecord"])['basisOfRecord'].count().unstack().plot.barh(stacked=True)

# COMMAND ----------

# MAGIC %md
# MAGIC And we can use `plot.hist()` to make a histogram:

# COMMAND ----------

records_pdf['elevation'].plot.hist()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### Mapping
# MAGIC
# MAGIC Now we can map our latitude and longitude points. We'll want to color code by `collectionCode` so we can see which collection made the observation or has the specimen. We'll use a function that does this automatically, but it will randomly generate a color, so if you get a lot of a similar color maybe run the cell a couple times!

# COMMAND ----------

color_dict, html_key = assign_colors(records_pdf, "collectionCode")
display(HTML(html_key))

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can map the points!

# COMMAND ----------

mapa = folium.Map(location=[37.359276, -122.179626], zoom_start=5) # Folium is a useful library for generating
                                                                   # Google maps-like map visualizations.
for r in records_pdf.iterrows():
    lat = r[1]['decimalLatitude']
    long = r[1]['decimalLongitude']
    folium.CircleMarker((lat, long), color=color_dict[r[1]['collectionCode']], popup=r[1]['basisOfRecord']).add_to(mapa)
mapa

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC To get the boundries for all the reserves, we will need to send a request to get GeoJSON, which is a format for encoding a variety of geographic data structures. With this code, we can request GeoJSON for all reserves and plot ocurrences of the species. First we'll assign the API URL that has the data to a new variable `url`:

# COMMAND ----------

url = 'https://ecoengine.berkeley.edu/api/layers/reserves/features/'

# COMMAND ----------

# MAGIC %md
# MAGIC Now we make the requests just like we did earlier through the GBIF:

# COMMAND ----------

reserves = requests.get(url, params={'page_size': 30}).json()
reserves

# COMMAND ----------

# MAGIC %md
# MAGIC If you look closely, this is just bounding boxes for the latitude and longitude of the reserves.
# MAGIC
# MAGIC There are some reserves that the EcoEngine didn't catch, we'll add the information for "Blodgett", "Hopland", and "Sagehen":

# COMMAND ----------

station_urls = {
    'Blodgett': 'https://raw.githubusercontent.com/BNHM/spatial-layers/master/wkt/BlodgettForestResearchStation.wkt',
    'Hopland': 'https://raw.githubusercontent.com/BNHM/spatial-layers/master/wkt/HoplandResearchAndExtensionCenter.wkt',
    'Sagehen': 'https://raw.githubusercontent.com/BNHM/spatial-layers/master/wkt/SagehenCreekFieldStation.wkt'
}
reserves['features'] += [{'type': "Feature", 'properties': {"name": name}, 'geometry': mapping(wkt.loads(requests.get(url).text))}
                for name, url in station_urls.items()]

# COMMAND ----------

# MAGIC %md
# MAGIC We can see all the station names by indexing the `name` value for the reserves:

# COMMAND ----------

[r['properties']['name'] for r in reserves['features']]

# COMMAND ----------

# MAGIC %md
# MAGIC We can send this `geojson` directly to our mapping library `folium`. You'll have to zoom in, but you should see blue outlines areas, there are the reserves!:

# COMMAND ----------

import folium

mapb = folium.Map(location=[37.359276, -122.179626], zoom_start=5) 

wkt = folium.features.GeoJson(reserves)
mapb.add_child(wkt)
for r in records_pdf.iterrows():
    lat = r[1]['decimalLatitude']
    long = r[1]['decimalLongitude']
    folium.CircleMarker((lat, long), color=color_dict[r[1]['collectionCode']], popup=r[1]['basisOfRecord']).add_to(mapb)
mapb

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC We can also find out which stations have how many *Argia argrioides*. First we'll have to add a column to our `DataFrame` that makes points out of the latitude and longitude coordinates:

# COMMAND ----------

def make_point(row):
    return Point(row['decimalLongitude'], row['decimalLatitude'])

records_pdf["point"] = records_pdf.apply(lambda row: make_point (row),axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can write a little function to check whether that point is in one of the stations, and if it is, we'll add that station in a new column called `station`:

# COMMAND ----------

def in_station(reserves, row):
    
    reserve_polygons = []

    for r in reserves['features']:
        name = r['properties']['name']
        poly = sg.shape(r['geometry'])
        reserve_polygons.append({"id": name,
                                 "geometry": poly})
    
    sid = False
    for r in reserve_polygons:
        if r['geometry'].contains(row['point']):
            sid = r['id']
            sid = r['id']
    if sid:
        return sid
    else:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC Now apply this function to the `DataFrame`:

# COMMAND ----------

records_pdf["station"] = records_pdf.apply(lambda row: in_station(reserves, row),axis=1)
in_stations_pdf = records_pdf[records_pdf["station"] != False]
in_stations_pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see if this corresponds to what we observed on the map:

# COMMAND ----------

in_stations_pdf.groupby(["species", "station"])['station'].count().unstack().plot.barh(stacked=True)

# COMMAND ----------

# MAGIC %md
# MAGIC

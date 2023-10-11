# Databricks notebook source
# MAGIC %md
# MAGIC Content from here: https://github.com/BNHM/jupyter/blob/master/BNHM-integrateData-2020.ipynb
# MAGIC
# MAGIC Run the cell below to setup the notebook (_shift+return in cell to run_ or Press Run button in the menu)
# MAGIC Do you see a number in the left margin of the cell below? If so, click on _Kernel->Restart and Clear Output_

# COMMAND ----------

# MAGIC %pip install shapely folium pygbif

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
# MAGIC # ESPM / IB 105 Natural History Museums and Data Science
# MAGIC
# MAGIC The goal of this notebook is to access and integrate diverse data sets to visualize correlations and discover patterns to address questions of species’ responses to environmental change. We will use programmatic tools to show how to use Berkeley resources such as the biodiversity data from biocollections and online databases, field stations, climate models, and other environmental data.
# MAGIC
# MAGIC Before we begin analyzing and visualizing biodiversity data, this introductory notebook will help familiarize you with the basics of programming in Python.
# MAGIC
# MAGIC ## Table of Contents
# MAGIC
# MAGIC 0 - [Jupyter Notebooks](#jupyter)
# MAGIC     
# MAGIC 1 - [Python Basics](#basics)
# MAGIC
# MAGIC 2 - [GBIF API](#gbif)
# MAGIC
# MAGIC 3 - [Comparing California Oak Species](#oak)
# MAGIC
# MAGIC 4 - [Cal-Adapt API](#adapt)

# COMMAND ----------

# MAGIC %md
# MAGIC Since each column (or row) above can be thought of as a `list`, that means we can use list functions to interact with them! One such function is the `len` function to get the number of elements in a `list`:

# COMMAND ----------

records_df = spark.sql("SELECT * FROM occurences WHERE collectionCode IS NOT NULL")


print(f"Column count {len(records_df.columns)}")
print(f"Record count: {records_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC How many columns are there? (hint: there are 118) How many records/rows? (hint: it's the other value)! That's a lot of information. What variables do we have in the columns?

# COMMAND ----------

records_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC We can use two methods from `pandas` to do a lot more. The `value_counts()` method will tabulate the frequency of the row value in a column, and the `plot.barh()` will plot us a horizontal bar chart:

# COMMAND ----------

records_pdf = records_df.toPandas()
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
# MAGIC ---
# MAGIC
# MAGIC # Part 3: Comparing California Oak species:<a id='oak'></a>
# MAGIC
# MAGIC
# MAGIC | ![quercus douglassi](https://upload.wikimedia.org/wikipedia/commons/thumb/6/6d/Large_Blue_Oak.jpg/220px-Large_Blue_Oak.jpg)  | ![quercus lobata](https://upload.wikimedia.org/wikipedia/commons/thumb/8/86/Valley_Oak_Mount_Diablo.jpg/220px-Valley_Oak_Mount_Diablo.jpg) | ![quercus durata](https://upload.wikimedia.org/wikipedia/commons/thumb/b/b8/Quercusduratadurata.jpg/220px-Quercusduratadurata.jpg) | ![quercus agrifolia](https://upload.wikimedia.org/wikipedia/commons/thumb/d/d1/Quercus_agrifolia_foliage.jpg/220px-Quercus_agrifolia_foliage.jpg) |
# MAGIC |:---:|:---:|:---:|:---:|
# MAGIC | *Quercus douglassi* | *Quercus lobata* | *Quercus durata* | *Quercus agrifolia*|
# MAGIC
# MAGIC
# MAGIC Let's search for these different species of oak using our `GBIF` API and collect the observations:

# COMMAND ----------

species_records = []

import pygbif
# caching is off by default
from pygbif import occurrences


species = ["Quercus douglassi", "Quercus lobata", "Quercus durata", "Quercus agrifolia"]

for s in species:


    req = GBIFRequest()  # creating a request to the API
    params = {'scientificName': s}  # setting our parameters (the specific species we want)
    pages = req.get_pages(params)  # using those parameters to complete the request
    records = [rec for page in pages for rec in page['results'] if rec.get('decimalLatitude')]  # sift out valid records
    species_records.extend(records)
    time.sleep(3)

# COMMAND ----------

# MAGIC %md
# MAGIC We can convert this JSON to a `DataFrame` again:

# COMMAND ----------

records_df = pd.DataFrame(species_records)
records_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC How many records do we have now?

# COMMAND ----------

len(records_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see how they're distributed among our different species queries:

# COMMAND ----------

records_df['species'].value_counts().plot.barh()

# COMMAND ----------

# MAGIC %md
# MAGIC We can group this again by `collectionCode`:

# COMMAND ----------

records_df.groupby(["species", "collectionCode"])['collectionCode'].count().unstack().plot.barh(stacked=True)

# COMMAND ----------

# MAGIC %md
# MAGIC We can also map these like we did with the *Argia arioides* above:

# COMMAND ----------

color_dict, html_key = assign_colors(records_df, "species")
display(HTML(html_key))

# COMMAND ----------

mapc = folium.Map([37.359276, -122.179626], zoom_start=5)

points = folium.features.GeoJson(reserves)
mapc.add_child(points)
for r in records_df.iterrows():
    lat = r[1]['decimalLatitude']
    long = r[1]['decimalLongitude']
    folium.CircleMarker((lat, long), color=color_dict[r[1]['species']]).add_to(mapc)
mapc

# COMMAND ----------

# MAGIC %md
# MAGIC We can use the same code we wrote earlier to see which reserves have which species of oak:

# COMMAND ----------

records_df["point"] = records_df.apply(lambda row: make_point (row),axis=1)
records_df["station"] = records_df.apply(lambda row: in_station(reserves, row),axis=1)
in_stations_df = records_df[records_df["station"] != False]
in_stations_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can make a bar graph like we did before, grouping by `species` and stacking the bar based on `station`:

# COMMAND ----------

in_stations_df.groupby(["species", "station"])['station'].count().unstack().plot.barh(stacked=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ---
# MAGIC
# MAGIC # Part 4: Cal-Adapt API<a id='adapt'></a>
# MAGIC
# MAGIC Let's get back the data from *Argia agrioides* with the GBIF API:

# COMMAND ----------

req = GBIFRequest()  # creating a request to the API
params = {'scientificName': 'Argia agrioides'}  # setting our parameters (the specific species we want)
pages = req.get_pages(params)  # using those parameters to complete the request
records = [rec for page in pages for rec in page['results'] if rec.get('decimalLatitude')]  # sift out valid records
records[:5]  # print first 5 records

# COMMAND ----------

# MAGIC %md
# MAGIC We'll make a `DataFrame` again for later use:

# COMMAND ----------

records_df = pd.DataFrame(records)
records_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we will use the [Cal-Adapt](http://api.cal-adapt.org/api/) web API to work with time series raster data. It will request an entire time series for any geometry and return a Pandas `DataFrame` object for each record in all of our *Argia agrioides* records:

# COMMAND ----------

req = CalAdaptRequest()
records_g = [dict(rec, geometry=sg.Point(rec['decimalLongitude'], rec['decimalLatitude']))
             for rec in records]
ca_df = req.concat_features(records_g, 'gbifID')

# COMMAND ----------

# MAGIC %md
# MAGIC Let's look at the first five rows:

# COMMAND ----------

ca_df.head()

# COMMAND ----------

len(ca_df.columns), len(ca_df)

# COMMAND ----------

# MAGIC %md
# MAGIC This looks like the time series data we want for each record (the unique ID numbers as the columns). Each record has the projected temperature in Fahrenheit for 273 years (every row!). We can plot predictions for few random records:

# COMMAND ----------

# Make a line plot using the first 9 columns of dataframe
ca_df.iloc[:,:9].plot()

# Use matplotlib to title your plot.
plot.title('Argia agrioides - %s' % req.slug)

# Use matplotlib to add labels to the x and y axes of your plot.
plot.xlabel('Year', fontsize=18)
plot.ylabel('Degrees (Fahrenheit)', fontsize=16)


# COMMAND ----------

# MAGIC %md
# MAGIC It looks like temperature is increasing across the board wherever these observations are occuring. We can calculate the average temperature for each year across observations in California:

# COMMAND ----------

tmax_means = ca_df.mean(axis=1)
tmax_means

# COMMAND ----------

# MAGIC %md
# MAGIC What's happening to the average temperature that *Argia agrioides* is going to experience in the coming years across California?

# COMMAND ----------

tmax_means.plot()

# COMMAND ----------

# MAGIC %md
# MAGIC Is there a temperature at which the *Argia agrioides* cannot survive? Is there one in which they particularly thrive?

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC What if we look specifically at the field stations and reserves? We can grab our same code that checked whether a record was within a station, and then map those `gbifID`s back to this temperature dataset:

# COMMAND ----------

records_df["point"] = records_df.apply(lambda row: make_point (row),axis=1)
records_df["station"] = records_df.apply(lambda row: in_station(reserves, row),axis=1)
in_stations_df = records_df[records_df["station"] != False]
in_stations_df[['gbifID', 'station']].head()

# COMMAND ----------

# MAGIC %md
# MAGIC Recall the column headers of our `ca_df` are the `gbifID`:

# COMMAND ----------

ca_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we subset the temperature dataset for only the observations that occurr within the bounds of a reserve or field station:

# COMMAND ----------

station_obs = [str(id) for id in list(in_stations_df['gbifID'])]
ca_df[station_obs]

# COMMAND ----------

# MAGIC %md
# MAGIC Let's graph these observations from Santa Cruz Island against the average temperature across California where this species was observed:

# COMMAND ----------

plot.plot(tmax_means)
plot.plot(ca_df[station_obs])

# Use matplotlib to title your plot.
plot.title('Argia agrioides and temperatures in Santa Cruz Island')

# Use matplotlib to add labels to the x and y axes of your plot.
plot.xlabel('Year', fontsize=18)
plot.ylabel('Degrees (Fahrenheit)', fontsize=16)
plot.legend(["CA Average", "Santa Cruz Island"])

# COMMAND ----------

# MAGIC %md
# MAGIC What does this tell you about Santa Cruz Island? As time goes on and the temperature increases, might Santa Cruz Island serve as a refuge for *Argia agrioides*?

# COMMAND ----------

# MAGIC %md
# MAGIC

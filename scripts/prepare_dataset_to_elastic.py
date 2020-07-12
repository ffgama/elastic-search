from geopandas import read_file, GeoDataFrame
from shapely.geometry import Point, Polygon
from datetime import datetime
from elasticsearch import Elasticsearch
from pandas import read_csv, merge,DataFrame, concat
from numpy import random

# load shapefiles
usa_states = read_file("data/cb_2018_us_state_500k.shp")
usa_states.to_file("data/usa_states.geojson",driver="GeoJSON")

# usa_states.head()
# usa_states.info()

# usa_states["NAME"].duplicated()
# usa_states["GEOID"].value_counts()

usa_states.columns = [col.lower() for col in usa_states.columns] 

# connect to elastic
es = Elasticsearch()
# list indexes
es.indices.get_alias()

INDEX_NAME="places_visited"
if es.indices.exists(index=INDEX_NAME): es.indices.delete(index=INDEX_NAME)

# source: https://public.opendatasoft.com/explore/dataset/1000-largest-us-cities-by-population-with-geographic-coordinates/table/?sort=-rank
dataset_state = read_csv("data/city_state_pop.csv", sep=";")
#  dict(dataset_state["Coordinates"]).values()

dataset_state["Coordinates"] = dataset_state["Coordinates"].apply(lambda m: m.split(","))

for i, row in enumerate(dataset_state["Coordinates"]):
    dataset_state["Coordinates"][i] = dict(lat = row[0], long=row[1])

dataset_state_sub = dataset_state.iloc[50:200:,]

# select 
dataset_state_sub = dataset_state_sub.sample(n=100, replace=True, random_state=42).reset_index()

dataset_state_sub["person_id"] = random.randint(low=1, high=8, size=dataset_state_sub.shape[0])

dataset_state_sub.columns = [col.lower() for col in dataset_state_sub.columns]

dataset_state_sub.head()

# combine datasets
dataset_visited_state = dataset_state_sub

columns_choosen = ["city","state","population","coordinates","person_id"]
dataset_visited_state = dataset_visited_state[columns_choosen] 

# create new columns
# number of visitations 
dataset_visited_state["tot_visited_state"] = dataset_visited_state.groupby("state")["person_id"].transform("count")
dataset_visited_state["tot_visited_city"] = dataset_visited_state.groupby("city")["person_id"].transform("count")

# (high recurrence): the same people had  come back the place (high recurrence)
dataset_visited_state["recurrence_state"] = dataset_visited_state.groupby(["state"])["person_id"].transform("nunique")
dataset_visited_state["recurrence_city"] = dataset_visited_state.groupby(["city"])["person_id"].transform("nunique")
            
# insert a data (document) into a INDEX         
for row in range(0, len(dataset_visited_state)):    

    document = dict(dataset_visited_state.apply(lambda x: x[row]))
    results = es.index(INDEX_NAME, 
                       id=row, 
                       body=document)


# show all documents that has been inserted
all_docs = [es.get(index=INDEX_NAME, id=doc) for doc in range(0, len(dataset_visited_state))]
all_docs

# only data
[all_docs[item]["_source"] for item in range(0, len(all_docs) )]

# became /cities_visited endpoint available
# For example
# http://localhost:9200/cities_visited/_search?q=City:Hollywood

df = DataFrame(all_docs)

docs = [df.loc[:,"_source"][d] for d in range(0, len(df))]
dataset_places = DataFrame.from_dict(docs)



from geopandas import read_file, GeoDataFrame
from shapely.geometry import Point, Polygon
from datetime import datetime
from elasticsearch import Elasticsearch
from pandas import read_csv, DataFrame
from numpy import random
from pandas import set_option
import json
from utils.to_geojson import *

set_option("display.max_rows", 500)

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
es.indices.get_alias()

INDEX_NAME="places_visited"
if es.indices.exists(index=INDEX_NAME): es.indices.delete(index=INDEX_NAME)

# source: https://public.opendatasoft.com/explore/dataset/1000-largest-us-cities-by-population-with-geographic-coordinates/table/?sort=-rank
dataset_state = read_csv("data/city_state_pop.csv", sep=";")

dataset_state[['latitude', 'longitude']] = \
    dataset_state['Coordinates'].str.split(',', n=1, expand=True)

dataset_state_sub = dataset_state.iloc[50:200:,]

# random select 
dataset_visited_state = dataset_state_sub.sample(n=100, replace=True, random_state=42).reset_index()

dataset_visited_state["person_id"] = random.randint(low=1, high=8, size=dataset_visited_state.shape[0])

dataset_visited_state.columns = [col.lower() for col in dataset_visited_state.columns]

columns_choosen = ["city","state","population","coordinates",
                   "person_id","latitude","longitude"]
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
# http://localhost:9200/places_visited/_search?q=state:Texas

# es.search(index=INDEX_NAME, body={"from":0, 
# "size":0, 
# "query":{"match": {"state":"Texas"} } })

# case insensitve
es.search(index=INDEX_NAME, 
          body={"from":0, "size":0, "query":{"match": {"state":"TEXAS"} } })

# change parameter "size": iterate over sentence whose state is "Texas"
es.search(index=INDEX_NAME, 
          body={ "from":0, "size":5, "query": {"match":{"state":"Texas"} } })

# change parameter "from": define the offset from of query". In this case, we return only two last documents 
es.search(index=INDEX_NAME, 
          body={ "from":3, "size":5, "query": {"match":{"state":"Texas"} } })

# match phrase prefix: get documents that contains words "park"
# Cities: Lincoln Park and Oak Park
es.search(index=INDEX_NAME, 
          body={ "from":0, "size":3, "query": {"match_phrase_prefix":{"city":"Park"} }})

# Combine multiple queries
es.search(index=INDEX_NAME, 
          body={ "from":0, "size":4, "query": {"bool": 
                                        {
                                        "must_not": {"match": {"city":"Texarkana"} },
                                        "should": {"match": {"state":"Texas"} }
                                        } } } )

# Regex
#  all states that begin with "i" letter
# syntax regexp: https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html
es.search(index=INDEX_NAME, 
          body={ "from":0, "size":17, "query": {"regexp": 
                                    {"state":"i.*"}
                                    } })

df = DataFrame(all_docs)

docs = [df.loc[:,"_source"][d] for d in range(0, len(df))]
dataset_places = DataFrame.from_dict(docs)

dataset_places['latitude'] = dataset_places['latitude'].astype(float)
dataset_places['longitude'] = dataset_places['latitude'].astype(float)

geojson = df_to_geojson(dataset_places, dataset_places.columns.to_list())

with open('data/geo_places_visited.json', 'w') as json_file:
    json.dump(geojson, json_file)
    
    
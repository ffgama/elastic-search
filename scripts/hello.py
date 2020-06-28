from datetime import datetime
from elasticsearch import Elasticsearch
from pandas import read_csv
from pandas import DataFrame
from numpy import random

es = Elasticsearch()

# create, check and delete index 
es.indices.create(index='cities_month')
es.indices.exists(index='cities_month')
es.indices.delete(index='cities_month')

# source: https://public.opendatasoft.com/explore/dataset/1000-largest-us-cities-by-population-with-geographic-coordinates/table/?sort=-rank
dataset_cities = read_csv('data/cities.csv', sep=';')

dataset_cities = dataset_cities.iloc[50:90:,]

# select 
select_cities = dataset_cities.sample(n=20, replace=True, random_state=42).reset_index()

select_cities['person_id'] = random.randint(low=1, high=8, size=select_cities.shape[0])


INDEX_NAME='cities_visited'
# each row is a document
for row in range(0, len(select_cities)):
    document = dict( select_cities.apply(lambda x: x[row]) )
    response[row] = es.index(INDEX_NAME, id=row, body=document)
       
# read
es.get(index=INDEX_NAME, id=1)
es.get(index=INDEX_NAME, id=19)

# search
# es.search(index=INDEX_NAME, body={'City':{'match_all':() }})
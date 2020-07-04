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

if es.indices.exists(index=INDEX_NAME): es.indices.delete(index=INDEX_NAME)
            
# insert a data (document) into a INDEX         
for row in range(0, len(select_cities)):    

    document = dict(select_cities.apply(lambda x: x[row]))
    results = es.index(INDEX_NAME, 
                       id=row, 
                       doc_type='cities', 
                       body=document)

results

# show all documents that has been inserted
all_docs = [es.get(index=INDEX_NAME, doc_type='cities', id=doc) for doc in range(0, len(select_cities))]
all_docs

# print only data
[all_docs[item]['_source'] for item in range(0, len(all_docs) )]

# became /cities_visited available
# For example
# http://localhost:9200/cities_visited/_search?q=City:Hollywood
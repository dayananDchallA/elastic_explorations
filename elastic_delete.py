from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
print(es.info().body)

# Delete Documents From the Index
es.delete(index="movies", id="2500")

# Delete an Index
es.indices.delete(index='movies')
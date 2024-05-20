from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
print(es.info().body)

resp = es.search(
    index="movies",
    query={
            "bool": {
                "must": {
                    "match_phrase": {
                        "cast": "jack nicholson",
                    }
                },
                "filter": {"bool": {"must_not": {"match_phrase": {"director": "roman polanski"}}}},
            },
        },            
)

print(resp.body)
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd

es = Elasticsearch("http://localhost:9200")
print(es.info().body)

input_data = "C:/Users/DayanandChalla/Downloads/wiki_movie_plots_deduped.csv"

df = (
    pd.read_csv(input_data)
    .dropna()
    .sample(5000, random_state=42)
    .reset_index()
)

# Create an Index
# An index is a collection of documents that Elasticsearch stores and represents through a data structure called an inverted index. 
# This data structure identifies the documents in which each unique word appears.

mappings = {
        "properties": {
            "title": {"type": "text", "analyzer": "english"},
            "ethnicity": {"type": "text", "analyzer": "standard"},
            "director": {"type": "text", "analyzer": "standard"},
            "cast": {"type": "text", "analyzer": "standard"},
            "genre": {"type": "text", "analyzer": "standard"},
            "plot": {"type": "text", "analyzer": "english"},
            "year": {"type": "integer"},
            "wiki_page": {"type": "keyword"}
    }
}

es.indices.create(index="movies", mappings=mappings)

# Add Data to Your Index
# You can use es.index() or bulk() to add data to an index. 
# es.index() adds one item at a time while bulk() lets you add multiple items at the same time.


## Using es.index()
for i, row in df.iterrows():
    doc = {
        "title": row["Title"],
        "ethnicity": row["Origin/Ethnicity"],
        "director": row["Director"],
        "cast": row["Cast"],
        "genre": row["Genre"],
        "plot": row["Plot"],
        "year": row["Release Year"],
        "wiki_page": row["Wiki Page"]
    }
            
    es.index(index="movies", id=i, document=doc)

# counting the number of items in the index
es.indices.refresh(index="movies")
print(es.cat.count(index="movies", format="json"))

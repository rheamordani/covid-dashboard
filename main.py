import urllib.request
import json
from elasticsearch import Elasticsearch
from datetime import datetime

with urllib.request.urlopen('https://api.covidactnow.org/v2/states.json?apiKey=74e6cc3af61e4a529530f672c3303c55') as response:
    data = json.load(response)

# _source data for the Elasticsearch document
doc_source = {
    "string field": "covid_index",
    "integer field": 42,
    "boolean field": False,
    "timestamp": str(datetime.now())
}

# Elasticsearch document structure as a Python dict
doc = {
    "_index": "covid_index",
    "_id": 12345,
    "doc_type": "_doc",
    "_source": data,
}

# use the 'indent' parameter with json.dumps() for more readable JSON
json_str = json.dumps(doc, indent=4)
print ("\nDOC json_str:", json_str)

# build the Elasticsearch document from a dict
build_doc = {}
build_doc["_index"] = "covid_index"
build_doc["_id"] = 12345
build_doc["doc_type"] = "_doc" # doc type deprecated
build_doc["_source"] = data

# print the mapping
print ("\nbuild_doc items:", build_doc.items())
json_str = json.dumps(build_doc)
print ("json_str without indent:", json_str)
print ("\nJSON objects are the same:", build_doc == doc)

client = Elasticsearch(
    ['localhost'],
    http_auth=('elastic', 'changeme'),
    port=9200
)

try:
    # create JSON string of doc _source data
    json_source = json.dumps(build_doc["_source"])

    # get the dict object's _id
    json_id = build_doc["_id"]

    # make an API call to the Elasticsearch cluster
    response = client.index(
        index = 'covid_index',
        doc_type = '_doc',
        id = json_id,
        body = json_source
    )

    # print a pretty response to the index() method call response
    print ("\nclient.index response:", json.dumps(response, indent=4))

except Exception as error:
    print ("Error type:", type(error))
    print ("client.index() ERROR:", error)

# build a JSON Python dict to query all documents in an Elasticsearch index
all_docs = {}
all_docs["size"] = 9999
all_docs["query"] = {"match_all" : {}}
print ("\nall_docs:", all_docs)
print ("all_docs TYPE:", type(all_docs))

# validate the JSON format using the loads() method
try:
    # pass the JSON string in an API call to the Elasticsearch cluster
    response = client.search(
        index = "covid_index",
        body = all_docs
    )

    # print all of the documents in the Elasticsearch index
    print ("all_docs query response:", response)

    # use the dumps() method's 'indent' parameter to print a pretty response
    print ("all_docs pretty:", json.dumps(response, indent=4))

except Exception as error:
    print ("Error type:", type(error))
    print ("client.search() ValueError for JSON object:", error)
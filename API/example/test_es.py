from elasticsearch import Elasticsearch
from elasticsearch import helpers as EShelpers
import datetime
import pandas as pd
import time

host = "https://9db53c7bb4f5be2d856033a9aeb6e5a5.us-central1.gcp.cloud.es.io"
username = "elastic"
password = "W0y1miwmrSMZKkSIARzbxJgb"
cloud_ID = "TopicRecommenderstag:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDlkYjUzYzdiYjRmNWJlMmQ4NTYwMzNhOWFlYjZlNWE1JGUwYjEwZTQ3MTY5ZjU4MTNmMGZhYzViNjlkMDE1MjY1"

es = Elasticsearch([host], port=9243, http_auth=(username, password))
# print(es.info())
"""
es = Elasticsearch([host],
                    port=9243,
                    http_auth="elastic:W0y1miwmrSMZKkSIARzbxJgb",
                    use_ssl=False,
                    verify_certs=False,
                    ca_certs=None
                   )

es.index(index='posts', doc_type='blog', id=2, body={
    'author': 'Benjamin Pollack',
    'blog': 'bitquabit',
    'title': 'Having Fun: Python and Elasticsearch',
    'topics': ['elasticsearch', 'python', 'parseltongue'],
    'awesomeness': 0.7
})

print es.get(index='posts', doc_type='blog', id=2)
"""

"""
# test bulk insert
# ~ use this if wanna insert large data ~
j = 0
actions = []
while (j <= 10):
    action = {
        "_index": "tickets-index",
        "_type": "tickets",
        "_id": j,
        "_source": {
            "any": "data" + str(j),
            "timestamp": datetime.datetime.now()
            }
        }
    actions.append(action)
    j += 1

# EShelpers.bulk(es, actions)
# ~ get data by id ~
# print es.get(index='tickets-index', doc_type='tickets', id=2)
"""

# ~ get all data ~
# doc = {'size': 5,
#       'query': {'match_all': {}}
#      }
start_total_time = time.time()
# ~ get some data ~  
doc = {
       'query': {
           'match': {
                    'user_id':'037b1545-b5c9-46cc-a5d7-d0e50d17868b'
                    }
                 },
        '_source' : ["topic_id", "is_general", "rank", "p0_posterior"]
       }
params = {"size":50}  #, "search_type":"query_then_fetch"}
res = es.search(index='transform_index', doc_type='transform_type', body=doc, params=params)
hits = res['hits']['hits']
# data = []
# for hit in hits:
#    data.append(hit["_source"])
data = [hit["_source"] for hit in hits]
df = pd.DataFrame(data, columns=["topic_id", "is_general", "rank", "p0_posterior"])
df = df.sort_values(['is_general', 'rank'], ascending=[False, True])
print df
end_total_time = time.time() - start_total_time
print 'Time taken to transform output: %.7f' % end_total_time

# ~ delete index ~
# es.indices.delete(index='transform_index')
# es.indices.create(index=index, body=body)

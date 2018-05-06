from elasticsearch import Elasticsearch
from elasticsearch import helpers as EShelpers
import datetime
import pandas as pd
import time

pd.set_option('display.width', 1000)

host = "https://9db53c7bb4f5be2d856033a9aeb6e5a5.us-central1.gcp.cloud.es.io"
username = "elastic"
password = "W0y1miwmrSMZKkSIARzbxJgb"

es = Elasticsearch([host], port=9243, 
                    http_auth=(username, password))
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
# https://stackoverflow.com/questions/18695310/search-on-multiple-fields-with-elastic-search
# col_source = ["uid_topid", "pt_posterior_x_Nt", "smoothed_pt_posterior", "p0_cat_ci", "sigma_Nt"]
col_source = ["user_id", "topic_id", "topic_is_general", "interest_score"]  # , "interest_score_created_at"]
"""
doc = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "user_id": "162904dc73329-0a5f32b3be10d6-134d5652-38400-162904dc73579"
                        }
                    },
                    {
                        "term": {
                            "topic_id": "1019283820"
                        }
                    }
                ]
            }
        },
        '_source' : col_source
       }

doc = {
        "query" : {
            "bool":{
                "must":[
                        {
                            "terms" : {
                                "user_id": ["162904dc73329-0a5f32b3be10d6-134d5652-38400-162904dc73579", "162909704bffc-0ff782d5aa87e1-4e2a047a-3f480-162909704c10"]
                            }
                        },
                        {
                            "terms": {
                                "topic_id": ["1019283820","27431110790313914"]
                            }
                        }    
                ]
            }
        }    
      }

# select all
doc = {
        "query" : {
            "match_all" : {}
        },
        '_source' : col_source
      }

# select in
uidtopid = ['162747f79e55a-02e2ea15823949-356f4b4e-38400-162747f79e723_188400148', '1612d19bc2f113-0819fdf6c9972-3130446b-38400-1612d19bc309e_27435843',
            '162747f79e55a-02e2ea15823949-356f4b4e-38400-162747f79e723_27431099', '162747f79e55a-02e2ea15823949-356f4b4e-38400-162747f79e723_36060446',
            '162747f79e55a-02e2ea15823949-356f4b4e-38400-162747f79e723_48870594', '162747f79e55a-02e2ea15823949-356f4b4e-38400-162747f79e723_22596701'
            ]
doc = {
        "query" : {
                "bool" : {
                    "must" : {
                        "terms" : {
                            "uid_topid" : uidtopid
                        }
                    }
                }
            }
        }

doc = {
        "query": {
            "match": {
                "user_id": "1612d19bc2f113-0819fdf6c9972-3130446b-38400-1612d19bc309e"
            }
        },
        '_source' : col_source
       }

params = {"size":  30}  #, "search_type":"query_then_fetch"}
res = es.search(index='transform_index', doc_type='transform_type', body=doc, params=params)
# print res
hits = res['hits']['hits']
# data = []
# for hit in hits:
#    data.append(hit["_source"])
# print "hits:", hits
data = [hit["_source"] for hit in hits]
# print data
df = pd.DataFrame(data, columns=col_source)
# df = df.sort_values(['user_id', 'topic_is_general'], ascending=[True, False])
df['rank'] = df.groupby(['user_id', 'topic_is_general'])['interest_score'].rank(ascending=False)
df = df.sort_values(['topic_is_general', 'rank'], ascending=[False, True])
print df
end_total_time = time.time() - start_total_time
print 'Time taken to transform output: %.7f' % end_total_time
"""

# ~ delete index ~
es.indices.delete(index='transform_index')
# es.indices.create(index=index, body=body)

"""
# ~ create index
INDEX_NAME = "fitted_hist_index"
request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings" : {
        "fitted_hist_type" : {
            "properties" : {
                "uid_topid": { "type": "keyword" },
                "pt_posterior_x_Nt": { "type": "double" },
                "smoothed_pt_posterior": { "type": "double" },
                "p0_cat_ci": { "type": "double" },
                "sigma_Nt": { "type": "double" }
            }
        }
    }
}
"""
INDEX_NAME = "transform_index"
request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings" : {
        "transform_type" : {
            "properties" : {
                "user_id": { "type": "keyword" },
                "topic_id" : { "type": "keyword" },
                "topic_is_general": { "type": "boolean"},
                "interest_score": { "type": "double" },
                "interest_score_created_at": { "type": "date" }
            }
        }
    }
}

print("creating '%s' index..." % (INDEX_NAME))
res = es.indices.create(index = INDEX_NAME, body = request_body)


# ~ update data ~
"""print "\nTry update data"
q = {
     "script": {
        "inline": "ctx._source.p0_posterior=25",
        "inline": "ctx._source.rank=12",
        "lang": "painless"
     },
     "query": {
        "match": {
            "user_id": "037b1545-b5c9-46cc-a5d7-d0e50d17868b",
        },
        "match": {
            "topic_id": "22553321"
          }
     }
}
es.update_by_query(body=q, doc_type='transform_type', index='transform_index')
"""

# ~ upsert ~
#https://discuss.elastic.co/t/python-elasticsearch-bulk-conditional-upsert/71573
"""es.update(index='transform_index', doc_type='transform_type',
          body={
              'doc':{
                     'p0_posterior':6,
                     'topic_id':'22553321',
                     'user_id':'037b1545-b5c9-46cc-a5d7-d0e50d17868b',
                     'rank':14
                     },
              'doc_as_upsert':True})

from elasticsearch import helpers
actions = []
document = {
    'topic_id': '1153773792',
    'p0_posterior': 14,
    'user_id':'037b1545-b5c9-46cc-a5d7-d0e50d17868b',
    'rank':14
}
action = {
    "_op_type": "update",
    "_index": "transform_index",
    "_type": "transform_type",
    "_id": 12345678987654321,
    'doc':{
            'p0_posterior':6,
            'topic_id':'22553321',
            'user_id':'037b1545-b5c9-46cc-a5d7-d0e50d17868b',
            'rank':9,
            'is_general': True
            },
    'doc_as_upsert':True
}
actions.append(action)
helpers.bulk(es, actions)
"""

# ~ delete data ~
#https://discuss.elastic.co/t/find-and-delete-duplicate-documents/26575/4
"""q = {

     "query": {
        "match": {
            "user_id": "037b1545-b5c9-46cc-a5d7-d0e50d17868b",
        },
        "match": {
            "topic_id": "22553321"
          }
     }
}
es.delete_by_query(index='transform_index',doc_type='transform_type', body=q)"""

# ~ test duplicate ~
"""
import json
import requests
from requests.auth import HTTPBasicAuth
curl_data = {
  "size": 0,
  "aggs": {
    "duplicateCount": {
      "terms": {
      "field": "name",
        "min_doc_count": 2
      },
      "aggs": {
        "duplicateDocuments": {
          "top_hits": {}
        }
      }
    }
  }
}
headers = {'Content-type': 'application/json'}
print host+"/"+"transform_index"+"/_search?pretty=true"
r = requests.get(host+"/"+"transform_index"+"/_search?pretty=true", auth=HTTPBasicAuth(username, password), data=json.dumps(curl_data), headers=headers)
print r.json()
print r.content
"""
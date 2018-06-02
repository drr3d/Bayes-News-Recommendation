from google.cloud import datastore
import pandas as pd
import time
import json

pd.set_option('display.width', 1000)

def basic_query(client, kind):
    # [START basic_query]
    query = client.query(kind=kind)
    # query.add_filter('user_id', '=', '037b1545-b5c9-46cc-a5d7-d0e50d17868b')
    # query.add_filter('priority', '>=', 4)
    # query.order = ['-rank']
    # [END basic_query]
    # print type(query.fetch())
    #for l in query.fetch():
    #    print l
    #return list(query.fetch())  # query.fetch(limit = 10)
    return query.fetch(limit = 10)

print "\nBegin querying datastore..."

start_total_time = time.time()
kind = 'topic_recomendation_history'
project_id = 'kumparan-data'
client = datastore.Client(project_id)
iterator = basic_query(client, kind)

end_total_time = time.time() - start_total_time
print 'Time taken to querying datastore: %.7f' % end_total_time

print "\nBegin transform output..."
start_total_time = time.time()
# user_data = []

def _q_iterator(iterators):
    for d in iterators:
        # user_data.append([d["user_id"], d["topic_id"], d["smoothed_pt_posterior"], d["sigma_Nt"], d["p0_cat_ci"]])
        yield [d["user_id"], d["topic_id"], d["smoothed_pt_posterior"], d["sigma_Nt"], d["p0_cat_ci"]]

# print _q_iterator(iterator)
# print list(_q_iterator(iterator))
A = pd.DataFrame(_q_iterator(iterator), columns=["user_id", "topic_id", "smoothed_pt_posterior", "sigma_Nt", "p0_cat_ci"])
# print A.user_id.unique()
# print A.topic_id.unique()
A["ds_id"] = A["user_id"].map(str) + "_" + A["topic_id"].map(str)
print  list(A["user_id"].head(2).map(str) + "_" + A["topic_id"].head(2).map(str))
# A = A.sort_values(['is_general', 'rank'], ascending=[False, True])
# print A.to_json(orient='values')
# print dict(zip(A.topic_id, A.is_general))
# print "\n", A.to_dict('records')
# print "\n", A.to_dict('index')
# print json.dumps(user_data)
end_total_time = time.time() - start_total_time
print 'Time taken to transform output: %.7f' % end_total_time

# Key = datastore.Key
keys = list()
keys.append(client.key('topic_recomendation_history', '037b1545-b5c9-46cc-a5d7-d0e50d17868b_103112822')) # // this user exists
# keys.append(client.key('topic_recomendation_history', '037b1545-b5c9-46cc-a5d7-d0e50d17868b_22553543')) # // this user DOES NOT exist
# print list(A["ds_id"])
# print list(A[["ds_id"]])
B = client.get_multi(keys=keys)
print B
print pd.DataFrame(B)
for p in B:
    print list(p.items())
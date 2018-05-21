#!/usr/bin/python
# -*- coding: utf8 -*-

"""
    File ini diperuntukkan untuk saving model hasil train.
    untuk dipergunakan juga pada.
    silahkan buat file berdeda jika ada metoda lain
"""

import os
import logging
import time
import multiprocessing as mp
from multiprocessing import Process
import pandas as pd
import six

from google.cloud import datastore
from google.cloud.datastore.entity import Entity

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            logger.info( '%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result
    return timed


def _getBig(keylist):
    project_id = 'kumparan-data'
    client = datastore.Client(project_id)
    return client.get_multi(keys=keylist)


def loadDSHistory(key_list, kinds='topic_recomendation_history'):
    kind = kinds
    project_id = 'kumparan-data'
    client = datastore.Client(project_id)

    keys = list()
    logger.info("Appending key of list history...")
    for key in key_list:
        keys.append(client.key(kinds, key))

    def partition(lst, n):
        division = len(lst) / float(n)
        return [ lst[int(round(division * i)): int(round(division * (i + 1)))] for i in xrange(n) ]
    
    logger.info("Split key of list history...")

    divider = 150
    index_list = partition(keys, divider)

    logger.info("len of index_list: %d", len(index_list))
    logger.info("len of index_list per-unit: %d", len(index_list[:1]))
    logger.info("Begin get_multi of list history...")

    cpu = 3
    pool = mp.Pool(processes=cpu) 
    multprocessA = [pool.apply_async(_getBig, args=(keylist, )) for keylist in index_list]

    logger.info("Flushing get_multi data...")
    output_multprocessA = [p.get() for p in multprocessA]

    logger.info("Cleaning up multiprocess...")

    pool.close()
    pool.terminate()
    return output_multprocessA


def loadESHistory(uid_topid, es_client, esindex_name='topicrecommendation_transform_index',
                  estype_name='topicrecommendation_transform_type'):
    logger.info("len of uid_topid in loadESHistory: %d", len(uid_topid))

    col_source = ["uid_topid", "pt_posterior_x_Nt", "smoothed_pt_posterior", "p0_cat_ci", "sigma_Nt"]
    doc = {
        "query" : {
                "bool" : {
                    "must" : {
                        "terms" : {
                            "uid_topid" : uid_topid
                        }
                    }
                }
            },
        '_source' : col_source
       }
    params = {"size":  len(uid_topid)}
    res = es_client.search(index=esindex_name, doc_type=estype_name, body=doc, params=params)

    hits = res['hits']['hits']
    data = [hit["_source"] for hit in hits]
    logger.info("len of data in loadESHistory: %d", len(data))

    if len(data) > 0:
        return pd.DataFrame(data, columns=col_source)
    else:
        return None


def dict_to_datastore_taskMp(client, _kind, _identifier, _insertedData):
    incomplete_key = client.key(_kind, _identifier)
    entity = datastore.Entity(key=incomplete_key)
    entity.update(_insertedData)
    return entity

def outputMp(df, _kind):
    start_time = time.time()
    client = datastore.Client('kumparan-data')

    batch_size = 1000
    for cur_index in xrange(0, len(df), batch_size):
        tasks = [dict_to_datastore_taskMp(client, _kind, "{}_{}".format(x['user_id'],
                                          x['topic_id']), x) for x in df[cur_index: cur_index + batch_size].T.to_dict().values()]
        client.put_multi(tasks)

    return

def saveDataStorePutMulti(df, kinds='topic_recomendation'):
    """
        Saving to google datastore using 'put_multi'.

        parameter:
        ----------

        df : pandas dataframe, required=True
            is model_tranform a.k.a final model
    """
    start_total_time = time.time()
    logger.info("Saving by Multiprocess with 'put_multi' ..")

    def _split_data(divider, df):
        n = df.shape[0] / divider

        index_list = []

        for index in xrange(0, len(df), n):
            index_list.append(df[index: index + n])

        return index_list

    kind = kinds
    divider = 10

    index_list = _split_data(divider, df)

    cpu = 4
    pool = mp.Pool(processes=cpu)
    for df_temp in index_list:
        pool.apply_async(outputMp, args=(df_temp, kind, ))

    logger.info("Cleaning up multiprocess...")
    pool.close()
    pool.terminate()

    end_total_time = time.time() - start_total_time
    logger.info('end of all insert batch entity to datastore with exec time : %.5f and total entity : %d' % (end_total_time, len(df)))


def saveElasticS(df, esp_client, esindex_name='topicrecommendation_transform_index',
                 estype_name='topicrecommendation_transform_type', save_type="history"):
    start_total_time = time.time()

    INDEX = esindex_name
    TYPE = estype_name

    if not isinstance(save_type, six.string_types):
        logger.info("save_type datatype must be string!!")
        return
    
    if save_type.strip().lower() == 'history':
        df['indexId'] = df['uid_topid'].map(str)
    elif save_type.strip().lower() == 'current':
        df['indexId'] = df["user_id"].map(str) + "_" + df["topic_id"].map(str)
    elif save_type.strip().lower() == 'fallback':
        df['indexId'] = df['topic_id'].map(str)

    logger.info("Bulk insert into ElasticSearch, chunksize=%d, time_out: %d" % (20000, 60))
    logger.info(esp_client.es_write(df, INDEX, TYPE, chunksize=20000, rto=60))

    end_total_time = time.time() - start_total_time
    logger.info('Finish bulk insert to Eslastic Search, time taken: %.5f with total entity: %d' % (end_total_time, len(df)))
    return

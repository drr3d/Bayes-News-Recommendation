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
from multiprocessing.pool import ThreadPool
from contextlib import contextmanager

from google.cloud import datastore
from google.cloud.datastore.entity import Entity

from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from elasticsearch import helpers as EShelpers

from espandas import Espandas

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


@contextmanager
def terminating(thing):
    try:
        yield thing
    finally:
        thing.terminate()


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
    # result = []
    # https://cloud.google.com/datastore/docs/concepts/limits
    # for keylist in index_list:
    # result.append(client.get_multi(keys=keylist))
    # yield client.get_multi(keys=keys[:1000])

    cpu = 4
    pool = mp.Pool(processes=cpu) 
    multprocessA = [pool.apply_async(_getBig, args=(keylist, )) for keylist in index_list]

    logger.info("Flushing get_multi data...")
    output_multprocessA = [p.get() for p in multprocessA]

    logger.info("Cleaning up multiprocess...")

    pool.close()
    pool.terminate()
    return output_multprocessA


def saveByPandasGBQ(model_fit, model_transform,
                    fitmodel_name="topic_recommender.model_fit",
                    transformodel_name='topic_recommender.model_transform',
                    gbq_projectid='kumparan-data', chunksize=10000):
    """
        method to save into google bigquery using pandas-gbq

        Parameters
        ----------
        model_fit :

        model_transform :
    """
    t0 = time.time()
    logger.info("saving fitted data...")
    model_fit = model_fit[['user_id', 'topic_id', 'date', 'num_x', 'num_y',
                           'date_all_click', 'Ntotal', 'joinprob_ci',
                           'p_cat_ci', 'posterior']]
    model_fit['user_id'] = model_fit.user_id.apply(str)
    model_fit['topic_id'] = model_fit['topic_id'].astype(str)

    # need to be careful if_exists='append', if we train data on same data set
    #   there would be a duplicate, so need to delete duplicate manually
    model_fit.to_gbq(fitmodel_name, gbq_projectid,
                     if_exists='append', chunksize=chunksize, verbose=True)

    logger.info("saving transformed data...")
    model_transform = model_transform[['user_id', 'topic_id', 'pt_posterior_x_Nt',
                                       'p0_cat_ci', 'smoothed_pt_posterior',
                                       'sigma_Nt', 'p0_posterior']]
    model_transform['user_id'] = model_transform.user_id.apply(str)
    model_transform['topic_id'] = model_transform['topic_id'].astype(str)

    # use if_exists='replace', so would force:
    #   If table exists, drop it, recreate it, and insert data
    model_transform.to_gbq(transformodel_name, gbq_projectid,
                           if_exists='replace', chunksize=chunksize, verbose=True)
    train_time = time.time() - t0
    logger.info("Total time consumed to push data into bigquery: %0.3fs", train_time)


def saveDatastore(df):
    """
        Saving to google datastore.

        parameter:
        ----------

        df : pandas dataframe, required=True
            is model_tranform a.k.a final model
    """
    start_total_time = time.time()
    logging.info("Saving main Transform model to Google DataStore...")
    def _ds_connection():
        """
            * This function handle connection to Data Store (DS)
            * To use it :
                - export google credential with command :
                    'export GOOGLE_APPLICATION_CREDENTIALS=(path to json credential for GCP)'
                    example :
                        'export GOOGLE_APPLICATION_CREDENTIALS="/your_dir_location/service-account-file.json"'
        """

        return datastore.Client('kumparan-data')

    def _get_batch(iterable, n=1):
        """
            * function to iterate batch which is will insert to datastore
        """

        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

    def _output(df, _kind):
        ind = 0
        all_entities = []

        for i in df.T.to_dict().values():
            _key = "{}_{}".format(i['user_id'], i['topic_id'])
            entity = Entity(key=client.key(_kind, _key))
            entity.update(i)
            all_entities.append(entity)

        for entities in _get_batch(all_entities, n=500):
            batch = client.batch()
            batch.begin()

            for entity in entities:
                batch.put(entity)
            batch.commit()

            ind += 500

    client = _ds_connection()
    kind = 'topic_recomendation'
    divider = 10

    _output(df, kind)

    end_total_time = time.time() - start_total_time
    logger.info('end of all insert batch entity to datastore with exec time : %.5f and total entity : %d' % (end_total_time, len(df)))
    return

def saveElasticSearch(df):
    """
        Saving to elastic search.

    """

    start_total_time = time.time()
    logger.info("Saving by Multiprocess with 'put_multi' ..")

    def _es_connection_aws_server():
        """
            * This function handle connection to AWS Elastic Search server
            * To use it :
                1. open terminal then ssh to 'ssh -p 5858 ec2-user@52.220.149.211 -D3333'
                2. then open terminal again in new tab, ssh again to : 'ssh -o ProxyCommand='nc -x 127.0.0.1:3333 %h %p' -lroot admin@10.0.1.101'
                3. set proxy to localhost:3333
                4. then you can use the elastic search
        """

        ## PRODUCTION ##
        # host = 'vpc-prod-onygcwdmytyr4fomfbckrbocai.ap-southeast-1.es.amazonaws.com'

        ## STAGING ##
        host = 'vpc-test-rq4u4ujemxy2q5lv4jsbb7pr6q.ap-southeast-1.es.amazonaws.com'
        region = 'ap-southeast-1'
        service = 'es'
        AWS_ACCESS_KEY = 'AKIAJFGTSPA2DEJR3GHA'
        AWS_SECRET_KEY = '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'
        ARANGO_HOST = 'http://127.0.0.1:8529'

        awsauth = AWS4Auth(AWS_ACCESS_KEY, AWS_SECRET_KEY, region, service)
        es = Elasticsearch(
            hosts = [{'host': host, 'port': 443}],
            http_auth = awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )

        return es

    def _es_insert(_index, _type, _id, _body):
        """Add indexing to elastic search"""

        try:
            es.index(index=_index, doc_type=_type, id=_id, body=_body)
        except Exception as e:
            print 'Something error on : '
            print str(e)
            pass

    def _convert_to_esform(_index, _type, _identifier, _insertedData):
        entity = {
            "_index": _index,
            "_type": _type,
            "_id": _identifier,
            "_source": _insertedData
          }
        return entity

    def _es_bulk_insert(df):
        start_time = time()
        _index = 'topic_recomendation_tests'
        _type = 'topic_recomendation_test'

        tasks = [_convert_to_esform(_index, _type, "{}_{}".format(x['user_id'], x['topic_id']), x) for x in df.to_dict(orient='records')]

        helpers.bulk(es, tasks)

        end_time = time() - start_time
        print 'Total time to insert {0} batch entity to elastic search : {1} '.format(len(df), end_time)

    def _es_insert_one(df):
        start_time = time()
        _index = 'topic_recomendation_tests'
        _type = 'topic_recomendation_test'
        [_es_insert(_index, _type, "{}_{}".format(x['user_id'], x['topic_id']), x) for x in df.to_dict(orient='records')]


    es = _es_connection_aws_server()
    divider = 10

    index_list = _split_data(divider, model_transform)

    procs = []

    # _es_insert_one(model_transform[:200])
    for df in index_list:
        proc = Process(target=_es_bulk_insert, args=(df))
        procs.append(proc)
        proc.start()

    end_total_time = time.time() - start_total_time
    logger.info('end of all insert batch entity to datastore with exec time : %.5f and total entity : %d' % (end_total_time, len(df)))



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
    logger.info("Saving main Transform model to Google DataStore...")

    def _ds_connection():
        """
            * This function handle connection to Data Store (DS)
            * To use it :
                - export google credential with command :
                    'export GOOGLE_APPLICATION_CREDENTIALS=(path to json credential for GCP)'
                    example :
                        'export GOOGLE_APPLICATION_CREDENTIALS="/your_dir_location/service-account-file.json"'
        """

        return datastore.Client('kumparan-data')

    def _dict_to_datastore_task(client, _kind, _identifier, _insertedData):
        incomplete_key = client.key(_kind, _identifier)
        entity = datastore.Entity(key=incomplete_key)
        entity.update(_insertedData)
        return entity

    def _output(df, _kind):
        start_time = time.time()
        client = _ds_connection()

        batch_size = 500
        for cur_index in xrange(0, len(df), batch_size):
            tasks = [_dict_to_datastore_task(client, _kind, "{}_{}".format(x['user_id'], x['topic_id']), x) for x in df[cur_index:cur_index+batch_size].T.to_dict().values()]
            client.put_multi(tasks)

    def _split_data(divider, df):
        n = df.shape[0] / divider

        index_list = []

        for index in xrange(0, len(df), n):
            index_list.append(df[index: index + n])

        return index_list

    client = _ds_connection()
    kind = kinds
    divider = 10

    index_list = _split_data(divider, df)

    procs = []
    for df_temp in index_list:
        proc = Process(target=_output, args=(df_temp, kind))
        procs.append(proc)
        proc.start()

    end_total_time = time.time() - start_total_time
    logger.info('end of all insert batch entity to datastore with exec time : %.5f and total entity : %d' % (end_total_time, len(df)))

@timeit
def saveDatastoreMP(df):
    """
        Saving to google datastore.

        parameter:
        ----------

        df : pandas dataframe, required=True
            is model_tranform a.k.a final model
    """
    # start_total_time = time.time()
    logger.info("Saving by Multiprocess..")
    logger.info("Saving main Transform model to Google DataStore...")
    def _ds_connection():
        """
            * This function handle connection to Data Store (DS)
            * To use it :
                - export google credential with command :
                    'export GOOGLE_APPLICATION_CREDENTIALS=(path to json credential for GCP)'
                    example :
                        'export GOOGLE_APPLICATION_CREDENTIALS="/your_dir_location/service-account-file.json"'
        """

        return datastore.Client('kumparan-data')

    def _get_batch(iterable, n=1):
        """
            * function to iterate batch which is will insert to datastore
        """

        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

    def _output(df, _kind):
        ind = 0
        all_entities = []

        for i in df.T.to_dict().values():
            _key = "{}_{}".format(i['user_id'], i['topic_id'])
            entity = Entity(key=client.key(_kind, _key))
            entity.update(i)
            all_entities.append(entity)

        for entities in _get_batch(all_entities, n=500):
            batch = client.batch()
            batch.begin()

            for entity in entities:
                batch.put(entity)
            batch.commit()

            ind += 500

    def _split_data(divider, df):
        n = df.shape[0] / divider

        index_list = []

        for index in xrange(0, len(df), n):
            index_list.append(df[index: index + n])

        return index_list

    client = _ds_connection()
    kind = 'topic_recomendation'
    divider = 10

    index_list = _split_data(divider, df)

    procs = []
    for df_temp in index_list:
        proc = Process(target=_output, args=(df_temp, kind))
        proc.daemon = True
        procs.append(proc)
        proc.start()
    return
    # end_total_time = time.time() - start_total_time
    # logger.info('Finish inserting batch entity to datastore, time taken: %.5f with total entity: %d' % (end_total_time, len(df)))

def saveElasticS(df, esindex_name='transform_index', estype_name='transform_type'):
    logging.info("Saving main Transform model to Elasticsearch...")
    start_total_time = time.time()

    elastic_host = "https://9db53c7bb4f5be2d856033a9aeb6e5a5.us-central1.gcp.cloud.es.io"
    elastic_username = "elastic"
    elastic_port = 9243
    elastic_password = "W0y1miwmrSMZKkSIARzbxJgb"

    INDEX = esindex_name
    TYPE = estype_name
    df['indexId'] = (df.index + 100).astype(str)
    esp = Espandas(hosts=[elastic_host], port=elastic_port, http_auth=(elastic_username, elastic_password))
    logger.info("Bulk insert into ElasticSearch, chunksize=%d, time_out: %d" % (20000, 60))
    logger.info("ElasticSearch host: %s", elastic_host)
    logger.info("ElasticSearch port: %s", elastic_port)
    logger.info(esp.es_write(df, INDEX, TYPE, chunksize=20000, rto=60))

    end_total_time = time.time() - start_total_time
    logger.info('Finish bulk insert to Eslastic Search, time taken: %.5f with total entity: %d' % (end_total_time, len(df)))
    return
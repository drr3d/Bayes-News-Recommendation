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

from google.cloud import datastore
from google.cloud.datastore.entity import Entity

from espandas import Espandas

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


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


def dict_to_datastore_taskMp(client, _kind, _identifier, _insertedData):
    incomplete_key = client.key(_kind, _identifier)
    entity = datastore.Entity(key=incomplete_key)
    entity.update(_insertedData)
    return entity

def outputMp(df, _kind):
    start_time = time.time()
    client = datastore.Client('kumparan-data')

    batch_size = 500
    for cur_index in xrange(0, len(df), batch_size):
        tasks = [dict_to_datastore_taskMp(client, _kind, "{}_{}".format(x['user_id'], x['topic_id']), x) for x in df[cur_index: cur_index + batch_size].T.to_dict().values()]
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
    logger.info("Saving main Transform model to Google DataStore...")

   
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

    # procs = []
    # for df_temp in index_list:
    #     proc = Process(target=_output, args=(df_temp, kind))
    #     procs.append(proc)
    #     proc.start()

    end_total_time = time.time() - start_total_time
    logger.info('end of all insert batch entity to datastore with exec time : %.5f and total entity : %d' % (end_total_time, len(df)))


def saveDatastoreMP(df):
    """
        Saving to google datastore.

        parameter:
        ----------

        df : pandas dataframe, required=True
            is model_tranform a.k.a final model
    """
    start_total_time = time.time()
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
        procs.append(proc)
        proc.start()

    end_total_time = time.time() - start_total_time
    logger.info('end of all insert batch entity to datastore with exec time : %.5f and total entity : %d' % (end_total_time, len(df)))
    return


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

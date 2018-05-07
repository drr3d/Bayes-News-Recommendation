#!/usr/bin/python
# -*- coding: utf8 -*-

import logging
import os
import sys
import time
import json
import numpy as np
import argparse
import psutil
import datetime
from datetime import timedelta
import pandas as pd
import multiprocessing as mp
import gc

from elasticsearch import Elasticsearch
from elasticsearch import helpers as EShelpers
from espandas import Espandas

from google.cloud import bigquery
from googlenews.BayesTopicRecommender import GBayesTopicRecommender
import modelhandler as mh  # comment if you have another method for saving models

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

bq_client = bigquery.Client()
job_config = bigquery.QueryJobConfig()

elastic_host = "10.23.255.51"
elastic_port = 9200

logger.info("ElasticSearch host: %s", elastic_host)
logger.info("ElasticSearch port: %s", elastic_port)
es = Elasticsearch([elastic_host], port=elastic_port)
esp = Espandas(hosts=[elastic_host], port=elastic_port)

pd.set_option('display.width', 1000)


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def load_bigquery(client, query, job_config):
    """
        Bigquery to dataframe
    """
    job_config.use_legacy_sql = False
    job_config.allowLargeResults = True

    df = client.query(query, job_config=job_config).result().to_dataframe()
    return df


def loadBQ(client, query, job_config, tabletype="history"):
    # https://github.com/ofek/pypinfo/issues/27
    job_config.use_legacy_sql = False
    job_config.allowLargeResults = True

    result  = client.query(query, job_config=job_config)
    rows = result.result()

    col_name = [field.name for field in rows.schema]
    
    def _q_iterator():
        for row in rows:
            yield list(row)

    df = pd.DataFrame( _q_iterator() , columns=col_name)
    del result
 
    return df


def dateValidate(date_text):
    """
        to validate date input format
    """
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def main(df_input, df_current, current_date, G,
         project_id, savetrain=False, multproc=True,
         threshold=0, start_date=None, end_date=None,
         saveto="datastore"):
    """
        Main cron method
    """
    # ~ Data Preprocessing ~
    # split data train, untuk menggambarkan data berasal dari 2 table
    # D(u, t)
    logger.info("~~~~~~~~~~~~~~~~~~ Begin Main Process ~~~~~~~~~~~~~~~~~~~~~")
    df_dut = df_input.copy(deep=True)
    df_dut['date'] = pd.to_datetime(df_dut['date'], format='%Y-%m-%d', errors='coerce')

    # D(t)
    df_dt = df_current.copy(deep=True)
    df_dt['date'] = pd.to_datetime(df_dt['date'], format='%Y-%m-%d', errors='coerce')

    # ~~~~~~ Begin train ~~~~~~
    t0 = time.time()
    logger.info("train on: %d total history data(D(u, t))", len(df_dut))
    logger.info("transform on: %d total current data(D(t))", len(df_dt))

    # instantiace class
    BR = GBayesTopicRecommender(current_date, G=G)

    # ~~ Fit ~~
    #   handling genuine news interest < current date
    NB = BR.processX(df_dut)
    # mulai dari sini sampai df_input_x setiap fit dan transform
    #   nanti dipindah ke class train utama
    result = pd.merge(df_dut, NB, on=['date', 'topic_id'])
    """
        num_y = total global click for category=ci on periode t
        num_x = total click from user_U for category=ci on periode t
    """
    fitby_sigmant = False
    df_input_X = result[['date', 'user_id',
                         'topic_id', 'num_x', 'num_y',
                         'is_general']]
    # agar sama dengan hasil hitungan si bos, maka
    #  set full_bayes = False
    model_fit = BR.fit(df_dut, df_input_X,
                       full_bayes=False, use_sigmant=fitby_sigmant,
                       verbose=False)
    logger.info("Len of model_fit: %d", len(model_fit))
    logger.info("Len of df_dut: %d", len(df_dut))

    # ~~ and Transform ~~
    #   handling current news interest == current date
    if df_dt.empty:
        print "Cek your df_dt, cannot be emtpy!!"
        return None
    NB = BR.processX(df_dt)
    result = pd.merge(df_dt, NB, on=['date', 'topic_id'])

    df_input_X = result[['date', 'user_id', 'topic_id',
                         'num_x', 'num_y', 'is_general']]
    model_transform, fitted_models = BR.transform(df1=df_dt, df2=df_input_X,
                                                  fitted_model=model_fit,
                                                  verbose=False)
    # ~~~ filter is general and specific topic ~~~
    # the idea is just we need to rerank every topic according
    # user_id and and is_general by p0_posterior
    map_topic_isgeneral = df_dut[['topic_id',
                                  'is_general']].groupby(['topic_id',
                                                          'is_general']
                                                         ).size().to_frame().reset_index()
    model_transform['is_general'] = model_transform['topic_id'].map(map_topic_isgeneral.drop_duplicates('topic_id').set_index('topic_id')['is_general'])

    # ~ start by provide rank for each topic type ~
    model_transform['rank'] = model_transform.groupby(['user_id', 'is_general'])['p0_posterior'].rank(ascending=False)
    model_transform = model_transform.sort_values(['is_general', 'rank'], ascending=[False, True])

    # ~ set threshold to filter output
    if threshold > 0:
        logger.info("Filtering topic by top: %d for each(General-Specific) !!", threshold)
        model_transform = model_transform[(model_transform['rank'] <= threshold) &
                                          (model_transform['p0_posterior'] > 0.)]
    
    train_time = time.time() - t0
    logger.info("Total train time: %0.3fs", train_time)

    logger.info("memory left before cleaning: %.3f percent memory...", psutil.virtual_memory().percent)
    logger.info("cleaning up some objects...")
    del df_dut
    logger.info("deleting df_dut...")
    del df_dt
    logger.info("deleting df_dt...")
    del df_input
    logger.info("deleting df_input...")
    del df_input_X
    logger.info("deleting df_input_X...")
    del df_current
    logger.info("deleting df_current...")
    del map_topic_isgeneral
    logger.info("deleting map_topic_isgeneral...")
    del model_fit
    logger.info("deleting model_fit...")
    del result
    logger.info("deleting result...")

    gc.collect()
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Save model Here ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if savetrain:
        model_transformsv = model_transform[['user_id', 'topic_id', 'is_general', 'p0_posterior']].copy(deep=True)
        model_transformsv['date'] = current_date.strftime("%Y-%m-%d")  # we need manually adding date, because table not support
        model_transformsv['date'] = pd.to_datetime(model_transformsv['date'],
                                                   format='%Y-%m-%d', errors='coerce')         
        model_transformsv = model_transformsv.rename(columns={'is_general': 'topic_is_general', 'p0_posterior': 'interest_score',
                                                              'date':'interest_score_created_at'})
        del model_transform
        logger.info("deleting model_transform...")
        logger.info("memory left after cleaning: %.3f percent memory...", psutil.virtual_memory().percent)

        logger.info("Begin saving trained data...")
        # ~ Place your code to save the training model here ~
        if str(saveto).lower() == "datastore":
            logger.info("Using google datastore as storage...")
            if multproc:
                logger.info("Saving main Transform model to Google DataStore...")
                logger.info("Saving total data: %d", len(model_transformsv))
                mh.saveDataStorePutMulti(model_transformsv)

                logger.info("Saving fitted_models as history to Google DataStore...")
                save_sigma_nt = BR.sum_all_nt.copy(deep=True)
                fitted_models_sigmant = pd.merge(fitted_models, save_sigma_nt, on=['user_id'])
                X_split = np.array_split(fitted_models_sigmant, 10)
                logger.info("Saving total data: %d", len(fitted_models_sigmant))
                logger.info("Len of X_split for batch save fitted_models: %d", len(X_split))
                for ix in range(len(X_split)):
                    logger.info("processing batch-%d", ix)
                    mh.saveDataStorePutMulti(X_split[ix], kinds='topic_recomendation_history')

                del X_split
                del BR
                logger.info("deleting BR...")
                del save_sigma_nt
                logger.info("deleting save_sigma_nt...")
                del fitted_models_sigmant
                logger.info("deleting fitted_models_sigmant...")
            else:
                mh.saveDatastore(model_transformsv)
                
        elif str(saveto).lower() == "elastic":
            logger.info("Using ElasticSearch as storage...")
            logging.info("Saving main Transform model to Elasticsearch...")
            
            X_split = np.array_split(model_transformsv, 5)
            logger.info("Saving total data: %d", len(model_transformsv))
            logger.info("Len of X_split for batch save model_transformsv: %d", len(X_split))
            for ix in range(len(X_split)):
                logger.info("processing batch-%d", ix)
                mh.saveElasticS(X_split[ix], esp, ishist=False)
            del X_split

            logger.info("Saving fitted_models as history to Elasticsearch...")
            save_sigma_nt = BR.sum_all_nt.copy(deep=True)
            fitted_models_sigmant = pd.merge(fitted_models, save_sigma_nt, on=['user_id'])

            fitted_models_sigmant['uid_topid'] = fitted_models_sigmant["user_id"].map(str) + "_" + fitted_models_sigmant["topic_id"].map(str)
            fitted_models_sigmant = fitted_models_sigmant[["uid_topid", "pt_posterior_x_Nt", "smoothed_pt_posterior", "p0_cat_ci", "sigma_Nt"]]

            X_split = np.array_split(fitted_models_sigmant, 5)
            logger.info("Saving total data: %d", len(fitted_models_sigmant))
            logger.info("Len of X_split for batch save fitted_models: %d", len(X_split))
            for ix in range(len(X_split)):
                logger.info("processing batch-%d", ix)
                mh.saveElasticS(X_split[ix], esp, esindex_name="fitted_hist_index",  estype_name='fitted_hist_type', ishist=True)
            del X_split
            
            del BR
            logger.info("deleting BR...")
            del save_sigma_nt
            logger.info("deleting save_sigma_nt...")
            del fitted_models_sigmant
            logger.info("deleting fitted_models_sigmant...")

    return


def getBig(procdate, loadmp, query_fit):
    bq_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    logger.info("Collecting training data for date: %s", procdate)
    # ~ get genuine news interest ~
    query_fit_where = "WHERE _PARTITIONTIME = TIMESTAMP(@start_date)"

    # safe handling of query parameter
    query_params = [
        bigquery.ScalarQueryParameter('start_date', 'STRING', procdate)
    ]

    job_config.query_parameters = query_params
    # temp_df = load_bigquery(client, query_fit + query_fit_where, job_config)
    temp_df = loadBQ(bq_client, query_fit + query_fit_where, job_config)

    if temp_df.empty:
        logger.info("%s data is empty!", procdate)
        return None
    else:
        logger.info("getting total: %d training data(genuine interest) for date: %s" % (len(temp_df), procdate))
        if loadmp:
                logger.info("Exiting: %s", mp.current_process().name)
        return temp_df


def BQPreprocess(loadmp, cpu, date_generated, client, query_fit):
    bq_client = client
    job_config = bigquery.QueryJobConfig()

    datalist = []

    if loadmp:
        logger.info("Starting data fetch multiprocess..")
        logger.info("number of process: %d", len(date_generated))

        pool = mp.Pool(processes=cpu)
        multprocessA = [pool.apply_async(getBig, args=(ndate.strftime("%Y-%m-%d"), loadmp, query_fit, )) for ndate in date_generated]
        output_multprocessA = [p.get() for p in multprocessA]

        for m in output_multprocessA:
            if m is not None:
                if not m.empty:
                    datalist.append(m)

        pool.close()
        pool.terminate()

        logger.info("len datalist: %d", len(datalist))
        logger.info("All data fetch multiprocess done!!")
    else:
        logger.info("Starting data fetch iterative...")
        for ndate in date_generated:
            tframe = getBig(ndate.strftime("%Y-%m-%d"), loadmp, query_fit)
            if tframe is not None:
                if not tframe.empty:
                    datalist.append(tframe)
            else: 
                logger.info("tframe for date: %s is empty", ndate.strftime("%Y-%m-%d"))
        logger.info("len datalist: %d", len(datalist))
        logger.info("All data fetch iterative done!!")

    return datalist


def preprocess(loadmp, cpu, cd, query_fit, date_generated):
    bq_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    # ~~~ Begin collecting data ~~~
    t0 = time.time()
    
    datalist = BQPreprocess(loadmp, cpu, date_generated, bq_client, query_fit)

    if len(datalist) > 1:
        big_frame = pd.concat(datalist)
        del datalist
    else:
        big_frame = datalist
    big_frame['date'] = pd.to_datetime(big_frame['date'], format='%Y-%m-%d', errors='coerce')

    # ~ get current news interest ~
    if not cd:
        logger.info("Collecting training data(current date interest)..")
        current_frame = loadBQ(bq_client, query_transform, job_config)
        logger.info("getting total: %d training data(current date interest)" % (len(current_frame)))
    else:
        logger.info("Collecting training data(current date interest) using argument: %s", cd)
        query_fit_where = "WHERE _PARTITIONTIME = TIMESTAMP(@start_date)"

        # safe handling of query parameter
        query_params = [
            bigquery.ScalarQueryParameter('start_date', 'STRING', str_datecurrent)
        ]

        job_config.query_parameters = query_params
        current_frame = loadBQ(bq_client, query_fit + query_fit_where, job_config)
        logger.info("getting total: %d training data(current date interest) for date: %s" % (len(current_frame), str_datecurrent))

    current_frame['date'] = date_current  # we need manually adding date, because table not support
    current_frame['date'] = pd.to_datetime(current_frame['date'],
                                           format='%Y-%m-%d', errors='coerce')

    train_time = time.time() - t0
    logger.info("loading time of: %d total genuine-current interest data ~ take %0.3fs" % (len(current_frame) + len(big_frame), train_time))

    return big_frame, current_frame

if __name__ == "__main__":
    """
        argument:
        ---------
            you can choose between using separate settings file(.json) or
            using quick setting (-N, -G, -p) but can not use both.
    """
    parser_description = "Legacy Training of Topic Recommender using Bayesian Framework"
    parser = argparse.ArgumentParser(description=parser_description)

    parser.add_argument("-N", metavar='N', type=int, default=25, required=False,
                        help="N of total back date you wan to use as training set.")
    parser.add_argument("-G", metavar='G', type=int, default=10, required=False,
                        help="G represent virtual click as smoothing factor on training.")
    parser.add_argument("-T", metavar='T', type=int, default=15, required=False,
                        help="Threshold for filtering output, set this > 0 then output will be fitered.")
    parser.add_argument("-p", metavar='P', type=str, default='kumparan-data', required=False,
                        help="Project ID on bigquery, mainly using this for pandas-gbq")
    parser.add_argument("-cd", type=str, default=None, required=False,
                        help="set current date manually, if not using current timestampt.")
    parser.add_argument("-savemp", type=str2bool, default="true", required=False,
                        help="set whether save data flow is handle by python multiprocess.")
    parser.add_argument("-loadmp", type=str2bool, default="false", required=False,
                        help="set whether load data flow is handle by python multiprocess.")
    parser.add_argument('-storage', choices=['datastore', 'elastic'], default="datastore",
                        help="set storage for saving trained model.")
    parser.add_argument("-savetrain", type=str2bool, default="true", required=False,
                        help="whether system would do save training data or not.")
    parser.add_argument("-cpu", type=int, default=4, required=False,
                        help="number of cpu use on multiprocess.")
    parser.add_argument('-ids', required=False,
                        help='input directory settings, set this and will overwrite some arguments')
    parser.add_argument('-isn', type=str, default="settings.json", required=False,
                        help='Input setting file name')

    args = parser.parse_args()

    if args.ids:
        if not args.isn:
            # file setting should be in json format
            logger.debug("Please input the file name...")
            sys.exit()

        if os.path.exists(args.ids + args.isn):
            logger.info("using separate settings file on: %s", args.ids + args.isn)

            with open(args.ids + args.isn, 'r') as f:
                config = json.load(f)

                fit_table = config["fit_dbtable"]
                transform_table = config["transform_dbtable"]

                # ~ bagian ini nanti bisa dibuatkan fungsi khusus contruct_query
                query_fit = """SELECT """ + fit_table['date_columnname'] + """ as date,
                    """ + fit_table['uid_columnname'] + """ as user_id,
                    """ + fit_table['topid_columnname'] + """ as topic_id,
                    """ + fit_table['isgeneral_columnname'] + """ as is_general,
                    """ + fit_table['topiccount_columnname'] + """ as num
                    FROM `""" + fit_table['db_table_name'] + """` """

                query_transform = """SELECT
                    """ + transform_table['uid_columnname'] + """ as user_id,
                    """ + transform_table['topid_columnname'] + """ as topic_id,
                    """ + transform_table['isgeneral_columnname'] + """ as is_general,
                    """ + transform_table['topiccount_columnname'] + """ as num
                    FROM `""" + transform_table['db_table_name'] + """` CDH
                    """

        project_id = config["project_id"]
        N = config["N"]
        G = config["G"]
        T = config["threshold"]
    else:
        project_id = args.p  # assign bigquery project id
        N = args.N
        G = args.G
        T = args.T
        query_fit = """ SELECT _PARTITIONTIME AS date,
                            click_user_alias_id as user_id,
                            click_topic_id as topic_id, 
                            click_topic_is_general as is_general,
                            click_topic_count as num
                        FROM `kumparan-data.topic_recommender.click_distribution_daily`
                    """

        query_transform = """
                            SELECT click_user_alias_id as user_id,
                              click_topic_id as topic_id,
                              click_topic_is_general as is_general,
                              click_topic_count as num
                            FROM `kumparan-data.topic_recommender.click_distribution_hourly` CDH
                          """
    if N <= 0:
        logger.DEBUG("N cannot smaller or equal to 0...")
        sys.exit()

    if G <= 0:
        logger.DEBUG("G cannot smaller or equal to 0...")
        sys.exit()

    # ~~ Create the index on elastic search ~~
    es.indices.delete(index='fitted_hist_index')
    es.indices.delete(index='transform_index')

    logger.info("Checking transform_index availability...")
    index_name = "transform_index"
    type_name = "transform_type"
    is_index_exist = es.indices.exists(index=index_name)

    request_body_tr = {
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

    request_body_hs = {
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

    # Create the index
    if not is_index_exist:
        logger.info("transform_index is Not available.")
        es.indices.create(index=index_name, body=request_body_tr)
        logger.info("transform_index Created !!")
    else:
        logger.info("transform_index is %s", str(is_index_exist))
    
    # ~ creating fitted_hist
    logger.info("Checking fitted_hist_index availability...")
    index_name = "fitted_hist_index"
    type_name = "fitted_hist_type"
    is_index_exist = es.indices.exists(index=index_name)

    # Create the index
    if not is_index_exist:
        logger.info("fitted_hist_index is Not available.")
        es.indices.create(index=index_name, body=request_body_hs)
        logger.info("fitted_hist_index Created !!")
    else:
        logger.info("fitted_hist_index is %s", str(is_index_exist))
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # ~~~ Generate date range for training set ~~~
    logger.info("Generating date range with N: %d", N)
    if args.cd:
        if dateValidate(args.cd):  # check if date is valid format
            date_current = datetime.datetime.strptime(args.cd, '%Y-%m-%d')
    else:
        date_current = datetime.datetime.now()

    str_datecurrent = date_current.strftime('%Y-%m-%d')
    date_1_days_ago = date_current - timedelta(days=1)
    if N == 1:
        N = 2
    date_N_days_ago = date_current - timedelta(days=N) # date_1_days_ago - timedelta(days=N)

    start = datetime.datetime.strptime(date_N_days_ago.strftime('%Y-%m-%d'), "%Y-%m-%d")
    end = date_1_days_ago  # datetime.datetime.strptime(str_datecurrent, "%Y-%m-%d")
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    date_generated.append(date_1_days_ago)
    logger.info("using current date: %s", str_datecurrent)
    logger.info("using start date: %s", start)
    logger.info("using end date: %s", date_1_days_ago.strftime('%Y-%m-%d'))

    big_frame, current_frame = preprocess(args.loadmp, args.cpu, args.cd, query_fit, date_generated)

    # ~~~ Proses Train ~~~
    main(big_frame, current_frame, date_current,
         G, project_id, savetrain=args.savetrain,
         multproc=args.savemp, threshold=T,
         start_date=None, end_date=None, saveto=args.storage)
    logger.info("~~~~~~~~~~~~~~ All Legacy Train operation is complete ~~~~~~~~~~~~~~~~~")

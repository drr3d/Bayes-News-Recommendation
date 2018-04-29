#!/usr/bin/python
# -*- coding: utf8 -*-

import logging
import os
import sys
import time
import json
import argparse
import datetime
import psutil
from datetime import timedelta
import pandas as pd
import multiprocessing as mp

from google.cloud import bigquery
from googlenews.BayesTopicRecommender import GBayesTopicRecommender
import modelhandler as mh  # comment if you have another method for saving models

# logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
# logger = logging.getLogger(__name__)

# ~~~~~~~~~~~~~
logger = logging.getLogger('memory_profile6_log')
logger.setLevel(logging.DEBUG)

from memory_profiler import profile
# create file handler which logs even debug messages
fh = logging.FileHandler("memory_profile6.log")
fh.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
from memory_profiler import LogFile
import sys
sys.stdout = LogFile('memory_profile6_log', reportIncrementFlag=False)
# ~~~~~~~~~~~~~

pd.set_option('display.width', 1000)


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

@profile
def load_bigquery(client, query, job_config):
    """
        Bigquery to dataframe
    """
    job_config.use_legacy_sql = False
    job_config.allowLargeResults = True

    result = client.query(query, job_config=job_config)  # .result()
    df = result.to_dataframe()
    return df

@profile
def loadBQ(client, query, job_config, tabletype="history"):
    # https://github.com/ofek/pypinfo/issues/27
    job_config.use_legacy_sql = False
    job_config.allowLargeResults = True

    result  = client.query(query, job_config=job_config)
    rows = result.result()

    col_name = [field.name for field in rows.schema]
    
    data = []
    for row in list(rows):
        data.append(list(row))

    df = pd.DataFrame(data, columns=col_name)
    del rows
    del result
    del data
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

@profile
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
    fitby_sigmant = True
    df_input_X = result[['date', 'user_id',
                         'topic_id', 'num_x', 'num_y',
                         'is_general']]
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

    # map_topic_isgeneral = map_topic_isgeneral.loc[~map_topic_isgeneral.index.duplicated(keep='first')]
    # model_transform = model_transform.loc[~model_transform.index.duplicated(keep='first')]
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
    del BR
    logger.info("deleting BR...")
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Save model Here ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if savetrain:
        model_transformsv = model_transform[['user_id', 'topic_id', 'is_general', 'rank']].copy(deep=True)
        del model_transform
        logger.info("deleting model_transform...")
        logger.info("memory left after cleaning: %.3f percent memory...", psutil.virtual_memory().percent)

        logger.info("Begin saving trained data...")
        # ~ Place your code to save the training model here ~
        if str(saveto).lower() == "datastore":
            logger.info("Using google datastore as storage...")
            if multproc:
                mh.saveDataStorePutMulti(model_transformsv)
                mh.saveDataStorePutMulti(fitted_models, kinds='topic_recomendation_history')
            else:
                mh.saveDatastore(model_transformsv)
                
        elif str(saveto).lower() == "elastic":
            logger.info("Using ElasticSearch as storage...")
            mh.saveElasticS(model_transformsv)

        # need save sigma_nt for daily train
        # secara defaul perhitungan fit menggunakan sigma_Nt
        #   jadi prosedur ini hanya berlaku jika fitby_sigmant = False
        if start_date and end_date:
            if not fitby_sigmant:
                logging.info("Saving sigma Nt...")
                save_sigma_nt = BR.sum_all_nt.copy(deep=True)
                save_sigma_nt['start_date'] = start_date
                save_sigma_nt['end_date'] = end_date
                print save_sigma_nt.head(5)
                print "len(save_sigma_nt): %d" % len(save_sigma_nt)
                # mh.saveDatastoreMP(save_sigma_nt)
    return

@profile
def preprocess(loadmp, cpu, cd, query_fit, date_generated):
    bq_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    # ~~~ Begin collecting data ~~~
    t0 = time.time()
    datalist = []

    def _getBig(procdate, client):
        logger.info("Collecting training data for date: %s", procdate)
        # ~ get genuine news interest ~
        query_fit_where = "WHERE _PARTITIONTIME = TIMESTAMP(@start_date)"

        # safe handling of query parameter
        query_params = [
            bigquery.ScalarQueryParameter('start_date', 'STRING', procdate)
        ]

        job_config.query_parameters = query_params
        temp_df = loadBQ(client, query_fit + query_fit_where, job_config)

        if temp_df.empty:
            logger.info("%s data is empty!", procdate)
            return None
        else:
            logger.info("getting total: %d training data(genuine interest) for date: %s" % (len(temp_df), procdate))
            if loadmp:
                logger.info("Exiting: %s", mp.current_process().name)
            return temp_df

    if loadmp:
        logger.info("Starting data fetch multiprocess..")
        logger.info("number of process: %d", len(date_generated))

        pool = mp.Pool(processes=cpu)
        multprocessA = [pool.apply_async(_getBig, args=(ndate.strftime("%Y-%m-%d"), bq_client, )) for ndate in date_generated]
        output_multprocessA = [p.get() for p in multprocessA]

        for m in output_multprocessA:
            if m is not None:
                if not m.empty:
                    datalist.append(m)

        logger.info("len datalist: %d", len(datalist))
        logger.info("All data fetch multiprocess done!!")
    else:
        logger.info("Starting data fetch iterative...")
        for ndate in date_generated:
            tframe = _getBig(ndate.strftime("%Y-%m-%d"), bq_client)
            if tframe is not None:
                if not tframe.empty:
                    datalist.append(tframe)
            else: 
                logger.info("tframe for date: %s is empty", ndate.strftime("%Y-%m-%d"))
        logger.info("len datalist: %d", len(datalist))
        logger.info("All data fetch iterative done!!")

    if len(datalist) > 1:
        big_frame = pd.concat(datalist)
        del datalist
    else:
        big_frame = datalist
    big_frame['date'] = pd.to_datetime(big_frame['date'], format='%Y-%m-%d', errors='coerce')

    # ~ get current news interest ~
    if not cd:
        logger.info("Collecting training data(current date interest)..")
        current_frame = load_bigquery(bq_client, query_transform, job_config)
        logger.info("getting total: %d training data(current date interest)" % (len(current_frame)))
    else:
        logger.info("Collecting training data(current date interest) using argument: %s", cd)
        query_fit_where = "WHERE _PARTITIONTIME = TIMESTAMP(@start_date)"

        # safe handling of query parameter
        query_params = [
            bigquery.ScalarQueryParameter('start_date', 'STRING', str_datecurrent)
        ]

        job_config.query_parameters = query_params
        current_frame = load_bigquery(bq_client, query_fit + query_fit_where, job_config)
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
                    FROM `""" + transform_table['db_table_name'] + """` """

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

        query_transform = """ SELECT click_user_alias_id as user_id,
                              click_topic_id as topic_id,
                              click_topic_is_general as is_general,
                              click_topic_count as num
                              FROM `kumparan-data.topic_recommender.click_distribution_hourly`
                          """
    if N <= 0:
        logger.DEBUG("N cannot smaller or equal to 0...")
        sys.exit()

    if G <= 0:
        logger.DEBUG("G cannot smaller or equal to 0...")
        sys.exit()

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
    print "date_generated: ", date_generated
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

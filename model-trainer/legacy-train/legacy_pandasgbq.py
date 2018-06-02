#!/usr/bin/python
# -*- coding: utf8 -*-

import logging
import os
import sys
import time
import json
import argparse
import datetime
from datetime import timedelta
import pandas as pd

from googlenews.BayesTopicRecommender import GBayesTopicRecommender
import modelhandler as mh  # comment if you have another method for saving models

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def dateValidate(date_text):
    """
        to validate date input format
    """
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def main(df_input, df_current, current_date,
         G, project_id, savetrain=False):
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
    result = pd.merge(df_dut, NB, on=['date', 'topic_id'])
    """
        num_y = total global click for category=ci on periode t
        num_x = total click from user_U for category=ci on periode t
    """
    df_input_X = result[['date', 'user_id',
                         'topic_id', 'num_x', 'num_y']]
    model_fit = BR.fit(df_dut, df_input_X,
                       full_bayes=False, use_sigmant=False,
                       verbose=False)

    # ~~ and Transform ~~
    #   handling current news interest == current date
    NB = BR.processX(df_dt)
    result = pd.merge(df_dt, NB, on=['date', 'topic_id'])

    df_input_X = result[['date', 'user_id', 'topic_id',
                         'num_x', 'num_y']]
    model_transform = BR.transform(df1=df_dt, df2=df_input_X,
                                   df3=df_dut, fitted_model=model_fit,
                                   verbose=False)

    train_time = time.time() - t0
    logger.info("Total train time: %0.3fs", train_time)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Save model Here ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if savetrain:
        logger.info("Begin saving trained data...")
        print model_fit.head(5)
        print model_fit.tail(5)
        print "\n", model_transform.head(10)
        # ~ Place your code to save the training model here ~
        mh.saveDatastore(model_transform)


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
    parser.add_argument("-p", metavar='P', type=str, default='kumparan-data', required=False,
                        help="Project ID on bigquery, mainly using this for pandas-gbq")
    parser.add_argument("-cd", type=str, default=None, required=False,
                        help="set current date manually, if not using current timestampt.")
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

    else:
        project_id = args.p  # assign bigquery project id
        N = args.N
        G = args.G

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
    date_N_days_ago = date_1_days_ago - timedelta(days=N)

    start = datetime.datetime.strptime(date_N_days_ago.strftime('%Y-%m-%d'), "%Y-%m-%d")
    end = datetime.datetime.strptime(str_datecurrent, "%Y-%m-%d")
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    logger.info("using current date: %s", str_datecurrent)
    logger.info("using start date: %s", start)
    logger.info("using end date: %s", date_1_days_ago.strftime('%Y-%m-%d'))

    # ~~~ Begin collecting data ~~~
    t0 = time.time()
    datalist = []
    for date in date_generated:
        # ~ get genuine news interest
        logger.info("Collecting training data for date: %s", date.strftime("%Y-%m-%d"))
        query_fit_where = "WHERE _PARTITIONTIME = TIMESTAMP('" + date.strftime("%Y-%m-%d") + "')"

        temp_df = pd.read_gbq(query_fit + query_fit_where, project_id=project_id, index_col=None,
                              col_order=None, reauth=False, verbose=True,
                              private_key=None,
                              dialect='standard')
        if temp_df.empty:
            logger.info("%s data is empty!", date.strftime("%Y-%m-%d"))
        else:
            datalist.append(temp_df)

    train_time = time.time() - t0
    big_frame = pd.concat(datalist)
    big_frame['date'] = pd.to_datetime(big_frame['date'], format='%Y-%m-%d', errors='coerce')
    logger.info("loading time of: %d total data ~ take %0.3fs" % (len(big_frame), train_time))

    # ~ get current news interest
    current_frame = pd.read_gbq(query_transform, project_id=project_id, index_col=None,
                                col_order=None, reauth=False, verbose=True,
                                private_key=None,
                                dialect='standard')
    current_frame['date'] = date_current  # we need manually adding date, because table not support
    current_frame['date'] = pd.to_datetime(current_frame['date'],
                                           format='%Y-%m-%d', errors='coerce')

    # ~~~ Proses Train ~~~
    main(big_frame, current_frame, date_current,
         G, project_id, savetrain=False)
    logger.info("All Legacy Train operation is complete!")

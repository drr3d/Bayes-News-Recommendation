#!/usr/bin/python
# -*- coding: utf8 -*-

import logging
import pandas as pd
import numpy as np
import psutil

import modelhandler as mh

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def savetoGooDatastore(multproc, model_transformsv, BR, fitted_models):
    logger.info("Using google datastore as storage...")
    if multproc:
        # ~ save transform models ~
        logger.info("Saving main Transform model to Google DataStore...")
        logger.info("Saving total data: %d", len(model_transformsv))
        mh.saveDataStorePutMulti(model_transformsv)

        # ~ save fitted models ~
        logger.info("Saving fitted_models as history to Google DataStore...")
        save_sigma_nt = BR.sum_all_nt.copy(deep=True)
        fitted_models_sigmant = pd.merge(fitted_models, save_sigma_nt, on=['user_id'])
        logger.info("Saving total data: %d", len(fitted_models_sigmant))
        X_split = np.array_split(fitted_models_sigmant, 10)
        logger.info("Len of X_split for batch save fitted_models: %d", len(X_split))
        for ix in range(len(X_split)):
            logger.info("processing batch-%d", ix)
            mh.saveDataStorePutMulti(X_split[ix], kinds='topic_recomendation_history')

        del X_split
        logger.info("deleting X_split...")
        del save_sigma_nt
        logger.info("deleting save_sigma_nt...")
        del fitted_models_sigmant
        logger.info("deleting fitted_models_sigmant...")


def savetoElasticsearch(model_transformsv, esp, BR, fitted_models, df_mtf):
    logger.info("Using ElasticSearch as storage...")
    logging.info("Saving main Transform model to Elasticsearch...")

    X_split = np.array_split(model_transformsv, 15)
    logger.info("Saving total data: %d", len(model_transformsv))
    logger.info("Len of X_split for batch save model_transformsv: %d", len(X_split))
    for ix in range(len(X_split)):
        logger.info("processing batch-%d", ix)
        mh.saveElasticS(X_split[ix], esp, save_type="current")
    del X_split       

    logger.info("Saving fitted_models as history to Elasticsearch...")
    save_sigma_nt = BR.sum_all_nt.copy(deep=True)
    fitted_models_sigmant = pd.merge(fitted_models, save_sigma_nt, on=['user_id'])

    fitted_models_sigmant['uid_topid'] = fitted_models_sigmant["user_id"].map(str) + "_" + fitted_models_sigmant["topic_id"].map(str)
    fitted_models_sigmant = fitted_models_sigmant[["uid_topid", "pt_posterior_x_Nt",
                                                    "smoothed_pt_posterior", "p0_cat_ci", "sigma_Nt"]]

    X_split = np.array_split(fitted_models_sigmant, 35)
    logger.info("Saving total data: %d", len(fitted_models_sigmant))
    logger.info("Len of X_split for batch save fitted_models: %d", len(X_split))
    for ix in range(len(X_split)):
        logger.info("processing batch-%d", ix)
        mh.saveElasticS(X_split[ix], esp,
                        esindex_name="topicrecommendation_fitted_hist_index",
                        estype_name='topicrecommendation_fitted_hist_type',
                        save_type="history")

    # ~ saving fallback dataset ~
    logger.info("saving fallback dataset")
    logger.info(df_mtf)
    mh.saveElasticS(df_mtf.sort_values('interest_score', ascending=False),
                    esp, esindex_name="topicrecommendation_transform_fallback_index",
                    estype_name='topicrecommendation_transform_fallback_type',
                    save_type="fallback")
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    del X_split
    logger.info("deleting X_split...")
    del save_sigma_nt
    logger.info("deleting save_sigma_nt...")
    del fitted_models_sigmant
    logger.info("deleting fitted_models_sigmant...")


def saveFallback(model_transform, map_topic_name):
    """
        buat mekanisme fallback general topik ,
        digunakan untuk handling new un-registered user.
        factor yg dapat digunakan untuk kalkulasi ini diantaranya:
            1. p0_cat_ci
            2. p0_posterior
            3. num_y
    """

    logger.info("Calculating fallback by p0_cat_ci")
    model_transform_fallback = model_transform[['user_id', 'topic_id', 'is_general',
                                                'topic_name',
                                                'p0_cat_ci']].loc[model_transform['is_general'] == True].copy(deep=True)
    df_mtf = model_transform_fallback.groupby(['topic_id'])['p0_cat_ci'].agg('sum').reset_index()
    df_mtf = df_mtf.rename(columns={'p0_cat_ci': 'interest_score'})
    df_mtf['topic_name'] = df_mtf['topic_id'].map(map_topic_name.drop_duplicates('topic_id').set_index('topic_id')['topic_name'])
    # ~~
    return df_mtf[['topic_id', 'topic_name', 'interest_score']]


def saveMainModel(savetrain, model_transform, BR, current_date,
                  saveto, multproc, map_topic_name,
                  fitted_models, esp):
    """
        Saving handler
    """
    logger.info("Calling transporthandler saveMainModel()...")
    if savetrain:
        model_transformsv = model_transform[['user_id', 'topic_id', 'is_general', 'topic_name', 'p0_posterior']].copy(deep=True)
        model_transformsv['date'] = current_date.strftime("%Y-%m-%d")  # we need manually adding date, because table not support
        model_transformsv['date'] = pd.to_datetime(model_transformsv['date'],
                                                   format='%Y-%m-%d', errors='coerce')

        model_transformsv = model_transformsv.rename(columns={'is_general': 'topic_is_general', 'p0_posterior': 'interest_score',
                                                              'date':'interest_score_created_at'})

        # save fallback
        df_mtf = saveFallback(model_transform, map_topic_name)

        del model_transform
        logger.info("deleting model_transform...")

        logger.info("Begin saving trained data...")
        # ~ Place your code to save the training model here ~
        if str(saveto).lower() == "datastore":
            savetoGooDatastore(multproc, model_transformsv, BR, fitted_models)

        elif str(saveto).lower() == "elastic":
            savetoElasticsearch(model_transformsv, esp, BR, fitted_models, df_mtf)


        del model_transformsv
        logger.info("deleting model_transformsv...")
        del df_mtf
        logger.info("deleting df_mtf...")
        del BR
        logger.info("deleting BR...")
        del fitted_models
        logger.info("deleting fitted_models...")
    
        logger.info("memory left after cleaning: %.3f percent memory...", psutil.virtual_memory().percent)

    return
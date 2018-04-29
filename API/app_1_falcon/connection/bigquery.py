#!/usr/bin/env python
# coding=utf-8

import json, requests, sys, re, google.cloud.exceptions, pandas as pd
from datetime import timedelta, datetime
from google.cloud import bigquery

def _bq_connection():
    """
        * This function handle connection to Google Cloud Console (GCP)
        * To use it :
            1. export google credential with command : 'export GOOGLE_APPLICATION_CREDENTIALS=(path to json credential for GCP)'
                example : 'export GOOGLE_APPLICATION_CREDENTIALS="/Users/denid.suswanto/Documents/work/projects/python3/datawarehouse/dwh/service-account-file.json"'
    """

    return bigquery.Client(project='kumparan-data')

def bq_input():
    client = bq_connection()
    query = "SELECT * FROM `kumparan-data.temp_data.reco_topic_sample_input`"

    job_config = bigquery.QueryJobConfig()

    job_config.use_legacy_sql = False
    query_job = client.query(query, job_config=job_config).result().to_dataframe()

    return query_job

def bq_input_dataframe(_):
    client = bq_connection()
    query = "SELECT * FROM `kumparan-data.temp_data.reco_topic_sample_input`"

    job_config = bigquery.QueryJobConfig()

    job_config.use_legacy_sql = False
    query_job = client.query(query, job_config=job_config).result().to_dataframe()

    return query_job

def _bq_read_query(QUERY):
    client = bq_connection()

    job_config.use_legacy_sql = False
    query_job = client.query(QUERY, job_config=job_config).result().to_dataframe()

    return query_job

def _bq_insert_query():
    client = bq_connection()
    bucket_ = mp._mapper_repo_topic()

    dataset_ref = client.dataset(bucket_['dataset'])
    table_ref = dataset_ref.table(bucket_['table'])

    table = bigquery.Table(table_ref)
    table.schema = bucket_['schema']

    rows_to_insert = [
        (1000003, 'Olahraga', 7.6, 0.089, 1520401241),
        (1000004, 'Politik', 6.5, 0.01,1520401223),
    ]

    errors = client.insert_rows(table, rows_to_insert)  # API request

    assert errors

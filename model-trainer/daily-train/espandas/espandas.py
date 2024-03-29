#!/usr/bin/python
# -*- coding: utf8 -*-

"""
    Reading and writing pandas DataFrames to ElasticSearch

    this class is modified version from the original:
        - https://github.com/dashaub/espandas

    modification list:
        method `es_write`:
            - adding chunk_size
            - adding rto to handling request-time-out
            - change reindex_axis() to reindex()
            - try to handle OOM if you work on limited memory resource
"""
import psutil
import pandas as pd
import numpy as np
import ujson as json
import logging

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Espandas(object):
    """Reading and writing pandas DataFrames to ElasticSearch"""
    def __init__(self, **kwargs):
        """
        Construct an espandas reader/writer
        :params **kwargs: arguments to pass for establishing the connection to ElasticSearch
        """
        self.client = Elasticsearch(**kwargs)
        # print self.client.info()
        self.successful_ = None
        self.failed_ = None
        self.index_name = None

    def es_read(self, keys, index, doc_type):
        """
        Read from an ElasticSearch index and return a DataFrame
        :param keys: a list of keys to extract in elasticsearch
        :param index: the ElasticSearch index to read
        :param doc_type: the ElasticSearch doc_type to read
        """
        self.successful_ = 0
        self.failed_ = 0

        # Collect records for all of the keys
        records = []
        for key in keys:
            try:
                record = self.client.get(index=index, doc_type=doc_type, id=key)
                self.successful_ += 1
                records.append(pd.DataFrame([record.get('_source')]))
            except NotFoundError as nfe:
                print('Key not found: %s' % nfe)
                self.failed_ += 1

        # Prepare the records into a single DataFrame
        df = None
        if len(records) > 1:
            df = pd.concat(records)
            df.index = [i for i in range(df.shape[0])]
            df.fillna(value=np.nan, inplace=True)
            df = df.reindex_axis(sorted(df.columns), axis=1)
        return df


    def es_write(self, df, index, doc_type,
                 index_name='indexId', chunksize=5000, rto=60):
        """
        Insert a Pandas DataFrame into ElasticSearch
        :param df: the DataFrame, must contain the column 'indexId' for a unique identifier
        :param index: the ElasticSearch index
        :param doc_type: the ElasticSearch doc_type
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError('df must be a pandas DataFrame')

        if not self.client.indices.exists(index=index):
            print('index does not exist, creating index')
            self.client.indices.create(index)

        if not index_name in df.columns:
            raise ValueError('the index_name must be a column in the DataFrame')

        if len(df[index_name]) != len(set(df[index_name])):
            message = 'the values in index_name must be unique to use as an ElasticSearch _id'
            raise ValueError(message)
        self.index_name = index_name

        def generate_dict(df):
            """
            Generator for creating a dict to be inserted into ElasticSearch
            for each row of a pd.DataFrame
            :param df: the input pd.DataFrame to use, must contain an '_id' column
            """
            records = df.to_json(orient='records')
            records = json.loads(records)
            for record in records:
                yield record

        # The dataframe should be sorted by column name
        # df = df.reindex_axis(sorted(df.columns), axis=1)

        # ~ to handling OOM ~
        df = df.reindex(sorted(df.columns), axis=1).copy()
        logger.info("reindex using %.3f percent memory...", psutil.virtual_memory().percent)

        """
        data = ({'_op_type': 'update'
                 '_index': index,
                 '_type': doc_type,
                 '_id': record[index_name],
                 'doc': {x: record[x] for x in record if x not in {index_name}},
                 'doc_as_upsert': True}
                for record in generate_dict(df))
        logger.info("doc_as_upsert is True !!")
        """

        data = ({'_op_type': 'update',
                 '_index': index,
                 '_type': doc_type,
                 '_id': record[index_name],
                 'doc': {x: record[x] for x in record if x not in {index_name}},
                 'doc_as_upsert': True}
                for record in generate_dict(df))
        logger.info("doc_as_upsert is True ~ Modified version + using op_type(update) !!")
        print list(data)[:3]
        del df
        logger.info("final espandas, using %.3f percent memory...", psutil.virtual_memory().percent)
        helpers.bulk(self.client, data, chunk_size=chunksize, request_timeout=rto)

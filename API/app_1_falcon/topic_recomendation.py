#!/usr/bin/env python
# coding=utf-8

import falcon, MySQLdb, time, json, datetime, re, pandas as pd, google.cloud.exceptions, sys

from elasticsearch import Elasticsearch, RequestsHttpConnection
from connection.datastore import *
from connection.elasticsearch import *
from helper.helper import *

class TopicRecomendationResourceDataStore(object):
    """
        * this class is to handle all api for topic recomendation.
    """

    def __init__(self):
        self.kind = 'topic_recomendation'
        self.datastore_conn = DataStoreAPI()
        self.normalize = Normalize()

    def _lookup(self, key):
        ''' Handling lookup entities datastore by id/name (key of datastore) '''

        try:
            entity = self.datastore_conn._ds_lookup(self.kind, key)
            results_dicts = self.normalize._reco_topic_map_compact_ver(entity)

        except Exception as ex:
            results_dicts = {"Can't lookup data" : str(ex.message)}

        return { "topic_recomendation" : results_dicts }

    def _batch_lookup(self, keys):
        ''' Handling lookup entities datastore by id/name (key of datastore) '''

        try:
            entity = self.datastore_conn._ds_batch_lookup(self.kind, keys)
            results_dicts = []
            for row in entity:
                results_dicts.append(row)

        except Exception as ex:
            results_dicts = {"Can't lookup data" : str(ex.message)}

        return { "topic_recomendation" : results_dicts }

    def _query_key_filter(self, query):
        ''' Handling get entities data by query '''

        try:
            entity = self.datastore_conn._ds_query_key_filter(self.kind, query)
            results_dicts = []
            for row in entity:
                results_dicts.append(self.normalize._reco_topic_map_compact_ver(row))
                # results_dicts.append(row)

        except Exception as ex:
            results_dicts = {"Can't lookup data" : str(ex.message)}

        return { "topic_recomendation" : results_dicts }

    def _query(self, query):
        ''' Handling get entities data by query '''

        try:
            entity = self.datastore_conn._ds_query(self.kind, query)
            results_dicts = []
            for row in entity:
                results_dicts.append(self.normalize._reco_topic_map_compact_ver(row))
                # results_dicts.append(row)

        except Exception as ex:
            results_dicts = {"Can't lookup data" : str(ex.message)}

        return { "topic_recomendation" : results_dicts }

    def add(self, key, entity):
        ''' Handling add entities data to datastore '''

        try:
            data = self.normalize._reco_topic_map(entity)
            entity = self.datastore_conn._ds_insert(self.kind, key, data)
            results_dicts = self.datastore_conn._ds_lookup(self.kind, key)

        except Exception as ex:
            results_dicts = {"Can't lookup data" : str(ex.message)}

        return { "Success Insert " : results_dicts }

    def _update(self, key, parameters):
        ''' Handling update entities data to datastore '''

        updated_data = []

        for row in parameters:
            updated_data.append({"property":row['property'], "value":row['value']})

        try:
            query = self.datastore_conn._ds_update(self.kind, key, updated_data)
            results_dicts = self.datastore_conn._ds_lookup(self.kind, key)
        except Exception as ex:
            results_dicts = {"Can't update data" : str(ex.message)}

        return { "Succes Update " : results_dicts }

    def _remove(self, key):
        ''' Handling remove entities data from datastore '''

        try:
            results_dicts = self.datastore_conn._ds_lookup(self.kind, key)
            query = self.datastore_conn._ds_delete(self.kind, key)
        except Exception as ex:
            results_dicts = {"Can't delete entity" : str(ex.message)}

        return { "Success delete entity " : results_dicts }

    def _list(self):
        ''' Handling get list of data limited to 20 entities from datastore '''

        try:
            entity = self.datastore_conn._ds_list(self.kind)
            results_dicts = []
            for row in entity:
                results_dicts.append(row)

        except Exception as ex:
            results_dicts = {"Can't lookup data" : str(ex.message)}

        return { "topic_recomendation" : results_dicts }

    def _list_keys(self):
        ''' Handling get list of data limited to 20 entities from datastore '''

        try:
            entity = self.datastore_conn._ds_list_keys(self.kind)
            results_dicts = []
            for row in entity:
                results_dicts.append({row})

        except Exception as ex:
            results_dicts = {"Can't lookup data" : str(ex.message)}

        return { "topic_recomendation" : results_dicts }

    def on_post(self, req, resp):
        """
            Handles POST requests for topic recomendation
        """
        resp.set_header('Content-Type', 'text/plain')
        resp.status = falcon.HTTP_200

        action = req.get_param('action') or ''

        start_time = time.time()

        try:
            raw_json = json.loads(req.stream.read())
        except Exception as e:
            resp.body = json.dumps('Something error : {}'.format(str(e)), encoding='utf-8')

        if action == 'lookup':
            results = self._lookup(raw_json['id'])

        elif action == 'batch_lookup':
            results = self._batch_lookup(raw_json['keys'])

        elif action == 'query':
            results = self._query(raw_json)

        elif action == 'query_key':
            results = self._query_key_filter(raw_json)

        elif action == 'add':
            results = self._add(raw_json, str(raw_json['user_id']) + '_' + str(raw_json['topic_id']), raw_json)

        elif action == 'update':
            results = self._update(raw_json['id'], raw_json['data'])

        elif action == 'remove':
            results = self._remove(raw_json['id'])

        elif action == 'list_keys':
            results = self._list_keys()

        else:
            results = self._list()

        end_time = time.time() - start_time
        dict = json.dumps(results, encoding='utf-8')

        resp.body = '{} \n \n Execution time : {} '.format(dict, end_time)

class TopicRecomendationResourceElasticSearch(object):
    """
        * this class is to handle all api for topic recomendation to elastic search.
    """

    def __init__(self):
        self.index = 'topic_recomendations'
        self.type = 'topic_recomendation'
        self.connection = ElasticSearchAPI()
        self.es = self.connection._es_connection_aws_server(Elasticsearch, RequestsHttpConnection)
        self.normalize = Normalize()

    def on_post(self, req, resp):
        """
            Handles POST requests for topic recomendation
        """

        resp.set_header('Content-Type', 'text/plain')
        resp.status = falcon.HTTP_200

        action = req.get_param('action') or ''

        try:
            raw_json = json.loads(req.stream.read())
        except Exception as e:
            resp.body = json.dumps('Something error : {}'.format(str(e)), encoding='utf-8')

        if action == 'add':
            ''' Handling add entities data to elastic search '''

            try:
                # data = self.normalize._reco_topic_map(raw_json)
                entity = self.connection._insert(self.es, self.index, self.type, str(raw_json['user_id']) + '_' + str(raw_json['topic_id']), data)
                lookup_entity = self.connection._lookup(self.es, self.index, self.type, str(raw_json['user_id']) + '_' + str(raw_json['topic_id']))
                results_dicts = { "reco_topic" : lookup_entity }

            except Exception as ex:
                results_dicts = {"Can't insert data" : str(ex.message)}

            resp.body = json.dumps(results_dicts, encoding='utf-8')

        elif action == 'set':
            ''' Handling update entities data elastic search '''

            try:
                entity = self.connection._update(self.es, self.index, self.type, raw_json['doc']['id'], raw_json)
                results_dicts = { "Success update id " : raw_json['doc']['id'] }

            except Exception as ex:
                results_dicts = {"Can't update data" : str(ex.message)}

            resp.body = json.dumps(results_dicts, encoding='utf-8')

        elif action == 'remove':
            ''' Handling remove entities data from elastic search '''

            try:
                entity = self.connection._delete(self.es, self.index, self.type, raw_json['id'])
                results_dicts = { "Success remove id " : raw_json['id'] }

            except Exception as ex:
                results_dicts = {"Can't remove data" : str(ex.message)}

            resp.body = json.dumps(results_dicts, encoding='utf-8')

        elif action == 'lookup':
            ''' Handling lookup entities data from elastic search '''

            try:
                entity = self.connection._lookup(self.es, self.index, self.type, str(raw_json['id']))
                results_dicts = { "reco_topic" : entity }

            except Exception as ex:
                results_dicts = {"Can't lookup data" : str(ex.message)}

            resp.body = json.dumps(results_dicts, encoding='utf-8')

        elif action == 'query':
            ''' Handling query for get entities data from elastic search '''

            try:
                entity = self.connection._query(self.es, self.index, self.type, raw_json)
                results_dicts = { "reco_topic" : entity }

            except Exception as ex:
                results_dicts = {"Can't show data" : str(ex.message)}

            resp.body = json.dumps(results_dicts, encoding='utf-8')

        else:
            try:
                entity = self.connection._list(self.es, self.index)
                results_dicts = { "reco_topic" : entity }

            except Exception as ex:
                results_dicts = {"Can't show data" : str(ex.message)}

            resp.body = json.dumps(results_dicts, encoding='utf-8')

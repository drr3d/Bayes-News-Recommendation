#!/usr/bin/env python
# coding=utf-8

import json, requests, sys, re, google.cloud.exceptions, pandas as pd
from datetime import timedelta, datetime
from google.cloud import datastore
# from google.appengine.ext import ndb


# class TopicRecomendation(ndb.Model):
#     username = ndb.StringProperty()
#     userid = ndb.IntegerProperty()
#     email = ndb.StringProperty()
#     topic_id = ndb.IntegerProperty()
#     user_id = ndb.IntegerProperty()
#     sigma_Nt = ndb.FloatProperty()
#     joinprob_ci = ndb.FloatProperty()
#     p_cat_ci = ndb.FloatProperty()
#     pt_posterior = ndb.FloatProperty()
#     posterior_x_Nt = ndb.FloatProperty()
#     smoothed_posteriorxNt = ndb.FloatProperty()
#     p0_cat_ci = ndb.FloatProperty()
#     p0_posterior = ndb.FloatProperty()

class DataStoreAPI(object):
    """docstring for [object Object]."""

    def __init__(self):
        self.client = self._ds_connection()

    def _ds_connection(self):
        """
            * This function handle connection to Data Store (DS)
            * To use it :
                - export google credential with command : 'export GOOGLE_APPLICATION_CREDENTIALS=(path to json credential for GCP)'
                    example : 'export GOOGLE_APPLICATION_CREDENTIALS="/Users/denid.suswanto/Documents/work/projects/python3/datawarehouse/dwh/service-account-file.json"'
        """

        return datastore.Client('kumparan-data')


    def _ds_list(self, _kind):
        """
            * This function is to get all data limited 20 entities.
        """

        query = self.client.query(kind=_kind)

        query.order = "-p0_posterior"

        return list(query.fetch(limit=200))

    def _ds_list_keys(self, _kind):
        query = self.client.query(kind=_kind)
        query.keys_only()
        keys = list([entity.key.id_or_name for entity in query.fetch(limit=200)])

        return keys

    def _ds_lookup(self, _kind, _key):
        """
            * This function is to get all data by id/name (key of datastore).
        """

        key = self.client.key(_kind, _key)
        entity = self.client.get(key)

        return entity

    def _ds_batch_lookup(self, _kind, _keys):
        """
            * This function is to get all data by id/name (key of datastore).
        """

        keys = [self.client.key('topic_recomendation', str(x)) for x in _keys]

        tasks = self.client.get_multi(keys)

        return tasks

    def _ds_query_key_filter(self, _kind, _filter):
        """
            * This function is to get entities data by query from user.
        """

        query = self.client.query(kind=_kind)

        first_key = self.client.key(_kind, _filter['key_start'])
        last_key = self.client.key(_kind, _filter['key_end'])

        query.key_filter(last_key, '<=')
        query.key_filter(first_key, '>=')

        if _filter.has_key('order'):
            query.order = str(_filter['order'])
        else:
            query.order = [
                            "__key__",
                            "-is_general",
                            "-rank"
                        ]

        return list(query.fetch(limit=200))

    def _ds_query(self, _kind, _filter):
        """
            * This function is to get entities data by query from user.
        """

        query = self.client.query(kind=_kind)

        for param in _filter['filter']:

            operator = _filter['filter'][str(param)]['operator']
            value = _filter['filter'][str(param)]['value']

            query.add_filter(str(param), str(operator), value)

        if _filter.has_key('order'):
            query.order = str(_filter['order'])
        else:
            query.order = [
                            "-is_general",
                            "-rank"
                        ]

        return list(query.fetch(limit=200))

    def _ds_insert(self, _kind, _identifier, _insertedData):
        """
            * This function is to get entities data by query from user.
        """

        with self.client.transaction():
            incomplete_key = self.client.key(_kind, _identifier)

            entity = datastore.Entity(key=incomplete_key)

            entity.update(_insertedData)

            self.client.put(entity)

        return entity

    def _ds_update(self, _kind, _key, _updatedData):
        """
            * This function is to get entities data by query from user.
        """

        client = datastore.Client('kumparan-data')

        with self.client.transaction():
            key = self.client.key(_kind, _key)
            entity = self.client.get(key)

            for data in _updatedData:
                entity[data['property']] = data['value']
                if isinstance(data['value'], basestring):
                    entity[data['property']] = unicode(entity[data['property']])

            self.client.put(entity)

        return entity

    def _ds_delete(self, _kind, _key):
        key = self.client.key(_kind, _key)
        self.client.delete(key)

        return key

    """ with app engine """

    def _get_entity(entity_key):
        entity = entity_key.get()
        return entity

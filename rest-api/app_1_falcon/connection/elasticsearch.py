#!/usr/bin/env python
# coding=utf-8

import json, requests, sys, re, certifi, pandas as pd
# from elasticsearch import Elasticsearch , RequestsHttpConnection
from datetime import timedelta, datetime
from requests_aws4auth import AWS4Auth

class ElasticSearchAPI(object):
    """docstring for [object Object]."""

    def __init__(self):
        """
            initiate value
        """

        # self.client = self._es_connection_sl_server()

    def _es_connection_aws_server(self, Elasticsearch, RequestsHttpConnection):
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

    def _es_connection_sl_server(self, Elasticsearch):
        """Handles connection to elastic search"""
        host='https://el.kumpar.com/'
        port='443'
        use_ssl=True
        username='eldata'
        password='$toidr%'

        mode = "ssl"
        url = ""
        es = ""
        if(mode=="ssl"):
            es = Elasticsearch(
                                    [host],
                                    http_auth=(username, password),
                                    port=port,
                                    use_ssl=True,
                                    ca_certs=certifi.where()
                                )
        else:
            es = Elasticsearch(host=host,port=port)
        url = "http://{}:{}".format(host, port)

        return es

    def _list(self, es, _index):
        """GET All Data from elastic search"""
        result = es.search(index=_index, body={"query":{"match_all":{}}})
        return result

    def _lookup(self, es, _index, _type, _id):
        """GET Data from elastic search"""
        result = es.search(index=_index, doc_type=_type, body={"query":{"match":{"_id":str(_id)}},"from":0,"size":100})
        return result

    def _query(self, es, _index, _fields, _query):
        """GET Data from elastic search"""
        result = es.search(index=_index, body=_query)
        return result

    def _query_1(self, es, _index, _fields, _query):
        """GET Data from elastic search"""
        result = es.search(index=_index, body={"query": {"multi_match": {"fields": _fields, "query": _query, "fuzziness": "AUTO" } }, "from":0, "size":100})
        return result

    def _insert(self, es, _index, _type, _id, _body):
        """Add indexing to elastic search"""

        try:
            es.index(index=_index, doc_type=_type, id=_id, body=_body)
        except Exception as e:
            print 'Something error on : '
            print str(e)
            pass

    def _update(self, es, _index, _type, _id, _body):
        """Update indexing categorization data"""
        try:
            es.update(index=_index, doc_type=_type, id=_id, body=_body)
        except Exception as e:
            print 'Something error on : '
            print str(e)
            pass

    def _delete(self, es, _index, _type, _id):
        """Delete indexing account from elastic search"""

        try:
            es.delete(index=_index, doc_type=_type, id=_id)
        except Exception as e:
            print 'Something error on : '
            print str(e)

#!/usr/bin/python
# -*- coding: utf8 -*-

import json
import pandas as pd
import time
import logging

from flask import Response
from flask_restful import reqparse
from flask_restful import Resource


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def str2bool(v):
    """
        validate boolean on argparse
    """
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise ValueError('Boolean value expected.')


class Selections(Resource):
    def __init__(self, client, kind, es_client):
        self.client = client
        self.kind = kind
        self.es_client = es_client
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('uid', type=str, required=True, location='json',
                                   help='User Id to fetch ranked topic recommender')
        self.reqparse.add_argument('orient', choices=['list', 'records','index'], required=True,
                                   location='json', default="list",
                                   help='json pandas output orientation, should be one of [list, records, index]')
        self.reqparse.add_argument('storage', choices=['datastore', 'elastic'], required=True,
                                   location='json', default="datastore",
                                   help='storage use to fetch the data, should be one of [datastore, elastic]')
        self.reqparse.add_argument('verbose', type=str2bool, required=True, location='json', default="false",
                                   help='show full output our not.')

        super(Selections, self).__init__()

    def basic_query(self, client, kind, uid):
        # [START basic_query]
        query = client.query(kind=kind)
        query.add_filter('user_id', '=', uid)
        return query.fetch()

    def post(self):
        start_all_time = time.time()
        args = self.reqparse.parse_args()

        verbose = args['verbose']
        uid = str(args['uid']).strip()
        orient = str(args['orient']).strip()
        storage = str(args['storage']).strip()
        
        logger.info("uid: %s", uid)
        logger.info("storage: %s", storage)
        logger.info("verbose: %s", str(verbose))
        logger.info("orient: %s", orient)

        if storage.strip().lower() == "datastore":
            logger.info("Begin querying datastore...")
            start_total_time = time.time()

            client = self.client
            kind = self.kind
            iterator = self.basic_query(client, kind, uid)

            end_total_time = time.time() - start_total_time
            logger.info('Time taken to querying datastore: %.7f', end_total_time)
            
            logger.info("Begin transform output...")
            
            user_data = []
            for d in iterator:
                user_data.append([d["topic_id"], d["is_general"], d["rank"], d["p0_posterior"]])

            A = pd.DataFrame(user_data, columns=["topic_id", "is_general", "rank", "p0_posterior"])
            A = A.sort_values(['is_general', 'rank'], ascending=[False, True])

        elif storage.strip().lower() == "elastic":
            logger.info("Begin querying elastic...")
            esclient = self.es_client
            col_source = ["user_id", "topic_id", "topic_is_general", "topic_name", "interest_score"]
            doc = {
                    "query": {
                        "match": {
                            "user_id": uid
                        }
                    },
                    '_source' : col_source
                }

            params = {"size":  30}
            res = esclient.search(index='topicrecommendation_transform_index',
                                  doc_type='topicrecommendation_transform_type',
                                  body=doc, params=params)

            hits = res['hits']['hits']
            data = [hit["_source"] for hit in hits]
            A = pd.DataFrame(data, columns=col_source)
            if len(A.index) > 0:
                A['rank'] = A.groupby(['user_id', 'topic_is_general'])['interest_score'].rank(ascending=False)
                A = A.sort_values(['topic_is_general', 'rank'], ascending=[False, True])
            else:
                # ~~ here for handling new_user ~~
                col_source = ["topic_id", "topic_name", "interest_score"]
                # select all
                doc = {
                        "query" : {
                                    "match_all" : {}
                                   },
                        '_source' : col_source
                       }
                params = {"size":  30}
                res = esclient.search(index='topicrecommendation_transform_fallback_index',
                                    doc_type='topicrecommendation_transform_fallback_type',
                                    body=doc, params=params)

                hits = res['hits']['hits']
                data = [hit["_source"] for hit in hits]
                logger.info(data)
                A = pd.DataFrame(data, columns=col_source)
                logger.info(A)
                A['rank'] = A['interest_score'].rank(ascending=False)
                A = A.sort_values(['rank'], ascending=[True])
        
        end_all_time = time.time() - start_all_time
        print 'Time taken to transform output: %.7f' % end_all_time

        ### Construct JSON output
        if orient == 'list':
            results = A.to_json(orient='values')
        elif orient == 'index':
            results = A.to_dict('index')
        elif orient == 'records':
            results = A.to_dict('records')
        else:
            logger.info("unknow orient %s, fallback to records orient !!", str(orient))
            results = A.to_dict('records')

        if verbose:
            response = {'status': 200,
                        'data': {
                                    'topics':results,
                                    'limit_per_page': 0,
                                    'page': 1,
                                    'took_ms': end_all_time
                                },
                        'error': "null"}
        else:
            response = {'status': 200,
                        'data': {
                                    'topics':results
                                }}
        return Response(json.dumps(response), status=200, mimetype='application/json')
    
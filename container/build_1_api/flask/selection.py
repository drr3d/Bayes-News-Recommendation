#!/usr/bin/python
# -*- coding: utf8 -*-

import json
import pandas as pd
import time
import logging

from flask import Response
from flask_restful import reqparse, Resource


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Selections(Resource):
    def __init__(self, client, kind):
        self.client = client
        self.kind = kind
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('uid', type=str, required=True, location='json',
                                   help='User Id to fetch ranked topic recommender')
        self.reqparse.add_argument('orient', choices=['list', 'records','index'], required=True,
                                   location='json', default="list",
                                   help='json pandas output orientation, should be one of [list, records, index]')
        self.reqparse.add_argument('storage', choices=['datastore', 'elastic'], required=True,
                                   location='json', default="datastore",
                                   help='storage use to fetch the data, should be one of [datastore, elastic]')

        super(Selections, self).__init__()

    def basic_query(self, client, kind, uid):
        # [START basic_query]
        query = client.query(kind=kind)
        query.add_filter('user_id', '=', uid)
        return query.fetch()

    def post(self):
        args = self.reqparse.parse_args()

        uid = str(args['uid']).strip()
        orient = str(args['orient']).strip()
        storage = str(args['storage']).strip()

        logger.info("Begin querying datastore...")
        start_total_time = time.time()

        client = self.client
        kind = self.kind
        iterator = self.basic_query(client, kind, uid)

        end_total_time = time.time() - start_total_time
        logger.info('Time taken to querying datastore: %.7f', end_total_time)
        
        logger.info("Begin transform output...")
        start_total_time = time.time()
        user_data = []
        for d in iterator:
            user_data.append([d["topic_id"], d["is_general"], d["rank"], d["p0_posterior"]])

        A = pd.DataFrame(user_data, columns=["topic_id", "is_general", "rank", "p0_posterior"])
        A = A.sort_values(['is_general', 'rank'], ascending=[False, True])

        end_total_time = time.time() - start_total_time
        logger.info('Time taken to transform output: %.7f', end_total_time)

        ### Construct JSON output
        if orient == 'list':
            results = A.to_json(orient='values')
        elif orient == 'index':
            results = A.to_dict('index')
        elif orient == 'records':
            results = A.to_dict('records')

        response = {'status': 'ok', 'result': results}
        return Response(json.dumps(response), status=200, mimetype='application/json')
    
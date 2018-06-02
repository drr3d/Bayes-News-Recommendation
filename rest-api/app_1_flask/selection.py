#!/usr/bin/python
# -*- coding: utf8 -*-

import json
import pandas as pd
import time
import logging

from flask import Response
from flask import request
from flask_restful import reqparse, Resource
from google.cloud import datastore

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Selections(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('uid', type=str, required=True, location='json',
                                    help='User Id to fetch ranked topic recommender')

        super(Selections, self).__init__()

    def basic_query(self, client, kind, uid):
        # [START basic_query]
        query = client.query(kind=kind)
        query.add_filter('user_id', '=', uid)
        return list(query.fetch())

    def post(self):
        args = self.reqparse.parse_args()
        uid = str(args['uid']).strip()

        logger.info("Begin querying datastore...")
        start_total_time = time.time()

        kind = 'topic_recomendation'
        project_id = 'kumparan-data'

        client = datastore.Client(project_id)
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
        response = {'status': 'ok', 'result': A.to_json(orient='values')}

        return Response(json.dumps(response), status=200, mimetype='application/json')
    
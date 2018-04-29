#!/usr/bin/python
# -*- coding: utf8 -*-

import logging
import json

from google.cloud import datastore
from flask import Flask, Response
from flask_restful import Api

from selection import Selections

app = Flask(__name__)

app.logger.addHandler(logging.StreamHandler())
app.logger.setLevel(logging.INFO)
app.config['DEBUG'] = True

api = Api(app)

kind = 'topic_recomendation'
project_id = 'kumparan-data'
client = datastore.Client(project_id)
api.add_resource(Selections, '/api/selection', endpoint = 'sentselection',
                 resource_class_kwargs={'client': client, 'kind': kind })
@app.route('/service/info')
def info():
    metadata = {'name': 'reco-topic', 'version': '0.1.X', 'api': ['/selection']}
    return Response(json.dumps(metadata), status=200, mimetype='application/json')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

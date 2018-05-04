#!/usr/bin/python
# -*- coding: utf8 -*-

import os
import logging
import json

from google.cloud import datastore
from flask import Flask, Response
from flask_restful import Api
from elasticsearch import Elasticsearch
from elasticsearch import helpers as EShelpers

from selection import Selections

app = Flask(__name__)

app.logger.addHandler(logging.StreamHandler())
app.logger.setLevel(logging.INFO)
app.config['DEBUG'] = True

api = Api(app)

# ~ open config file
dir_path = os.path.dirname(os.path.realpath(__file__))
file_path = "\\"
with open(dir_path + file_path + "settings.json", 'r') as f:
    config = json.load(f)
    es_host = config['elastic']['hostname']
    es_username = config['elastic']['username']
    es_password = config['elastic']['password']
    es_port = config['elastic']['port']

    kind = config['gdatastore']['kind']
    project_id = config['gdatastore']['projectid']


client = datastore.Client(project_id)
es = Elasticsearch([es_host], port=es_port, 
                    http_auth=(es_username, es_password))

# ~ add api resource
api.add_resource(Selections, '/api/selection', endpoint = 'sentselection',
                 resource_class_kwargs={'client': client, 'kind': kind, 'es_client':es })

@app.route('/service/info')
def info():
    metadata = {'name': 'reco-topic-flask-api', 'version': '0.1.X', 'api': ['/selection']}
    return Response(json.dumps(metadata), status=200, mimetype='application/json')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

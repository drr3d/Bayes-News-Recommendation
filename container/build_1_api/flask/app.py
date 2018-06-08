#!/usr/bin/python
# -*- coding: utf8 -*-

import os
import logging
import json

from google.cloud import datastore
from flask import Flask, Response
from flask_restful import Api
from elasticsearch import Elasticsearch
# from elasticsearch import helpers as EShelpers

from selection_path import Selections
from selection_query import SelectionsQuery

app = Flask(__name__)

app.logger.addHandler(logging.StreamHandler())
app.logger.setLevel(logging.INFO)
app.config['DEBUG'] = True

api = Api(app)

# ~ open config file
# https://stackoverflow.com/questions/3759981/get-ip-address-of-visitors
dir_path = os.path.dirname(os.path.realpath(__file__))
file_path = "/"
with open(dir_path + file_path + "settings.json", 'r') as f:
    config = json.load(f)
    es_host = config['elastic']['hostname']
    es_username = config['elastic']['username']
    es_password = config['elastic']['password']
    es_port = config['elastic']['port']

    kind = config['gdatastore']['kind']
    project_id = config['gdatastore']['projectid']


client = datastore.Client(project_id)
if es_username.strip() == "":
    es = Elasticsearch([es_host], port=es_port)
else:
    es = Elasticsearch([es_host], port=es_port, 
                        http_auth=(es_username, es_password))


# ~~ add api resource ~~
api.add_resource(Selections, '/v1/topics/path/<string:uid>/<string:storage>/<string:orient>/<string:verbose>',
                 endpoint = 'topicselection-1',
                 resource_class_kwargs={'client': client, 'kind': kind, 'es_client':es },
                 methods=['GET'])
api.add_resource(Selections, '/v1/topics/path/<string:uid>/<string:storage>/<string:orient>',
                 endpoint = 'topicselection-2',
                 resource_class_kwargs={'client': client, 'kind': kind, 'es_client':es },
                 methods=['GET'])
api.add_resource(Selections, '/v1/topics/path/<string:uid>/<string:storage>',
                 endpoint = 'topicselection-3',
                 resource_class_kwargs={'client': client, 'kind': kind, 'es_client':es },
                 methods=['GET'])
api.add_resource(Selections, '/v1/topics/path/<string:uid>',
                 endpoint = 'topicselection-4',
                 resource_class_kwargs={'client': client, 'kind': kind, 'es_client':es },
                 methods=['GET'])

api.add_resource(SelectionsQuery, '/v1/topics/query/',
                 endpoint = 'topicselection-querystring-1',
                 resource_class_kwargs={'client': client, 'kind': kind, 'es_client':es },
                 methods=['GET'])
# ~~~~~~

@app.route('/service/info')
def info():
    metadata = {'name': 'reco-topic-flask-api', 'version': '0.1.X', 'api': ['/selection']}
    return Response(json.dumps(metadata), status=200, mimetype='application/json')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

"""
About Flask tutorial:
---------------------
https://scotch.io/bar-talk/processing-incoming-request-data-in-flask
https://stackoverflow.com/questions/36735341/how-do-i-access-a-resource-with-multiple-endpoints-with-flask-restful
https://github.com/flask-restful/flask-restful/issues/666
https://medium.com/@moschan/when-should-you-use-path-variable-and-query-parameter-a346790e8a6d
https://stackoverflow.com/questions/29952341/flask-restful-custom-routes-for-each-http-method

:query-string:
--------------
https://stackoverflow.com/questions/14032066/can-flask-have-optional-url-parameters
https://stackoverflow.com/questions/45060043/flask-routing-with-multiple-optional-parameters
https://stackoverflow.com/questions/15182696/multiple-parameters-in-in-flask-approute
https://stackoverflow.com/questions/11774265/how-do-you-get-a-query-string-on-flask
http://www.compciv.org/guides/python/how-tos/creating-proper-url-query-strings/
https://stackoverflow.com/questions/40369016/using-request-args-in-flask-for-a-variable-url
https://github.com/flask-restful/flask-restful/issues/114
http://flask.pocoo.org/snippets/129/
https://stackoverflow.com/questions/34587634/get-query-string-as-function-parameters-on-flask
https://stackoverflow.com/questions/11774265/how-do-you-get-a-query-string-on-flask

:path-variabel:
---------------
https://blog.miguelgrinberg.com/post/designing-a-restful-api-using-flask-restful

:combine path-variable and query-string:
----------------------------------------
https://stackoverflow.com/questions/40369016/using-request-args-in-flask-for-a-variable-url

:handling error:
https://damyanon.net/post/flask-series-logging/
https://opensource.com/article/17/3/python-flask-exceptions

"""

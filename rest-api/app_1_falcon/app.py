import falcon
from topic_recomendation import *

api = application = falcon.API()

reco_topic_es = TopicRecomendationResourceElasticSearch()
reco_topic = TopicRecomendationResourceDataStore()
# reco_topic = TopicRecomendationResourceBigQuery()

"""Handles POST for Topic Recomendation for datastore"""
api.add_route('/reco_topic/', reco_topic)

"""Handles POST for Topic Recomendation for elasticsearch"""
api.add_route('/reco_topic_es/', reco_topic_es)

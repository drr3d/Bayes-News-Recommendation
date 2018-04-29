import falcon
from topic_recomendation import *

api = application = falcon.API()

# reco_topic = TopicRecomendationResourceElasticSearch()
reco_topic = TopicRecomendationResourceDataStore()
# reco_topic = TopicRecomendationResourceBigQuery()

'''Handles POST for Topic Recomendation'''
api.add_route('/reco_topic/', reco_topic)

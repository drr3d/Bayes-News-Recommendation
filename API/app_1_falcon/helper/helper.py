#!/usr/bin/env python
# coding=utf-8

class Normalize(object):
    """
        * Handles all helper for topic recomendation.
    """

    def __init__(self):
        pass

    def _reco_topic_map(self, reco_topic):
        reco_topic_dict = {
                        'p0_cat_ci' : reco_topic['p0_cat_ci'],
                        'p0_posterior' : reco_topic['p0_posterior'],
                        'pt_posterior_x_Nt' : reco_topic['pt_posterior_x_Nt'],
                        'sigma_Nt' : reco_topic['sigma_Nt'],
                        'smoothed_pt_posterior' : reco_topic['smoothed_pt_posterior'],
                        'topic_id' : reco_topic['topic_id'],
                        'user_id' : reco_topic['user_id']
                        }

        return reco_topic_dict

    def _reco_topic_map_compact_ver(self, reco_topic):
        entity_key_split =  reco_topic.key.id_or_name.split('_')

        reco_topic_dict = {
                        'key' : reco_topic.key.id_or_name,
                        'rank' : reco_topic['rank'],
                        'is_general' : reco_topic['is_general'],
                        'topic_id' : entity_key_split[1],
                        'user_id' : entity_key_split[0]
                        }

        return reco_topic_dict

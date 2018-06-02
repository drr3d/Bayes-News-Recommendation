#!/usr/bin/python
# -*- coding: utf8 -*-

import os
from time import time
import pandas as pd

from .. src.googlenews.BayesTopicRecommender import GBayesTopicRecommender

# ~ load training data
#  please replace this with your training data provider
root_path = os.path.abspath(os.path.join(os.path.dirname('__file__')))
dataset_path = '/topic-recommender/src/datasets/'
df_input = pd.read_csv(root_path + dataset_path + 'temp_data-reco_topic_sample_input_new.csv',
                       delimiter="\t")
df_input['date'] = pd.to_datetime(df_input['date'], format='%Y-%m-%d', errors='coerce')

# ~ set current date for p0, later should be
#   date + 4-8 hours windows range,
#   ex: '2018-03-14 01:00' - '2018-03-14 09:00'
current_date = '2018-03-25'

# ~ Data Preprocessing ~
# split data train, untuk menggambarkan data berasal dari 2 table
# D(u, t)
df_dut = df_input.loc[df_input['date'] < current_date]
df_dut['date'] = pd.to_datetime(df_dut['date'], format='%Y-%m-%d', errors='coerce')

# D(t)
df_dt = df_input.loc[df_input['date'] == current_date]
df_dt['date'] = pd.to_datetime(df_dt['date'], format='%Y-%m-%d', errors='coerce')

# just for see some detail on training data
time_slide = df_dut.loc[df_dut['date'] < current_date].groupby('date').size().to_frame().reset_index()
time_slide['date'] = time_slide['date'].dt.date
print time_slide

# ~~~~~~ Begin train ~~~~~~
t0 = time()

print "\ntrain on: %d total history data(D(u, t))" % len(df_dut)
print "transform on: %d total current data(D(t))" % len(df_dt)
# instantiace class
BR = GBayesTopicRecommender(current_date, G=10)

# ~~ Fit ~~
#   handling genuine news interest < current date
NB = BR.processX(df_dut)
result = pd.merge(df_dut, NB, on=['date', 'topic_id'])
"""
     num_y = total global click for category=ci on periode t
     num_x = total click from user_U for category=ci on periode t
"""
df_input_X = result[['date', 'user_id',
                     'topic_id', 'num_x', 'num_y']]
model_fit = BR.fit(df_dut, df_input_X,
                   full_bayes=False, use_sigmant=False,
                   verbose=False)

# ~~ and Transform ~~
#   handling current news interest == current date
NB = BR.processX(df_dt)
result = pd.merge(df_dt, NB, on=['date', 'topic_id'])

df_input_X = result[['date', 'user_id', 'topic_id',
                     'num_x', 'num_y']]
model_transform = BR.transform(df1=df_dt, df2=df_input_X,
                               df3=df_dut, fitted_model=model_fit,
                               verbose=False)

train_time = time() - t0
print model_transform.head(5)
print ("\nTotal train time: %0.3fs" % train_time)